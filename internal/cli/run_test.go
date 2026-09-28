package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestWorkerStatusUsesTicketRepositoryIdentityAndDiagnosticPath(t *testing.T) {
	repositoryName := "Ticket project"
	worker := supervisor.RunWorker{
		Name: "coder",
		Config: supervisor.RoleConfig{
			Repository: "/orc/config/path", RepositoryKey: "project", RepositoryIdentity: joinOtherRepositoryID,
		},
		TicketInfo: &ticketclient.RepositoryInfo{ID: joinTestRepositoryID, Path: "/ticket/reported/path", Name: &repositoryName},
	}
	var status daemon.WorkerStatus
	addWorkerRepositoryStatus(&status, worker)
	if status.RepositoryID != joinTestRepositoryID || status.RepositoryKey != "project" || status.RepositoryName != repositoryName || status.RepositoryPath != "/ticket/reported/path" {
		t.Fatalf("worker repository status=%#v", status)
	}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{`"repository_id":"` + joinTestRepositoryID + `"`, `"repository_key":"project"`, `"repository_name":"Ticket project"`, `"repository_path":"/ticket/reported/path"`} {
		if !strings.Contains(text, want) {
			t.Errorf("worker status missing %s: %s", want, text)
		}
	}
	if strings.Contains(text, `"repository":`) || strings.Contains(text, `"repository_path":"`+joinTestRepositoryID+`"`) {
		t.Fatalf("worker status used an ambiguous identity or UUID as path: %s", text)
	}
}

func TestWorkerRuntimeEventsCarryRepositoryIdentity(t *testing.T) {
	event := supervisor.RuntimeEvent{Type: "worker.state", Worker: "coder"}
	workers := []supervisor.RunWorker{{
		Name:       "coder",
		Config:     supervisor.RoleConfig{RepositoryKey: "project", RepositoryIdentity: joinOtherRepositoryID},
		TicketInfo: &ticketclient.RepositoryInfo{ID: joinTestRepositoryID},
	}}

	addWorkerRepositoryEventContext(&event, workers)
	if event.RepositoryID != joinTestRepositoryID || event.RepositoryKey != "project" {
		t.Fatalf("worker event repository identity = (%q, %q), want (%q, %q)", event.RepositoryID, event.RepositoryKey, joinTestRepositoryID, "project")
	}
}

func TestMain(m *testing.M) {
	if os.Getenv("TICKET_ORC_TEST_FAKE_TICKET") == "1" {
		os.Exit(runTestFakeTicket())
	}
	if value := os.Getenv("TICKET_ORC_TEST_EXIT_CODE"); value != "" {
		code, _ := strconv.Atoi(value)
		os.Exit(code)
	}
	if os.Getenv("TICKET_ORC_TEST_CODEX") == "1" {
		path := os.Getenv("TICKET_ORC_TEST_CODEX_LOG")
		if path != "" {
			file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err == nil {
				_, _ = fmt.Fprintln(file, strings.Join(os.Args[1:], " "))
				_ = file.Close()
			}
		}
		os.Exit(0)
	}
	if os.Getenv("TICKET_ORC_TEST_ROLE") == "1" {
		resolved, help, err := parseRoleConfig(supervisor.Role(os.Args[1]), os.Args[2:], os.LookupEnv)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if help {
			os.Exit(0)
		}
		if err := executeRole(resolved); err != nil {
			os.Exit(17)
		}
		os.Exit(0)
	}
	if os.Getenv("TICKET_ORC_TEST_CHILD") == "1" {
		os.Exit(runSupervisorTestChild())
	}
	os.Exit(m.Run())
}

func startSupervisorTestRun(t *testing.T, cancel context.CancelFunc, run func() error) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- run()
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Errorf("supervisor goroutine did not stop during test cleanup")
		}
	})
	return done
}

func stopRunChildAtTestCleanup(t *testing.T, child *runChild) {
	t.Helper()
	t.Cleanup(func() {
		stopRunChildForTest(t, child)
	})
}

func stopRunChildForTest(t *testing.T, child *runChild) {
	t.Helper()
	if child == nil || child.waited == nil {
		return
	}
	select {
	case <-child.waited:
		return
	default:
	}
	_ = child.requestStop()
	timer := time.NewTimer(runChildStopTimeout)
	defer timer.Stop()
	select {
	case <-child.waited:
		return
	case <-timer.C:
		_ = child.forceStop()
	}
	select {
	case <-child.waited:
	case <-time.After(2 * time.Second):
		t.Errorf("worker child %q was not reaped during test cleanup", child.worker.Name)
	}
}

func runTestFakeTicket() int {
	workingDir, err := os.Getwd()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "read fake Ticket working directory: %v\n", err)
		return 81
	}
	state := "open"
	repositoryID := ""
	matchedRepository := false
	for _, candidate := range []struct {
		path         string
		state        string
		repositoryID string
	}{
		{path: os.Getenv("TICKET_ORC_TEST_FAKE_TICKET_REPO_A"), state: "closed", repositoryID: joinTestRepositoryID},
		{path: os.Getenv("TICKET_ORC_TEST_FAKE_TICKET_REPO_B"), state: "open", repositoryID: joinOtherRepositoryID},
	} {
		if candidate.path == "" {
			continue
		}
		sameDirectory, err := sameSupervisorTestDirectory(workingDir, candidate.path)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "compare fake Ticket working directory: %v\n", err)
			return 81
		}
		if !sameDirectory {
			continue
		}
		state = candidate.state
		repositoryID = candidate.repositoryID
		matchedRepository = true
		break
	}
	if !matchedRepository {
		_, _ = fmt.Fprintf(os.Stderr, "unexpected fake Ticket working directory: %q\n", workingDir)
		return 82
	}

	encoder := json.NewEncoder(os.Stdout)
	if reflect.DeepEqual(os.Args[1:], []string{"info", "-j"}) {
		if err := encoder.Encode(map[string]any{"path": workingDir, "id": repositoryID, "format_version": 1, "storage_version": 1}); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "write fake Ticket repository info: %v\n", err)
			return 85
		}
		return 0
	}
	if !reflect.DeepEqual(os.Args[1:], []string{"-i", "-j"}) {
		_, _ = fmt.Fprintf(os.Stderr, "unexpected fake Ticket startup args: %q\n", os.Args[1:])
		return 80
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request struct {
			Args []string `json:"args"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "decode fake Ticket request: %v\n", err)
			return 83
		}
		if len(request.Args) != 2 || request.Args[0] != "show" {
			_, _ = fmt.Fprintf(os.Stderr, "unexpected fake Ticket command: %q\n", request.Args)
			return 84
		}
		if err := encoder.Encode(map[string]string{"id": request.Args[1], "state": state}); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "write fake Ticket response: %v\n", err)
			return 85
		}
	}
	if err := scanner.Err(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "read fake Ticket request: %v\n", err)
		return 86
	}
	return 0
}

func TestRoleFailureRenderingUsesTypedWaitClassification(t *testing.T) {
	typed := &orc.WorkWaitError{Role: "coder", Cause: &ticketclient.CommandError{Code: "unavailable"}}
	var output bytes.Buffer
	renderRoleFailure(&output, typed)
	if got := output.String(); !strings.Contains(got, "error: wait for implementation work failed") {
		t.Fatalf("typed wait failure output = %q", got)
	}

	output.Reset()
	untyped := fmt.Errorf("wait for implementation work: %w", &ticketclient.CommandError{Code: "unavailable"})
	renderRoleFailure(&output, untyped)
	if got := output.String(); strings.Contains(got, "wait for implementation work failed") {
		t.Fatalf("rendered error text was incorrectly classified: %q", got)
	}
}

func TestWorkerStatusReasonUsesStructuredStartupFailure(t *testing.T) {
	plain := supervisor.WorkerTransition{State: WorkerFailed, Error: "worker exited during startup verification"}
	if got := workerStatusReason(plain); got == "worker exited during startup verification" {
		t.Fatalf("rendered wording classified startup failure: %q", got)
	}

	structured := workerTransitionFailure(&supervisor.LifecycleError{Code: "worker_startup_failed", Message: "wording can vary"})
	if structured == nil || structured.Phase != "worker readiness" {
		t.Fatalf("startup lifecycle metadata = %#v", structured)
	}
	transition := supervisor.WorkerTransition{State: WorkerFailed, Failure: structured}
	if got := workerStatusReason(transition); got != "worker exited during startup verification" {
		t.Fatalf("structured startup reason = %q", got)
	}
}

func testCodexPath(t *testing.T, logPath string) string {
	t.Helper()
	directory := t.TempDir()
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(directory, name)
	data, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TICKET_ORC_TEST_CODEX", "1")
	t.Setenv("TICKET_ORC_TEST_CODEX_LOG", logPath)
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	return path
}

func runSupervisorTestChild() int {
	dir := os.Getenv("TICKET_ORC_TEST_CHILD_DIR")
	worker := os.Getenv("TICKET_ORC_WORKER")
	if dir == "" || worker == "" {
		return 90
	}
	resolved, _, resolveErr := parseRoleConfig(supervisor.Role(os.Args[1]), os.Args[2:], os.LookupEnv)
	configPath := ""
	for i := 1; i+1 < len(os.Args); i++ {
		if os.Args[i] == "--config" {
			configPath = os.Args[i+1]
			break
		}
	}
	resolvedLine := fmt.Sprintf("resolved_actor=%s\nresolved_ticket_prompt=%s\nresolved_sandbox=%s\n", resolved.Actor, resolved.TicketPrompt, resolved.Codex.Sandbox)
	if resolveErr != nil {
		resolvedLine = "resolved_error=" + strings.ReplaceAll(resolveErr.Error(), "\n", " ") + "\n"
	}
	workingDir, _ := os.Getwd()
	readyPath := filepath.Join(dir, worker+".ready")
	readyContents := []byte(fmt.Sprintf("role=%s\nworker=%s\nconfig=%s\ninstance=%s\nactor=%s\nstate=%s\ncwd=%s\nargs=%s\n%s", os.Args[1], worker, configPath, os.Getenv("TICKET_ORC"), os.Getenv("TICKET_ORC_ACTOR"), os.Getenv("TICKET_ORC_STATE_DIR"), workingDir, strings.Join(os.Args[1:], " "), resolvedLine))
	if err := writeSupervisorTestFileAtomically(readyPath, readyContents); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "publish supervisor test readiness: %v\n", err)
		return 91
	}
	if worker == os.Getenv("TICKET_ORC_TEST_CHILD_FAIL") {
		appendSupervisorTestEvent(dir, worker, "failed")
		if value, err := strconv.Atoi(os.Getenv("TICKET_ORC_TEST_CHILD_EXIT")); err == nil && value > 0 {
			return value
		}
		return 17
	}
	if os.Getenv("TICKET_ORC_EVENT_STREAM") == "1" {
		ready, _ := json.Marshal(orc.Event{Type: orc.WorkerReadyEventType, Worker: worker, Role: string(supervisor.Role(os.Args[1])), State: "ready", Phase: "startup"})
		_, _ = fmt.Fprintf(os.Stderr, "%s%s\n", orc.EventStreamPrefix, ready)
	}
	appendSupervisorTestEvent(dir, worker, "running")
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, os.Interrupt)
	defer signal.Stop(stopped)
	if worker == os.Getenv("TICKET_ORC_TEST_CHILD_OUTPUT") {
		_, _ = os.Stdout.Write([]byte("partial"))
		time.Sleep(20 * time.Millisecond)
		_, _ = os.Stdout.Write([]byte("\n"))
		_ = os.WriteFile(filepath.Join(dir, worker+".output"), []byte("done"), 0o600)
	} else if os.Getenv("TICKET_ORC_TEST_CHILD_OUTPUT") != "" {
		_, _ = fmt.Fprintln(os.Stdout, "complete")
		_ = os.WriteFile(filepath.Join(dir, worker+".output"), []byte("done"), 0o600)
	}
	if os.Getenv("TICKET_ORC_TEST_CHILD_EVENT_FRAGMENT") == "1" {
		if _, err := fmt.Fprintf(os.Stderr, "%s%s", "[ticket-orc-event] ", `{"type":"worker.state","worker":"fragment","state":"running"}`); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "write supervisor test event fragment: %v\n", err)
			return 93
		}
		fragmentPath := filepath.Join(dir, worker+".fragment-written")
		if err := writeSupervisorTestFileAtomically(fragmentPath, []byte("written\n")); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "publish supervisor test fragment completion: %v\n", err)
			return 92
		}
	}
	if worker == os.Getenv("TICKET_ORC_TEST_CHILD_IGNORE") {
		_ = os.WriteFile(filepath.Join(dir, worker+".armed"), []byte("ready"), 0o600)
		<-stopped
		select {}
	}
	<-stopped
	appendSupervisorTestEvent(dir, worker, "stopped")
	return 0
}

func appendSupervisorTestEvent(dir, worker, event string) {
	file, err := os.OpenFile(filepath.Join(dir, worker+".events"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintln(file, event)
	_ = file.Close()
}

func writeSupervisorTestFileAtomically(path string, contents []byte) error {
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := temp.Write(contents); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func sameSupervisorTestDirectory(first, second string) (bool, error) {
	firstInfo, err := os.Stat(first)
	if err != nil {
		return false, fmt.Errorf("stat %q: %w", first, err)
	}
	if !firstInfo.IsDir() {
		return false, fmt.Errorf("%q is not a directory", first)
	}
	secondInfo, err := os.Stat(second)
	if err != nil {
		return false, fmt.Errorf("stat %q: %w", second, err)
	}
	if !secondInfo.IsDir() {
		return false, fmt.Errorf("%q is not a directory", second)
	}
	return os.SameFile(firstInfo, secondInfo), nil
}

func supervisorTestReadyField(contents, field string) (string, bool) {
	for _, line := range strings.Split(contents, "\n") {
		name, value, ok := strings.Cut(line, "=")
		if ok && name == field {
			return value, true
		}
	}
	return "", false
}

func waitForSupervisorTestFile(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			return string(data)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
	return ""
}

func supervisorTestWorker(name, stateDir string) supervisor.RunWorker {
	return supervisor.RunWorker{Name: name, Config: supervisor.RoleConfig{
		WorkerName: name, Role: RoleCoder, Harness: "codex", Actor: "actor-" + name,
		Model: "test-model", Reasoning: "low", MaxBounces: 1,
		SessionPolicy: SessionPolicyTicket, SessionCleanup: CleanupKeep,
		StateDir: stateDir, Output: OutputCompact,
	}}
}

func defaultTestLocalDir(configDir string) string {
	return filepath.Join(configDir, ".local", "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa")
}

func supervisorTestConfigPath(t *testing.T) string {
	t.Helper()
	configDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(configDir, "config.json")
	writeConfigFixture(t, configPath, `{"version":1}`)
	return configPath
}

func TestLaunchSnapshotComparisonIncludesResolvedLaunchPolicy(t *testing.T) {
	base := supervisorTestWorker("worker", t.TempDir())
	base.RequiredSkills = []string{"ticket"}
	cases := []struct {
		name   string
		change func(*supervisor.RunWorker)
	}{
		{name: "actor", change: func(worker *supervisor.RunWorker) { worker.Config.Actor = "changed" }},
		{name: "harness", change: func(worker *supervisor.RunWorker) { worker.Config.Harness = "pi" }},
		{name: "model", change: func(worker *supervisor.RunWorker) { worker.Config.Model = "changed" }},
		{name: "reasoning", change: func(worker *supervisor.RunWorker) { worker.Config.Reasoning = "high" }},
		{name: "sandbox", change: func(worker *supervisor.RunWorker) { worker.Config.Codex.Sandbox = "read-only" }},
		{name: "ticket target", change: func(worker *supervisor.RunWorker) {
			worker.Config.Ticket = supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/changed/repository"}
		}},
		{name: "skills", change: func(worker *supervisor.RunWorker) { worker.RequiredSkills = []string{"ticket", "custom"} }},
		{name: "review policy", change: func(worker *supervisor.RunWorker) { worker.Config.ReviewCompletion = ReviewCompletionClose }},
		{name: "ticket prompt", change: func(worker *supervisor.RunWorker) { worker.Config.TicketPrompt = "changed ticket prompt" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.change(&changed)
			if equalLaunchSnapshots(base, changed) {
				t.Fatalf("launch snapshots considered equal after %s change", test.name)
			}
		})
	}
	live := base
	live.Groups = []string{"changed-group"}
	if !equalLaunchSnapshots(base, live) {
		t.Fatal("group-only change should remain live-reloadable")
	}
}

func TestLaunchSnapshotCarriesResolvedLocalRootToChild(t *testing.T) {
	localDir := filepath.Join(t.TempDir(), ".local", "instance-id")
	worker := supervisorTestWorker("worker-a", localDir)
	encoded, err := encodeLaunchSnapshot(worker)
	if err != nil {
		t.Fatal(err)
	}
	lookup := mapEnv(map[string]string{launchSnapshotEnv: encoded})
	resolved, help, err := parseRoleConfig(RoleCoder, []string{"--config", "/untrusted/cwd/config.json", "--worker", worker.Name}, lookup)
	if err != nil || help {
		t.Fatalf("decode worker launch config=%#v help=%t err=%v", resolved, help, err)
	}
	if resolved.StateDir != localDir || resolved.WorkerName != worker.Name {
		t.Fatalf("child config local root=%q worker=%q, want %q and %q", resolved.StateDir, resolved.WorkerName, localDir, worker.Name)
	}
}

func TestRenderServiceStartupSectionsAndIdentity(t *testing.T) {
	var output bytes.Buffer
	status := daemon.Status{
		Steer:        []daemon.SteerStatus{{RepositoryID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", Role: "coder", Actor: "reviewer", Session: "01a0dcb8-6a64-4b9b-95c9-d5598a150e86", State: "checking"}},
		Repositories: []daemon.RepositoryStatus{{ID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", Path: "/work/ticket-orc", State: "healthy"}},
	}
	renderServiceReady(&output, "./ticket-orc/config.json", "http://127.0.0.1:1234", status)
	text := output.String()
	for _, want := range []string{"[ticket-orc] loaded ./ticket-orc/config.json", "[ticket-orc] endpoint: orc://127.0.0.1:1234", "Repositories:", "ticket-orc", "Repository path", "ready", "Sessions:", "Role", "Ticket actor", "reviewer", "01a0dcb8…"} {
		if !strings.Contains(text, want) {
			t.Fatalf("startup output missing %q: %q", want, text)
		}
	}
	if strings.Contains(text, "Managed workers:") || strings.Contains(text, "dynamic:") {
		t.Fatalf("startup output included empty/internal detail: %q", text)
	}
	if strings.Index(text, "Repositories:") > strings.Index(text, "Sessions:") {
		t.Fatalf("startup repositories should precede sessions: %q", text)
	}

	output.Reset()
	status.Workers = []daemon.WorkerStatus{{Name: "coder", Role: "coder", TicketActor: "reviewer", State: "running"}}
	renderServiceReady(&output, "./ticket-orc/config.json", "http://127.0.0.1:1234", status)
	if !strings.Contains(output.String(), "Managed workers:") || !strings.Contains(output.String(), "Ticket actor") {
		t.Fatalf("managed worker section missing: %q", output.String())
	}
}

func TestRunSupervisorReportsUnexpectedCleanAndErrorExits(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		want      string
		wantState supervisor.WorkerState
	}{
		{name: "clean", want: "unexpected_exit state=stopped", wantState: WorkerStopped},
		{name: "error", err: errors.New("opaque target and prompt"), want: "unexpected_exit state=failed", wantState: WorkerFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			worker := supervisorTestWorker("one", t.TempDir())
			childDone := make(chan error, 1)
			starter := func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
				return &runChild{worker: worker, cmd: &exec.Cmd{}, done: childDone, waited: make(chan struct{})}, nil
			}
			var output bytes.Buffer
			var outputMu sync.Mutex
			stderr := &lockedWriter{dst: &output, mu: &outputMu}
			runtimeState := &supervisor.RuntimeState[supervisor.RunWorker]{}
			stateDir := t.TempDir()
			done := startSupervisorTestRun(t, cancel, func() error {
				return runSupervisor(ctx, RunConfig{ConfigPath: "config.json", StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, startChild: starter}, os.Args[0], io.Discard, stderr)
			})
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				outputMu.Lock()
				ready := strings.Contains(output.String(), "Managed workers:")
				outputMu.Unlock()
				if ready {
					break
				}
				time.Sleep(time.Millisecond)
			}
			childDone <- test.err
			deadline = time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				outputMu.Lock()
				seen := strings.Contains(output.String(), test.want)
				outputMu.Unlock()
				if seen {
					break
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			outputMu.Lock()
			text := output.String()
			outputMu.Unlock()
			if !strings.Contains(text, test.want) || strings.Contains(text, "opaque target") || strings.Contains(text, "prompt") {
				t.Fatalf("output = %q", text)
			}
			if got := runtimeState.Snapshot()["one"].State; got != test.wantState {
				t.Fatalf("runtime state = %q, want %q", got, test.wantState)
			}
			failure := runtimeState.Snapshot()["one"].Failure
			if failure == nil {
				t.Fatal("worker failure metadata missing")
			}
			if test.name == "clean" && failure.Classification != "silent_exit" {
				t.Fatalf("clean failure metadata=%#v", failure)
			}
			if test.name == "error" && failure.Classification != "worker_error" {
				t.Fatalf("error failure metadata=%#v", failure)
			}
		})
	}
}

func TestRunSupervisorUsesChildFailureEnvelope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker := supervisorTestWorker("reviewer", t.TempDir())
	childDone := make(chan error, 1)
	starter := func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
		child := &runChild{worker: candidate, cmd: &exec.Cmd{}, done: childDone, waited: make(chan struct{})}
		child.captureFailure(orc.Event{Type: "worker.failure", Worker: candidate.Name, Role: "reviewer", Failure: &orc.Failure{
			Classification: "ticket_command_failed", Phase: "worker operation", ExitCode: 1, TicketCode: "ticket_not_ready",
		}})
		return child, nil
	}
	var output bytes.Buffer
	var outputMu sync.Mutex
	stderr := &lockedWriter{dst: &output, mu: &outputMu}
	runtimeState := &supervisor.RuntimeState[supervisor.RunWorker]{}
	stateDir := t.TempDir()
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{ConfigPath: "config.json", StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, startChild: starter}, os.Args[0], io.Discard, stderr)
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		outputMu.Lock()
		ready := strings.Contains(output.String(), "Managed workers:")
		outputMu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	childDone <- errors.New("ticket JSONL failed: bearer-token=secret prompt=opaque")
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		outputMu.Lock()
		seen := strings.Contains(output.String(), "ticket_code=ticket_not_ready")
		outputMu.Unlock()
		if seen {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	outputMu.Lock()
	text := output.String()
	outputMu.Unlock()
	if !strings.Contains(text, "classification=ticket_command_failed") || !strings.Contains(text, "phase=worker operation") || !strings.Contains(text, "ticket_code=ticket_not_ready") {
		t.Fatalf("output = %q", text)
	}
	if strings.Contains(text, "bearer-token") || strings.Contains(text, "prompt=opaque") || strings.Contains(text, "secret") {
		t.Fatalf("child diagnostic leaked raw details: %q", text)
	}
	failure := runtimeState.Snapshot()["reviewer"].Failure
	if failure == nil || failure.Classification != "ticket_command_failed" || failure.TicketCode != "ticket_not_ready" {
		t.Fatalf("runtime failure = %#v", failure)
	}
}

func TestQuietSupervisedCancellationOnlyAppliesToSignalOwnedRole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fmt.Errorf("wait for review work: %w", context.Canceled)
	if got := quietSupervisedCancellation(true, ctx, err); got != nil {
		t.Fatalf("supervised cancellation = %v, want nil", got)
	}
	if got := quietSupervisedCancellation(false, ctx, err); !errors.Is(got, context.Canceled) {
		t.Fatalf("standalone cancellation = %v, want context.Canceled", got)
	}
	active := context.Background()
	if got := quietSupervisedCancellation(true, active, err); !errors.Is(got, context.Canceled) {
		t.Fatalf("active-context cancellation = %v, want context.Canceled", got)
	}
}

func TestWorkerFailureClassificationBoundsSecretsAndPhases(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		class     string
		phase     string
		origin    string
		operation string
	}{
		{name: "silent", class: "silent_exit", phase: "worker operation"},
		{name: "reviewer baseline", err: orc.NewFailureContextError(orc.FailureContext{Origin: "reviewer", Operation: "baseline observation", Phase: "reviewer ownership-baseline observation"}, errors.New("rewritten message one")), class: "worker_error", phase: "reviewer ownership-baseline observation", origin: "reviewer", operation: "baseline observation"},
		{name: "ticket startup", err: orc.NewFailureContextError(orc.FailureContext{Origin: "ticket", Operation: "client startup", Phase: "Ticket client startup"}, errors.New("rewritten message two")), class: "worker_error", phase: "Ticket client startup", origin: "ticket", operation: "client startup"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := classifyWorkerFailure(test.err)
			if failure.Classification != test.class || failure.Phase != test.phase || failure.Origin != test.origin || failure.Operation != test.operation {
				t.Fatalf("failure=%#v", failure)
			}
			if test.name == "bounce limit" && (!failure.Contained || failure.Ticket != "20260923-80098") {
				t.Fatalf("bounce failure lost structured containment/ticket: %#v", failure)
			}
			if strings.Contains(failure.Classification, "secret") || strings.Contains(failure.Phase, "secret") {
				t.Fatalf("failure leaked secret=%#v", failure)
			}
		})
	}
}

func TestFailureClassificationDoesNotDependOnWrappedMessage(t *testing.T) {
	metadata := orc.FailureContext{Origin: "ticket", Operation: "claim observation", Phase: "ticket observation", Ticket: "20260923-80098", Contained: true}
	first := classifyWorkerFailure(orc.NewFailureContextError(metadata, errors.New("wording A: old details")))
	second := classifyWorkerFailure(orc.NewFailureContextError(metadata, errors.New("wording B: entirely different details")))
	if first.Classification != second.Classification || first.Phase != second.Phase || first.Origin != second.Origin || first.Operation != second.Operation || first.Ticket != second.Ticket || first.Contained != second.Contained {
		t.Fatalf("message wording changed classification: first=%#v second=%#v", first, second)
	}
}

func TestRoleFailureEnvelopePreservesSafeTicketCommandCode(t *testing.T) {
	err := fmt.Errorf("wait for queued ticket: %w", &ticketclient.CommandError{ExitCode: -1, Code: "ticket_not_ready", Message: "prompt=secret"})
	failure := roleFailureEnvelope(err)
	if failure == nil || failure.Classification != "ticket_command_failed" || failure.TicketCode != "ticket_not_ready" || failure.Phase != "worker operation" {
		t.Fatalf("failure envelope=%#v", failure)
	}
	if strings.Contains(failure.TicketCode, "secret") || strings.Contains(failure.Phase, "prompt") {
		t.Fatalf("failure envelope leaked unsafe data=%#v", failure)
	}
}

func TestRoleFailureEnvelopePreservesBoundedTicketTransportExit(t *testing.T) {
	err := orc.NewFailureContextError(orc.FailureContext{Origin: "ticket", Operation: "ownership observation", Phase: "reviewer ownership-baseline observation"}, fmt.Errorf("human-readable wording is independent: %w", &ticketclient.TransportError{
		Category: "process_exit",
		ExitCode: 9,
	}))
	failure := roleFailureEnvelope(err)
	if failure == nil || failure.Classification != "ticket_transport_failed" || failure.TransportCategory != "process_exit" || failure.ExitCode != 9 || failure.Phase != "reviewer ownership-baseline observation" {
		t.Fatalf("failure envelope=%#v", failure)
	}
	if failure.Signal != "" || failure.TicketCode != "" {
		t.Fatalf("absent optional metadata was synthesized: %#v", failure)
	}

	var output bytes.Buffer
	renderRoleFailure(&output, err)
	text := output.String()
	for _, want := range []string{"classification=ticket_transport_failed", "transport_category=process_exit", "exit_code=9", "phase=reviewer ownership-baseline observation"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output=%q, missing %q", text, want)
		}
	}
	if strings.Contains(text, "bearer-token") || strings.Contains(text, "secret") || strings.Contains(text, "unknown") {
		t.Fatalf("unsafe or synthesized metadata leaked: %q", text)
	}
}

func runSupervisedFailureEnvelope(t *testing.T, envelope *orc.Failure) (supervisor.WorkerTransition, string) {
	t.Helper()
	configPath := supervisorTestConfigPath(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	worker := supervisorTestWorker("reviewer-baseline", stateDir)
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	hooks := make(chan supervisor.WorkerTransition, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var diagnostics bytes.Buffer
	var outputMu sync.Mutex
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{
			ConfigPath: configPath, StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState,
			startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
				child := &runChild{worker: candidate, done: make(chan error, 1), waited: make(chan struct{})}
				child.captureFailure(orc.Event{Type: "worker.failure", Worker: candidate.Name, Role: "reviewer", Failure: envelope})
				child.done <- errors.New("opaque baseline failure")
				return child, nil
			},
			Hooks: supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }},
		}, os.Args[0], io.Discard, &lockedWriter{dst: &diagnostics, mu: &outputMu})
	})
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-hooks:
			if event.Worker != worker.Name || event.State != WorkerFailed {
				continue
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("supervisor = %v", err)
			}
			outputMu.Lock()
			text := diagnostics.String()
			outputMu.Unlock()
			return runtimeState.Snapshot()[worker.Name], text
		case err := <-done:
			t.Fatalf("supervisor stopped before worker failure hook: %v", err)
			return supervisor.WorkerTransition{}, ""
		case <-deadline.C:
			t.Fatal("timed out waiting for supervised baseline failure")
			return supervisor.WorkerTransition{}, ""
		}
	}
}

func TestRenderRoleFailureOmitsRawCommandDetails(t *testing.T) {
	var output bytes.Buffer
	err := fmt.Errorf("wait for queued ticket: %w", &ticketclient.CommandError{ExitCode: -1, Code: "ticket_not_ready", Message: "bearer-token=secret prompt=opaque"})
	renderRoleFailure(&output, err)
	text := output.String()
	if !strings.Contains(text, "classification=ticket_command_failed") || !strings.Contains(text, "ticket_code=ticket_not_ready") {
		t.Fatalf("output=%q", text)
	}
	if strings.Contains(text, "bearer-token") || strings.Contains(text, "secret") || strings.Contains(text, "prompt=opaque") {
		t.Fatalf("raw command details leaked: %q", text)
	}
}

func TestRunSupervisorDefaultOutputUsesServiceSummaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stateDir := t.TempDir()
	var stderr bytes.Buffer
	var outputMu sync.Mutex
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{ConfigPath: "/private/repository/config.json", StateDir: stateDir}, os.Args[0], io.Discard, &lockedWriter{dst: &stderr, mu: &outputMu})
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		outputMu.Lock()
		ready := strings.Contains(stderr.String(), "[ticket-orc] loaded ") && strings.Contains(stderr.String(), "[ticket-orc] endpoint: orc://")
		outputMu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	outputMu.Lock()
	text := stderr.String()
	outputMu.Unlock()
	if strings.Contains(text, "loading config:") || strings.Contains(text, "starting workers:") || !strings.Contains(text, "[ticket-orc] loaded ") || !strings.Contains(text, "[ticket-orc] endpoint: unavailable") || strings.Contains(text, "Managed workers:") || strings.Contains(text, "Sessions:") || !strings.Contains(text, "Repositories: none") || !strings.Contains(text, "[ticket-orc] stopped workers=0") {
		t.Fatalf("service output = %q", text)
	}
	for _, forbidden := range []string{"version=", "working_dir=", "state_dir=", "repository_id=", "Ticket", "phase=preflight", "daemon state"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("service output contains %q: %q", forbidden, text)
		}
	}
}

func TestRunSupervisorLockFailureHasSafeDiagnostic(t *testing.T) {
	stateDir := t.TempDir()
	lock, err := state.TryAcquireLock(context.Background(), filepath.Join(stateDir, "run"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	var probes atomic.Int32
	config := RunConfig{
		StateDir:     stateDir,
		Repositories: supervisor.RepositoryRegistry{"project": {Key: "project"}},
		repositoryProbe: func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
			probes.Add(1)
			return ticketclient.RepositoryInfo{}, nil
		},
	}
	var stderr bytes.Buffer
	err = runSupervisor(context.Background(), config, os.Args[0], io.Discard, &stderr)
	if err == nil || !strings.Contains(err.Error(), "already running") || !strings.Contains(stderr.String(), "this Orc instance is already running") || !strings.Contains(stderr.String(), stateDir) || strings.Contains(stderr.String(), "startup failed worker=daemon phase=lock") {
		t.Fatalf("err=%v stderr=%q", err, stderr.String())
	}
	if probes.Load() != 0 {
		t.Fatalf("repository preflight ran before ownership decision: probes=%d", probes.Load())
	}
}

func TestRunSupervisorReportsOccupiedExplicitPort(t *testing.T) {
	reserved, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback sockets unavailable: %v", err)
	}
	defer reserved.Close()
	port := reserved.Addr().(*net.TCPAddr).Port
	const endpointKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	stateDir := t.TempDir()
	config := RunConfig{
		StateDir:      stateDir,
		ListenAddress: "127.0.0.1",
		Port:          port,
		EndpointKey:   endpointKey,
	}
	config.startDaemon = func(context.Context, *supervisor.RuntimeState[supervisor.RunWorker], []supervisor.RunWorker) (*daemon.Server, error) {
		return daemon.NewServer(daemon.Config{
			StateDir:      stateDir,
			ListenAddress: config.ListenAddress,
			Port:          config.Port,
			EndpointKey:   endpointKey,
		})
	}
	var stderr bytes.Buffer
	err = runSupervisor(context.Background(), config, os.Args[0], io.Discard, &stderr)
	var listenErr *net.OpError
	if err == nil || !errors.As(err, &listenErr) || listenErr.Op != "listen" || listenErr.Addr == nil {
		t.Fatalf("run error = %v, want a wrapped listener bind failure", err)
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	wantPrefix := "[ticket-orc] daemon listen failed address=" + address + ": "
	diagnostic := strings.TrimSpace(stderr.String())
	if !strings.HasPrefix(diagnostic, wantPrefix) || strings.TrimPrefix(diagnostic, wantPrefix) == "" {
		t.Fatalf("startup diagnostic = %q, want listen address and bind cause", stderr.String())
	}
	if strings.Contains(stderr.String(), endpointKey) {
		t.Fatalf("startup diagnostic exposed endpoint capability: %q", stderr.String())
	}
}

// A stop racing with child exit remains a deliberate stop, not a second failure.
func TestRunSupervisorDeliberateStopIgnoresStaleExit(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	worker := supervisorTestWorker("one", t.TempDir())
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	control := &daemon.Control{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stateDir := t.TempDir()
	var stderr bytes.Buffer
	var outputMu sync.Mutex
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{ConfigPath: configPath, StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, control: control}, os.Args[0], io.Discard, &lockedWriter{dst: &stderr, mu: &outputMu})
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		outputMu.Lock()
		ready := strings.Contains(stderr.String(), "Managed workers:")
		outputMu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if control.StopWorker == nil {
		t.Fatal("stop control was not installed")
	}
	if _, err := control.StopWorker(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	outputMu.Lock()
	text := stderr.String()
	outputMu.Unlock()
	if strings.Contains(text, "unexpected_exit") || runtimeState.Snapshot()["one"].State != WorkerStopped {
		t.Fatalf("deliberate stop output=%q state=%q", text, runtimeState.Snapshot()["one"].State)
	}
}

func TestSelectRunWorkersUnionsAndDeduplicates(t *testing.T) {
	config := FileConfig{Workers: map[string]WorkerFileConfig{
		"coder":    {Role: "coder", Groups: []string{"default", "backend"}},
		"reviewer": {Role: "reviewer", Groups: []string{"default"}},
		"frontend": {Role: "coder", Groups: []string{"frontend"}},
	}}
	got, err := selectRunWorkers(config, []string{"reviewer", "coder"}, []string{"backend"}, false)
	if err != nil {
		t.Fatalf("selectRunWorkers: %v", err)
	}
	want := []string{"reviewer", "coder"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected workers = %#v, want %#v", got, want)
	}

	got, err = selectRunWorkers(config, nil, nil, true)
	if err != nil {
		t.Fatalf("--all selection: %v", err)
	}
	want = []string{"coder", "frontend", "reviewer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all workers = %#v, want %#v", got, want)
	}
}

func TestParseRunConfigAcceptsAllShortAlias(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"short-alias"}}}`)
	config, help, err := parseRunConfig([]string{"-a", "-c", path}, mapEnv(map[string]string{"TICKET_ORC_ACTOR": "short-alias"}))
	if err != nil || help || len(config.Workers) != 1 || config.Workers[0].Name != "coder" {
		t.Fatalf("run -a config=%#v help=%v err=%v", config, help, err)
	}
	if _, _, err := parseRunConfig([]string{"-a", "--all"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "duplicate flag --all") {
		t.Fatalf("duplicate -a/--all error = %v", err)
	}
}

func TestParseRunConfigResolvesStateDirectoryFromConfigPath(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "settings")
	workingDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(configDir, "orc.json")
	writeConfigFixture(t, path, `{"version":1,"defaults":{"working_dir":"../workspace"},"workers":{"coder":{"role":"coder","actor":"state-test"}}}`)
	otherCWD := t.TempDir()
	t.Chdir(otherCWD)

	config, help, err := parseRunConfig([]string{"--all", "--config", path}, emptyEnv)
	if err != nil || help {
		t.Fatalf("parseRunConfig config=%#v help=%t err=%v", config, help, err)
	}
	want := filepath.Join(configDir, ".local", "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa")
	if config.StateDir != want || len(config.Workers) != 1 || config.Workers[0].Config.StateDir != want {
		t.Fatalf("supervisor/worker state dirs = %q/%q, want %q", config.StateDir, config.Workers[0].Config.StateDir, want)
	}
	if config.Workers[0].Config.WorkingDir != workingDir {
		t.Fatalf("worker working directory = %q, want %q", config.Workers[0].Config.WorkingDir, workingDir)
	}

	writeConfigFixture(t, path, `{"version":1,"local_dir":"../state","defaults":{"working_dir":"../workspace"},"workers":{"coder":{"role":"coder","actor":"state-test"}}}`)
	config, _, err = parseRunConfig([]string{"--all", "--config", path}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	want = filepath.Join(root, "state")
	if config.StateDir != want || config.Workers[0].Config.StateDir != want {
		t.Fatalf("relative state dirs = %q/%q, want %q", config.StateDir, config.Workers[0].Config.StateDir, want)
	}
	absoluteStateDir := filepath.Join(root, "absolute-state")
	writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"local_dir":%q,"defaults":{"working_dir":"../workspace"},"workers":{"coder":{"role":"coder","actor":"state-test"}}}`, absoluteStateDir))
	config, _, err = parseRunConfig([]string{"--all", "--config", path}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	if config.StateDir != absoluteStateDir || config.Workers[0].Config.StateDir != absoluteStateDir {
		t.Fatalf("absolute state dirs = %q/%q, want %q", config.StateDir, config.Workers[0].Config.StateDir, absoluteStateDir)
	}
}

func TestParseRunConfigAcceptsInteractiveAlias(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"interactive"}}}`)
	config, help, err := parseRunConfig([]string{"-i", "--config", path}, emptyEnv)
	if err != nil || help || !config.Interactive {
		t.Fatalf("run -i config=%#v help=%v err=%v", config, help, err)
	}
	if _, _, err := parseRunConfig([]string{"-i", "--interactive", "--config", path}, emptyEnv); err == nil || !strings.Contains(err.Error(), "duplicate flag --interactive") {
		t.Fatalf("duplicate -i/--interactive error=%v", err)
	}
}

func TestParseRunConfigListenCLIOverridesConfig(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"supervisor":{"listen":"127.0.0.2","port":43123},"workers":{"coder":{"role":"coder","actor":"listen-test"}}}`)
	config, help, err := parseRunConfig([]string{"--config", path, "--listen", "127.0.0.1", "--port=43210"}, emptyEnv)
	if err != nil || help {
		t.Fatalf("parseRunConfig config=%#v help=%t err=%v", config, help, err)
	}
	if config.ListenAddress != "127.0.0.1" || config.Port != 43210 {
		t.Fatalf("listen config = %#v", config)
	}
	fromFile, _, err := parseRunConfig([]string{"--config", path}, emptyEnv)
	if err != nil || fromFile.ListenAddress != "127.0.0.2" || fromFile.Port != 43123 {
		t.Fatalf("config listen values = %#v err=%v", fromFile, err)
	}
	for _, args := range [][]string{{"--config", path, "--listen", "localhost"}, {"--config", path, "--port", "80"}, {"--config", path, "--port", "not-a-port"}} {
		if _, _, err := parseRunConfig(args, emptyEnv); err == nil {
			t.Fatalf("invalid listen arguments accepted: %v", args)
		}
	}
}

func TestParseRunConfigListenDefaultsAndEnvironmentPrecedence(t *testing.T) {
	defaults, _, err := parseRunConfig(nil, testInstanceEnv(t))
	if err != nil || defaults.ListenAddress != "127.0.0.1" || defaults.Port != 0 {
		t.Fatalf("default listener = %#v, err=%v", defaults, err)
	}

	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"supervisor":{"listen":"::1","port":0}}`)
	env := mapEnv(map[string]string{"TICKET_ORC_LISTEN": "0.0.0.0", "TICKET_ORC_PORT": "43210"})
	fromEnv, _, err := parseRunConfig([]string{"--config", path}, env)
	if err != nil || fromEnv.ListenAddress != "0.0.0.0" || fromEnv.Port != 43210 {
		t.Fatalf("environment listener = %#v, err=%v", fromEnv, err)
	}
	fromCLI, _, err := parseRunConfig([]string{"--config", path, "--listen", "::1", "--port", "0"}, env)
	if err != nil || fromCLI.ListenAddress != "::1" || fromCLI.Port != 0 {
		t.Fatalf("CLI listener = %#v, err=%v", fromCLI, err)
	}
	fromConfig, _, err := parseRunConfig([]string{"--config", path}, emptyEnv)
	if err != nil || fromConfig.ListenAddress != "::1" || fromConfig.Port != 0 {
		t.Fatalf("explicit config port zero was lost: %#v, err=%v", fromConfig, err)
	}
	fromEnvZero, _, err := parseRunConfig([]string{"--config", path}, mapEnv(map[string]string{"TICKET_ORC_PORT": "0"}))
	if err != nil || fromEnvZero.Port != 0 {
		t.Fatalf("explicit environment port zero was lost: %#v, err=%v", fromEnvZero, err)
	}
	if _, _, err := parseRunConfig(nil, mapEnv(map[string]string{"TICKET_ORC_PORT": "invalid"})); err == nil {
		t.Fatal("invalid TICKET_ORC_PORT was accepted")
	}
}

func TestSelectRunWorkersRejectsUnknownAndEmptyGroups(t *testing.T) {
	config := FileConfig{Workers: map[string]WorkerFileConfig{
		"coder": {Role: "coder", Groups: []string{"default"}},
	}}
	for _, test := range []struct {
		name string
		call func() error
		want string
	}{
		{"worker", func() error {
			_, err := selectRunWorkers(config, []string{"missing"}, nil, false)
			return err
		}, "not configured"},
		{"group", func() error {
			_, err := selectRunWorkers(config, nil, []string{"empty"}, false)
			return err
		}, "has no configured workers"},
		{"all", func() error {
			_, err := selectRunWorkers(FileConfig{}, nil, nil, true)
			return err
		}, "no workers"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSelectRunWorkersUsesConfiguredStartupGroups(t *testing.T) {
	config := FileConfig{
		Workers: map[string]WorkerFileConfig{
			"coder": {Role: "coder", Groups: []string{"default"}},
		},
		Supervisor: SupervisorFileConfig{StartupGroups: []string{"default"}},
	}
	got, err := selectRunWorkers(config, nil, nil, false)
	if err != nil {
		t.Fatalf("startup group selection: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"coder"}) {
		t.Fatalf("startup workers = %#v", got)
	}

	got, err = selectRunWorkers(FileConfig{}, nil, nil, false)
	if err != nil || len(got) != 0 {
		t.Fatalf("bare idle selection = %#v, %v", got, err)
	}
}

func TestSelectRunWorkersUsesAdditiveRoleGroups(t *testing.T) {
	config := FileConfig{
		DefaultRole: "coder",
		Roles: map[string]RoleFileConfig{
			"coder":    {TicketQueue: "open", NudgePrompt: coderNudgePrompt, Groups: []string{"role", "shared"}},
			"reviewer": {TicketQueue: "review", NudgePrompt: reviewerNudgePrompt, Groups: []string{"shared"}},
		},
		Workers: map[string]WorkerFileConfig{
			"coder":    {Role: "coder", Groups: []string{"worker", "shared"}},
			"reviewer": {Role: "reviewer", Groups: []string{"reviewer"}},
		},
		Supervisor: SupervisorFileConfig{StartupGroups: []string{"role"}},
	}
	got, err := selectRunWorkers(config, nil, []string{"shared"}, false)
	if err != nil {
		t.Fatalf("role group selection: %v", err)
	}
	if want := []string{"coder", "reviewer"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected workers = %#v, want %#v", got, want)
	}
	if got := effectiveWorkerGroups(config, "coder"); !reflect.DeepEqual(got, []string{"role", "shared", "worker"}) {
		t.Fatalf("effective coder groups = %#v", got)
	}
}

func TestParseRunConfigResolvesDynamicReviewerCompletion(t *testing.T) {
	tests := []struct {
		name       string
		defaults   string
		rolePolicy string
		envValue   string
		want       string
		wantErr    bool
	}{
		{name: "default signoff", want: ReviewCompletionSignoff},
		{name: "configured default close", defaults: `,"defaults":{"review_completion":"close"}`, want: ReviewCompletionClose},
		{name: "role signoff overrides default close", defaults: `,"defaults":{"review_completion":"close"}`, rolePolicy: `,"review_completion":"signoff"`, want: ReviewCompletionSignoff},
		{name: "role close overrides default signoff", defaults: `,"defaults":{"review_completion":"signoff"}`, rolePolicy: `,"review_completion":"close"`, want: ReviewCompletionClose},
		{name: "environment close overrides role signoff", rolePolicy: `,"review_completion":"signoff"`, envValue: ReviewCompletionClose, want: ReviewCompletionClose},
		{name: "environment signoff overrides role close", rolePolicy: `,"review_completion":"close"`, envValue: ReviewCompletionSignoff, want: ReviewCompletionSignoff},
		{name: "invalid environment override", envValue: "bogus", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := `{"version":1,"default_role":"coder"` + test.defaults + `,"roles":{"coder":{"ticket_queue":"open","nudge_prompt":"code"},"quality":{"ticket_queue":"review","nudge_prompt":"review"` + test.rolePolicy + `}}}`
			writeConfigFixture(t, path, data)
			lookup := emptyEnv
			if test.envValue != "" {
				lookup = mapEnv(map[string]string{"TICKET_ORC_REVIEW_COMPLETION": test.envValue})
			}
			config, _, err := parseRunConfig([]string{"--config", path}, lookup)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "review_completion.invalid") {
					t.Fatalf("invalid environment review completion error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := config.SteerRoles["quality"].ReviewCompletion; got != test.want {
				t.Fatalf("dynamic reviewer completion=%q, want %q", got, test.want)
			}
			if got := config.SteerRoles["coder"].ReviewCompletion; got != "" {
				t.Fatalf("non-review role acquired reviewer completion policy %q", got)
			}
		})
	}
}

func TestRunSupervisorIdleIsPersistentAndReleasesDaemonLock(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{StateDir: stateDir}, "unused", &stdout, &stderr)
	})
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("idle supervisor exited early: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("idle supervisor = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle supervisor did not stop")
	}
	lock, err := state.AcquireLock(context.Background(), filepath.Join(stateDir, "run"), time.Second)
	if err != nil {
		t.Fatalf("daemon lock was not released: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("release daemon lock: %v", err)
	}
}

func TestAttributedWritersKeepPartialLinesSeparate(t *testing.T) {
	var output bytes.Buffer
	var mu sync.Mutex
	first := &attributedWriter{dst: &output, prefix: "[first] ", start: true, mu: &mu}
	second := &attributedWriter{dst: &output, prefix: "[second] ", start: true, mu: &mu}
	if _, err := first.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("complete\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("\n")); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "[second] complete\n[first] partial\n"; got != want {
		t.Fatalf("attributed output = %q, want %q", got, want)
	}
}

func TestRunSupervisorExecutableFailureIsolationAndHooks(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "failed")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output, diagnostics bytes.Buffer
	hooks := make(chan supervisor.WorkerTransition, 16)
	config := RunConfig{
		ConfigPath: configPath, StateDir: stateDir,
		Workers: []supervisor.RunWorker{supervisorTestWorker("running", stateDir), supervisorTestWorker("failed", stateDir)},
		Runtime: NewRuntimeState([]supervisor.RunWorker{supervisorTestWorker("running", stateDir), supervisorTestWorker("failed", stateDir)}),
		Hooks:   supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }},
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, config, os.Args[0], &output, &diagnostics)
	})
	waitForSupervisorTestFile(t, filepath.Join(childDir, "running.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "failed.ready"))
	deadline := time.After(3 * time.Second)
	for {
		select {
		case event := <-hooks:
			if event.Worker == "failed" && event.State == WorkerFailed {
				snapshot := config.Runtime.Snapshot()
				if snapshot["running"].State != WorkerRunning || snapshot["failed"].State != WorkerFailed {
					t.Fatalf("failure isolation snapshot = %#v", snapshot)
				}
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("supervisor = %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("supervisor did not stop after cancellation")
				}
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for failed worker hook")
		}
	}
}

func supervisedRoleFailureConfig(t *testing.T, workerName string) (string, supervisor.RunWorker) {
	t.Helper()
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	repository := filepath.Join(root, "tickets")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"local_dir":%q,"defaults":{"working_dir":%q,"repository":%q},"workers":{%q:{"role":"coder","actor":%q}}}`, stateDir, root, repository, workerName, "actor-"+workerName))
	workerConfig, help, err := parseRoleConfig(RoleCoder, []string{"--config", configPath, "--worker", workerName}, os.LookupEnv)
	if err != nil || help {
		t.Fatalf("parse role config=%#v help=%t err=%v", workerConfig, help, err)
	}
	return configPath, supervisor.RunWorker{Name: workerName, Config: workerConfig}
}

func runSupervisedRoleFailure(t *testing.T, configPath string, worker supervisor.RunWorker) (supervisor.WorkerTransition, string) {
	t.Helper()
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	hooks := make(chan supervisor.WorkerTransition, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var diagnostics bytes.Buffer
	var outputMu sync.Mutex
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{
			ConfigPath: configPath,
			StateDir:   worker.Config.StateDir,
			Workers:    []supervisor.RunWorker{worker},
			Runtime:    runtimeState,
			Hooks:      supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }},
		}, os.Args[0], io.Discard, &lockedWriter{dst: &diagnostics, mu: &outputMu})
	})
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case err := <-done:
			outputMu.Lock()
			text := diagnostics.String()
			outputMu.Unlock()
			t.Fatalf("supervisor exited before worker failure event: err=%v diagnostics=%q", err, text)
		case event := <-hooks:
			if event.Worker != worker.Name || event.State != WorkerFailed {
				continue
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("supervisor = %v", err)
			}
			outputMu.Lock()
			text := diagnostics.String()
			outputMu.Unlock()
			return runtimeState.Snapshot()[worker.Name], text
		case <-deadline.C:
			t.Fatalf("timed out waiting for supervised role failure")
			return supervisor.WorkerTransition{}, ""
		}
	}
}

func TestSupervisedRoleEmitsTicketStartupFailureEnvelope(t *testing.T) {
	t.Setenv("TICKET_ORC_TEST_ROLE", "1")
	t.Setenv("PATH", t.TempDir())
	configPath, worker := supervisedRoleFailureConfig(t, "ticket-startup")
	status, diagnostics := runSupervisedRoleFailure(t, configPath, worker)
	if status.Failure == nil || status.Failure.Classification != "worker_error" || status.Failure.Phase != "Ticket client startup" {
		t.Fatalf("status failure=%#v", status.Failure)
	}
	if !strings.Contains(diagnostics, "classification=worker_error phase=Ticket client startup") {
		t.Fatalf("diagnostics=%q", diagnostics)
	}
	if got := strings.Count(diagnostics, "classification=worker_error"); got != 1 {
		t.Fatalf("worker failure diagnostics=%d, want one: %q", got, diagnostics)
	}
}

func TestSupervisedRoleEmitsLeaseFailureEnvelope(t *testing.T) {
	t.Setenv("TICKET_ORC_TEST_ROLE", "1")
	configPath, worker := supervisedRoleFailureConfig(t, "lease-conflict")
	lease, err := state.AcquireLock(context.Background(), workerLeasePath(worker.Config), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	status, diagnostics := runSupervisedRoleFailure(t, configPath, worker)
	if status.Failure == nil || status.Failure.Classification != "lease_conflict" || status.Failure.Phase != "lease acquisition" || !status.Failure.Contained {
		t.Fatalf("status failure=%#v", status.Failure)
	}
	if !strings.Contains(diagnostics, "classification=lease_conflict phase=lease acquisition") || !strings.Contains(diagnostics, "contained=true") {
		t.Fatalf("diagnostics=%q", diagnostics)
	}
	if got := strings.Count(diagnostics, "classification=lease_conflict"); got != 1 {
		t.Fatalf("lease failure diagnostics=%d, want one: %q", got, diagnostics)
	}
}

func TestRunSupervisorClassifiesWorkerLeaseConflict(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	worker := supervisorTestWorker("conflict", stateDir)
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	hooks := make(chan supervisor.WorkerTransition, 4)
	var diagnostics bytes.Buffer
	var diagnosticsMu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exitCommand := exec.Command(os.Args[0])
	exitCommand.Env = append(os.Environ(), "TICKET_ORC_TEST_EXIT_CODE="+strconv.Itoa(workerLeaseConflictExitCode))
	conflictErr := exitCommand.Run()
	if conflictErr == nil {
		t.Fatal("exit helper unexpectedly succeeded")
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{ConfigPath: configPath, StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
			child := &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}
			child.done <- conflictErr
			return child, nil
		}, Hooks: supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }}}, os.Args[0], io.Discard, &lockedWriter{dst: &diagnostics, mu: &diagnosticsMu})
	})
	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-hooks:
			if event.State == WorkerConflict {
				goto conflictObserved
			}
		case <-deadline:
			t.Fatal("timed out waiting for worker conflict")
		}
	}
conflictObserved:
	if got := runtimeState.Snapshot()[worker.Name].State; got != WorkerConflict {
		t.Fatalf("conflict runtime state=%q", got)
	}
	// The state hook runs before the coordinator writes its diagnostic, so wait
	// for the diagnostic instead of racing the asynchronous result handling.
	diagnosticDeadline := time.Now().Add(time.Second)
	var diagnosticSeen bool
	for time.Now().Before(diagnosticDeadline) {
		diagnosticsMu.Lock()
		text := diagnostics.String()
		diagnosticsMu.Unlock()
		if strings.Contains(text, "containment conflict") {
			diagnosticSeen = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	diagnosticsMu.Lock()
	text := diagnostics.String()
	diagnosticsMu.Unlock()
	if !diagnosticSeen || !strings.Contains(text, "containment conflict") || strings.Contains(text, "secret") {
		t.Fatalf("conflict diagnostic=%q", text)
	}
}

func TestRunSupervisorExecutableIdentityCancellationAndNoRespawn(t *testing.T) {
	childDir := t.TempDir()
	instanceDir := t.TempDir()
	configPath := filepath.Join(instanceDir, "config.json")
	stateDir := filepath.Join(instanceDir, ".local", "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa")
	if err := os.WriteFile(configPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_OUTPUT", "alpha")
	ctx, cancel := context.WithCancel(context.Background())
	var output, diagnostics bytes.Buffer
	config := RunConfig{
		ConfigPath: configPath, StateDir: stateDir,
		Workers: []supervisor.RunWorker{supervisorTestWorker("alpha", stateDir), supervisorTestWorker("beta", stateDir)},
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, config, os.Args[0], &output, &diagnostics)
	})
	alpha := waitForSupervisorTestFile(t, filepath.Join(childDir, "alpha.ready"))
	beta := waitForSupervisorTestFile(t, filepath.Join(childDir, "beta.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "alpha.output"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "beta.output"))
	for name, content := range map[string]string{"alpha": alpha, "beta": beta} {
		for _, want := range []string{"role=coder", "worker=" + name, "config=" + configPath, "instance=" + instanceDir, "actor=actor-" + name, "state="} {
			if !strings.Contains(content, want) {
				t.Fatalf("%s identity missing %q in %q", name, want, content)
			}
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("supervisor = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not reap cancelled children")
	}
	for _, name := range []string{"alpha", "beta"} {
		events, err := os.ReadFile(filepath.Join(childDir, name+".events"))
		if err != nil {
			t.Fatalf("%s events: %v", name, err)
		}
		if got := strings.Count(string(events), "running\n"); got != 1 {
			t.Fatalf("%s running events = %d, want one", name, got)
		}
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 2 || !((lines[0] == "[alpha] partial" && lines[1] == "[beta] complete") || (lines[0] == "[beta] complete" && lines[1] == "[alpha] partial")) {
		t.Fatalf("attributed executable output = %q, want two attributed lines", output.String())
	}
}

func TestRunSupervisorRejectsSecondDaemon(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output, diagnostics bytes.Buffer
	var diagnosticsMu sync.Mutex
	firstDone := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{StateDir: stateDir}, os.Args[0], &output, &lockedWriter{dst: &diagnostics, mu: &diagnosticsMu})
	})
	readyDeadline := time.NewTimer(10 * time.Second)
	defer readyDeadline.Stop()
	readyPoll := time.NewTicker(10 * time.Millisecond)
	defer readyPoll.Stop()
	for {
		diagnosticsMu.Lock()
		ready := strings.Contains(diagnostics.String(), "[ticket-orc] endpoint: unavailable")
		diagnosticsMu.Unlock()
		if ready {
			break
		}
		select {
		case err := <-firstDone:
			t.Fatalf("first daemon exited before readiness: %v", err)
		case <-readyDeadline.C:
			t.Fatal("first daemon did not report startup readiness after acquiring state ownership")
		case <-readyPoll.C:
		}
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	var secondDiagnostics bytes.Buffer
	started := time.Now()
	err := runSupervisor(secondCtx, RunConfig{StateDir: stateDir}, os.Args[0], io.Discard, &secondDiagnostics)
	if err == nil || !strings.Contains(err.Error(), "already running") || !strings.Contains(secondDiagnostics.String(), stateDir) {
		t.Fatalf("second daemon error = %v diagnostics=%q", err, secondDiagnostics.String())
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("second daemon waited %s before failing", elapsed)
	}
	cancel()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first daemon = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first daemon did not stop")
	}
}

func TestRunSupervisorAllowsDistinctStateRoots(t *testing.T) {
	firstState := filepath.Join(t.TempDir(), "first")
	secondState := filepath.Join(t.TempDir(), "second")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var firstDone, secondDone <-chan error
	makeConfig := func(stateDir string) RunConfig {
		key, err := daemon.GenerateEndpointKey()
		if err != nil {
			t.Fatal(err)
		}
		return RunConfig{
			StateDir:    stateDir,
			EndpointKey: key,
			startDaemon: func(context.Context, *supervisor.RuntimeState[supervisor.RunWorker], []supervisor.RunWorker) (*daemon.Server, error) {
				return daemon.NewServer(daemon.Config{StateDir: stateDir, EndpointKey: key})
			},
		}
	}
	firstConfig := makeConfig(firstState)
	secondConfig := makeConfig(secondState)
	firstDone = startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, firstConfig, os.Args[0], io.Discard, io.Discard)
	})
	secondDone = startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, secondConfig, os.Args[0], io.Discard, io.Discard)
	})
	waitForSupervisorTestFile(t, daemon.EndpointPath(firstState))
	waitForSupervisorTestFile(t, daemon.EndpointPath(secondState))
	cancel()
	for name, done := range map[string]<-chan error{"first": firstDone, "second": secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s supervisor = %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s supervisor did not stop", name)
		}
	}
}

// Startup readiness remains pending until the child emits its explicit event.
func TestVerifyWorkerStartupWaitsForExplicitReadiness(t *testing.T) {
	oldTimeout := workerStartupVerificationTime
	workerStartupVerificationTime = 200 * time.Millisecond
	t.Cleanup(func() { workerStartupVerificationTime = oldTimeout })
	worker := supervisorTestWorker("slow", t.TempDir())
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	manager := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState}, os.Args[0], io.Discard, io.Discard, nil)
	child := &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{}), ready: make(chan struct{})}
	manager.lifecycle.TrackChild(worker.Name, child)
	readySignalFinished := make(chan struct{})
	go func() {
		defer close(readySignalFinished)
		time.Sleep(25 * time.Millisecond)
		child.readyOnce.Do(func() { close(child.ready) })
	}()
	t.Cleanup(func() {
		select {
		case <-readySignalFinished:
		case <-time.After(time.Second):
			t.Errorf("startup readiness goroutine did not stop during test cleanup")
		}
	})
	started := time.Now()
	if err := manager.verifyWorkerStartup(context.Background(), worker.Name); err != nil {
		t.Fatalf("verifyWorkerStartup: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond {
		t.Fatalf("startup verification returned before readiness signal: %s", elapsed)
	}
}

// Startup verification must preserve the child's original pre-ready failure.
func TestVerifyWorkerStartupPreservesPreReadyFailureEnvelope(t *testing.T) {
	worker := supervisorTestWorker("failed", t.TempDir())
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	manager := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState}, os.Args[0], io.Discard, io.Discard, nil)
	child := &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{}), ready: make(chan struct{})}
	child.captureFailure(orc.Event{Type: "worker.failure", Worker: worker.Name, Failure: &orc.Failure{Classification: "ticket_command_failed", Phase: "reviewer baseline", TicketCode: "ticket_not_ready"}})
	child.setExitResult(errors.New("child exited"))
	child.done <- child.exitResult()
	close(child.waited)
	manager.lifecycle.TrackChild(worker.Name, child)
	err := manager.verifyWorkerStartup(context.Background(), worker.Name)
	var controlErr *supervisor.LifecycleError
	if !errors.As(err, &controlErr) || controlErr.Failure == nil || controlErr.Failure.Classification != "ticket_command_failed" || controlErr.Failure.TicketCode != "ticket_not_ready" {
		t.Fatalf("startup error=%v control=%#v", err, controlErr)
	}
	if got := runtimeState.Snapshot()[worker.Name].Failure; got == nil || got.Classification != "ticket_command_failed" || got.TicketCode != "ticket_not_ready" {
		t.Fatalf("runtime failure=%#v", got)
	}
}

// Legacy children without readiness events still reconcile early exits once.
func TestVerifyWorkerStartupReconcilesExitedLegacyChildWithoutReadinessChannel(t *testing.T) {
	worker := supervisorTestWorker("legacy-failed", t.TempDir())
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	manager := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState}, os.Args[0], io.Discard, io.Discard, nil)
	child := &runChild{worker: worker, done: make(chan error, 1), waited: make(chan struct{})}
	child.setExitResult(errors.New("legacy child exited"))
	child.done <- child.exitResult()
	close(child.waited)
	manager.lifecycle.TrackChild(worker.Name, child)
	err := manager.verifyWorkerStartup(context.Background(), worker.Name)
	var controlErr *supervisor.LifecycleError
	if !errors.As(err, &controlErr) || controlErr.Failure == nil || controlErr.Failure.Classification != "worker_error" {
		t.Fatalf("startup error=%v control=%#v", err, controlErr)
	}
	if got := runtimeState.Snapshot()[worker.Name].Failure; got == nil || got.Classification != "worker_error" {
		t.Fatalf("runtime failure=%#v", got)
	}
}

// Startup reconciliation preserves the real exit after the result forwarder drains child.done.
func TestVerifyWorkerStartupPreservesExitAfterResultForwarding(t *testing.T) {
	childDir := t.TempDir()
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "pre-ready-exit")
	t.Setenv("TICKET_ORC_TEST_CHILD_EXIT", "23")
	worker := supervisorTestWorker("pre-ready-exit", t.TempDir())
	worker.EventSink = func(orc.Event) {}
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	var transitionMu sync.Mutex
	var transitions []supervisor.WorkerState
	manager := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState}, os.Args[0], io.Discard, io.Discard, func(name string, state supervisor.WorkerState, err error) {
		transitionMu.Lock()
		transitions = append(transitions, state)
		transitionMu.Unlock()
		message := ""
		if err != nil {
			message = err.Error()
		}
		runtimeState.Set(supervisor.WorkerTransition{Worker: name, State: state, Error: message})
	})
	child, err := startRunChild(context.Background(), supervisorTestConfigPath(t), os.Args[0], worker, io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start pre-ready child: %v", err)
	}
	stopRunChildAtTestCleanup(t, child)
	manager.lifecycle.TrackChild(worker.Name, child)
	forwarded := make(chan struct{})
	go func() {
		manager.results <- struct {
			child *runChild
			err   error
		}{child: child, err: <-child.done}
		close(forwarded)
	}()
	t.Cleanup(func() {
		stopRunChildForTest(t, child)
		select {
		case <-forwarded:
		case <-time.After(2 * time.Second):
			t.Errorf("worker result forwarder did not stop during test cleanup")
		}
	})
	select {
	case <-child.waited:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pre-ready child exit")
	}
	select {
	case <-forwarded:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for supervisor result forwarding")
	}

	startupErr := manager.verifyWorkerStartup(context.Background(), worker.Name)
	var controlErr *supervisor.LifecycleError
	if !errors.As(startupErr, &controlErr) || controlErr.Failure == nil || controlErr.Failure.ExitCode != 23 {
		t.Fatalf("startup error=%v control=%#v, want original child exit code 23", startupErr, controlErr)
	}
	if got := runtimeState.Snapshot()[worker.Name].Failure; got == nil || got.ExitCode != 23 {
		t.Fatalf("runtime failure=%#v, want original child exit code 23", got)
	}
	result := <-manager.results
	var exitErr *exec.ExitError
	if !errors.As(result.err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("forwarded result=%v, want original child exit code 23", result.err)
	}
	if manager.isCurrentChild(worker.Name, child) {
		t.Fatal("startup reconciliation left the exited child current for duplicate classification")
	}
	transitionMu.Lock()
	defer transitionMu.Unlock()
	if !reflect.DeepEqual(transitions, []supervisor.WorkerState{WorkerFailed}) {
		t.Fatalf("worker transitions = %#v, want one failure classification", transitions)
	}
}

// A result already classified by the supervisor remains visible to the
// startup caller after the child has been removed from the current-child map.
func TestVerifyWorkerStartupReportsFailureAfterResultReconciliation(t *testing.T) {
	childDir := t.TempDir()
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "result-first")
	t.Setenv("TICKET_ORC_TEST_CHILD_EXIT", "23")
	worker := supervisorTestWorker("result-first", t.TempDir())
	worker.EventSink = func(orc.Event) {}
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	var transitions atomic.Int32
	manager := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState}, os.Args[0], io.Discard, io.Discard, func(_ string, state supervisor.WorkerState, _ error) {
		if state == WorkerFailed {
			transitions.Add(1)
		}
	})
	started, err := manager.start(context.Background(), worker.Name)
	if err != nil || !started.Applied {
		t.Fatalf("production worker start = %#v, %v", started, err)
	}
	child, _ := manager.lifecycle.Child(worker.Name)
	attempt := manager.lifecycle.StartupAttempt(worker.Name)
	if child == nil || attempt == nil || child.startupAttempt != attempt {
		t.Fatal("production start did not register the child's startup attempt")
	}
	var result struct {
		child *runChild
		err   error
	}
	select {
	case result = <-manager.results:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for supervisor result forwarding")
	}

	// Let the supervisor result path win before the readiness verifier. This
	// consumes the result produced by startLocked's production forwarder.
	manager.lifecycle.Lock()
	manager.lifecycle.ForgetChild(worker.Name)
	manager.markStartupResultPending(result.child)
	manager.lifecycle.Unlock()
	failure := classifyWorkerFailureWithEnvelope(result.err, result.child.failureEnvelope())
	manager.completeStartupResult(result.child, result.err, false, failure)
	runtimeState.SetFailure(worker.Name, failure)
	manager.transition(worker.Name, WorkerFailed, result.err)

	startupErr := manager.verifyWorkerStartup(context.Background(), worker.Name)
	var controlErr *supervisor.LifecycleError
	if !errors.As(startupErr, &controlErr) || controlErr.Failure == nil || controlErr.Failure.Classification != "child_exit" || controlErr.Failure.ExitCode != 23 {
		t.Fatalf("startup error=%v control=%#v, want reconciled child exit code 23", startupErr, controlErr)
	}
	if got := runtimeState.Snapshot()[worker.Name].Failure; got == nil || got.Classification != "child_exit" || got.ExitCode != 23 {
		t.Fatalf("runtime failure=%#v, want reconciled child exit code 23", got)
	}
	if got := transitions.Load(); got != 1 {
		t.Fatalf("startup verification changed result-loop transition count: %d", got)
	}
}

func TestRunSupervisorOwnsHTTPDaemonLifecycle(t *testing.T) {
	stateDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := RunConfig{
		StateDir: stateDir,
		startDaemon: func(_ context.Context, _ *supervisor.RuntimeState[supervisor.RunWorker], _ []supervisor.RunWorker) (*daemon.Server, error) {
			return daemon.NewServer(daemon.Config{EndpointKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", StateDir: stateDir, Version: "test"})
		},
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, config, os.Args[0], io.Discard, io.Discard)
	})
	waitForSupervisorTestFile(t, daemon.EndpointPath(stateDir))
	endpoint, err := daemon.ReadEndpoint(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, endpoint.CapabilityURL()+"/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status response = %d", response.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}
	if _, err := os.Stat(daemon.EndpointPath(stateDir)); !os.IsNotExist(err) {
		t.Fatalf("endpoint after supervisor stop: %v", err)
	}
}

func TestRunSupervisorHTTPShutdownReleasesLockAndExits(t *testing.T) {
	stateDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control := &daemon.Control{}
	config := RunConfig{StateDir: stateDir, control: control, startDaemon: func(_ context.Context, _ *supervisor.RuntimeState[supervisor.RunWorker], _ []supervisor.RunWorker) (*daemon.Server, error) {
		return daemon.NewServer(daemon.Config{EndpointKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", StateDir: stateDir, Version: "test", Control: control})
	}}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, config, os.Args[0], io.Discard, io.Discard)
	})
	waitForSupervisorTestFile(t, daemon.EndpointPath(stateDir))
	endpoint, err := daemon.ReadEndpoint(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, endpoint.CapabilityURL()+"/v1/shutdown", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("shutdown status = %d", response.StatusCode)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not exit after HTTP shutdown")
	}
	if _, err := os.Stat(daemon.EndpointPath(stateDir)); !os.IsNotExist(err) {
		t.Fatalf("endpoint after shutdown = %v", err)
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	lock, err := state.AcquireLock(secondCtx, filepath.Join(stateDir, "run"), time.Second)
	if err != nil {
		t.Fatalf("lock after shutdown: %v", err)
	}
	_ = lock.Release()
}

func TestWorkerManagerSerializesConcurrentStarts(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	var calls atomic.Int32
	starter := func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
		calls.Add(1)
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(ctx, RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker}), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	first := make(chan error, 1)
	firstFinished := make(chan struct{})
	go func() {
		defer close(firstFinished)
		_, err := m.start(ctx, "one")
		close(started)
		first <- err
	}()
	t.Cleanup(func() {
		select {
		case <-firstFinished:
		case <-time.After(2 * time.Second):
			t.Errorf("worker start goroutine did not stop during test cleanup")
		}
		m.lifecycle.ForgetChild("one")
	})
	<-started
	_, secondErr := m.start(ctx, "one")
	if secondErr == nil {
		t.Fatal("concurrent start was accepted")
	}
	if calls.Load() != 1 {
		t.Fatalf("starter calls = %d, want one", calls.Load())
	}
	// The fake child has no process, so remove it directly for test cleanup.
	m.lifecycle.ForgetChild("one")
	<-first
}

func TestInteractiveWorkerOutputDoesNotBypassConsoleRenderer(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	var stdout, stderr bytes.Buffer
	manager := newWorkerManager(context.Background(), RunConfig{
		Interactive: true, Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker}),
		startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, childOut, childErr io.Writer, _, _ *sync.Mutex) (*runChild, error) {
			_, _ = io.WriteString(childOut, "queue delivery accepted by codex target \"secret-session\"\n")
			_, _ = io.WriteString(childErr, "queue delivery accepted by codex target \"secret-session\"\n")
			return &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
		},
	}, os.Args[0], &stdout, &stderr, nil)
	if _, err := manager.start(context.Background(), worker.Name); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("interactive child output bypassed console renderer: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	manager.lifecycle.ForgetChild(worker.Name)
}

func TestWorkerManagerPreflightsExplicitStartAndRetainsMetadata(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	var probes atomic.Int32
	var started supervisor.RunWorker
	manager := newWorkerManager(context.Background(), RunConfig{
		Workers: []supervisor.RunWorker{worker},
		ticketProbe: func(context.Context, supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
			probes.Add(1)
			return ticketclient.RepositoryInfo{Path: "/repo", ID: joinTestRepositoryID, FormatVersion: 1, StorageVersion: 1}, nil
		},
		startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
			started = candidate
			return &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
		},
	}, os.Args[0], io.Discard, io.Discard, nil)
	result, err := manager.start(context.Background(), worker.Name)
	if err != nil || !result.Applied {
		t.Fatalf("start = %#v, %v", result, err)
	}
	if probes.Load() != 1 || started.TicketInfo == nil || started.TicketInfo.Path != "/repo" {
		t.Fatalf("probe count/metadata = %d/%#v", probes.Load(), started.TicketInfo)
	}
}

func TestRunSupervisorIsolatesTicketPreflightFailure(t *testing.T) {
	bad := supervisorTestWorker("bad", t.TempDir())
	good := supervisorTestWorker("good", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 2)
	hooks := make(chan supervisor.WorkerTransition, 4)
	config := RunConfig{
		StateDir: t.TempDir(), Workers: []supervisor.RunWorker{bad, good},
		ticketProbe: func(_ context.Context, worker supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
			if worker.Name == "bad" {
				return ticketclient.RepositoryInfo{}, &ticketclient.ProbeError{Code: "unknown_scope", Message: "scope was not found"}
			}
			return ticketclient.RepositoryInfo{Path: "/repo", ID: joinTestRepositoryID}, nil
		},
		startChild: func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
			started <- worker.Name
			return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
		},
		Hooks: supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }},
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, config, os.Args[0], io.Discard, io.Discard)
	})
	var sawFailed bool
	deadline := time.After(2 * time.Second)
	for !sawFailed {
		select {
		case event := <-hooks:
			if event.Worker == "bad" && event.State == WorkerFailed {
				sawFailed = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for failed preflight worker")
		}
	}
	select {
	case name := <-started:
		if name != "good" {
			t.Fatalf("started worker = %q, failed target must not launch", name)
		}
	case <-time.After(time.Second):
		t.Fatal("valid worker was not started")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("supervisor = %v", err)
	}
}

func TestWorkerManagerReloadRejectsChangedTicketTargetTransactionally(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"one","repository":"old"}}}`)
	localDir := defaultTestLocalDir(root)
	initial := supervisorTestWorker("one", localDir)
	initial.Config.Actor = "one"
	initial.Config.Repository = filepath.Join(root, "old")
	manager := newWorkerManager(context.Background(), RunConfig{
		ConfigPath: configPath, StateDir: localDir,
		Workers: []supervisor.RunWorker{initial},
		ticketProbe: func(_ context.Context, candidate supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
			if candidate.Config.Repository == filepath.Join(root, "new") {
				return ticketclient.RepositoryInfo{}, &ticketclient.ProbeError{Code: "missing_config"}
			}
			return ticketclient.RepositoryInfo{Path: candidate.Config.Repository, ID: joinTestRepositoryID}, nil
		},
		Runtime: NewRuntimeState([]supervisor.RunWorker{initial}),
	}, os.Args[0], io.Discard, io.Discard, nil)
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"one","repository":"new"}}}`)
	if _, err := manager.reload(context.Background()); err == nil || !strings.Contains(err.Error(), "ticket_target_unavailable") {
		t.Fatalf("changed target reload error = %v", err)
	}
	current, _ := manager.lifecycle.Worker("one")
	if current.Config.Repository != filepath.Join(root, "old") {
		t.Fatalf("active target after rejected reload = %q", current.Config.Repository)
	}
}

func TestWorkerManagerReloadRejectsLocalRootChangeBeforeProbingOrApplying(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	const firstID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	const secondID = "2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"id":%q,"workers":{"one":{"role":"coder","actor":"one"}}}`, firstID))
	localDir := filepath.Join(root, ".local", firstID)
	worker := supervisorTestWorker("one", localDir)
	worker.Config.Actor = "one"
	var probes atomic.Int32
	manager := newWorkerManager(context.Background(), RunConfig{
		ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker},
		repositoryProbe: func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
			probes.Add(1)
			return ticketclient.RepositoryInfo{}, nil
		},
		Runtime: NewRuntimeState([]supervisor.RunWorker{worker}),
	}, os.Args[0], io.Discard, io.Discard, nil)
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"id":%q,"workers":{"one":{"role":"coder","actor":"one"}}}`, secondID))

	if _, err := manager.reload(context.Background()); err == nil || !strings.Contains(err.Error(), "local_root_changed") {
		t.Fatalf("local-root reload error = %v, want local_root_changed", err)
	}
	if probes.Load() != 0 {
		t.Fatalf("candidate repositories were probed before root rejection: %d", probes.Load())
	}
	current, ok := manager.lifecycle.Worker("one")
	if !ok || current.Config.StateDir != localDir || len(manager.snapshotWorkers()) != 1 {
		t.Fatalf("active worker changed after root rejection: %#v", current)
	}
	if _, err := os.Stat(filepath.Join(root, ".local", secondID)); !os.IsNotExist(err) {
		t.Fatalf("reload created the candidate local root: %v", err)
	}
}

func TestWorkerManagerPauseBlocksStartUntilResume(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	var starts atomic.Int32
	starter := func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
		starts.Add(1)
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtime, startChild: starter}, os.Args[0], io.Discard, io.Discard, func(name string, state supervisor.WorkerState, err error) {
		runtime.Set(supervisor.WorkerTransition{Worker: name, State: state})
	})
	paused, err := m.pause(context.Background(), worker.Name)
	if err != nil || !paused.Applied || paused.State != string(WorkerPaused) {
		t.Fatalf("pause = %#v, %v", paused, err)
	}
	if got := runtime.Snapshot()[worker.Name].State; got != WorkerPaused {
		t.Fatalf("runtime state after pause = %q", got)
	}
	repeatedPause, err := m.pause(context.Background(), worker.Name)
	if err != nil || repeatedPause.Applied || repeatedPause.State != string(WorkerPaused) {
		t.Fatalf("repeated pause = %#v, %v", repeatedPause, err)
	}
	if _, err := m.start(context.Background(), worker.Name); err == nil || !strings.Contains(err.Error(), "worker_paused") {
		t.Fatalf("start while paused error = %v", err)
	}
	if starts.Load() != 0 {
		t.Fatalf("start calls while paused = %d", starts.Load())
	}
	resumed, err := m.resume(context.Background(), worker.Name)
	if err != nil || !resumed.Applied || resumed.State != string(WorkerRunning) {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}
	if starts.Load() != 1 {
		t.Fatalf("start calls after resume = %d", starts.Load())
	}
	repeatedResume, err := m.resume(context.Background(), worker.Name)
	if err != nil || repeatedResume.Applied || repeatedResume.State != string(WorkerRunning) {
		t.Fatalf("repeated resume = %#v, %v", repeatedResume, err)
	}
	if starts.Load() != 1 {
		t.Fatalf("repeated resume start calls = %d, want one", starts.Load())
	}
	m.lifecycle.ForgetChild(worker.Name)
}

// A pause cannot be overwritten by a concurrent explicit start.
func TestWorkerManagerPauseSerializesWithStartup(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	childDir := t.TempDir()
	starterCalled := make(chan struct{})
	releaseStarter := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseStarter) }) }
	var childMu sync.Mutex
	var startedChild *runChild
	starter := func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
		close(starterCalled)
		<-releaseStarter
		cmd := exec.Command(os.Args[0], "-test.run=TestMain")
		cmd.Env = append(os.Environ(), "TICKET_ORC_TEST_CHILD=1", "TICKET_ORC_TEST_CHILD_DIR="+childDir, "TICKET_ORC_WORKER="+worker.Name)
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		child := &runChild{worker: worker, cmd: cmd, done: make(chan error, 1), waited: make(chan struct{})}
		go func() {
			err := cmd.Wait()
			child.done <- err
			close(child.waited)
		}()
		childMu.Lock()
		startedChild = child
		childMu.Unlock()
		return child, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker}), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	started := make(chan error, 1)
	startedFinished := make(chan struct{})
	go func() {
		defer close(startedFinished)
		_, err := m.start(context.Background(), worker.Name)
		started <- err
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-startedFinished:
		case <-time.After(5 * time.Second):
			t.Errorf("worker start goroutine did not stop during test cleanup")
		}
		childMu.Lock()
		child := startedChild
		childMu.Unlock()
		stopRunChildForTest(t, child)
	})
	select {
	case <-starterCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("worker starter was not called")
	}
	paused := make(chan error, 1)
	pausedFinished := make(chan struct{})
	go func() {
		defer close(pausedFinished)
		_, err := m.pause(context.Background(), worker.Name)
		paused <- err
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-pausedFinished:
		case <-time.After(5 * time.Second):
			t.Errorf("worker pause goroutine did not stop during test cleanup")
		}
	})
	select {
	case err := <-paused:
		t.Fatalf("pause completed before startup released: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if err := <-paused; err != nil {
		t.Fatalf("pause after startup: %v", err)
	}
	_, running := m.lifecycle.Child(worker.Name)
	pausedState := m.lifecycle.Paused(worker.Name)
	if running || !pausedState {
		t.Fatalf("startup/pause state running=%v paused=%v", running, pausedState)
	}
}

func TestWorkerManagerRejectsResumeAfterShutdownBegins(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.pause(context.Background(), worker.Name); err != nil {
		t.Fatal(err)
	}
	m.lifecycle.SetStopping(true)
	if _, err := m.resume(context.Background(), worker.Name); err == nil || !strings.Contains(err.Error(), "daemon_stopping") {
		t.Fatalf("resume during shutdown error = %v", err)
	}
}

// A reloaded desired config survives pausing and restarting the active child.
func TestWorkerManagerPausesActiveChildAndResumesDesiredConfig(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", root)
	worker := supervisorTestWorker("one", localDir)
	worker.Config.Actor = "old-actor"
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"new-actor"}}}`)
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	var failureTransitions atomic.Int32
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtime}, os.Args[0], io.Discard, io.Discard, func(name string, state supervisor.WorkerState, err error) {
		if state == WorkerFailed {
			failureTransitions.Add(1)
		}
		runtime.Set(supervisor.WorkerTransition{Worker: name, State: state})
	})
	started, err := manager.start(context.Background(), worker.Name)
	if err != nil || !started.Applied {
		t.Fatalf("start = %#v, %v", started, err)
	}
	waitForSupervisorTestFile(t, filepath.Join(root, "one.ready"))
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"new-actor"}}}`)
	if result, err := manager.reload(context.Background()); err != nil || !result.Applied {
		t.Fatalf("reload = %#v, %v", result, err)
	}
	paused, err := manager.pause(context.Background(), worker.Name)
	if err != nil || paused.State != string(WorkerPaused) || !paused.Applied {
		t.Fatalf("pause active child = %#v, %v", paused, err)
	}
	effective := runtime.EffectiveWorkers()
	if len(effective) != 1 || effective[0].Config.Actor != "new-actor" {
		t.Fatalf("effective worker after pause = %#v", effective)
	}
	if got := runtime.Snapshot()[worker.Name].State; got != WorkerPaused {
		t.Fatalf("runtime state after active pause = %q", got)
	}
	resumed, err := manager.resume(context.Background(), worker.Name)
	if err != nil || !resumed.Applied || resumed.State != string(WorkerRunning) {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}
	child, _ := manager.lifecycle.Child(worker.Name)
	if child == nil || child.worker.Config.Actor != "new-actor" {
		t.Fatalf("resumed child = %#v", child)
	}
	if _, err := manager.stop(context.Background(), worker.Name); err != nil {
		t.Fatal(err)
	}
	if got := failureTransitions.Load(); got != 0 {
		t.Fatalf("deliberate pause/restart/stop classified %d child failures", got)
	}
}

func TestWorkerManagerRejectsRemovingPausedWorker(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	worker := supervisorTestWorker("one", localDir)
	worker.Config.Actor = "actor-one"
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker})}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := manager.pause(context.Background(), worker.Name); err != nil {
		t.Fatal(err)
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{}}`)
	if _, err := manager.reload(context.Background()); err == nil || !strings.Contains(err.Error(), "candidate removes all configured workers") {
		t.Fatalf("paused worker removal error = %v", err)
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	if _, err := manager.reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.start(context.Background(), worker.Name); err == nil || !strings.Contains(err.Error(), "worker_paused") {
		t.Fatalf("re-added paused worker start error = %v", err)
	}
}

func TestWorkerManagerRejectsActiveIdentityConflicts(t *testing.T) {
	first := supervisorTestWorker("coder", t.TempDir())
	second := supervisorTestWorker("reviewer", t.TempDir())
	first.Config.Actor, second.Config.Actor = "shared", "shared"
	workers := []supervisor.RunWorker{first, second}
	starter := func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: workers, Runtime: NewRuntimeState(workers), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.start(context.Background(), "reviewer"); err == nil || !strings.Contains(err.Error(), "worker_identity_conflict") {
		t.Fatalf("duplicate actor start error = %v", err)
	}
	m.lifecycle.ForgetChild("coder")

	first.Config.Actor, second.Config.Actor = "coder", "reviewer"
	second.Config.Role = RoleReviewer
	first.Config.Harness, second.Config.Harness = "codex", "codex"
	for name := range m.lifecycle.Children() {
		m.lifecycle.ForgetChild(name)
	}
	m.lifecycle.SetWorker("coder", first)
	m.lifecycle.SetWorker("reviewer", second)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
}

func TestRoleRejectsSurvivingWorkerLease(t *testing.T) {
	config := supervisorTestWorker("one", t.TempDir()).Config
	lease, err := state.AcquireLock(context.Background(), workerLeasePath(config), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	err = executeRole(config)
	if !errors.Is(err, ErrWorkerLeaseConflict) {
		t.Fatalf("role lease conflict error=%v, want ErrWorkerLeaseConflict", err)
	}
}

func TestWorkerLeaseIdentityDoesNotDependOnHarnessOrModel(t *testing.T) {
	config := supervisorTestWorker("", t.TempDir()).Config
	config.Actor = "project-coder"
	config.WorkingDir = "/work/project"
	basePath := workerLeasePath(config)
	baseName := filepath.Base(basePath)
	if filepath.Dir(basePath) != filepath.Join(config.StateDir, "workers") || len(strings.TrimSuffix(baseName, ".lease")) != 64 || strings.Contains(baseName, "project-coder") {
		t.Fatalf("worker lease path is not a collision-safe key beneath the local root: %q", basePath)
	}

	changedExecution := config
	changedExecution.Harness = "claude"
	changedExecution.Model = "changed-model"
	changedExecution.Pi.Provider = "different-provider"
	if got := workerLeasePath(changedExecution); got != basePath {
		t.Fatalf("harness/model changed worker lease identity: %q != %q", got, basePath)
	}

	changedRole := config
	changedRole.Role = RoleReviewer
	if got := workerLeasePath(changedRole); got == basePath {
		t.Fatal("role change did not change the unnamed worker lease identity")
	}
}

func TestWorkerLeaseReleaseAllowsExplicitReplacement(t *testing.T) {
	config := supervisorTestWorker("replace", t.TempDir()).Config
	path := workerLeasePath(config)
	old, err := state.AcquireLock(context.Background(), path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if replacement, acquireErr := state.AcquireLock(context.Background(), path, 25*time.Millisecond); acquireErr == nil {
		replacement.Release()
		t.Fatal("replacement acquired a lease held by the older worker")
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	replacement, err := state.AcquireLock(context.Background(), path, time.Second)
	if err != nil {
		t.Fatalf("replacement after release: %v", err)
	}
	defer replacement.Release()
}

func TestWorkerManagerScopesIdentityConflictsByRepository(t *testing.T) {
	first := supervisorTestWorker("coder", t.TempDir())
	second := supervisorTestWorker("reviewer", t.TempDir())
	first.Config.Actor, second.Config.Actor = "shared", "shared"
	first.Config.Repository = filepath.Join(t.TempDir(), "repo-one")
	second.Config.Repository = filepath.Join(t.TempDir(), "repo-two")
	workers := []supervisor.RunWorker{first, second}
	starter := func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: workers, Runtime: NewRuntimeState(workers), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.start(context.Background(), "reviewer"); err != nil {
		t.Fatalf("distinct repository actor start failed: %v", err)
	}

	first.Config.Repository = filepath.Join(t.TempDir(), "same-repo")
	second.Config.Repository = first.Config.Repository
	workers = []supervisor.RunWorker{first, second}
	m = newWorkerManager(context.Background(), RunConfig{Workers: workers, Runtime: NewRuntimeState(workers), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.start(context.Background(), "reviewer"); err == nil || !strings.Contains(err.Error(), "worker_identity_conflict") {
		t.Fatalf("same repository actor start error = %v", err)
	}
}

func TestWorkerManagerRetainsChildWhenStopSignalFails(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker})}, os.Args[0], io.Discard, io.Discard, nil)
	command := exec.Command(os.Args[0])
	command.Env = setChildEnvironment(os.Environ(), "TICKET_ORC_TEST_EXIT_CODE", "0")
	if err := command.Start(); err != nil {
		t.Fatalf("start stop-failure helper process: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait for stop-failure helper process: %v", err)
	}
	child := &runChild{worker: worker, cmd: command}
	m.lifecycle.TrackChild(worker.Name, child)
	_, err := m.stop(context.Background(), worker.Name)
	if err == nil {
		t.Fatal("stop unexpectedly succeeded for an already-exited process")
	}
	var lifecycleErr *supervisor.LifecycleError
	if !errors.As(err, &lifecycleErr) || lifecycleErr.Code != "worker_unavailable" {
		t.Fatalf("stop error = %v, want worker_unavailable lifecycle error", err)
	}
	_, retained := m.lifecycle.Child(worker.Name)
	if !retained {
		t.Fatal("failed stop lost the managed child")
	}
}

func TestWorkerManagerReloadAtomicVisibilityAndRollback(t *testing.T) {
	t.Setenv("TICKET_ORC_ACTOR", "reload-test")
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"roles":{"coder":{"groups":["role-old"]}},"workers":{"one":{"role":"coder","groups":["old"]}}}`)
	initial := supervisorTestWorker("one", localDir)
	runtime := NewRuntimeState([]supervisor.RunWorker{initial})
	m := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{initial}, Runtime: runtime}, os.Args[0], io.Discard, io.Discard, nil)
	writeConfigFixture(t, configPath, `{"version":1,"roles":{"coder":{"groups":["role-new"]}},"workers":{"one":{"role":"coder","groups":["new","role-new"]},"two":{"role":"reviewer","groups":["new"]}}}`)
	if result, err := m.reload(context.Background()); err != nil || !result.Applied {
		t.Fatalf("reload add/change = %#v, %v", result, err)
	}
	snapshot := m.snapshotWorkers()
	if len(snapshot) != 2 || snapshot[0].Name != "one" || !reflect.DeepEqual(snapshot[0].Groups, []string{"role-new", "new"}) || snapshot[1].Name != "two" {
		t.Fatalf("workers after add/change = %#v", snapshot)
	}
	if got := runtime.Snapshot()["two"].State; got != WorkerStopped {
		t.Fatalf("added worker state = %q, want stopped", got)
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"two":{"role":"reviewer","groups":["new"]}}}`)
	if _, err := m.reload(context.Background()); err != nil {
		t.Fatalf("reload remove stopped worker: %v", err)
	}
	if got := len(m.snapshotWorkers()); got != 1 || m.snapshotWorkers()[0].Name != "two" {
		t.Fatalf("workers after removal = %#v", m.snapshotWorkers())
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"two":{"role":"reviewer","future":true}}}`)
	if _, err := m.reload(context.Background()); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	if got := len(m.snapshotWorkers()); got != 1 || m.snapshotWorkers()[0].Name != "two" {
		t.Fatalf("invalid reload changed workers = %#v", m.snapshotWorkers())
	}
	worker, _ := m.lifecycle.Worker("two")
	m.lifecycle.TrackChild("two", &runChild{worker: worker})
	writeConfigFixture(t, configPath, `{"version":1,"workers":{}}`)
	if _, err := m.reload(context.Background()); err == nil {
		t.Fatal("running worker removal succeeded")
	}
}

func TestWorkerManagerReloadKeepsRunningEffectiveConfigAndUpdatesDesired(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"defaults":{"required_skills":["ticket"]},"workers":{"one":{"role":"coder","actor":"new-actor","groups":["old"]},"two":{"role":"reviewer","actor":"old-actor"}}}`)
	initial := supervisorTestWorker("one", localDir)
	initial.Config.Actor = "old-actor"
	initial.RequiredSkills = []string{"ticket"}
	runtime := NewRuntimeState([]supervisor.RunWorker{initial})
	m := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{initial}, Runtime: runtime}, os.Args[0], io.Discard, io.Discard, nil)
	child := &runChild{worker: initial, cmd: &exec.Cmd{}}
	m.lifecycle.TrackChild("one", child)
	writeConfigFixture(t, configPath, `{"version":1,"defaults":{"required_skills":["ticket"]},"workers":{"one":{"role":"coder","actor":"new-actor","groups":["old"]},"two":{"role":"reviewer","actor":"old-actor"}}}`)
	reloadResult, err := m.reload(context.Background())
	if err != nil {
		t.Fatalf("reload changed running worker: %v", err)
	}
	if len(reloadResult.Warnings) == 0 || reloadResult.Warnings[0].Code != "reload.restart_required" {
		t.Fatalf("reload warnings = %#v, want restart_required", reloadResult.Warnings)
	}
	desired, _ := m.lifecycle.Worker("one")
	effectiveChild, _ := m.lifecycle.Child("one")
	effective := effectiveChild.worker
	if desired.Config.Actor != "new-actor" {
		t.Fatalf("desired actor = %q, want new-actor", desired.Config.Actor)
	}
	if effective.Config.Actor != "old-actor" {
		t.Fatalf("effective actor changed to %q while running", effective.Config.Actor)
	}
	if !reflect.DeepEqual(desired.RequiredSkills, []string{"ticket"}) {
		t.Fatalf("desired required skills = %#v", desired.RequiredSkills)
	}
	if !reflect.DeepEqual(effective.RequiredSkills, []string{"ticket"}) {
		t.Fatalf("effective required skills changed to %#v while running", effective.RequiredSkills)
	}
	if got := reloadResult.Warnings[0].Remediation; !strings.Contains(got, "restart this worker") {
		t.Fatalf("Skill reload remediation = %q", got)
	}
	effectiveWorkers := runtime.EffectiveWorkers()
	if len(effectiveWorkers) != 2 || effectiveWorkers[0].Config.Actor != "old-actor" {
		t.Fatalf("runtime effective workers = %#v; status must use the running configuration", effectiveWorkers)
	}
	if _, err := m.start(context.Background(), "two"); err == nil || !strings.Contains(err.Error(), "worker_identity_conflict") {
		t.Fatalf("start while old effective worker is running error = %v", err)
	}
	var started supervisor.RunWorker
	m.starter = func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
		started = worker
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m.lifecycle.ForgetChild("one")
	if result, err := m.start(context.Background(), "one"); err != nil || !result.Applied {
		t.Fatalf("restart after reload = %#v, %v", result, err)
	}
	if !reflect.DeepEqual(started.RequiredSkills, []string{"ticket"}) {
		t.Fatalf("restarted worker required skills = %#v", started.RequiredSkills)
	}
}

func TestRunSupervisorRejectsJSONMultiplexing(t *testing.T) {
	first := supervisorTestWorker("one", t.TempDir())
	second := supervisorTestWorker("two", t.TempDir())
	first.Config.Output = OutputJSON
	second.Config.Output = OutputJSON
	err := runSupervisor(context.Background(), RunConfig{StateDir: t.TempDir(), Workers: []supervisor.RunWorker{first, second}}, os.Args[0], io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot multiplex json") {
		t.Fatalf("JSON multiplexing error = %v", err)
	}
}

func TestRunChildEnvironmentBindsResolvedTicketTarget(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	base := []string{"TICKET_ACTOR=inherited", "TICKET_REPOSITORY=inherited-repository", "TICKET_CONFIG=inherited-config", "TICKET_SCOPE=inherited-scope", "TICKET_ROOT=inherited-root", "TICKET_ORC=wrong-instance", "TICKET_ORC_CONFIG=wrong-config", "TICKET_ORC_STATE_DIR=wrong-root", "TICKET_ORC_DAEMON_TOKEN=secret", "TICKET_ORC_ENDPOINT_TOKEN=secret", "TICKET_ORC_BEARER_TOKEN=secret", "OTHER=preserved"}
	value := func(environment []string, key string) string {
		for _, entry := range environment {
			if strings.HasPrefix(entry, key+"=") {
				return strings.TrimPrefix(entry, key+"=")
			}
		}
		return "<missing>"
	}
	const capabilityURL = "http://127.0.0.1:43123/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	instanceDir := filepath.Dir(configPath)
	direct := runChildEnvironment(base, configPath, supervisor.RoleConfig{Actor: "worker", Repository: "/repo", Ticket: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/repo"}}, nil)
	if value(direct, "TICKET_ACTOR") != "worker" || value(direct, "TICKET_REPOSITORY") != "/repo" || value(direct, "TICKET_CONFIG") != "<missing>" || value(direct, "TICKET_SCOPE") != "<missing>" || value(direct, "TICKET_ROOT") != "<missing>" || value(direct, "TICKET_ORC") != instanceDir || value(direct, "TICKET_ORC_CONFIG") != "<missing>" || value(direct, "TICKET_ORC_STATE_DIR") != "<missing>" || value(direct, "TICKET_ORC_DAEMON_TOKEN") != "<missing>" || value(direct, "TICKET_ORC_ENDPOINT_TOKEN") != "<missing>" || value(direct, "TICKET_ORC_BEARER_TOKEN") != "<missing>" || value(direct, "OTHER") != "preserved" {
		t.Fatalf("direct child environment = %#v", direct)
	}
	scoped := runChildEnvironment(base, configPath, supervisor.RoleConfig{Actor: "worker", Ticket: supervisor.TicketTarget{Mode: TicketTargetScoped, Config: "/config/ticket.json", Scope: "named"}}, &ticketclient.RepositoryInfo{Path: "/resolved/repository"})
	if value(scoped, "TICKET_ACTOR") != "worker" || value(scoped, "TICKET_REPOSITORY") != "/resolved/repository" || value(scoped, "TICKET_CONFIG") != "<missing>" || value(scoped, "TICKET_SCOPE") != "<missing>" || value(scoped, "TICKET_ROOT") != "<missing>" || value(scoped, "TICKET_ORC") != instanceDir || value(scoped, "TICKET_ORC_CONFIG") != "<missing>" || value(scoped, "OTHER") != "preserved" {
		t.Fatalf("scoped child environment = %#v", scoped)
	}
}

func TestRunChildEnvironmentRoutesScopedTargetThroughTicketProtocol(t *testing.T) {
	ticket, err := exec.LookPath("ticket")
	if err != nil {
		t.Skip("Ticket CLI is not installed")
	}
	root := t.TempDir()
	repository := filepath.Join(root, "tickets")
	configPath := filepath.Join(root, "ticket-config.json")
	config, err := json.Marshal(map[string]any{"scopes": map[string]any{
		"managed": map[string]any{"repository": repository},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	workingDir := root
	init := exec.Command(ticket, "--config", configPath, "--scope", "managed", "init", "-j")
	init.Dir = workingDir
	init.Env = ticketclient.ApplyTargetEnvironment(os.Environ(), "coder", ticketclient.Target{Config: configPath, Scope: "managed"})
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("initialize configured scope: %v\n%s", err, output)
	}

	info, err := ticketclient.ProbeInfo(context.Background(), "coder", workingDir, ticketclient.Target{Config: configPath, Scope: "managed"})
	if err != nil {
		t.Fatalf("resolve configured scope: %v", err)
	}
	workerConfig := supervisor.RoleConfig{Actor: "coder", WorkingDir: workingDir, Ticket: supervisor.TicketTarget{Mode: TicketTargetScoped, Config: configPath, Scope: "managed"}}
	env := runChildEnvironment(os.Environ(), filepath.Join(root, "orc.json"), workerConfig, &info)
	command := exec.Command(ticket, "info", "-j")
	command.Dir = workingDir
	command.Env = env
	output, err := command.Output()
	if err != nil {
		t.Fatalf("Ticket info through managed child environment: %v", err)
	}
	var got ticketclient.RepositoryInfo
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode child Ticket info %q: %v", output, err)
	}
	if got.Path != info.Path || got.Scope != nil {
		t.Fatalf("child Ticket target = %#v, want resolved repository %q without unsupported scope/config environment", got, info.Path)
	}
}

func TestWorkerEventWriterConsumesStructuredEventsAndPreservesDiagnostics(t *testing.T) {
	var output bytes.Buffer
	var events []orc.Event
	writer := &workerEventWriter{dst: &output, sink: func(event orc.Event) { events = append(events, event) }}
	input := "ordinary diagnostic\n" + orc.EventStreamPrefix + `{"type":"steer.delivery","worker":"coder","role":"coder","phase":"accepted","code":"delivery_accepted","ticket":"20260920-12345"}` + "\npartial"
	if n, err := writer.Write([]byte(input)); err != nil || n != len(input) {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := output.String(); got != "ordinary diagnostic\npartial\n" {
		t.Fatalf("diagnostic output = %q", got)
	}
	if len(events) != 1 || events[0].Type != "steer.delivery" || events[0].Ticket != "20260920-12345" || events[0].Code != "delivery_accepted" {
		t.Fatalf("events = %#v", events)
	}
}

func TestWorkerEventWriterCapturesChildFailureEnvelope(t *testing.T) {
	var output bytes.Buffer
	var events []orc.Event
	child := &runChild{}
	writer := &workerEventWriter{dst: &output, sink: func(event orc.Event) {
		if child.captureFailure(event) {
			return
		}
		events = append(events, event)
	}}
	failureLine := orc.EventStreamPrefix + `{"type":"worker.failure","worker":"reviewer","role":"reviewer","failure":{"classification":"ticket_command_failed","phase":"worker operation","exit_code":1,"ticket_code":"ticket_not_ready"}}` + "\n"
	queueLine := orc.EventStreamPrefix + `{"type":"worker.state","worker":"reviewer","role":"reviewer","state":"quiescent"}` + "\n"
	input := failureLine + queueLine
	if n, err := writer.Write([]byte(input)); err != nil || n != len(input) {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if output.Len() != 0 || len(events) != 1 || events[0].Type != "worker.state" {
		t.Fatalf("output=%q events=%#v", output.String(), events)
	}
	failure := child.failureEnvelope()
	if failure == nil || failure.Classification != "ticket_command_failed" || failure.TicketCode != "ticket_not_ready" || failure.ExitCode != 1 {
		t.Fatalf("captured failure=%#v", failure)
	}
}

func TestStopRunChildrenReapsExitedAndForcedKillChildren(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "exited")
	t.Setenv("TICKET_ORC_TEST_CHILD_IGNORE", "forced")
	oldTimeout := runChildStopTimeout
	runChildStopTimeout = 40 * time.Millisecond
	t.Cleanup(func() { runChildStopTimeout = oldTimeout })
	var output, diagnostics bytes.Buffer
	executable := os.Args[0]
	exited, err := startRunChild(context.Background(), configPath, executable, supervisorTestWorker("exited", stateDir), &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start exited child: %v", err)
	}
	stopRunChildAtTestCleanup(t, exited)
	forced, err := startRunChild(context.Background(), configPath, executable, supervisorTestWorker("forced", stateDir), &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start forced child: %v", err)
	}
	stopRunChildAtTestCleanup(t, forced)
	waitForSupervisorTestFile(t, filepath.Join(childDir, "exited.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "forced.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "forced.armed"))
	stopRunChildren([]*runChild{exited, forced})
	for _, child := range []*runChild{exited, forced} {
		select {
		case <-child.waited:
		case <-time.After(time.Second):
			t.Fatalf("child %s was not reaped", child.worker.Name)
		}
		if got := atomic.LoadInt32(child.waitCalls); got != 1 {
			t.Fatalf("child %s wait calls = %d, want one", child.worker.Name, got)
		}
	}
}

func TestStartRunChildFlushesTrailingStructuredEventFragment(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_EVENT_FRAGMENT", "1")
	var output, diagnostics bytes.Buffer
	var events []orc.Event
	worker := supervisorTestWorker("fragment", t.TempDir())
	worker.EventSink = func(event orc.Event) { events = append(events, event) }
	child, err := startRunChild(context.Background(), configPath, os.Args[0], worker, &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	stopRunChildAtTestCleanup(t, child)
	waitForSupervisorTestFile(t, filepath.Join(childDir, "fragment.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "fragment.fragment-written"))
	select {
	case <-child.ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for child readiness event before stopping it")
	}
	stopRunChildren([]*runChild{child})
	if len(events) != 2 || events[0].Type != orc.WorkerReadyEventType || events[1].Type != "worker.state" || events[1].State != "running" {
		t.Fatalf("flushed events = %#v", events)
	}
	if strings.Contains(diagnostics.String(), "ticket-orc-event") {
		t.Fatalf("structured event leaked to diagnostics: %q", diagnostics.String())
	}
}

func TestStartRunChildUsesEachWorkersWorkingDirectory(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_STATE_DIR", "inherited-state-root")
	var output, diagnostics bytes.Buffer
	first := supervisorTestWorker("first", t.TempDir())
	first.Config.WorkingDir = firstDir
	first.Config.StateDir = filepath.Join(t.TempDir(), "first-state")
	second := supervisorTestWorker("second", t.TempDir())
	second.Config.WorkingDir = secondDir
	second.Config.StateDir = filepath.Join(t.TempDir(), "second-state")
	firstChild, err := startRunChild(context.Background(), configPath, os.Args[0], first, &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start first child: %v", err)
	}
	stopRunChildAtTestCleanup(t, firstChild)
	secondChild, err := startRunChild(context.Background(), configPath, os.Args[0], second, &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		stopRunChildren([]*runChild{firstChild})
		t.Fatalf("start second child: %v", err)
	}
	stopRunChildAtTestCleanup(t, secondChild)
	firstReady := waitForSupervisorTestFile(t, filepath.Join(childDir, "first.ready"))
	secondReady := waitForSupervisorTestFile(t, filepath.Join(childDir, "second.ready"))
	firstWorkingDir, firstHasWorkingDir := supervisorTestReadyField(firstReady, "cwd")
	secondWorkingDir, secondHasWorkingDir := supervisorTestReadyField(secondReady, "cwd")
	if !firstHasWorkingDir || !secondHasWorkingDir {
		t.Fatalf("child working directories missing from readiness payloads: %q and %q", firstReady, secondReady)
	}
	firstMatches, err := sameSupervisorTestDirectory(firstWorkingDir, firstDir)
	if err != nil {
		t.Fatal(err)
	}
	secondMatches, err := sameSupervisorTestDirectory(secondWorkingDir, secondDir)
	if err != nil {
		t.Fatal(err)
	}
	if !firstMatches || !secondMatches || !strings.Contains(firstReady, "state=\n") || !strings.Contains(secondReady, "state=\n") {
		t.Fatalf("child working directories = %q and %q", firstReady, secondReady)
	}
	if sameDirectory, err := sameSupervisorTestDirectory(firstWorkingDir, secondWorkingDir); err != nil || sameDirectory {
		t.Fatalf("child working directories must remain distinct: same=%t err=%v", sameDirectory, err)
	}
	stopRunChildren([]*runChild{firstChild, secondChild})
}

func TestRunSupervisorPartialStartFailureCleansAllChildren(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "exited")
	t.Setenv("TICKET_ORC_TEST_CHILD_IGNORE", "forced")
	oldTimeout := runChildStopTimeout
	runChildStopTimeout = 40 * time.Millisecond
	t.Cleanup(func() { runChildStopTimeout = oldTimeout })
	workers := []supervisor.RunWorker{supervisorTestWorker("exited", stateDir), supervisorTestWorker("forced", stateDir), supervisorTestWorker("fail", stateDir)}
	runtimeState := NewRuntimeState(workers)
	var output, diagnostics bytes.Buffer
	hooks := make(chan supervisor.WorkerTransition, 16)
	var children []*runChild
	starter := func(ctx context.Context, configPath, executable string, worker supervisor.RunWorker, stdout, stderr io.Writer, outputMu, errorMu *sync.Mutex) (*runChild, error) {
		if worker.Name == "fail" {
			waitForSupervisorTestFile(t, filepath.Join(childDir, "exited.ready"))
			waitForSupervisorTestFile(t, filepath.Join(childDir, "forced.armed"))
			return nil, errors.New("deterministic later child start failure")
		}
		child, err := startRunChild(ctx, configPath, executable, worker, stdout, stderr, outputMu, errorMu)
		if err == nil {
			children = append(children, child)
		}
		return child, err
	}
	config := RunConfig{ConfigPath: configPath, StateDir: stateDir, Workers: workers, Runtime: runtimeState, startChild: starter, Hooks: supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }}}
	started := time.Now()
	err := runSupervisor(context.Background(), config, os.Args[0], &output, &diagnostics)
	if err == nil || !strings.Contains(err.Error(), "deterministic later child start failure") {
		t.Fatalf("partial start error = %v", err)
	}
	assertPartialStartCleanupDuration(t, time.Since(started), runChildStopTimeout)
	for _, child := range children {
		select {
		case <-child.waited:
		case <-time.After(time.Second):
			t.Fatalf("child %s was not reaped", child.worker.Name)
		}
		if got := atomic.LoadInt32(child.waitCalls); got != 1 {
			t.Fatalf("child %s wait calls = %d, want one", child.worker.Name, got)
		}
	}
	snapshot := runtimeState.Snapshot()
	if snapshot["exited"].State != WorkerStopped || snapshot["forced"].State != WorkerStopped || snapshot["fail"].State != WorkerFailed {
		t.Fatalf("partial start runtime state = %#v", snapshot)
	}
	seen := make(map[string]map[supervisor.WorkerState]bool)
	for len(hooks) > 0 {
		event := <-hooks
		if seen[event.Worker] == nil {
			seen[event.Worker] = make(map[supervisor.WorkerState]bool)
		}
		seen[event.Worker][event.State] = true
	}
	for _, name := range []string{"exited", "forced"} {
		if !seen[name][WorkerStarting] || !seen[name][WorkerRunning] || !seen[name][WorkerStopped] {
			t.Fatalf("%s transitions = %#v", name, seen[name])
		}
	}
	if !seen["fail"][WorkerStarting] || !seen["fail"][WorkerFailed] {
		t.Fatalf("fail transitions = %#v", seen["fail"])
	}
}
