package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestMain(m *testing.M) {
	if os.Getenv("ORC_TEST_DELETE") == "1" {
		os.Exit(runRepositoryDeleteTicket())
	}
	if os.Getenv("ORC_TEST_CREATE") == "1" {
		os.Exit(runRepositoryCreateTicket())
	}
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
	if os.Getenv(testLocalTimeProcessEnv) == "1" {
		time.Local = time.FixedZone("test-local", -7*60*60)
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

type diagnosticChildExitError struct {
	detail string
	cause  error
}

func (e *diagnosticChildExitError) Error() string { return e.detail }
func (e *diagnosticChildExitError) Unwrap() error { return e.cause }

func childExitCauseForTest(t *testing.T) error {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^$")
	command.Env = append(os.Environ(), "TICKET_ORC_TEST_EXIT_CODE=23")
	err := command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("test child error=%v, want exit code 23", err)
	}
	return err
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
	return filepath.Join(configDir, ".local")
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
			ConfigPath: configPath, StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, StartupWorkers: []string{worker.Name}, Runtime: runtimeState,
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

type errorAfterErrChecks struct {
	context.Context
	calls  atomic.Int32
	failAt int32
}

func (ctx *errorAfterErrChecks) Err() error {
	if ctx.calls.Add(1) >= ctx.failAt {
		return context.Canceled
	}
	return ctx.Context.Err()
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
			ConfigPath:     configPath,
			StateDir:       worker.Config.StateDir,
			Workers:        []supervisor.RunWorker{worker},
			StartupWorkers: []string{worker.Name},
			Runtime:        runtimeState,
			Hooks:          supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }},
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
