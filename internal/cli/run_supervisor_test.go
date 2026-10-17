package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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

func TestManagedSupervisorControlAndStateEventsUseConfiguredRole(t *testing.T) {
	stateDir := t.TempDir()
	worker := supervisorTestWorker("backend-worker", stateDir)
	worker.Config.RoleName = "backend"
	worker.Config.TicketQueue = "open"
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	config := RunConfig{
		StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtime,
		dispatchGate: newDispatchGate(state.NewDaemonControlStore(stateDir), state.DaemonRunning), control: &daemon.Control{},
	}
	var published []daemon.Event
	emit, setPublisher := newSupervisorEventPublisher(config)
	setPublisher(func(event daemon.Event) { published = append(published, event) })
	manager := newWorkerManager(context.Background(), config, "unused", io.Discard, io.Discard, newSupervisorWorkerTransition(config, emit))
	wireSupervisorControlCallbacks(&config, manager, emit, nil, nil)
	result, err := config.control.PauseWorker(context.Background(), worker.Name)
	if err != nil || !result.Applied || result.State != string(WorkerPaused) {
		t.Fatalf("pause result=%#v err=%v", result, err)
	}
	if len(published) != 3 {
		t.Fatalf("published events=%#v, want control intent, state, and result", published)
	}
	wantTypes := []string{"worker.control", "worker.state", "worker.control"}
	for index, event := range published {
		if event.Type != wantTypes[index] || event.Role != "backend" {
			t.Errorf("published event[%d]=%#v, want type %s and role backend", index, event, wantTypes[index])
		}
	}
	if published[0].Code != "intent" || published[0].Phase != "pause" || published[2].Code != "mutation_applied" || published[2].Phase != "pause" {
		t.Fatalf("control events did not include intent and result: %#v", published)
	}
}

func TestManagedSupervisorFailureEventUsesEffectiveConfiguredRoleAcrossPendingReload(t *testing.T) {
	stateDir := t.TempDir()
	effective := supervisorTestWorker("worker", stateDir)
	effective.Config.RoleName = "backend"
	effective.Config.TicketQueue = "open"
	desired := effective
	desired.Config.RoleName = "backend-v2"
	runtime := NewRuntimeState([]supervisor.RunWorker{effective})
	runtime.SetConfiguredWorkers([]supervisor.RunWorker{desired})
	config := RunConfig{StateDir: stateDir, Workers: []supervisor.RunWorker{desired}, Runtime: runtime}
	var published []daemon.Event
	emit, setPublisher := newSupervisorEventPublisher(config)
	setPublisher(func(event daemon.Event) { published = append(published, event) })
	manager := newWorkerManager(context.Background(), config, "unused", io.Discard, io.Discard, nil)
	failureSink := wireSupervisorRuntimeEvents(config, manager, emit)
	failureSink(effective.Name, WorkerFailed, &supervisor.WorkerFailure{Classification: "startup_failure", Phase: "worker readiness"})
	if len(published) != 1 || published[0].Type != "worker.failure" || published[0].Role != "backend" || published[0].Code != "startup_failure" {
		t.Fatalf("published pending-reload failure event=%#v, want effective role backend", published)
	}
}

type supervisorTestSignalWriter struct {
	dst   io.Writer
	match string
	seen  chan struct{}
	once  sync.Once
}

func (writer *supervisorTestSignalWriter) Write(data []byte) (int, error) {
	n, err := writer.dst.Write(data)
	if err == nil && strings.Contains(string(data), writer.match) {
		writer.once.Do(func() { close(writer.seen) })
	}
	return n, err
}

func awaitSupervisorSignal(t *testing.T, signal <-chan struct{}, milestone string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", milestone)
	}
}

type supervisorTestContext struct {
	context.Context
	doneCalls chan struct{}
}

func (ctx *supervisorTestContext) Done() <-chan struct{} {
	select {
	case ctx.doneCalls <- struct{}{}:
	default:
	}
	return ctx.Context.Done()
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
			startupSignal := make(chan struct{})
			diagnosticSignal := make(chan struct{})
			stderr := &supervisorTestSignalWriter{
				dst:   &lockedWriter{dst: &output, mu: &outputMu},
				match: "Managed workers:", seen: startupSignal,
			}
			stderr = &supervisorTestSignalWriter{dst: stderr, match: test.want, seen: diagnosticSignal}
			runtimeState := &supervisor.RuntimeState[supervisor.RunWorker]{}
			stateDir := t.TempDir()
			done := startSupervisorTestRun(t, cancel, func() error {
				return runSupervisor(ctx, RunConfig{ConfigPath: "config.json", StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, startChild: starter}, os.Args[0], io.Discard, stderr)
			})
			awaitSupervisorSignal(t, startupSignal, "managed worker startup")
			childDone <- test.err
			awaitSupervisorSignal(t, diagnosticSignal, "worker exit diagnostic")
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
	startupSignal := make(chan struct{})
	diagnosticSignal := make(chan struct{})
	stderr := &supervisorTestSignalWriter{
		dst: &supervisorTestSignalWriter{
			dst:   &lockedWriter{dst: &output, mu: &outputMu},
			match: "Managed workers:", seen: startupSignal,
		},
		match: "ticket_code=ticket_not_ready", seen: diagnosticSignal,
	}
	runtimeState := &supervisor.RuntimeState[supervisor.RunWorker]{}
	stateDir := t.TempDir()
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{ConfigPath: "config.json", StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, startChild: starter}, os.Args[0], io.Discard, stderr)
	})
	awaitSupervisorSignal(t, startupSignal, "managed worker startup")
	childDone <- errors.New("ticket JSONL failed: bearer-token=secret prompt=opaque")
	awaitSupervisorSignal(t, diagnosticSignal, "child failure diagnostic")
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

func TestRunSupervisorDefaultOutputUsesServiceSummaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stateDir := t.TempDir()
	var stderr bytes.Buffer
	var outputMu sync.Mutex
	ready := make(chan struct{})
	stderrWriter := &supervisorTestSignalWriter{
		dst:   &lockedWriter{dst: &stderr, mu: &outputMu},
		match: "[ticket-orc] endpoint:", seen: ready,
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{ConfigPath: "/private/repository/config.json", StateDir: stateDir}, os.Args[0], io.Discard, stderrWriter)
	})
	awaitSupervisorSignal(t, ready, "service startup output")
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
	ready := make(chan struct{})
	stderrWriter := &supervisorTestSignalWriter{
		dst:   &lockedWriter{dst: &stderr, mu: &outputMu},
		match: "Managed workers:", seen: ready,
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{ConfigPath: configPath, StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, control: control}, os.Args[0], io.Discard, stderrWriter)
	})
	awaitSupervisorSignal(t, ready, "managed worker startup")
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

func TestRunSupervisorIdleIsPersistentAndReleasesDaemonLock(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	ready := make(chan struct{})
	stderrWriter := &supervisorTestSignalWriter{
		dst:   &stderr,
		match: "[ticket-orc] endpoint:", seen: ready,
	}
	done := startSupervisorTestRun(t, cancel, func() error {
		return runSupervisor(ctx, RunConfig{StateDir: stateDir}, "unused", &stdout, stderrWriter)
	})
	awaitSupervisorSignal(t, ready, "idle supervisor startup")
	select {
	case err := <-done:
		t.Fatalf("idle supervisor exited early: %v", err)
	default:
	}
	lock, err := state.TryAcquireLock(context.Background(), filepath.Join(stateDir, "run"))
	if err == nil {
		_ = lock.Release()
		t.Fatal("idle supervisor did not retain daemon lock after startup")
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
	releasedLock, err := state.AcquireLock(context.Background(), filepath.Join(stateDir, "run"), time.Second)
	if err != nil {
		t.Fatalf("daemon lock was not released: %v", err)
	}
	if err := releasedLock.Release(); err != nil {
		t.Fatalf("release daemon lock: %v", err)
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
	diagnosticSignal := make(chan struct{})
	diagnosticWriter := &supervisorTestSignalWriter{
		dst:   &lockedWriter{dst: &diagnostics, mu: &diagnosticsMu},
		match: "containment conflict", seen: diagnosticSignal,
	}
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
		}, Hooks: supervisor.SupervisorHooks{WorkerState: func(event supervisor.WorkerTransition) { hooks <- event }}}, os.Args[0], io.Discard, diagnosticWriter)
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
	// for the diagnostic publication event before shutting down.
	awaitSupervisorSignal(t, diagnosticSignal, "containment conflict diagnostic")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	diagnosticsMu.Lock()
	text := diagnostics.String()
	diagnosticsMu.Unlock()
	if !strings.Contains(text, "containment conflict") || strings.Contains(text, "secret") {
		t.Fatalf("conflict diagnostic=%q", text)
	}
}

func TestRunSupervisorExecutableIdentityCancellationAndNoRespawn(t *testing.T) {
	childDir := t.TempDir()
	instanceDir := t.TempDir()
	configPath := filepath.Join(instanceDir, "config.json")
	stateDir := filepath.Join(instanceDir, ".local")
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
	makeConfig := func(stateDir string, serverResult **daemon.Server) RunConfig {
		key, err := daemon.GenerateEndpointKey()
		if err != nil {
			t.Fatal(err)
		}
		return RunConfig{
			StateDir:    stateDir,
			EndpointKey: key,
			startDaemon: func(context.Context, *supervisor.RuntimeState[supervisor.RunWorker], []supervisor.RunWorker) (*daemon.Server, error) {
				server, err := daemon.NewServer(daemon.Config{StateDir: stateDir, EndpointKey: key})
				*serverResult = server
				return server, err
			},
		}
	}
	var firstServer, secondServer *daemon.Server
	firstConfig := makeConfig(firstState, &firstServer)
	secondConfig := makeConfig(secondState, &secondServer)
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
	waitForTestDaemonStopped(t, firstServer)
	waitForTestDaemonStopped(t, secondServer)
}

func TestRunSupervisorOwnsHTTPDaemonLifecycle(t *testing.T) {
	stateDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var server *daemon.Server
	config := RunConfig{
		StateDir: stateDir,
		startDaemon: func(_ context.Context, _ *supervisor.RuntimeState[supervisor.RunWorker], _ []supervisor.RunWorker) (*daemon.Server, error) {
			var err error
			server, err = daemon.NewServer(daemon.Config{EndpointKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", StateDir: stateDir, Version: "test"})
			return server, err
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
	waitForTestDaemonStopped(t, server)
}

func TestRunSupervisorHTTPShutdownReleasesLockAndExits(t *testing.T) {
	stateDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control := &daemon.Control{}
	var server *daemon.Server
	config := RunConfig{StateDir: stateDir, control: control, startDaemon: func(_ context.Context, _ *supervisor.RuntimeState[supervisor.RunWorker], _ []supervisor.RunWorker) (*daemon.Server, error) {
		var err error
		server, err = daemon.NewServer(daemon.Config{EndpointKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", StateDir: stateDir, Version: "test", Control: control})
		return server, err
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
	waitForTestDaemonStopped(t, server)
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	lock, err := state.AcquireLock(secondCtx, filepath.Join(stateDir, "run"), time.Second)
	if err != nil {
		t.Fatalf("lock after shutdown: %v", err)
	}
	_ = lock.Release()
}

func waitForTestDaemonStopped(t *testing.T, server *daemon.Server) {
	t.Helper()
	if server == nil {
		t.Fatal("test daemon was not constructed")
	}
	select {
	case err, ok := <-server.Done():
		if ok && err != nil {
			t.Fatalf("test daemon serve loop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("test daemon serve loop did not stop")
	}
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
