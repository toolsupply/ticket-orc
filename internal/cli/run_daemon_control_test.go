package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestRunSupervisorLoadsControlBeforeStartupDispatch(t *testing.T) {
	for _, mode := range []state.DaemonControlMode{state.DaemonPaused, state.DaemonAborted} {
		t.Run(string(mode), func(t *testing.T) {
			stateDir := t.TempDir()
			controlStore := state.NewDaemonControlStore(stateDir)
			if _, err := controlStore.PauseDaemon(context.Background()); err != nil {
				t.Fatal(err)
			}
			if mode == state.DaemonAborted {
				if _, err := controlStore.AbortDaemon(context.Background()); err != nil {
					t.Fatal(err)
				}
			}

			var starts atomic.Int32
			worker := supervisorTestWorker("coder", stateDir)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := runSupervisor(ctx, RunConfig{
				StateDir: stateDir,
				Workers:  []supervisor.RunWorker{worker},
				Runtime:  NewRuntimeState([]supervisor.RunWorker{worker}),
				startChild: func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
					starts.Add(1)
					return nil, errors.New("unexpected child start")
				},
			}, "unused", io.Discard, io.Discard)
			if err != nil {
				t.Fatalf("runSupervisor = %v", err)
			}
			if got := starts.Load(); got != 0 {
				t.Fatalf("started %d managed workers while mode=%s", got, mode)
			}
		})
	}
}

func TestRunSupervisorRejectsMalformedControlBeforeDispatch(t *testing.T) {
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "daemon-control.json"), []byte(`{"version":9,"mode":"running"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var starts atomic.Int32
	worker := supervisorTestWorker("coder", stateDir)
	err := runSupervisor(context.Background(), RunConfig{
		StateDir: stateDir,
		Workers:  []supervisor.RunWorker{worker},
		Runtime:  NewRuntimeState([]supervisor.RunWorker{worker}),
		startChild: func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
			starts.Add(1)
			return nil, errors.New("unexpected child start")
		},
	}, "unused", io.Discard, io.Discard)
	if !errors.Is(err, state.ErrMalformed) {
		t.Fatalf("malformed startup control error = %v", err)
	}
	if got := starts.Load(); got != 0 {
		t.Fatalf("started %d managed workers with malformed control state", got)
	}
}

func TestDispatchGatePauseWaitsForInFlightDispatch(t *testing.T) {
	store := state.NewDaemonControlStore(t.TempDir())
	gate := newDispatchGate(store, state.DaemonRunning)
	entered := make(chan struct{})
	release := make(chan struct{})
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		allowed, err := gate.WithDispatch(func() error {
			close(entered)
			<-release
			return nil
		})
		if !allowed || err != nil {
			t.Errorf("in-flight dispatch allowed=%t err=%v", allowed, err)
		}
	}()
	<-entered

	paused := make(chan error, 1)
	go func() {
		mode, _, err := gate.Pause(context.Background())
		if err == nil && mode.Mode != state.DaemonPaused {
			err = fmt.Errorf("pause mode = %q", mode.Mode)
		}
		paused <- err
	}()
	gate.mu.Lock()
	for !gate.changing {
		gate.changed.Wait()
	}
	gate.mu.Unlock()
	select {
	case err := <-paused:
		t.Fatalf("pause returned before the in-flight dispatch completed: %v", err)
	default:
	}
	close(release)
	<-dispatchDone
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	called := false
	allowed, err := gate.WithDispatch(func() error { called = true; return nil })
	if err != nil || allowed || called {
		t.Fatalf("dispatch after successful pause allowed=%t called=%t err=%v", allowed, called, err)
	}
	mode, _, err := gate.Resume(context.Background())
	if err != nil || mode.Mode != state.DaemonRunning {
		t.Fatalf("resume mode=%#v err=%v", mode, err)
	}
}

func TestDispatchGateRepeatedTransitionsReportNoop(t *testing.T) {
	gate := newDispatchGate(state.NewDaemonControlStore(t.TempDir()), state.DaemonRunning)
	mode, applied, err := gate.Pause(context.Background())
	if err != nil || !applied || mode.Mode != state.DaemonPaused {
		t.Fatalf("first pause mode=%#v applied=%t err=%v", mode, applied, err)
	}
	mode, applied, err = gate.Pause(context.Background())
	if err != nil || applied || mode.Mode != state.DaemonPaused {
		t.Fatalf("repeated pause mode=%#v applied=%t err=%v", mode, applied, err)
	}
	mode, applied, err = gate.Abort(context.Background())
	if err != nil || !applied || mode.Mode != state.DaemonAborted {
		t.Fatalf("abort mode=%#v applied=%t err=%v", mode, applied, err)
	}
	mode, applied, err = gate.Abort(context.Background())
	if err != nil || applied || mode.Mode != state.DaemonAborted {
		t.Fatalf("repeated abort mode=%#v applied=%t err=%v", mode, applied, err)
	}
	mode, applied, err = gate.Resume(context.Background())
	if err != nil || !applied || mode.Mode != state.DaemonRunning {
		t.Fatalf("resume mode=%#v applied=%t err=%v", mode, applied, err)
	}
	mode, applied, err = gate.Resume(context.Background())
	if err != nil || applied || mode.Mode != state.DaemonRunning {
		t.Fatalf("repeated resume mode=%#v applied=%t err=%v", mode, applied, err)
	}
}

func TestDispatchGateDoesNotChangeModeWhenPersistenceFails(t *testing.T) {
	stateDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(stateDir, "daemon-control.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	gate := newDispatchGate(state.NewDaemonControlStore(stateDir), state.DaemonRunning)
	if _, _, err := gate.Pause(context.Background()); err == nil {
		t.Fatal("pause succeeded while control state path was not a file")
	}
	if got := gate.Mode(); got != state.DaemonRunning {
		t.Fatalf("failed pause changed gate mode to %q", got)
	}
	called := false
	allowed, err := gate.WithDispatch(func() error { called = true; return nil })
	if err != nil || !allowed || !called {
		t.Fatalf("failed pause unexpectedly closed dispatch: allowed=%t called=%t err=%v", allowed, called, err)
	}
}

func TestDispatchGateResumePersistenceFailureKeepsDispatchInhibited(t *testing.T) {
	for _, initial := range []state.DaemonControlMode{state.DaemonPaused, state.DaemonAborted} {
		t.Run(string(initial), func(t *testing.T) {
			stateDir := t.TempDir()
			store := state.NewDaemonControlStore(stateDir)
			var persisted state.DaemonControlState
			var err error
			if initial == state.DaemonPaused {
				persisted, err = store.PauseDaemon(context.Background())
			} else {
				persisted, err = store.AbortDaemon(context.Background())
			}
			if err != nil {
				t.Fatal(err)
			}
			gate := newDispatchGate(store, persisted.Mode)
			controlPath := filepath.Join(stateDir, "daemon-control.json")
			if err := os.Remove(controlPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(controlPath, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, _, err := gate.Resume(context.Background()); err == nil {
				t.Fatal("resume succeeded when durable control state could not be written")
			}
			if got := gate.Mode(); got != initial {
				t.Fatalf("failed resume opened dispatch mode %q", got)
			}
			called := false
			allowed, err := gate.WithDispatch(func() error { called = true; return nil })
			if err != nil || allowed || called {
				t.Fatalf("failed resume opened dispatch: allowed=%t called=%t err=%v", allowed, called, err)
			}
		})
	}
}

func TestDaemonControlCallbacksPublishNamedEventsAndMode(t *testing.T) {
	stateDir := t.TempDir()
	store := state.NewDaemonControlStore(stateDir)
	gate := newDispatchGate(store, state.DaemonRunning)
	dirty := newSteerDirtySet()
	config := RunConfig{StateDir: stateDir, Runtime: NewRuntimeState(nil), dispatchGate: gate, steerDirty: dirty, control: &daemon.Control{}}
	manager := newWorkerManager(context.Background(), config, "unused", io.Discard, io.Discard, nil)
	var events []supervisor.RuntimeEvent
	wireSupervisorControlCallbacks(&config, manager, func(event supervisor.RuntimeEvent) { events = append(events, event) }, nil, nil)
	if config.control.DaemonMode() != "running" {
		t.Fatalf("initial control mode = %q", config.control.DaemonMode())
	}
	if _, err := config.control.PauseDaemon(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := config.control.ResumeDaemon(context.Background()); err != nil {
		t.Fatal(err)
	}
	if resumed := dirty.Drain(); !resumed.all {
		t.Fatalf("daemon resume dirty snapshot=%#v, want full reconciliation", resumed)
	}
	result, err := config.control.AbortDaemon(context.Background())
	if err != nil || result.Mode != "aborted" || !result.Applied {
		t.Fatalf("abort result=%#v err=%v", result, err)
	}
	want := []string{"daemon.paused", "daemon.resumed", "daemon.aborted"}
	if len(events) != len(want) {
		t.Fatalf("events = %#v", events)
	}
	for i, event := range events {
		if event.Type != want[i] {
			t.Fatalf("event[%d].Type = %q, want %q", i, event.Type, want[i])
		}
	}
	if config.control.DaemonMode() != "aborted" {
		t.Fatalf("final control mode = %q", config.control.DaemonMode())
	}
}

func TestAbortTargetsTrySteerBeforeManagedConsumesRequestDeadline(t *testing.T) {
	stateDir := t.TempDir()
	store := state.NewDaemonControlStore(stateDir)
	gate := newDispatchGate(store, state.DaemonRunning)
	worker := supervisorTestWorker("coder", stateDir)
	manager := newWorkerManager(context.Background(), RunConfig{
		StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, dispatchGate: gate,
	}, "unused", io.Discard, io.Discard, nil)
	manager.lifecycle.TrackChild(worker.Name, &runChild{worker: worker, done: make(chan error, 1), waited: make(chan struct{})})
	if _, err := manager.beginAbort(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	managedCalled := false
	results := collectDaemonAbortTargets(ctx,
		func(ctx context.Context) ([]steerAbortResult, error) {
			if gate.Mode() != state.DaemonAborted {
				t.Fatalf("steer abort attempted before durable mode: %q", gate.Mode())
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("steer abort received an already-expired context: %v", err)
			}
			<-ctx.Done() // Model a bounded steer attempt consuming the request deadline.
			return []steerAbortResult{{RepositoryID: "repo-id", Actor: "reviewer", Outcome: "request_failed", Code: "transport_unavailable"}}, nil
		},
		func(ctx context.Context) []supervisor.TerminationResult {
			managedCalled = true
			return manager.terminateActive(ctx)
		},
	)
	if !managedCalled {
		t.Fatal("managed termination was skipped after the steer attempt consumed its deadline")
	}
	if len(results) != 2 || results[0].Kind != "steer" || results[0].Outcome != "request_failed" || results[1].Kind != "managed" || results[1].Outcome != "termination_failed" {
		t.Fatalf("ordered abort targets = %#v", results)
	}
	if got := gate.Mode(); got != state.DaemonAborted {
		t.Fatalf("abort mode after bounded attempts = %q", got)
	}
}

func TestManagedAbortPersistenceFailurePreventsTermination(t *testing.T) {
	stateDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(stateDir, "daemon-control.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	worker := supervisorTestWorker("coder", stateDir)
	gate := newDispatchGate(state.NewDaemonControlStore(stateDir), state.DaemonRunning)
	manager := newWorkerManager(context.Background(), RunConfig{
		StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, dispatchGate: gate,
	}, "unused", io.Discard, io.Discard, nil)
	child := &runChild{worker: worker, done: make(chan error, 1), waited: make(chan struct{})}
	manager.lifecycle.TrackChild(worker.Name, child)

	results, _, err := manager.abortActive(context.Background())
	if err == nil || results != nil {
		t.Fatalf("abort results=%#v err=%v, want persistence failure before termination", results, err)
	}
	if got := gate.Mode(); got != state.DaemonRunning {
		t.Fatalf("failed abort changed dispatch mode to %q", got)
	}
	if current, ok := manager.lifecycle.Child(worker.Name); !ok || current != child {
		t.Fatalf("child ownership changed after failed persistence: current=%p present=%t", current, ok)
	}
	called := false
	allowed, dispatchErr := gate.WithDispatch(func() error { called = true; return nil })
	if dispatchErr != nil || !allowed || !called {
		t.Fatalf("failed persistence unexpectedly closed dispatch: allowed=%t called=%t err=%v", allowed, called, dispatchErr)
	}
}

func TestManagedAbortKeepsDurableAbortedModeAfterTerminationFailure(t *testing.T) {
	stateDir := t.TempDir()
	store := state.NewDaemonControlStore(stateDir)
	gate := newDispatchGate(store, state.DaemonRunning)
	worker := supervisorTestWorker("coder", stateDir)
	manager := newWorkerManager(context.Background(), RunConfig{
		StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, dispatchGate: gate,
	}, "unused", io.Discard, io.Discard, nil)
	// A tracked child without a process is an active but unavailable termination
	// target. The lifecycle helper reports the partial failure and retains it.
	child := &runChild{worker: worker, done: make(chan error, 1), waited: make(chan struct{})}
	manager.lifecycle.TrackChild(worker.Name, child)

	results, applied, err := manager.abortActive(context.Background())
	if err != nil {
		t.Fatalf("abort returned orchestration error after durable mode: %v", err)
	}
	if !applied {
		t.Fatal("first abort did not report the durable mode transition")
	}
	if len(results) != 1 || results[0].Worker != worker.Name || results[0].Outcome != "termination_failed" {
		t.Fatalf("abort results = %#v, want one termination_failed result", results)
	}
	if got := gate.Mode(); got != state.DaemonAborted {
		t.Fatalf("gate mode after termination failure = %q, want aborted", got)
	}
	durable, err := store.DaemonControl(context.Background())
	if err != nil || durable.Mode != state.DaemonAborted {
		t.Fatalf("durable control state=%#v err=%v, want aborted", durable, err)
	}
	called := false
	allowed, dispatchErr := gate.WithDispatch(func() error { called = true; return nil })
	if dispatchErr != nil || allowed || called {
		t.Fatalf("dispatch remained open after abort: allowed=%t called=%t err=%v", allowed, called, dispatchErr)
	}
	if current, ok := manager.lifecycle.Child(worker.Name); !ok || current != child {
		t.Fatalf("failed termination did not retain child ownership: current=%p present=%t", current, ok)
	}
}

func TestWorkerStartIsRejectedWhileDaemonPaused(t *testing.T) {
	stateDir := t.TempDir()
	store := state.NewDaemonControlStore(stateDir)
	paused, err := store.PauseDaemon(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var starts atomic.Int32
	worker := supervisorTestWorker("coder", stateDir)
	config := RunConfig{StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, dispatchGate: newDispatchGate(store, paused.Mode)}
	manager := newWorkerManager(context.Background(), config, "unused", io.Discard, io.Discard, nil)
	manager.starter = func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
		starts.Add(1)
		return nil, nil
	}
	result, err := manager.start(context.Background(), worker.Name)
	var lifecycleErr *supervisor.LifecycleError
	if !errors.As(err, &lifecycleErr) || lifecycleErr.Code != "daemon_paused" || result.Applied {
		t.Fatalf("paused worker start result=%#v err=%v", result, err)
	}
	if starts.Load() != 0 {
		t.Fatalf("start child called %d times while paused", starts.Load())
	}
	for _, test := range []struct {
		name string
		call func() error
	}{
		{name: "worker resume", call: func() error { _, err := manager.resume(context.Background(), worker.Name); return err }},
		{name: "worker restart", call: func() error { _, err := manager.restart(context.Background(), worker.Name); return err }},
		{name: "group start", call: func() error { _, err := manager.group(context.Background(), "all", true); return err }},
		{name: "doctor", call: func() error { _, err := manager.doctor(context.Background()); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			var got *supervisor.LifecycleError
			if !errors.As(err, &got) || got.Code != "daemon_paused" {
				t.Fatalf("operation error = %v, want daemon_paused", err)
			}
		})
	}
	if starts.Load() != 0 {
		t.Fatalf("blocked explicit operations started %d workers while paused", starts.Load())
	}
}

func TestManagedReconcileStartsStoppedWorkerAfterResume(t *testing.T) {
	for _, initial := range []state.DaemonControlMode{state.DaemonPaused, state.DaemonAborted} {
		t.Run(string(initial), func(t *testing.T) {
			stateDir := t.TempDir()
			store := state.NewDaemonControlStore(stateDir)
			var persisted state.DaemonControlState
			var err error
			if initial == state.DaemonPaused {
				persisted, err = store.PauseDaemon(context.Background())
			} else {
				persisted, err = store.AbortDaemon(context.Background())
			}
			if err != nil {
				t.Fatal(err)
			}
			gate := newDispatchGate(store, persisted.Mode)
			worker := supervisorTestWorker("coder", stateDir)
			var starts atomic.Int32
			manager := newWorkerManager(context.Background(), RunConfig{
				StateDir: stateDir, Workers: []supervisor.RunWorker{worker}, dispatchGate: gate,
			}, "unused", io.Discard, io.Discard, nil)
			childDone := make(chan error, 1)
			manager.starter = func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
				starts.Add(1)
				ready := make(chan struct{})
				close(ready)
				return &runChild{ready: ready, done: childDone, waited: make(chan struct{})}, nil
			}
			if err := manager.reconcileDispatch(context.Background()); err != nil || starts.Load() != 0 {
				t.Fatalf("inhibited reconciliation err=%v starts=%d", err, starts.Load())
			}
			if _, _, err := gate.Resume(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := manager.reconcileDispatch(context.Background()); err != nil || starts.Load() != 1 {
				t.Fatalf("resumed reconciliation err=%v starts=%d", err, starts.Load())
			}
			if err := manager.reconcileDispatch(context.Background()); err != nil || starts.Load() != 1 {
				t.Fatalf("repeated reconciliation err=%v starts=%d", err, starts.Load())
			}
			close(childDone)
		})
	}
}

func TestReloadPreservesPausedAndAbortedModesWithoutStartingNewWork(t *testing.T) {
	for _, mode := range []state.DaemonControlMode{state.DaemonPaused, state.DaemonAborted} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			stateDir := filepath.Join(root, "state")
			store := state.NewDaemonControlStore(stateDir)
			var persisted state.DaemonControlState
			var err error
			if mode == state.DaemonPaused {
				persisted, err = store.PauseDaemon(context.Background())
			} else {
				persisted, err = store.AbortDaemon(context.Background())
			}
			if err != nil {
				t.Fatal(err)
			}
			gate := newDispatchGate(store, persisted.Mode)
			configPath := filepath.Join(root, "config.json")
			writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"local_dir":%q,"workers":{"added":{"role":"coder","actor":"new-worker"}}}`, stateDir))
			var starts atomic.Int32
			manager := newWorkerManager(context.Background(), RunConfig{
				ConfigPath: configPath, StateDir: stateDir, dispatchGate: gate, Runtime: NewRuntimeState(nil),
				startChild: func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
					starts.Add(1)
					return nil, errors.New("unexpected start during reload")
				},
			}, "unused", io.Discard, io.Discard, nil)
			result, err := manager.reload(context.Background())
			if err != nil || !result.Applied {
				t.Fatalf("reload=%#v err=%v", result, err)
			}
			if starts.Load() != 0 {
				t.Fatalf("reload started work %d times while %s", starts.Load(), mode)
			}
			if got := gate.Mode(); got != mode {
				t.Fatalf("reload changed dispatch mode to %q, want %q", got, mode)
			}
			durable, err := store.DaemonControl(context.Background())
			if err != nil || durable.Mode != mode {
				t.Fatalf("reload durable mode=%#v err=%v, want %q", durable, err, mode)
			}
			if got := manager.runtime.Snapshot()["added"].State; got != supervisor.WorkerStopped {
				t.Fatalf("new worker state after reload=%q, want stopped", got)
			}
		})
	}
}
