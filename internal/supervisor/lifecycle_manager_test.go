package supervisor

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/testutil"
)

func TestUnconfirmedFailedWorkspaceBlocksOverlappingLaunchButNotIndependentWorker(t *testing.T) {
	root := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	starts := 0
	manager := NewLifecycleManager([]lifecycleTestWorker{{"failed"}, {"overlap"}, {"independent"}}, LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
		Name:        func(w lifecycleTestWorker) string { return w.name },
		ChildWorker: func(*lifecycleTestChild) lifecycleTestWorker { return lifecycleTestWorker{"failed"} },
		Workspace: func(w lifecycleTestWorker) string {
			if w.name == "independent" {
				return filepath.Join(root, "independent")
			}
			if w.name == "overlap" {
				return filepath.Join(root, "affected", "subdirectory")
			}
			return filepath.Join(root, "affected")
		},
		StartChild: func(context.Context, lifecycleTestWorker) (*lifecycleTestChild, error) {
			starts++
			return &lifecycleTestChild{}, nil
		},
		ClassifyExit: func(*lifecycleTestChild, error) ExitOutcome {
			close(entered)
			<-release
			return ExitOutcome{State: WorkerFailed, BlockWorkspace: true}
		},
	})
	if _, err := manager.Start(t.Context(), "failed"); err != nil {
		t.Fatal(err)
	}
	child, _ := manager.Child("failed")
	exited := make(chan struct{})
	go func() { defer close(exited); manager.ObserveExit("failed", child, errors.New("failed"), false) }()
	t.Cleanup(func() { unblock(); awaitFailedWorkspace(t, exited, "exit reconciliation") })
	awaitFailedWorkspace(t, entered, "classification barrier")
	started := make(chan error, 1)
	startDone := make(chan struct{})
	go func() { defer close(startDone); _, err := manager.Start(t.Context(), "overlap"); started <- err }()
	t.Cleanup(func() { unblock(); awaitFailedWorkspace(t, startDone, "concurrent start") })
	unblock()
	awaitFailedWorkspace(t, exited, "exit reconciliation")
	awaitFailedWorkspace(t, startDone, "serialized start")
	var lifecycleErr *LifecycleError
	if err := <-started; !errors.As(err, &lifecycleErr) || lifecycleErr.Code != "workspace_cleanup_unconfirmed" {
		t.Fatalf("overlap start: %v", err)
	}
	if _, err := manager.Restart(t.Context(), "failed"); err == nil {
		t.Fatal("manual restart bypassed uncertain cleanup")
	}
	if _, err := manager.Start(t.Context(), "independent"); err != nil {
		t.Fatalf("independent worker blocked: %v", err)
	}
	if starts != 2 {
		t.Fatalf("starts=%d, want original plus independent", starts)
	}
}

func TestLifecycleManagerFailedWorkerRequiresManualStart(t *testing.T) {
	for _, action := range []string{"start", "restart"} {
		t.Run(action, func(t *testing.T) {
			starts := 0
			var lastState WorkerState
			cause := errors.New("unexpected execution termination")
			manager := newLifecycleTestManager(LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
				StartChild: func(context.Context, lifecycleTestWorker) (*lifecycleTestChild, error) {
					starts++
					child := &lifecycleTestChild{ready: make(chan struct{}), waited: make(chan struct{})}
					close(child.ready)
					close(child.waited)
					return child, nil
				},
				ClassifyExit: func(*lifecycleTestChild, error) ExitOutcome {
					return ExitOutcome{State: WorkerFailed, Failure: &WorkerFailure{Classification: "harness_process_failed"}, TransitionError: cause}
				},
				Transition: func(_ string, state WorkerState, _ error) { lastState = state },
			})
			if _, err := manager.Start(t.Context(), "one"); err != nil {
				t.Fatal(err)
			}
			child, _ := manager.Child("one")
			observed := manager.ObserveExit("one", child, cause, false)
			if !observed.Current || observed.Outcome.State != WorkerFailed || lastState != WorkerFailed || starts != 1 {
				t.Fatalf("failure was not retained: %+v state=%s starts=%d", observed, lastState, starts)
			}
			if _, running := manager.Child("one"); running {
				t.Fatal("failed worker was automatically replaced")
			}
			if manager.ObserveExit("one", child, cause, false).Current || starts != 1 {
				t.Fatal("duplicate failure observation started a replacement")
			}
			manual := manager.Start
			if action == "restart" {
				manual = manager.Restart
			}
			if _, err := manual(t.Context(), "one"); err != nil {
				t.Fatal(err)
			}
			current, running := manager.Child("one")
			if !running || current == child || starts != 2 || lastState != WorkerRunning {
				t.Fatalf("ordinary manual %s did not start a new worker: starts=%d state=%s", action, starts, lastState)
			}
		})
	}
}

type lifecycleTestWorker struct{ name string }

type lifecycleTestChild struct {
	ready  chan struct{}
	waited chan struct{}
}

func newLifecycleTestManager(hooks LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]) *LifecycleManager[lifecycleTestWorker, *lifecycleTestChild] {
	hooks.Name = func(worker lifecycleTestWorker) string { return worker.name }
	hooks.ChildWorker = func(*lifecycleTestChild) lifecycleTestWorker { return lifecycleTestWorker{name: "one"} }
	if hooks.StartChild == nil {
		hooks.StartChild = func(context.Context, lifecycleTestWorker) (*lifecycleTestChild, error) {
			return &lifecycleTestChild{ready: make(chan struct{}), waited: make(chan struct{})}, nil
		}
	}
	hooks.ProcessAvailable = func(*lifecycleTestChild) bool { return true }
	hooks.RequestStop = func(*lifecycleTestChild) error { return nil }
	hooks.ForceStop = func(*lifecycleTestChild) error { return nil }
	hooks.Waited = func(child *lifecycleTestChild) <-chan struct{} { return child.waited }
	hooks.Ready = func(child *lifecycleTestChild) <-chan struct{} { return child.ready }
	hooks.ReadyObserved = func(child *lifecycleTestChild) bool {
		select {
		case <-child.ready:
			return true
		default:
			return false
		}
	}
	return NewLifecycleManager([]lifecycleTestWorker{{name: "one"}}, hooks)
}

func TestLifecycleManagerExitIsClassifiedExactlyOnce(t *testing.T) {
	var classified, completed, transitioned atomic.Int32
	manager := newLifecycleTestManager(LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
		ClassifyExit: func(*lifecycleTestChild, error) ExitOutcome {
			classified.Add(1)
			return ExitOutcome{State: WorkerFailed}
		},
		CompleteStartup: func(*lifecycleTestChild, error, bool, *WorkerFailure) { completed.Add(1) },
		Transition: func(_ string, state WorkerState, _ error) {
			if state == WorkerFailed {
				transitioned.Add(1)
			}
		},
	})
	if _, err := manager.Start(context.Background(), "one"); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	child, ok := manager.Child("one")
	if !ok || child == nil {
		t.Fatal("start did not publish managed child")
	}
	var current atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if manager.ObserveExit("one", child, nil, false).Current {
				current.Add(1)
			}
		}()
	}
	wg.Wait()
	if current.Load() != 1 {
		t.Fatalf("current exit observations = %d, want exactly one", current.Load())
	}
	if classified.Load() != 1 || completed.Load() != 1 || transitioned.Load() != 1 {
		t.Fatalf("classified=%d startup completions=%d failed transitions=%d, want 1 each", classified.Load(), completed.Load(), transitioned.Load())
	}
}

func TestLifecycleManagerStartupReconciliationConsumesStaleWaitResult(t *testing.T) {
	var startupExit, classified atomic.Int32
	manager := newLifecycleTestManager(LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
		StartupExited: func(string, *lifecycleTestChild, ExitReconciliation[lifecycleTestWorker]) error {
			startupExit.Add(1)
			return nil
		},
		ClassifyExit: func(*lifecycleTestChild, error) ExitOutcome {
			classified.Add(1)
			return ExitOutcome{State: WorkerFailed}
		},
	})
	if _, err := manager.Start(context.Background(), "one"); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	child, ok := manager.Child("one")
	if !ok || child == nil {
		t.Fatal("start did not publish managed child")
	}
	close(child.waited)
	if err := manager.VerifyStartup(context.Background(), "one", time.Second); err != nil {
		t.Fatalf("startup exit reconciliation: %v", err)
	}
	if got := manager.ObserveExit("one", child, nil, false); got.Current {
		t.Fatal("stale result was accepted after startup reconciled the child")
	}
	if startupExit.Load() != 1 || classified.Load() != 0 {
		t.Fatalf("startup exits=%d normal classifications=%d, want 1 and 0", startupExit.Load(), classified.Load())
	}
}

func TestLifecycleManagerReloadRejectsRunningRemovalWithoutMutation(t *testing.T) {
	manager := newLifecycleTestManager(LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{})
	if _, err := manager.Start(context.Background(), "one"); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	watcherCalls := atomic.Int32{}
	_, err := manager.ApplyReload(context.Background(), map[string]lifecycleTestWorker{}, ReloadHooks[lifecycleTestWorker]{
		ReplaceWatchers: func(context.Context) error { watcherCalls.Add(1); return nil },
	})
	if err == nil {
		t.Fatal("reload removing a running worker succeeded")
	}
	if watcherCalls.Load() != 0 {
		t.Fatalf("watcher replacement calls = %d, want none for rejected candidate", watcherCalls.Load())
	}
	if _, ok := manager.Worker("one"); !ok {
		t.Fatal("rejected reload changed the active worker map")
	}
	if _, ok := manager.Child("one"); !ok {
		t.Fatal("rejected reload changed the active child map")
	}
}

func TestLifecycleManagerReloadCommitsWatchersBeforeConcurrentStart(t *testing.T) {
	var committed, watchers atomic.Bool
	hooks := LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
		StartChild: func(_ context.Context, worker lifecycleTestWorker) (*lifecycleTestChild, error) {
			if worker.name == "two" && !committed.Load() {
				return nil, context.Canceled
			}
			return &lifecycleTestChild{ready: make(chan struct{}), waited: make(chan struct{})}, nil
		},
	}
	manager := newLifecycleTestManager(hooks)
	if _, err := manager.Start(context.Background(), "one"); err != nil {
		t.Fatalf("start initial worker: %v", err)
	}
	watchersGate := testutil.NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	reloaded := make(chan error, 1)
	reloadDone := make(chan struct{})
	var startDone chan struct{}
	t.Cleanup(func() {
		watchersGate.Release()
		cancel()
		for _, operation := range []struct {
			name string
			done <-chan struct{}
		}{{"reload", reloadDone}, {"start", startDone}} {
			if operation.done == nil {
				continue
			}
			select {
			case <-operation.done:
			case <-time.After(time.Second):
				t.Errorf("%s goroutine did not exit during cleanup", operation.name)
			}
		}
	})
	go func() {
		defer close(reloadDone)
		_, err := manager.ApplyReload(ctx, map[string]lifecycleTestWorker{
			"one": {name: "one"}, "two": {name: "two"},
		}, ReloadHooks[lifecycleTestWorker]{
			ReplaceWatchers: func(context.Context) error {
				watchers.Store(true)
				return watchersGate.Arrive(ctx)
			},
			Commit: func([]lifecycleTestWorker, map[string]bool) string {
				if !watchers.Load() {
					t.Error("workers committed before repository watchers")
				}
				committed.Store(true)
				return "revision-2"
			},
		})
		reloaded <- err
	}()
	watching, watchingCancel := context.WithTimeout(ctx, time.Second)
	defer watchingCancel()
	if err := watchersGate.WaitReached(watching); err != nil {
		t.Fatalf("wait for watcher replacement: %v", err)
	}
	startDone = make(chan struct{})
	started := make(chan error, 1)
	go func() {
		defer close(startDone)
		_, err := manager.Start(ctx, "two")
		started <- err
	}()
	// Start's first boundary is the global mutex, with no hook before lock
	// acquisition. Mutex waits are not durably blocked for synctest.Wait.
	// Retain this timed assertion until an observable boundary is available.
	select {
	case err := <-started:
		t.Fatalf("start crossed reload commit boundary: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	watchersGate.Release()
	if err := testutil.Receive(t, time.Second, "reload result", reloaded); err != nil {
		t.Fatalf("apply reload: %v", err)
	}
	if err := testutil.Receive(t, time.Second, "start result", started); err != nil {
		t.Fatalf("start newly configured worker after reload: %v", err)
	}
	if _, ok := manager.Worker("two"); !ok || !watchers.Load() || !committed.Load() {
		t.Fatal("successful reload did not commit workers and watcher state")
	}
}

func awaitFailedWorkspace(t *testing.T, done <-chan struct{}, milestone string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", milestone)
	}
}
