package supervisor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
	watching := make(chan struct{})
	releaseWatchers := make(chan struct{})
	reloaded := make(chan error, 1)
	go func() {
		_, err := manager.ApplyReload(context.Background(), map[string]lifecycleTestWorker{
			"one": {name: "one"}, "two": {name: "two"},
		}, ReloadHooks[lifecycleTestWorker]{
			ReplaceWatchers: func(context.Context) error {
				watchers.Store(true)
				close(watching)
				<-releaseWatchers
				return nil
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
	<-watching
	started := make(chan error, 1)
	go func() {
		_, err := manager.Start(context.Background(), "two")
		started <- err
	}()
	select {
	case err := <-started:
		t.Fatalf("start crossed reload commit boundary: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseWatchers)
	if err := <-reloaded; err != nil {
		t.Fatalf("apply reload: %v", err)
	}
	if err := <-started; err != nil {
		t.Fatalf("start newly configured worker after reload: %v", err)
	}
	if _, ok := manager.Worker("two"); !ok || !watchers.Load() || !committed.Load() {
		t.Fatal("successful reload did not commit workers and watcher state")
	}
}
