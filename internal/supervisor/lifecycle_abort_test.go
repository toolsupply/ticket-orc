package supervisor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleAbortActiveIsResumableAndLeavesWorkerPausesAlone(t *testing.T) {
	var starts, stopRequests atomic.Int32
	manager := NewLifecycleManager([]lifecycleTestWorker{{name: "alpha"}, {name: "beta"}}, LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
		Name: func(worker lifecycleTestWorker) string { return worker.name },
		StartChild: func(context.Context, lifecycleTestWorker) (*lifecycleTestChild, error) {
			starts.Add(1)
			return &lifecycleTestChild{ready: make(chan struct{}), waited: make(chan struct{})}, nil
		},
		ProcessAvailable: func(*lifecycleTestChild) bool { return true },
		RequestStop: func(child *lifecycleTestChild) error {
			stopRequests.Add(1)
			close(child.waited)
			return nil
		},
		ForceStop:   func(*lifecycleTestChild) error { t.Error("force-stop called after graceful exit"); return nil },
		Waited:      func(child *lifecycleTestChild) <-chan struct{} { return child.waited },
		StopTimeout: 20 * time.Millisecond,
	})
	if _, err := manager.Start(context.Background(), "alpha"); err != nil {
		t.Fatal(err)
	}
	manager.SetPaused("beta", true)

	results := manager.AbortActive(context.Background())
	if len(results) != 1 || results[0] != (TerminationResult{Worker: "alpha", Outcome: "terminated"}) {
		t.Fatalf("abort results = %#v", results)
	}
	if _, ok := manager.Child("alpha"); ok {
		t.Fatal("terminated child remains active")
	}
	if manager.Stopping() || manager.Paused("alpha") || !manager.Paused("beta") {
		t.Fatalf("abort changed lifecycle controls: stopping=%t alpha-paused=%t beta-paused=%t", manager.Stopping(), manager.Paused("alpha"), manager.Paused("beta"))
	}
	if _, ok := manager.Worker("alpha"); !ok {
		t.Fatal("abort removed configured worker")
	}
	if again := manager.AbortActive(context.Background()); len(again) != 0 || stopRequests.Load() != 1 {
		t.Fatalf("repeated abort results=%#v stop requests=%d", again, stopRequests.Load())
	}
	if _, err := manager.Start(context.Background(), "alpha"); err != nil {
		t.Fatalf("worker was not resumable after abort: %v", err)
	}
	if starts.Load() != 2 {
		t.Fatalf("worker starts=%d, want initial start and resumed start", starts.Load())
	}
}

func TestLifecycleAbortActiveForceStopsAndBoundsUnreapedChildren(t *testing.T) {
	var forceCalls atomic.Int32
	manager := NewLifecycleManager([]lifecycleTestWorker{{name: "one"}}, LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
		Name: func(worker lifecycleTestWorker) string { return worker.name },
		StartChild: func(context.Context, lifecycleTestWorker) (*lifecycleTestChild, error) {
			return &lifecycleTestChild{ready: make(chan struct{}), waited: make(chan struct{})}, nil
		},
		ProcessAvailable: func(*lifecycleTestChild) bool { return true },
		RequestStop:      func(*lifecycleTestChild) error { return nil },
		ForceStop:        func(*lifecycleTestChild) error { forceCalls.Add(1); return nil },
		Waited:           func(child *lifecycleTestChild) <-chan struct{} { return child.waited },
		StopTimeout:      15 * time.Millisecond,
	})
	if _, err := manager.Start(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	results := manager.AbortActive(context.Background())
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("unreaped abort took %s, want bounded return", elapsed)
	}
	if len(results) != 1 || results[0].Outcome != "termination_failed" || forceCalls.Load() != 1 {
		t.Fatalf("abort results=%#v force calls=%d", results, forceCalls.Load())
	}
	if _, ok := manager.Child("one"); !ok {
		t.Fatal("unreaped child ownership was discarded")
	}
	child, _ := manager.Child("one")
	manager.hooks.ForceStop = func(child *lifecycleTestChild) error {
		forceCalls.Add(1)
		close(child.waited)
		return nil
	}
	results = manager.AbortActive(context.Background())
	if len(results) != 1 || results[0].Outcome != "force_terminated" || forceCalls.Load() != 2 {
		t.Fatalf("retry results=%#v force calls=%d", results, forceCalls.Load())
	}
	if _, ok := manager.Child("one"); ok {
		t.Fatal("reaped child remains active")
	}
	if manager.Stopping() || manager.Paused("one") {
		t.Fatal("abort changed permanent stopping or worker pause state")
	}
	if manager.hooks.Waited(child) == nil {
		t.Fatal("test child lost its reap signal")
	}
}

func TestLifecycleAbortActiveEscalatesAfterStopRequestFailure(t *testing.T) {
	var forceCalls atomic.Int32
	manager := NewLifecycleManager([]lifecycleTestWorker{{name: "one"}}, LifecycleHooks[lifecycleTestWorker, *lifecycleTestChild]{
		Name: func(worker lifecycleTestWorker) string { return worker.name },
		StartChild: func(context.Context, lifecycleTestWorker) (*lifecycleTestChild, error) {
			return &lifecycleTestChild{ready: make(chan struct{}), waited: make(chan struct{})}, nil
		},
		ProcessAvailable: func(*lifecycleTestChild) bool { return true },
		RequestStop:      func(*lifecycleTestChild) error { return errors.New("request failed") },
		ForceStop: func(child *lifecycleTestChild) error {
			forceCalls.Add(1)
			close(child.waited)
			return nil
		},
		Waited:      func(child *lifecycleTestChild) <-chan struct{} { return child.waited },
		StopTimeout: time.Second,
	})
	if _, err := manager.Start(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	results := manager.AbortActive(context.Background())
	if len(results) != 1 || results[0].Outcome != "force_terminated" || forceCalls.Load() != 1 {
		t.Fatalf("abort results=%#v force calls=%d", results, forceCalls.Load())
	}
}
