package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestStartupTerminalPathsRetainUnconfirmedWorkspaceProtection(t *testing.T) {
	for _, mode := range []string{"exit without envelope", "timeout", "known pre-execution"} {
		t.Run(mode, func(t *testing.T) {
			worker := supervisorTestWorker("startup", t.TempDir())
			worker.Config.WorkingDir = t.TempDir()
			runtime := NewRuntimeState([]supervisor.RunWorker{worker})
			manager := newWorkerManager(t.Context(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtime}, os.Args[0], io.Discard, io.Discard, nil)
			child := &runChild{worker: worker, waited: make(chan struct{}), ready: make(chan struct{})}
			child.setExitResult(errors.New("abrupt startup exit"))
			if mode == "known pre-execution" {
				child.captureFailure(orc.Event{Type: "worker.failure", Failure: &orc.Failure{Classification: "harness_start_failed", Execution: &harness.ExecutionEvidence{Process: harness.ProcessOutcome{Start: "not_started"}}}})
			}
			manager.lifecycle.TrackChild(worker.Name, child)
			var err error
			if mode == "timeout" {
				// Exercise the synchronous timeout transition, without waiting on
				// a timer or claiming that the still-unconfirmed child has exited.
				manager.lifecycle.Lock()
				exit := manager.lifecycle.ReconcileExitLocked(worker.Name, child)
				err = manager.failStartupReadiness(worker.Name, child, exit)
				manager.lifecycle.Unlock()
			} else {
				close(child.waited)
				err = manager.verifyWorkerStartup(t.Context(), worker.Name)
			}
			if err == nil {
				t.Fatal("startup failure reported success")
			}
			launched := false
			manager.starter = func(_ context.Context, _, _ string, worker supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
				launched = true
				child := &runChild{worker: worker, done: make(chan error, 1), waited: make(chan struct{})}
				child.done <- nil
				close(child.waited)
				return child, nil
			}
			_, startErr := manager.start(t.Context(), worker.Name)
			if mode == "known pre-execution" {
				if startErr != nil || !launched {
					t.Fatalf("positive pre-execution failure blocked normal start: %v", startErr)
				}
			} else {
				var failureErr *supervisor.LifecycleError
				if !errors.As(startErr, &failureErr) || failureErr.Code != "workspace_cleanup_unconfirmed" || launched {
					t.Fatalf("startup path bypassed protection: launched=%v err=%v", launched, startErr)
				}
				failure := runtime.Snapshot()[worker.Name].Failure
				if failure == nil || failure.Remediation != orc.CleanupUnconfirmed || failure.Ticket != "" || failure.AttemptID != 0 {
					t.Fatalf("missing/fabricated startup diagnostic: %+v", failure)
				}
			}
			if manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false).Current {
				t.Fatal("startup exit observed twice")
			}
		})
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
	verifyCtx, cancelVerify := context.WithCancel(context.Background())
	ctx := &supervisorTestContext{Context: verifyCtx, doneCalls: make(chan struct{}, 1)}
	verified := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		verified <- manager.verifyWorkerStartup(ctx, worker.Name)
	}()
	t.Cleanup(func() {
		cancelVerify()
		child.readyOnce.Do(func() { close(child.ready) })
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("startup verification goroutine did not stop during cleanup")
		}
	})
	select {
	case <-ctx.doneCalls:
	case <-time.After(time.Second):
		t.Fatal("startup verification did not reach its readiness wait")
	}
	select {
	case err := <-verified:
		t.Fatalf("startup verification returned before readiness: %v", err)
	default:
	}
	child.readyOnce.Do(func() { close(child.ready) })
	select {
	case err := <-verified:
		if err != nil {
			t.Fatalf("verifyWorkerStartup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("startup verification did not complete after readiness")
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
