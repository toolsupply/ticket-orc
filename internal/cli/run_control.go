package cli

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func isDispatchSuppressed(err error) bool {
	var lifecycleErr *supervisor.LifecycleError
	if !errors.As(err, &lifecycleErr) {
		return false
	}
	return lifecycleErr.Code == "daemon_paused" || lifecycleErr.Code == "daemon_aborted"
}

func (m *workerManager) start(ctx context.Context, name string) (supervisor.MutationResult, error) {
	return m.withDispatch(ctx, name, func() (supervisor.MutationResult, error) { return m.lifecycle.Start(ctx, name) })
}

func (m *workerManager) startLocked(ctx context.Context, name string) (supervisor.MutationResult, error) {
	return m.withDispatch(ctx, name, func() (supervisor.MutationResult, error) { return m.lifecycle.StartLocked(ctx, name) })
}

func (m *workerManager) pause(ctx context.Context, name string) (supervisor.MutationResult, error) {
	return m.lifecycle.Pause(ctx, name)
}

func (m *workerManager) resume(ctx context.Context, name string) (supervisor.MutationResult, error) {
	return m.withDispatch(ctx, name, func() (supervisor.MutationResult, error) { return m.lifecycle.Resume(ctx, name) })
}

func (m *workerManager) stop(ctx context.Context, name string) (supervisor.MutationResult, error) {
	return m.lifecycle.Stop(ctx, name)
}

func (m *workerManager) stopLocked(ctx context.Context, name string) (supervisor.MutationResult, error) {
	return m.lifecycle.StopLocked(ctx, name)
}

func (m *workerManager) abortActive(ctx context.Context) ([]supervisor.TerminationResult, bool, error) {
	applied, err := m.beginAbort(ctx)
	if err != nil {
		return nil, false, err
	}
	return m.terminateActive(ctx), applied, nil
}

func (m *workerManager) beginAbort(ctx context.Context) (bool, error) {
	if m == nil || m.dispatchGate == nil {
		return false, &supervisor.LifecycleError{Code: "capability_unavailable", Message: "daemon control state is unavailable"}
	}
	_, applied, err := m.dispatchGate.Abort(ctx)
	if err != nil {
		return false, err
	}
	return applied, nil
}

func (m *workerManager) terminateActive(ctx context.Context) []supervisor.TerminationResult {
	return m.lifecycle.AbortActive(ctx)
}

func (m *workerManager) workerMutex(name string) *sync.Mutex { return m.lifecycle.WorkerLock(name) }

func (m *workerManager) doctor(ctx context.Context) (supervisor.DoctorResult, error) {
	if m.dispatchGate != nil && m.dispatchGate.Mode() != state.DaemonRunning {
		return supervisor.DoctorResult{}, m.dispatchGate.suppressedError()
	}
	reload, err := m.reload(ctx)
	if err != nil {
		return supervisor.DoctorResult{}, err
	}
	workers := m.lifecycle.Workers()
	names := make([]string, 0, len(workers))
	for name := range workers {
		names = append(names, name)
	}
	sort.Strings(names)
	result := supervisor.DoctorResult{Reloaded: reload.Applied, Workers: make([]supervisor.DoctorWorkerResult, 0, len(names))}
	for _, name := range names {
		outcome := supervisor.DoctorWorkerResult{Worker: name}
		paused := m.lifecycle.Paused(name)
		if paused {
			outcome.Outcome = "failed"
			outcome.Reason = "manual operator decision required: worker is paused; resume it explicitly"
			outcome.Failure = &supervisor.WorkerFailure{Classification: "explicit_safety_stop", Phase: "worker lifecycle", Contained: true}
			result.Workers = append(result.Workers, outcome)
			continue
		}
		_, running := m.lifecycle.Child(name)
		paused = m.lifecycle.Paused(name)
		if paused {
			outcome.Outcome = "failed"
			outcome.Reason = "manual operator decision required: worker is paused; resume it explicitly"
			outcome.Failure = &supervisor.WorkerFailure{Classification: "explicit_safety_stop", Phase: "worker lifecycle", Contained: true}
			result.Workers = append(result.Workers, outcome)
			continue
		}
		if running {
			if verifyErr := m.verifyWorkerStartup(ctx, name); verifyErr != nil {
				outcome.Outcome = "failed"
				outcome.Reason = doctorFailureReason(verifyErr)
				outcome.Failure = classifyWorkerFailureError(verifyErr)
				result.Workers = append(result.Workers, outcome)
				continue
			}
			outcome.Outcome = "recovered"
			if outcome.Action == "" {
				outcome.Action = "already running"
			}
			result.Workers = append(result.Workers, outcome)
			continue
		}
		if _, startErr := m.start(ctx, name); startErr != nil {
			outcome.Outcome = "failed"
			outcome.Action = strings.TrimSpace(strings.TrimSuffix(outcome.Action+"; start", ";"))
			outcome.Reason = doctorFailureReason(startErr)
			outcome.Failure = classifyWorkerFailureError(startErr)
			result.Workers = append(result.Workers, outcome)
			continue
		}
		if verifyErr := m.verifyWorkerStartup(ctx, name); verifyErr != nil {
			outcome.Outcome = "failed"
			outcome.Action = strings.TrimSpace(strings.TrimSuffix(outcome.Action+"; start", ";"))
			outcome.Reason = doctorFailureReason(verifyErr)
			outcome.Failure = classifyWorkerFailureError(verifyErr)
			result.Workers = append(result.Workers, outcome)
			continue
		}
		outcome.Outcome = "recovered"
		if outcome.Action == "" {
			outcome.Action = "started"
		} else {
			outcome.Action += "; started"
		}
		result.Workers = append(result.Workers, outcome)
	}
	return result, nil
}

func (m *workerManager) verifyWorkerStartup(ctx context.Context, name string) error {
	return m.lifecycle.VerifyStartup(ctx, name, workerStartupVerificationTime)
}

func (m *workerManager) startupAttemptError(ctx context.Context, attempt *supervisor.StartupAttempt) error {
	if m == nil || attempt == nil {
		return nil
	}
	pending, done := attempt.PendingResult()
	if pending {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	completed, failure := attempt.Result()
	if !completed || failure == nil {
		return nil
	}
	code, message := "worker_startup_failed", "worker exited before readiness"
	if failure.Classification == "startup_timeout" {
		code, message = "worker_startup_timeout", "worker did not become ready before startup timeout"
	}
	return &supervisor.LifecycleError{Code: code, Message: message, Failure: failure}
}

func (m *workerManager) markStartupResultPending(child *runChild) {
	if child != nil && child.startupAttempt != nil {
		child.startupAttempt.MarkResultPending()
	}
}

// completeStartupResult preserves the pre-readiness outcome before the
// supervisor publishes the worker's terminal transition.
func (m *workerManager) completeStartupResult(child *runChild, err error, shuttingDown bool, failure *supervisor.WorkerFailure) {
	if child == nil || child.startupAttempt == nil {
		return
	}
	var startupFailure *supervisor.WorkerFailure
	if !shuttingDown && !child.readyObserved() {
		startupFailure = failure
		if err == nil && child.failureEnvelope() == nil && startupFailure != nil {
			startupFailure = &supervisor.WorkerFailure{Classification: "startup_failure", Phase: "worker readiness"}
		}
	}
	child.startupAttempt.Complete(startupFailure)
}

func (m *workerManager) isCurrentChild(name string, child *runChild) bool {
	return child != nil && m.lifecycle.IsCurrentChild(name, child)
}

func (m *workerManager) reconcileStartupExit(name string, child *runChild, exit supervisor.ExitReconciliation[supervisor.RunWorker]) error {
	childFailure := child.failureEnvelope()
	// The result-forwarding goroutine also reads child.done. The wait goroutine
	// stores the result before closing child.waited, so startup reconciliation
	// preserves the real child exit even if forwarding consumed the channel.
	childErr := child.exitResult()
	failure := classifyWorkerFailureWithEnvelope(childErr, childFailure)
	if childFailure == nil && childErr == nil {
		failure = &supervisor.WorkerFailure{Classification: "startup_failure", Phase: "worker readiness"}
	}
	desired := exit.Desired
	if m.runtime != nil {
		m.runtime.SetEffectiveWorker(desired)
	}
	child.startupAttempt.Complete(failure)
	if m.transition != nil {
		m.transition(name, WorkerFailed, nil)
	} else if m.runtime != nil {
		m.runtime.Set(supervisor.WorkerTransition{Worker: name, State: WorkerFailed})
	}
	if m.runtime != nil {
		m.runtime.SetFailure(name, failure)
	}
	return &supervisor.LifecycleError{Code: "worker_startup_failed", Message: "worker exited before readiness", Failure: failure}
}

func (m *workerManager) failStartupReadiness(name string, child *runChild, exit supervisor.ExitReconciliation[supervisor.RunWorker]) error {
	if child != nil && child.cmd != nil && child.cmd.Process != nil {
		_ = child.requestStop()
		_ = child.forceStop()
	}
	failure := &supervisor.WorkerFailure{Classification: "startup_timeout", Phase: "worker readiness", Remediation: "inspect Ticket observations and external harness responsiveness before retrying"}
	child.startupAttempt.Complete(failure)
	desired := exit.Desired
	if m.runtime != nil {
		m.runtime.SetEffectiveWorker(desired)
	}
	if m.transition != nil {
		m.transition(name, WorkerFailed, nil)
	} else if m.runtime != nil {
		m.runtime.Set(supervisor.WorkerTransition{Worker: name, State: WorkerFailed})
	}
	if m.runtime != nil {
		m.runtime.SetFailure(name, failure)
	}
	return &supervisor.LifecycleError{Code: "worker_startup_timeout", Message: "worker did not become ready before startup timeout", Failure: failure}
}

func doctorFailureReason(err error) string {
	if err == nil {
		return ""
	}
	var controlErr *supervisor.LifecycleError
	if errors.As(err, &controlErr) {
		switch controlErr.Code {
		case "invalid_config", "worker_identity_conflict", "worker_session_conflict":
			return "configuration change required"
		case "ticket_target_unavailable", "capability_unavailable", "worker_unavailable":
			return "worker configuration or availability requires correction"
		case "worker_startup_failed":
			return "worker exited during startup verification"
		case "mutation_timeout":
			return "manual operator decision required"
		}
	}
	return "manual operator decision required"
}

func workingDirectory() string {
	directory, err := os.Getwd()
	if err != nil {
		return ""
	}
	return directory
}

func effectiveWorkingDirectory(config supervisor.RoleConfig) string {
	if config.WorkingDir != "" {
		return config.WorkingDir
	}
	return workingDirectory()
}

func (m *workerManager) restart(ctx context.Context, name string) (supervisor.MutationResult, error) {
	return m.withDispatch(ctx, name, func() (supervisor.MutationResult, error) { return m.lifecycle.Restart(ctx, name) })
}

func (m *workerManager) group(ctx context.Context, name string, start bool) (supervisor.GroupResult, error) {
	operation := func() (supervisor.GroupResult, error) {
		return m.lifecycle.Group(ctx, name, start, func(worker supervisor.RunWorker) []string { return worker.Groups })
	}
	if !start || m.dispatchGate == nil {
		return operation()
	}
	var result supervisor.GroupResult
	allowed, err := m.dispatchGate.WithDispatch(func() error {
		var callErr error
		result, callErr = operation()
		return callErr
	})
	if !allowed {
		return supervisor.GroupResult{Group: name}, m.dispatchGate.suppressedError()
	}
	return result, err
}

func (m *workerManager) withDispatch(ctx context.Context, name string, operation func() (supervisor.MutationResult, error)) (supervisor.MutationResult, error) {
	if m.dispatchGate == nil {
		return operation()
	}
	var result supervisor.MutationResult
	allowed, err := m.dispatchGate.WithDispatch(func() error {
		var callErr error
		result, callErr = operation()
		return callErr
	})
	if !allowed {
		return supervisor.MutationResult{Worker: name, Applied: false}, m.dispatchGate.suppressedError()
	}
	return result, err
}

func (m *workerManager) reconcileDispatch(ctx context.Context) error {
	workers := m.lifecycle.Workers()
	names := make([]string, 0, len(workers))
	for name := range workers {
		names = append(names, name)
	}
	sort.Strings(names)
	var failures []error
	for _, name := range names {
		if m.lifecycle.Paused(name) {
			continue
		}
		if _, running := m.lifecycle.Child(name); running {
			continue
		}
		if _, err := m.start(ctx, name); err != nil {
			if isDispatchSuppressed(err) || lifecycleErrorCode(err, "worker_paused", "invalid_worker_state") {
				continue
			}
			failures = append(failures, err)
			continue
		}
		if err := m.verifyWorkerStartup(ctx, name); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func lifecycleErrorCode(err error, codes ...string) bool {
	var lifecycleErr *supervisor.LifecycleError
	if !errors.As(err, &lifecycleErr) {
		return false
	}
	for _, code := range codes {
		if lifecycleErr.Code == code {
			return true
		}
	}
	return false
}

func (m *workerManager) groups() []string {
	workers := m.lifecycle.Workers()
	seen := make(map[string]struct{})
	for _, worker := range workers {
		for _, group := range worker.Groups {
			seen[group] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for group := range seen {
		result = append(result, group)
	}
	sort.Strings(result)
	return result
}

func (m *workerManager) stopAll(ctx context.Context) { m.lifecycle.StopAll(ctx) }
