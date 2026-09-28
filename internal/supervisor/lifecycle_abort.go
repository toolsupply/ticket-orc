package supervisor

import (
	"context"
	"sort"
	"time"
)

// AbortActive gracefully terminates the currently active managed children,
// force-stopping any child that does not exit within the configured timeout.
// Callers must first persist aborted mode and close dispatch. This operation
// leaves worker configuration and per-worker pause state intact and does not
// mark the lifecycle manager as permanently stopping.
func (m *LifecycleManager[W, C]) AbortActive(ctx context.Context) []TerminationResult {
	if ctx == nil {
		ctx = context.Background()
	}
	m.gate.Lock()
	defer m.gate.Unlock()

	m.mu.Lock()
	names := make([]string, 0, len(m.children))
	for name := range m.children {
		names = append(names, name)
	}
	m.mu.Unlock()
	sort.Strings(names)

	results := make([]TerminationResult, 0, len(names))
	for _, name := range names {
		workerLock := m.gate.WorkerLock(name)
		if workerLock == nil {
			continue
		}
		workerLock.Lock()
		m.mu.Lock()
		child, active := m.children[name]
		m.mu.Unlock()
		if active {
			results = append(results, m.abortChildLocked(ctx, name, child))
		}
		workerLock.Unlock()
	}
	return results
}

func (m *LifecycleManager[W, C]) abortChildLocked(ctx context.Context, name string, child C) TerminationResult {
	if childWaited(m.hooks.Waited(child)) {
		return m.finishAbortChild(name, child, "terminated")
	}
	if !m.processAvailable(child) {
		return TerminationResult{Worker: name, Outcome: "termination_failed"}
	}

	requestErr := m.hooks.RequestStop(child)
	if requestErr == nil && waitChild(ctx, m.hooks.Waited(child), m.stopTimeout()) {
		return m.finishAbortChild(name, child, "terminated")
	}
	if childWaited(m.hooks.Waited(child)) {
		return m.finishAbortChild(name, child, "terminated")
	}
	if err := m.hooks.ForceStop(child); err != nil {
		if childWaited(m.hooks.Waited(child)) {
			return m.finishAbortChild(name, child, "force_terminated")
		}
		return TerminationResult{Worker: name, Outcome: "termination_failed"}
	}
	if waitChild(context.Background(), m.hooks.Waited(child), m.stopTimeout()) {
		return m.finishAbortChild(name, child, "force_terminated")
	}
	return TerminationResult{Worker: name, Outcome: "termination_failed"}
}

func (m *LifecycleManager[W, C]) finishAbortChild(name string, child C, outcome string) TerminationResult {
	exit := m.ReconcileExitLocked(name, child)
	if !exit.Current {
		return TerminationResult{Worker: name, Outcome: "termination_failed"}
	}
	if m.hooks.SetEffective != nil && exit.Configured {
		m.hooks.SetEffective(exit.Desired)
	}
	if m.hooks.CompleteStartup != nil {
		m.hooks.CompleteStartup(child, nil, true, nil)
	}
	m.transition(name, WorkerStopped, nil)
	return TerminationResult{Worker: name, Outcome: outcome}
}

func (m *LifecycleManager[W, C]) stopTimeout() time.Duration {
	if m.hooks.StopTimeout > 0 {
		return m.hooks.StopTimeout
	}
	return 5 * time.Second
}

func waitChild(ctx context.Context, waited <-chan struct{}, timeout time.Duration) bool {
	if childWaited(waited) {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-waited:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

func childWaited(waited <-chan struct{}) bool {
	select {
	case <-waited:
		return true
	default:
		return false
	}
}
