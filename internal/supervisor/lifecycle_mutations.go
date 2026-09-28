package supervisor

import (
	"context"
	"sort"
	"time"
)

func (m *LifecycleManager[W, C]) start(ctx context.Context, name string) (MutationResult, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	return m.startLocked(ctx, name)
}

func (m *LifecycleManager[W, C]) Start(ctx context.Context, name string) (MutationResult, error) {
	return m.start(ctx, name)
}
func (m *LifecycleManager[W, C]) StartLocked(ctx context.Context, name string) (MutationResult, error) {
	return m.startLocked(ctx, name)
}
func (m *LifecycleManager[W, C]) Pause(ctx context.Context, name string) (MutationResult, error) {
	return m.pause(ctx, name)
}
func (m *LifecycleManager[W, C]) Resume(ctx context.Context, name string) (MutationResult, error) {
	return m.resume(ctx, name)
}
func (m *LifecycleManager[W, C]) Stop(ctx context.Context, name string) (MutationResult, error) {
	return m.stop(ctx, name)
}
func (m *LifecycleManager[W, C]) StopLocked(ctx context.Context, name string) (MutationResult, error) {
	return m.stopLocked(ctx, name)
}
func (m *LifecycleManager[W, C]) Restart(ctx context.Context, name string) (MutationResult, error) {
	return m.restart(ctx, name)
}

func (m *LifecycleManager[W, C]) startLocked(ctx context.Context, name string) (MutationResult, error) {
	lock := m.gate.WorkerLock(name)
	if lock == nil {
		return MutationResult{}, workerNotFound()
	}
	lock.Lock()
	defer lock.Unlock()
	m.mu.Lock()
	worker, ok := m.workers[name]
	if !ok {
		m.mu.Unlock()
		return MutationResult{}, workerNotFound()
	}
	if m.stopping {
		m.mu.Unlock()
		return MutationResult{}, &LifecycleError{Code: "daemon_stopping", Message: "daemon is stopping"}
	}
	if m.paused[name] {
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerPaused), Applied: false}, &LifecycleError{Code: "worker_paused", Message: "worker is paused; resume it before starting"}
	}
	if _, running := m.children[name]; running {
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerRunning), Applied: false}, &LifecycleError{Code: "invalid_worker_state", Message: "worker is already running"}
	}
	children := make(map[string]C, len(m.children))
	for key, child := range m.children {
		children[key] = child
	}
	m.mu.Unlock()
	if m.hooks.Conflict != nil {
		if err := m.hooks.Conflict(name, worker, children); err != nil {
			return MutationResult{Worker: name, State: string(WorkerFailed), Applied: false}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return MutationResult{}, err
	}
	if m.hooks.Prepare != nil {
		prepared, err := m.hooks.Prepare(ctx, worker)
		if err != nil {
			m.transition(name, WorkerFailed, err)
			return MutationResult{Worker: name, State: string(WorkerFailed), Applied: false}, &LifecycleError{Code: "ticket_target_unavailable", Message: "Ticket target preflight failed", Cause: err}
		}
		worker = prepared
		m.mu.Lock()
		m.workers[name] = worker
		m.mu.Unlock()
	}
	m.transition(name, WorkerStarting, nil)
	child, err := m.hooks.StartChild(ctx, worker)
	if err != nil {
		m.transition(name, WorkerFailed, err)
		return MutationResult{Worker: name, State: string(WorkerFailed), Applied: false}, &LifecycleError{Code: "worker_unavailable", Message: "worker could not be started", Cause: err}
	}
	attempt := NewStartupAttempt()
	if m.hooks.AttachStartupAttempt != nil {
		m.hooks.AttachStartupAttempt(child, attempt)
	}
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		if m.hooks.RequestStop != nil {
			_ = m.hooks.RequestStop(child)
		}
		return MutationResult{}, &LifecycleError{Code: "daemon_stopping", Message: "daemon is stopping"}
	}
	m.children[name] = child
	m.startupAttempts[name] = attempt
	m.mu.Unlock()
	if m.hooks.SetEffective != nil {
		m.hooks.SetEffective(worker)
	}
	m.transition(name, WorkerRunning, nil)
	if m.hooks.ChildStarted != nil {
		m.hooks.ChildStarted(child)
	}
	return MutationResult{Worker: name, State: string(WorkerRunning), Applied: true}, nil
}

func (m *LifecycleManager[W, C]) pause(ctx context.Context, name string) (MutationResult, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	lock := m.gate.WorkerLock(name)
	if lock == nil {
		return MutationResult{}, workerNotFound()
	}
	lock.Lock()
	defer lock.Unlock()
	m.mu.Lock()
	if _, ok := m.workers[name]; !ok {
		m.mu.Unlock()
		return MutationResult{}, workerNotFound()
	}
	if m.stopping {
		m.mu.Unlock()
		return MutationResult{}, &LifecycleError{Code: "daemon_stopping", Message: "daemon is stopping"}
	}
	child, running := m.children[name]
	if m.paused[name] && !running {
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerPaused), Applied: false}, nil
	}
	desired := m.workers[name]
	m.paused[name] = true
	if running {
		delete(m.children, name)
	}
	m.mu.Unlock()
	if !running {
		if m.hooks.SetEffective != nil {
			m.hooks.SetEffective(desired)
		}
		m.transition(name, WorkerPaused, nil)
		return MutationResult{Worker: name, State: string(WorkerPaused), Applied: true}, nil
	}
	if !m.processAvailable(child) {
		m.mu.Lock()
		delete(m.paused, name)
		m.children[name] = child
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerRunning), Applied: false}, &LifecycleError{Code: "worker_unavailable", Message: "worker process is unavailable"}
	}
	if err := m.hooks.RequestStop(child); err != nil {
		m.mu.Lock()
		delete(m.paused, name)
		m.children[name] = child
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerRunning), Applied: false}, &LifecycleError{Code: "worker_unavailable", Message: "worker pause failed"}
	}
	if err := m.waitStopped(ctx, child, WorkerPaused); err != nil {
		m.transition(name, WorkerPaused, nil)
		return MutationResult{Worker: name, State: string(WorkerPaused), Applied: true}, err
	}
	if m.hooks.SetEffective != nil {
		m.hooks.SetEffective(desired)
	}
	m.transition(name, WorkerPaused, nil)
	return MutationResult{Worker: name, State: string(WorkerPaused), Applied: true}, nil
}

func (m *LifecycleManager[W, C]) resume(ctx context.Context, name string) (MutationResult, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	lock := m.gate.WorkerLock(name)
	if lock == nil {
		return MutationResult{}, workerNotFound()
	}
	lock.Lock()
	m.mu.Lock()
	if _, ok := m.workers[name]; !ok {
		m.mu.Unlock()
		lock.Unlock()
		return MutationResult{}, workerNotFound()
	}
	paused := m.paused[name]
	if !paused {
		_, running := m.children[name]
		m.mu.Unlock()
		lock.Unlock()
		state := WorkerStopped
		if running {
			state = WorkerRunning
		}
		return MutationResult{Worker: name, State: string(state), Applied: false}, nil
	}
	if m.stopping {
		m.mu.Unlock()
		lock.Unlock()
		return MutationResult{Worker: name, State: string(WorkerPaused), Applied: false}, &LifecycleError{Code: "daemon_stopping", Message: "daemon is stopping"}
	}
	delete(m.paused, name)
	m.mu.Unlock()
	lock.Unlock()
	result, err := m.startLocked(ctx, name)
	if err != nil {
		m.mu.Lock()
		m.paused[name] = true
		m.mu.Unlock()
		m.transition(name, WorkerPaused, err)
		return MutationResult{Worker: name, State: string(WorkerPaused), Applied: false}, err
	}
	return result, nil
}

func (m *LifecycleManager[W, C]) stop(ctx context.Context, name string) (MutationResult, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	return m.stopLocked(ctx, name)
}

func (m *LifecycleManager[W, C]) stopLocked(ctx context.Context, name string) (MutationResult, error) {
	lock := m.gate.WorkerLock(name)
	if lock == nil {
		return MutationResult{}, workerNotFound()
	}
	lock.Lock()
	defer lock.Unlock()
	m.mu.Lock()
	if _, ok := m.workers[name]; !ok {
		m.mu.Unlock()
		return MutationResult{}, workerNotFound()
	}
	child, running := m.children[name]
	if !running {
		if m.paused[name] {
			delete(m.paused, name)
			m.mu.Unlock()
			m.transition(name, WorkerStopped, nil)
			return MutationResult{Worker: name, State: string(WorkerStopped), Applied: true}, nil
		}
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerStopped), Applied: false}, &LifecycleError{Code: "invalid_worker_state", Message: "worker is not running"}
	}
	delete(m.children, name)
	desired := m.workers[name]
	m.mu.Unlock()
	if !m.processAvailable(child) {
		m.mu.Lock()
		m.children[name] = child
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerStopped), Applied: false}, &LifecycleError{Code: "worker_unavailable", Message: "worker process is unavailable"}
	}
	if err := m.hooks.RequestStop(child); err != nil {
		m.mu.Lock()
		m.children[name] = child
		m.mu.Unlock()
		return MutationResult{Worker: name, State: string(WorkerRunning), Applied: false}, &LifecycleError{Code: "worker_unavailable", Message: "worker stop failed"}
	}
	if m.hooks.SetEffective != nil {
		m.hooks.SetEffective(desired)
	}
	if err := m.waitStopped(ctx, child, WorkerStopped); err != nil {
		return MutationResult{Worker: name, State: string(WorkerStopped), Applied: true}, err
	}
	m.transition(name, WorkerStopped, nil)
	return MutationResult{Worker: name, State: string(WorkerStopped), Applied: true}, nil
}

func (m *LifecycleManager[W, C]) restart(ctx context.Context, name string) (MutationResult, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	m.mu.Lock()
	_, configured := m.workers[name]
	_, running := m.children[name]
	m.mu.Unlock()
	if !configured {
		return MutationResult{}, workerNotFound()
	}
	if running {
		if _, err := m.stopLocked(ctx, name); err != nil {
			return MutationResult{Worker: name, Applied: false}, err
		}
	}
	return m.startLocked(ctx, name)
}

func (m *LifecycleManager[W, C]) Group(ctx context.Context, name string, start bool, groups func(W) []string) (GroupResult, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	m.mu.Lock()
	matches := make([]string, 0)
	for workerName, worker := range m.workers {
		for _, group := range groups(worker) {
			if group == name {
				matches = append(matches, workerName)
				break
			}
		}
	}
	m.mu.Unlock()
	if len(matches) == 0 {
		return GroupResult{}, &LifecycleError{Code: "group_not_found", Message: "unknown group"}
	}
	sort.Strings(matches)
	result := GroupResult{Group: name, Results: make([]MutationResult, 0, len(matches))}
	failures := 0
	for _, worker := range matches {
		var item MutationResult
		var err error
		if start {
			item, err = m.startLocked(ctx, worker)
		} else {
			item, err = m.stopLocked(ctx, worker)
		}
		result.Results = append(result.Results, item)
		if err != nil {
			failures++
		}
	}
	if failures > 0 {
		return result, &LifecycleError{Code: "group_partial_failure", Message: "one or more group workers failed", Applied: failures < len(matches)}
	}
	return result, nil
}

func (m *LifecycleManager[W, C]) StopAll(ctx context.Context) {
	m.gate.Lock()
	defer m.gate.Unlock()
	m.mu.Lock()
	m.stopping = true
	names := make([]string, 0, len(m.children))
	for name := range m.children {
		names = append(names, name)
	}
	m.mu.Unlock()
	sort.Strings(names)
	for _, name := range names {
		_, _ = m.stopLocked(ctx, name)
	}
}

func (m *LifecycleManager[W, C]) processAvailable(child C) bool {
	return m.hooks.ProcessAvailable == nil || m.hooks.ProcessAvailable(child)
}
func (m *LifecycleManager[W, C]) transition(name string, state WorkerState, err error) {
	if m.hooks.Transition != nil {
		m.hooks.Transition(name, state, err)
	}
}
func (m *LifecycleManager[W, C]) waitStopped(ctx context.Context, child C, state WorkerState) error {
	timeout := m.hooks.StopTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	stopCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-m.hooks.Waited(child):
		return nil
	case <-stopCtx.Done():
		_ = m.hooks.ForceStop(child)
		<-m.hooks.Waited(child)
		if state == WorkerPaused {
			return &LifecycleError{Code: "mutation_timeout", Message: "worker pause timed out", Applied: true}
		}
		return &LifecycleError{Code: "mutation_timeout", Message: "worker stop timed out", Applied: true}
	}
}

func workerNotFound() error {
	return &LifecycleError{Code: "worker_not_found", Message: "unknown worker"}
}
