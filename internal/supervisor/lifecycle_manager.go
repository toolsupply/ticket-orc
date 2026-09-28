package supervisor

import (
	"context"
	"sync"
	"time"
)

// LifecycleHooks adapt worker-specific process handling to the lifecycle
// state machine. Hooks perform external work; ordering, desired state,
// transition publication, and mutation results stay in this package.
type LifecycleHooks[W any, C comparable] struct {
	Name                 func(W) string
	ChildWorker          func(C) W
	Prepare              func(context.Context, W) (W, error)
	Conflict             func(string, W, map[string]C) error
	StartChild           func(context.Context, W) (C, error)
	AttachStartupAttempt func(C, *StartupAttempt)
	ChildStarted         func(C)
	ProcessAvailable     func(C) bool
	RequestStop          func(C) error
	ForceStop            func(C) error
	Waited               func(C) <-chan struct{}
	Ready                func(C) <-chan struct{}
	ReadyObserved        func(C) bool
	StartupAttemptOf     func(C) *StartupAttempt
	StartupAttemptError  func(context.Context, *StartupAttempt) error
	StartupExited        func(string, C, ExitReconciliation[W]) error
	StartupTimedOut      func(string, C, ExitReconciliation[W]) error
	ClassifyExit         func(C, error) ExitOutcome
	MarkStartupPending   func(C)
	CompleteStartup      func(C, error, bool, *WorkerFailure)
	SetEffective         func(W)
	Transition           func(string, WorkerState, error)
	StopTimeout          time.Duration
}

type ExitOutcome struct {
	State           WorkerState
	Failure         *WorkerFailure
	TransitionError error
	Shutdown        bool
}

type ExitObservation[W any] struct {
	Current bool
	Worker  W
	Outcome ExitOutcome
}

// LifecycleManager is the application-owned worker state machine. Its maps
// are private; callers observe or change desired state through its methods
// while all transitions follow the documented global/per-worker order.
type LifecycleManager[W any, C comparable] struct {
	mu              sync.Mutex
	gate            *LifecycleCoordinator
	name            func(W) string
	workers         map[string]W
	children        map[string]C
	startupAttempts map[string]*StartupAttempt
	paused          map[string]bool
	stopping        bool
	hooks           LifecycleHooks[W, C]
}

func NewLifecycleManager[W any, C comparable](workers []W, hooks LifecycleHooks[W, C]) *LifecycleManager[W, C] {
	manager := &LifecycleManager[W, C]{
		name: hooks.Name, workers: make(map[string]W, len(workers)), children: make(map[string]C),
		startupAttempts: make(map[string]*StartupAttempt), paused: make(map[string]bool), hooks: hooks,
	}
	names := make([]string, 0, len(workers))
	for _, worker := range workers {
		name := hooks.Name(worker)
		manager.workers[name] = worker
		names = append(names, name)
	}
	manager.gate = NewLifecycleCoordinator(names)
	return manager
}

func (m *LifecycleManager[W, C]) Lock()                              { m.gate.Lock() }
func (m *LifecycleManager[W, C]) Unlock()                            { m.gate.Unlock() }
func (m *LifecycleManager[W, C]) WorkerLock(name string) *sync.Mutex { return m.gate.WorkerLock(name) }
func (m *LifecycleManager[W, C]) AddWorker(name string)              { m.gate.AddWorker(name) }
func (m *LifecycleManager[W, C]) SetStopping(stopping bool) {
	m.mu.Lock()
	m.stopping = stopping
	m.mu.Unlock()
}
func (m *LifecycleManager[W, C]) Worker(name string) (W, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker, ok := m.workers[name]
	return worker, ok
}

func (m *LifecycleManager[W, C]) Child(name string) (C, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	child, ok := m.children[name]
	return child, ok
}

// TrackChild records a child created by the process adapter. Lifecycle
// transitions normally call this internally; it is also useful to reconcile
// externally observed child creation through the same state boundary.
func (m *LifecycleManager[W, C]) TrackChild(name string, child C) {
	m.mu.Lock()
	m.children[name] = child
	m.mu.Unlock()
}

// ForgetChild removes the named child and reports whether one was present.
func (m *LifecycleManager[W, C]) ForgetChild(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.children[name]; !ok {
		return false
	}
	delete(m.children, name)
	return true
}

func (m *LifecycleManager[W, C]) SetPaused(name string, paused bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if paused {
		m.paused[name] = true
	} else {
		delete(m.paused, name)
	}
}

func (m *LifecycleManager[W, C]) StartupAttempt(name string) *StartupAttempt {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startupAttempts[name]
}

func (m *LifecycleManager[W, C]) Workers() map[string]W {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]W, len(m.workers))
	for name, worker := range m.workers {
		result[name] = worker
	}
	return result
}

func (m *LifecycleManager[W, C]) Children() map[string]C {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]C, len(m.children))
	for name, child := range m.children {
		result[name] = child
	}
	return result
}

func (m *LifecycleManager[W, C]) Paused(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.paused[name]
}
func (m *LifecycleManager[W, C]) Stopping() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.stopping }

func (m *LifecycleManager[W, C]) PausedWorkers() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]bool, len(m.paused))
	for name, paused := range m.paused {
		if paused {
			result[name] = true
		}
	}
	return result
}

func (m *LifecycleManager[W, C]) IsCurrentChild(name string, child C) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.children[name]
	return ok && current == child
}

// VerifyStartup waits for a child's readiness or exit without holding the
// lifecycle gate. It reacquires the gate before reconciling an exit, so a
// concurrent stop, pause, or restart can only own that child's terminal
// result once.
func (m *LifecycleManager[W, C]) VerifyStartup(ctx context.Context, name string, timeout time.Duration) error {
	m.gate.Lock()
	m.mu.Lock()
	child, exists := m.children[name]
	attempt := m.startupAttempts[name]
	if exists && m.hooks.StartupAttemptOf != nil {
		if childAttempt := m.hooks.StartupAttemptOf(child); childAttempt != nil {
			attempt = childAttempt
		}
	}
	m.mu.Unlock()
	m.gate.Unlock()
	if !exists {
		if m.hooks.StartupAttemptError != nil {
			return m.hooks.StartupAttemptError(ctx, attempt)
		}
		return nil
	}
	var ready <-chan struct{}
	if m.hooks.Ready != nil {
		ready = m.hooks.Ready(child)
	}
	waited := (<-chan struct{})(nil)
	if m.hooks.Waited != nil {
		waited = m.hooks.Waited(child)
	}
	if ready == nil {
		if waited != nil {
			select {
			case <-waited:
				return m.reconcileStartup(name, child, false)
			default:
			}
		}
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ready:
			return nil
		case <-waited:
			return m.reconcileStartup(name, child, false)
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return m.reconcileStartup(name, child, true)
		}
	}
}

func (m *LifecycleManager[W, C]) reconcileStartup(name string, child C, timedOut bool) error {
	m.gate.Lock()
	defer m.gate.Unlock()
	if !m.IsCurrentChild(name, child) {
		if m.hooks.StartupAttemptError != nil {
			attempt := m.StartupAttempt(name)
			if m.hooks.StartupAttemptOf != nil {
				if childAttempt := m.hooks.StartupAttemptOf(child); childAttempt != nil {
					attempt = childAttempt
				}
			}
			return m.hooks.StartupAttemptError(context.Background(), attempt)
		}
		return nil
	}
	if m.hooks.ReadyObserved != nil && m.hooks.ReadyObserved(child) {
		return nil
	}
	exit := m.ReconcileExitLocked(name, child)
	if timedOut {
		if m.hooks.StartupTimedOut != nil {
			return m.hooks.StartupTimedOut(name, child, exit)
		}
		return nil
	}
	if m.hooks.StartupExited != nil {
		return m.hooks.StartupExited(name, child, exit)
	}
	return nil
}

type ExitReconciliation[W any] struct {
	Current      bool
	Configured   bool
	ShuttingDown bool
	Paused       bool
	Desired      W
}

func (m *LifecycleManager[W, C]) ReconcileExit(name string, child C) ExitReconciliation[W] {
	m.gate.Lock()
	defer m.gate.Unlock()
	return m.ReconcileExitLocked(name, child)
}

// ObserveExit is the sole normal-child exit classification path. A stale
// result from a child already consumed by stop, pause, shutdown, or startup
// reconciliation is ignored; only the current child can publish an outcome.
func (m *LifecycleManager[W, C]) ObserveExit(name string, child C, err error, shuttingDown bool) ExitObservation[W] {
	m.gate.Lock()
	defer m.gate.Unlock()
	reconciled := m.ReconcileExitLocked(name, child)
	if !reconciled.Current {
		return ExitObservation[W]{}
	}
	if m.hooks.MarkStartupPending != nil {
		m.hooks.MarkStartupPending(child)
	}
	shutdown := shuttingDown || reconciled.ShuttingDown
	outcome := ExitOutcome{State: WorkerStopped, Shutdown: shutdown}
	if !shutdown && m.hooks.ClassifyExit != nil {
		outcome = m.hooks.ClassifyExit(child, err)
		outcome.Shutdown = false
	}
	if reconciled.Configured && m.hooks.SetEffective != nil {
		m.hooks.SetEffective(reconciled.Desired)
	}
	if m.hooks.CompleteStartup != nil {
		m.hooks.CompleteStartup(child, err, outcome.Shutdown, outcome.Failure)
	}
	m.transition(name, outcome.State, outcome.TransitionError)
	return ExitObservation[W]{Current: true, Worker: reconciled.Desired, Outcome: outcome}
}

// ReconcileExitLocked consumes only the current child's terminal result. The
// caller must hold the global lifecycle lock so stop/restart cannot classify
// the same exit concurrently.
func (m *LifecycleManager[W, C]) ReconcileExitLocked(name string, child C) ExitReconciliation[W] {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.children[name]
	if !ok || current != child {
		return ExitReconciliation[W]{}
	}
	delete(m.children, name)
	desired, configured := m.workers[name]
	return ExitReconciliation[W]{Current: true, Configured: configured, ShuttingDown: m.stopping, Paused: m.paused[name], Desired: desired}
}
