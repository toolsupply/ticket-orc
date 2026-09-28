package supervisor

import (
	"sync"
)

// StartupAttempt arbitrates one child's startup result against readiness and
// shutdown observers. Completion is idempotent so only the first classified
// startup outcome is published.
type StartupAttempt struct {
	mu            sync.Mutex
	resultPending bool
	completed     bool
	done          chan struct{}
	failure       *WorkerFailure
}

func NewStartupAttempt() *StartupAttempt { return &StartupAttempt{done: make(chan struct{})} }

func (a *StartupAttempt) MarkResultPending() {
	if a == nil {
		return
	}
	a.mu.Lock()
	if !a.completed {
		a.resultPending = true
	}
	a.mu.Unlock()
}

func (a *StartupAttempt) Complete(failure *WorkerFailure) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.completed {
		return
	}
	if failure != nil {
		copy := *failure
		a.failure = &copy
	}
	a.resultPending = false
	a.completed = true
	close(a.done)
}

func (a *StartupAttempt) PendingResult() (bool, <-chan struct{}) {
	if a == nil {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.resultPending, a.done
}

func (a *StartupAttempt) Result() (bool, *WorkerFailure) {
	if a == nil {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.completed || a.failure == nil {
		return a.completed, nil
	}
	copy := *a.failure
	return true, &copy
}
