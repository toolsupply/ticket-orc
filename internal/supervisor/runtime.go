// Package supervisor owns runtime lifecycle state shared by the CLI's
// supervisor composition and daemon observers. It deliberately has no terminal
// rendering or command-line dependencies.
package supervisor

import (
	"sort"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

// WorkerState is the supervisor's canonical in-memory worker lifecycle state.
type WorkerState string

const (
	WorkerStarting WorkerState = "starting"
	WorkerRunning  WorkerState = "running"
	WorkerStopped  WorkerState = "stopped"
	WorkerPaused   WorkerState = "paused"
	WorkerFailed   WorkerState = "failed"
	WorkerConflict WorkerState = "conflict"
)

// WorkerTransition is a typed lifecycle notification. Error text is bounded
// to the child wait error and is not treated as Ticket lifecycle truth.
type WorkerTransition struct {
	Worker  string
	State   WorkerState
	Error   string
	Failure *WorkerFailure
}

// SupervisorHooks lets the process supervisor expose transitions without
// coupling lifecycle state to a daemon HTTP or event implementation.
type SupervisorHooks struct {
	WorkerState func(WorkerTransition)
}

type WorkerObservation struct {
	TicketAt     time.Time
	TicketEvent  string
	TicketID     string
	TicketSource string
}

// RuntimeState retains the latest worker state for one supervisor run. W is
// supplied by the application layer; key extracts its stable worker name so
// this package need not depend on CLI configuration types.
type RuntimeState[W any] struct {
	mu           sync.Mutex
	key          func(W) string
	workers      map[string]WorkerTransition
	configured   []W
	effective    map[string]W
	observed     map[string]WorkerObservation
	repositories map[string]RepositoryStatus
	revision     string
}

func (s *RuntimeState[W]) workerName(worker W) string {
	if s.key != nil {
		return s.key(worker)
	}
	// Keep the zero value useful when an application chooses to initialize
	// runtime state lazily. The adapter avoids coupling this package to its
	// worker configuration type.
	if named, ok := any(worker).(interface{ SupervisorWorkerName() string }); ok {
		return named.SupervisorWorkerName()
	}
	return ""
}

func NewRuntimeState[W any](workers []W, key func(W) string) *RuntimeState[W] {
	effective := make(map[string]W, len(workers))
	state := &RuntimeState[W]{key: key, workers: make(map[string]WorkerTransition, len(workers)), observed: make(map[string]WorkerObservation, len(workers)), repositories: make(map[string]RepositoryStatus), configured: append([]W(nil), workers...), effective: effective}
	for _, worker := range workers {
		name := state.workerName(worker)
		state.workers[name] = WorkerTransition{Worker: name, State: WorkerStopped}
		effective[name] = worker
	}
	return state
}

func (s *RuntimeState[W]) SetRevision(revision string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.revision = revision
	s.mu.Unlock()
}

func (s *RuntimeState[W]) Revision() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}

func (s *RuntimeState[W]) SetRepositoryStatus(status RepositoryStatus) {
	if s == nil || status.Key == "" {
		return
	}
	s.mu.Lock()
	if s.repositories == nil {
		s.repositories = make(map[string]RepositoryStatus)
	}
	s.repositories[status.Key] = status
	s.mu.Unlock()
}

func (s *RuntimeState[W]) RemoveRepositoryStatus(key string) {
	if s == nil || key == "" {
		return
	}
	s.mu.Lock()
	delete(s.repositories, key)
	s.mu.Unlock()
}

func (s *RuntimeState[W]) RepositoryStatuses() []RepositoryStatus {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]RepositoryStatus, 0, len(s.repositories))
	for _, status := range s.repositories {
		result = append(result, status)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}

func (s *RuntimeState[W]) SetConfiguredWorkers(workers []W) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.configured = append([]W(nil), workers...)
	if s.workers == nil {
		s.workers = make(map[string]WorkerTransition, len(workers))
	}
	if s.effective == nil {
		s.effective = make(map[string]W, len(workers))
	}
	if s.observed == nil {
		s.observed = make(map[string]WorkerObservation, len(workers))
	}
	configured := make(map[string]struct{}, len(workers))
	for _, worker := range workers {
		name := s.workerName(worker)
		configured[name] = struct{}{}
		if _, ok := s.workers[name]; !ok {
			s.workers[name] = WorkerTransition{Worker: name, State: WorkerStopped}
		}
		if _, ok := s.effective[name]; !ok {
			s.effective[name] = worker
		}
	}
	for name := range s.workers {
		if _, ok := configured[name]; !ok {
			delete(s.workers, name)
			delete(s.effective, name)
		}
	}
	s.mu.Unlock()
}

func (s *RuntimeState[W]) Observe(event orc.Event) {
	if s == nil || event.Worker == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.observed == nil {
		s.observed = make(map[string]WorkerObservation)
	}
	item := s.observed[event.Worker]
	now := time.Now().UTC()
	if event.Type == "ticket.claim" || event.Type == "ticket.lifecycle" {
		item.TicketAt = now
		item.TicketID = event.Ticket
		if event.Type == "ticket.claim" {
			item.TicketEvent = "claimed ticket"
		} else {
			item.TicketEvent = "ticket lifecycle: " + terminaltext.Sanitize(event.State, false)
		}
		item.TicketSource = "live Ticket observation"
	}
	s.observed[event.Worker] = item
}

func (s *RuntimeState[W]) Observed(worker string) WorkerObservation {
	if s == nil {
		return WorkerObservation{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observed[worker]
}

func (s *RuntimeState[W]) SetEffectiveWorker(worker W) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.effective == nil {
		s.effective = make(map[string]W)
	}
	s.effective[s.workerName(worker)] = worker
	s.mu.Unlock()
}

func (s *RuntimeState[W]) EffectiveWorkers() []W {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]W, 0, len(s.configured))
	for _, configured := range s.configured {
		if effective, ok := s.effective[s.workerName(configured)]; ok {
			result = append(result, effective)
		}
	}
	return result
}

func (s *RuntimeState[W]) ConfiguredWorkers() []W {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]W(nil), s.configured...)
}

func (s *RuntimeState[W]) Set(transition WorkerTransition) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.workers == nil {
		s.workers = make(map[string]WorkerTransition)
	}
	s.workers[transition.Worker] = transition
	s.mu.Unlock()
}

func (s *RuntimeState[W]) SetFailure(worker string, failure *WorkerFailure) {
	if s == nil || worker == "" || failure == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	transition, ok := s.workers[worker]
	if !ok {
		return
	}
	copy := *failure
	transition.Failure = &copy
	s.workers[worker] = transition
}

func (s *RuntimeState[W]) Snapshot() map[string]WorkerTransition {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]WorkerTransition, len(s.workers))
	for name, transition := range s.workers {
		result[name] = transition
	}
	return result
}
