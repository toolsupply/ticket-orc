package supervisor

import (
	"context"
	"sync"
	"testing"
	"time"
)

type activationTestProcess struct {
	done    chan error
	stopped chan struct{}
	once    sync.Once
}

func newActivationTestProcess() *activationTestProcess {
	return &activationTestProcess{done: make(chan error, 1), stopped: make(chan struct{})}
}

func (p *activationTestProcess) Wait() error { return <-p.done }

func (p *activationTestProcess) Stop() error {
	p.once.Do(func() {
		close(p.stopped)
		p.done <- context.Canceled
	})
	return nil
}

func TestRepositoryWatchManagerRejectsActivationAfterRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState[struct{}](nil, nil)
	repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
	started := make(chan struct{})
	releaseStart := make(chan struct{})
	returned := make(chan *activationTestProcess, 1)
	var lateNotify func(RepositoryWatchEvent)
	manager := NewRepositoryWatchManager(ctx, runtime, RepositoryRegistry{"project": repository}, nil, func(_ context.Context, _ ConfiguredRepository, notify func(RepositoryWatchEvent)) (RepositoryWatchProcess, error) {
		lateNotify = notify
		close(started)
		<-releaseStart
		process := newActivationTestProcess()
		returned <- process
		return process, nil
	})
	events := make(chan RuntimeEvent, 1)
	manager.SetEventSink(func(event RuntimeEvent) { events <- event })
	manager.StartObservers()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("observer starter did not begin")
	}

	// Hold the manager lock while startup returns. runObserver passes its
	// initial context check, then must wait to atomically install the process
	// and mark it healthy.
	manager.mu.Lock()
	observer := manager.observers[repository.Key]
	close(releaseStart)
	var process *activationTestProcess
	select {
	case process = <-returned:
	case <-time.After(time.Second):
		manager.mu.Unlock()
		t.Fatal("observer starter did not return a process")
	}
	time.Sleep(20 * time.Millisecond)
	delete(manager.observers, repository.Key)
	delete(manager.repositories, repository.Key)
	runtime.RemoveRepositoryStatus(repository.Key)
	observer.cancel()
	manager.mu.Unlock()
	lateNotify(RepositoryWatchEvent{Ticket: "20260930-00001", Event: "late", State: "open"})

	select {
	case <-process.stopped:
	case <-time.After(time.Second):
		t.Fatal("process returned after removal was not stopped promptly")
	}
	if statuses := runtime.RepositoryStatuses(); len(statuses) != 0 {
		t.Fatalf("removed observer status was restored by late activation: %#v", statuses)
	}
	select {
	case event := <-events:
		t.Fatalf("removed observer emitted stale event: %#v", event)
	default:
	}
	manager.StopObservers()
}

func TestRepositoryWatchManagerAcceptsPostReadyEventBeforeStarterReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState[struct{}](nil, nil)
	repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
	process := newActivationTestProcess()
	readyAtNotify := make(chan bool, 1)
	var manager *RepositoryWatchManager
	manager = NewRepositoryWatchManager(ctx, runtime, RepositoryRegistry{"project": repository}, nil, func(_ context.Context, _ ConfiguredRepository, notify func(RepositoryWatchEvent)) (RepositoryWatchProcess, error) {
		// The starter contract guarantees that callback events are post-READY.
		// Deliver one before returning, while the supervisor has not activated
		// the observer and observer.ready is still false.
		notify(RepositoryWatchEvent{Ticket: "20261003-00001", Event: "updated", State: "open"})
		manager.mu.Lock()
		observerReady := manager.observers[repository.Key].ready
		manager.mu.Unlock()
		readyAtNotify <- observerReady
		return process, nil
	})
	defer manager.StopObservers()
	events := make(chan RuntimeEvent, 1)
	manager.SetEventSink(func(event RuntimeEvent) { events <- event })
	manager.StartObservers()
	select {
	case ready := <-readyAtNotify:
		if ready {
			t.Fatal("observer was activated before the starter returned")
		}
	case <-time.After(time.Second):
		t.Fatal("repository-watch starter did not deliver its post-READY event")
	}
	select {
	case event := <-events:
		if event.Type != "ticket.repository_changed" || event.RepositoryID != repository.ID || event.RepositoryKey != repository.Key || event.Ticket != "20261003-00001" || event.Code != "updated" || event.State != "open" {
			t.Fatalf("downstream repository event = %#v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-READY event delivered before starter return was not observed downstream")
	}
}

func TestRepositoryWatchManagerRejectsStaleAttemptAndReplacedObserverEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
	manager := NewRepositoryWatchManager(ctx, nil, RepositoryRegistry{"project": repository}, nil, func(context.Context, ConfiguredRepository, func(RepositoryWatchEvent)) (RepositoryWatchProcess, error) {
		return nil, nil
	})
	current := &repositoryWatchObserver{key: repository.Key, repository: repository, attempt: 2, ready: true}
	manager.observers[repository.Key] = current
	event := RepositoryWatchEvent{Ticket: "20261003-00001", Event: "stale", State: "open"}
	hasPendingEvent := func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		_, accepted := manager.pending[repository.Key]
		return accepted
	}

	manager.ticketChangedFrom(current, 1, event)
	if hasPendingEvent() {
		t.Fatal("callback from an old watch attempt was accepted")
	}

	manager.mu.Lock()
	manager.observers[repository.Key] = &repositoryWatchObserver{key: repository.Key, repository: repository, attempt: 1, ready: true}
	manager.mu.Unlock()
	manager.ticketChangedFrom(current, current.attempt, event)
	if hasPendingEvent() {
		t.Fatal("callback from a replaced observer was accepted")
	}
}
