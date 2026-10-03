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
