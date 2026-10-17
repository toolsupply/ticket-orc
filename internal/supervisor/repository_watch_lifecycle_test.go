package supervisor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/toolsupply/ticket-orc/internal/testutil"
)

func TestRepositoryWatchBackoffArithmetic(t *testing.T) {
	var got []time.Duration
	for delay := repositoryWatchBackoff; ; delay = nextRepositoryBackoff(delay) {
		got = append(got, delay)
		if delay == repositoryWatchMaxBackoff {
			break
		}
	}
	got = append(got, nextRepositoryBackoff(repositoryWatchMaxBackoff))
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second}
	if len(got) != len(want) {
		t.Fatalf("backoff sequence=%v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("backoff sequence=%v, want %v", got, want)
		}
	}
}

func TestRepositoryWatchDebounceSeparatesPendingFromFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
		manager := NewRepositoryWatchManager(context.Background(), nil, RepositoryRegistry{"project": repository}, nil, nil)
		events := make(chan RuntimeEvent, 1)
		manager.SetEventSink(func(event RuntimeEvent) { events <- event })
		manager.TicketChanged("project", RepositoryWatchEvent{Ticket: "20261004-00001", Event: "claimed", State: "open"})
		manager.TicketChanged("project", RepositoryWatchEvent{Ticket: "20261004-00002", Event: "submitted", State: "review"})
		synctest.Wait()

		manager.mu.Lock()
		pending, found := manager.pending[repository.Key]
		timer := manager.timers[repository.Key]
		if timer != nil {
			timer.timer.Stop()
		}
		manager.mu.Unlock()
		if !found || pending.Ticket != "20261004-00002" || pending.Event != "submitted" {
			t.Fatalf("pending event=%#v found=%t, want latest accepted event", pending, found)
		}
		select {
		case event := <-events:
			t.Fatalf("event was published before explicit flush: %#v", event)
		default:
		}

		manager.flushChanged(repository.Key, timer)
		synctest.Wait()
		select {
		case event := <-events:
			if event.Type != "ticket.repository_changed" || event.RepositoryID != repository.ID || event.Ticket != "20261004-00002" || event.Code != "submitted" {
				t.Fatalf("flushed event=%#v", event)
			}
		default:
			t.Fatal("explicit flush did not publish the pending event")
		}
	})
}

func TestRepositoryWatchStartedDebounceCallbackDoesNotFlushReplacementEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
		manager := NewRepositoryWatchManager(context.Background(), nil, RepositoryRegistry{"project": repository}, nil, nil)
		events := make(chan RuntimeEvent, 2)
		manager.SetEventSink(func(event RuntimeEvent) { events <- event })
		var replacementTimer *repositoryWatchDebounceTimer
		t.Cleanup(func() {
			manager.StopObservers()
			if replacementTimer != nil {
				replacementTimer.timer.Stop()
			}
		})

		manager.TicketChanged(repository.Key, RepositoryWatchEvent{Ticket: "20261004-00001", Event: "claimed", State: "open"})
		manager.mu.Lock()
		time.Sleep(RepositoryWatchDebounce)
		oldTimer := manager.timers[repository.Key]
		if oldTimer == nil || oldTimer.timer.Stop() {
			manager.mu.Unlock()
			t.Fatal("old debounce callback had not started before replacement")
		}
		// The old AfterFunc has expired and its callback is dispatched while it
		// cannot read pending state because this test holds manager.mu.
		manager.ticketChangedLocked(repository.Key, RepositoryWatchEvent{Ticket: "20261004-00002", Event: "submitted", State: "review"})
		replacementTimer = manager.timers[repository.Key]
		if replacementTimer == nil || oldTimer == replacementTimer {
			manager.mu.Unlock()
			t.Fatalf("debounce timer replacement old=%p new=%p", oldTimer, replacementTimer)
		}
		manager.mu.Unlock()
		synctest.Wait()
		select {
		case event := <-events:
			t.Fatalf("stale callback flushed replacement before its debounce interval: %#v", event)
		default:
		}

		time.Sleep(RepositoryWatchDebounce)
		synctest.Wait()
		select {
		case event := <-events:
			if event.Ticket != "20261004-00002" || event.Code != "submitted" {
				t.Fatalf("replacement callback event=%#v", event)
			}
		default:
			t.Fatal("replacement callback did not flush its pending event")
		}
	})
}

func TestRepositoryWatchRemoveAndReaddKeepsReplacementEventAfterOldCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
		manager := NewRepositoryWatchManager(ctx, nil, RepositoryRegistry{"project": repository}, nil, nil)
		barrier := testutil.NewBarrier()
		events := make(chan RuntimeEvent, 2)
		manager.SetEventSink(func(event RuntimeEvent) {
			if err := barrier.Arrive(ctx); err != nil {
				t.Errorf("event sink barrier: %v", err)
			}
			events <- event
		})
		t.Cleanup(func() {
			barrier.Release()
			manager.StopObservers()
		})

		manager.TicketChanged(repository.Key, RepositoryWatchEvent{Ticket: "20261004-00003", Event: "old", State: "open"})
		time.Sleep(RepositoryWatchDebounce)
		synctest.Wait()
		waitRepositoryWatchBarrier(t, barrier, "old event sink")
		if err := manager.Replace(ctx, RepositoryRegistry{}); err != nil {
			t.Fatal(err)
		}
		if err := manager.Replace(ctx, RepositoryRegistry{repository.Key: repository}); err != nil {
			t.Fatal(err)
		}
		manager.TicketChanged(repository.Key, RepositoryWatchEvent{Ticket: "20261004-00004", Event: "new", State: "review"})
		barrier.Release()
		synctest.Wait()
		oldEvent := testutil.Receive(t, time.Second, "old callback completion", events)
		if oldEvent.Ticket != "20261004-00003" || oldEvent.Code != "old" {
			t.Fatalf("old callback completion=%#v", oldEvent)
		}
		select {
		case event := <-events:
			t.Fatalf("replacement callback completed before its debounce interval: %#v", event)
		default:
		}
		time.Sleep(RepositoryWatchDebounce)
		synctest.Wait()
		newEvent := testutil.Receive(t, time.Second, "replacement callback completion", events)
		if newEvent.Ticket != "20261004-00004" || newEvent.Code != "new" {
			t.Fatalf("replacement callback completion=%#v", newEvent)
		}
	})
}

func TestRepositoryWatchShutdownDuringCallbackPreservesInFlightPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
		manager := NewRepositoryWatchManager(ctx, nil, RepositoryRegistry{"project": repository}, nil, nil)
		barrier := testutil.NewBarrier()
		events := make(chan RuntimeEvent, 1)
		manager.SetEventSink(func(event RuntimeEvent) {
			if err := barrier.Arrive(ctx); err != nil {
				t.Errorf("event sink barrier: %v", err)
			}
			events <- event
		})
		t.Cleanup(func() {
			barrier.Release()
			manager.StopObservers()
		})

		manager.TicketChanged(repository.Key, RepositoryWatchEvent{Ticket: "20261004-00005", Event: "submitted", State: "review"})
		time.Sleep(RepositoryWatchDebounce)
		synctest.Wait()
		waitRepositoryWatchBarrier(t, barrier, "in-flight event sink")
		manager.StopObservers()
		barrier.Release()
		synctest.Wait()
		published := testutil.Receive(t, time.Second, "in-flight callback completion", events)
		if published.Ticket != "20261004-00005" || published.Code != "submitted" {
			t.Fatalf("in-flight event after shutdown=%#v", published)
		}
		if len(manager.pending) != 0 || len(manager.timers) != 0 {
			t.Fatalf("shutdown retained debounce state: pending=%#v timers=%#v", manager.pending, manager.timers)
		}
	})
}

func waitRepositoryWatchBarrier(t *testing.T, barrier *testutil.Barrier, description string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := barrier.WaitReached(ctx); err != nil {
		t.Fatalf("waiting for %s: %v", description, err)
	}
}

func TestRepositoryWatchRestartBackoffExpiryAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runtime := NewRuntimeState[struct{}](nil, nil)
		repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
		var starts atomic.Int32
		var replacement *activationTestProcess
		manager := NewRepositoryWatchManager(ctx, runtime, RepositoryRegistry{"project": repository}, nil, func(context.Context, ConfiguredRepository, func(RepositoryWatchEvent)) (RepositoryWatchProcess, error) {
			if starts.Add(1) == 1 {
				process := newActivationTestProcess()
				process.done <- errors.New("watch exited")
				return process, nil
			}
			replacement = newActivationTestProcess()
			return replacement, nil
		})
		events := make(chan RuntimeEvent, 4)
		manager.SetEventSink(func(event RuntimeEvent) { events <- event })
		manager.StartObservers()
		synctest.Wait()
		if starts.Load() != 1 {
			t.Fatalf("starts before backoff expiry=%d, want one", starts.Load())
		}
		statuses := runtime.RepositoryStatuses()
		if len(statuses) != 1 || statuses[0].State != "degraded" {
			t.Fatalf("status before restart=%#v, want degraded", statuses)
		}
		time.Sleep(repositoryWatchBackoff - time.Nanosecond)
		synctest.Wait()
		if starts.Load() != 1 {
			t.Fatalf("observer restarted before backoff expired: starts=%d", starts.Load())
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		statuses = runtime.RepositoryStatuses()
		if starts.Load() != 2 || replacement == nil || len(statuses) != 1 || statuses[0].State != "healthy" || statuses[0].RestartCount != 1 {
			t.Fatalf("restart status=%#v starts=%d", statuses, starts.Load())
		}
		var sawDegraded, sawRecovered bool
		for len(events) > 0 {
			event := <-events
			sawDegraded = sawDegraded || event.State == "degraded"
			sawRecovered = sawRecovered || event.Code == "observer_recovered"
		}
		if !sawDegraded || !sawRecovered {
			t.Fatalf("observer events degraded=%t recovered=%t", sawDegraded, sawRecovered)
		}
		manager.StopObservers()
		synctest.Wait()
		select {
		case <-replacement.stopped:
		default:
			t.Fatal("shutdown did not stop the replacement process")
		}
	})
}

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
	ctx := context.Background()
	runtime := NewRuntimeState[struct{}](nil, nil)
	repository := ConfiguredRepository{Key: "project", ID: "repository-id", Target: TicketTarget{Mode: TicketTargetRepository, Repository: "/ticket/project"}}
	manager := NewRepositoryWatchManager(ctx, runtime, RepositoryRegistry{"project": repository}, nil, nil)
	events := make(chan RuntimeEvent, 1)
	manager.SetEventSink(func(event RuntimeEvent) { events <- event })
	manager.mu.Lock()
	observer := &repositoryWatchObserver{key: repository.Key, repository: repository, attempt: 1}
	manager.observers[repository.Key] = observer
	delete(manager.observers, repository.Key)
	delete(manager.repositories, repository.Key)
	runtime.RemoveRepositoryStatus(repository.Key)
	manager.mu.Unlock()
	process := newActivationTestProcess()
	if _, active := manager.activateObserver(ctx, observer, observer.attempt, process); active {
		t.Fatal("removed observer was activated")
	}
	manager.ticketChangedFrom(observer, observer.attempt, RepositoryWatchEvent{Ticket: "20260930-00001", Event: "late", State: "open"})

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
