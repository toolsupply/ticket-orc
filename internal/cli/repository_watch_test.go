package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type failingRepositoryTicketDetailReader struct{}

func (failingRepositoryTicketDetailReader) ShowDetail(context.Context, string) (ticketclient.TicketDetail, error) {
	return ticketclient.TicketDetail{}, errors.New("authoritative reread unavailable")
}

func TestRereadRepositoryMutationPreservesAppliedChangeOnFailure(t *testing.T) {
	_, err := rereadRepositoryMutation(context.Background(), failingRepositoryTicketDetailReader{}, joinTestRepositoryID, "alpha", "close", "20260921-66945", true)
	if err == nil {
		t.Fatal("rereadRepositoryMutation returned nil error")
	}
	var mutationErr *daemon.RepositoryMutationError
	if !errors.As(err, &mutationErr) {
		t.Fatalf("error type = %T, want RepositoryMutationError", err)
	}
	if !mutationErr.Applied {
		t.Fatalf("mutation applied = false, want true after successful mutation and failed reread")
	}
}

func TestRepositoryWatchFailureCodeUsesTypedProtocolBoundary(t *testing.T) {
	if got := repositoryWatchFailureCode(errors.New("decode Ticket watch event: malformed JSON")); got != "watch_exit" {
		t.Fatalf("plain error classification = %q, want watch_exit", got)
	}
	first := repositoryWatchFailureCode(&supervisor.WatchProtocolError{Cause: errors.New("decoder wording A")})
	second := repositoryWatchFailureCode(&supervisor.WatchProtocolError{Cause: errors.New("decoder wording B")})
	if first != "watch_protocol_failed" || second != first {
		t.Fatalf("typed protocol classification changed with wording: first=%q second=%q", first, second)
	}
}

type fakeRepositoryWatchProcess struct {
	done     chan error
	stopOnce sync.Once
}

func newFakeRepositoryWatchProcess(err error) *fakeRepositoryWatchProcess {
	process := &fakeRepositoryWatchProcess{done: make(chan error, 1)}
	if err != nil {
		process.done <- err
	}
	return process
}

func (p *fakeRepositoryWatchProcess) wait() error { return <-p.done }
func (p *fakeRepositoryWatchProcess) Wait() error { return p.wait() }

func (p *fakeRepositoryWatchProcess) stop() error {
	p.stopOnce.Do(func() { p.done <- context.Canceled })
	return nil
}
func (p *fakeRepositoryWatchProcess) Stop() error { return p.stop() }

func testRepositoryRegistry() supervisor.RepositoryRegistry {
	return supervisor.RepositoryRegistry{
		"alpha": {Key: "alpha", Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/repo/alpha"}, ID: joinTestRepositoryID, Info: &ticketclient.RepositoryInfo{Path: "/repo/alpha", ID: joinTestRepositoryID}},
		"beta":  {Key: "beta", Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/repo/beta"}, ID: joinOtherRepositoryID, Info: &ticketclient.RepositoryInfo{Path: "/repo/beta", ID: joinOtherRepositoryID}},
	}
}

func TestRepositoryWatchManagerStartsOneObserverPerConfiguredRepository(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	var starts atomic.Int32
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, testRepositoryRegistry(), nil, func(context.Context, supervisor.ConfiguredRepository, func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		starts.Add(1)
		return newFakeRepositoryWatchProcess(nil), nil
	})
	manager.StartObservers()
	deadline := time.Now().Add(time.Second)
	for starts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("observer starts = %d, want one per repository", got)
	}
	statuses := runtime.RepositoryStatuses()
	if len(statuses) != 2 || statuses[0].Key != "alpha" || statuses[1].Key != "beta" {
		t.Fatalf("repository statuses = %#v", statuses)
	}
	for _, status := range statuses {
		if status.ID == "" || status.Path == status.ID || status.Path == "" {
			t.Fatalf("repository status identity/path are not distinct: %#v", status)
		}
		wantPath := "/repo/" + status.Key
		if status.Path != wantPath {
			t.Fatalf("repository status path=%q, want Ticket-reported path %q", status.Path, wantPath)
		}
	}
	manager.StopObservers()
}

func TestRepositoryWatchManagerStopsProcessStartedAfterObserverCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	started := make(chan struct{})
	releaseStart := make(chan struct{})
	processReady := make(chan struct{})
	var process *fakeRepositoryWatchProcess
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, supervisor.RepositoryRegistry{"alpha": testRepositoryRegistry()["alpha"]}, nil, func(context.Context, supervisor.ConfiguredRepository, func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		close(started)
		<-releaseStart
		process = newFakeRepositoryWatchProcess(nil)
		close(processReady)
		return process, nil
	})
	manager.StartObservers()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("observer starter did not begin")
	}
	manager.StopObservers()
	close(releaseStart)
	select {
	case <-processReady:
	case <-time.After(time.Second):
		t.Fatal("observer starter did not return")
	}
	select {
	case err := <-process.done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("late-start process stop error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("process returned after observer cancellation was not stopped")
	}
	for _, status := range runtime.RepositoryStatuses() {
		if status.Key == "alpha" && status.State == "healthy" {
			t.Fatalf("canceled observer reported healthy after startup: %#v", status)
		}
	}
}

func TestRepositoryWatchManagerIgnoresLateStartFailureAfterObserverRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	started := make(chan struct{})
	releaseStart := make(chan struct{})
	returned := make(chan struct{})
	events := make(chan supervisor.RuntimeEvent, 1)
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, supervisor.RepositoryRegistry{"alpha": testRepositoryRegistry()["alpha"]}, nil, func(context.Context, supervisor.ConfiguredRepository, func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		close(started)
		<-releaseStart
		close(returned)
		return nil, errors.New("late watcher startup failure")
	})
	manager.SetEventSink(func(event supervisor.RuntimeEvent) { events <- event })
	manager.StartObservers()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("observer starter did not begin")
	}
	if err := manager.Replace(ctx, supervisor.RepositoryRegistry{}); err != nil {
		t.Fatal(err)
	}
	if statuses := runtime.RepositoryStatuses(); len(statuses) != 0 {
		t.Fatalf("removed observer status before late failure=%#v", statuses)
	}
	close(releaseStart)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("observer starter did not return")
	}
	// Give runObserver time to process the returned error. The removed observer
	// must not recreate its status or publish a stale observer event.
	time.Sleep(20 * time.Millisecond)
	if statuses := runtime.RepositoryStatuses(); len(statuses) != 0 {
		t.Fatalf("late start failure restored removed observer status=%#v", statuses)
	}
	select {
	case event := <-events:
		t.Fatalf("late start failure published stale observer event=%#v", event)
	default:
	}
}

func TestRepositoryWatchManagerDoesNotRefreshStatusAfterObserverRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	probeReturned := make(chan struct{})
	processReady := make(chan *trackedDynamicWatchProcess, 1)
	events := make(chan supervisor.RuntimeEvent, 1)
	repository := testRepositoryRegistry()["alpha"]
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, supervisor.RepositoryRegistry{"alpha": repository}, func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
		close(probeStarted)
		<-releaseProbe
		close(probeReturned)
		name := "Ticket project"
		return ticketclient.RepositoryInfo{ID: repository.ID, Path: "/ticket/project", Name: &name}, nil
	}, func(context.Context, supervisor.ConfiguredRepository, func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		process := newTrackedDynamicWatchProcess()
		processReady <- process
		return process, nil
	})
	manager.SetEventSink(func(event supervisor.RuntimeEvent) { events <- event })
	manager.StartObservers()
	var process *trackedDynamicWatchProcess
	select {
	case process = <-processReady:
	case <-time.After(time.Second):
		t.Fatal("observer process did not start")
	}
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("initial repository probe did not begin")
	}
	if err := manager.Replace(ctx, supervisor.RepositoryRegistry{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-process.stopped:
	case <-time.After(time.Second):
		t.Fatal("removing observer did not stop its active process")
	}
	close(releaseProbe)
	select {
	case <-probeReturned:
	case <-time.After(time.Second):
		t.Fatal("repository probe did not return")
	}
	time.Sleep(20 * time.Millisecond)
	if statuses := runtime.RepositoryStatuses(); len(statuses) != 0 {
		t.Fatalf("removed observer refresh restored repository status=%#v", statuses)
	}
	select {
	case event := <-events:
		t.Fatalf("removed observer refresh published stale event=%#v", event)
	default:
	}
	manager.StopObservers()
}

func TestRepositoryWatchManagerCoalescesChangesAndPublishesRepositoryEvent(t *testing.T) {
	runtime := NewRuntimeState(nil)
	manager := newRepositoryWatchManager(context.Background(), runtime, testRepositoryRegistry(), nil)
	events := make(chan supervisor.RuntimeEvent, 2)
	manager.SetEventSink(func(event supervisor.RuntimeEvent) { events <- event })
	manager.TicketChanged("alpha", supervisor.RepositoryWatchEvent{Ticket: "20260921-00001", Event: "claimed", State: "open"})
	var latest supervisor.RepositoryWatchEvent
	if err := json.Unmarshal([]byte(`{"ticket":"20260921-00002","event":"submitted","state":"review","actor":"reviewer","from":"open","to":"review","message":"private Ticket message"}`), &latest); err != nil {
		t.Fatal(err)
	}
	manager.TicketChanged("alpha", latest)
	select {
	case event := <-events:
		if event.Type != "ticket.repository_changed" || event.RepositoryID != joinTestRepositoryID || event.RepositoryKey != "alpha" || event.Ticket != "20260921-00002" || event.Code != "submitted" || event.Actor != "reviewer" || event.State != "review" {
			t.Fatalf("repository event = %#v", event)
		}
		encoded, err := json.Marshal(daemon.RuntimeEventDTO(event))
		if err != nil || strings.Contains(string(encoded), "private Ticket message") || strings.Contains(string(encoded), `"message"`) {
			t.Fatalf("public repository event leaked Ticket message: %s err=%v", encoded, err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for coalesced repository event")
	}
	select {
	case event := <-events:
		t.Fatalf("unexpected duplicate repository event: %#v", event)
	case <-time.After(repositoryWatchDebounce * 2):
	}
	statuses := runtime.RepositoryStatuses()
	if len(statuses) != 2 || statuses[0].LastEventAt.IsZero() || statuses[0].State != "healthy" {
		t.Fatalf("repository health = %#v", statuses)
	}
}

func TestRepositoryWatchManagerRestartsAndRefreshesAfterUnexpectedExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	var starts atomic.Int32
	var probes atomic.Int32
	events := make(chan supervisor.RuntimeEvent, 4)
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, supervisor.RepositoryRegistry{"alpha": testRepositoryRegistry()["alpha"]}, func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
		probes.Add(1)
		return ticketclient.RepositoryInfo{Path: "/repo/alpha"}, nil
	}, func(context.Context, supervisor.ConfiguredRepository, func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		if starts.Add(1) == 1 {
			return newFakeRepositoryWatchProcess(errors.New("watch exited")), nil
		}
		return newFakeRepositoryWatchProcess(nil), nil
	})
	manager.SetEventSink(func(event supervisor.RuntimeEvent) { events <- event })
	manager.StartObservers()
	deadline := time.Now().Add(time.Second)
	var statuses []supervisor.RepositoryStatus
	for time.Now().Before(deadline) {
		statuses = runtime.RepositoryStatuses()
		if starts.Load() >= 2 && probes.Load() >= 2 && len(statuses) == 1 && statuses[0].RestartCount == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if starts.Load() < 2 || len(statuses) != 1 || statuses[0].RestartCount != 1 || probes.Load() < 2 {
		t.Fatalf("restart status=%#v probes=%d", statuses, probes.Load())
	}
	var sawDegraded, sawRecovered bool
	eventDeadline := time.NewTimer(3 * time.Second)
	defer eventDeadline.Stop()
	for !sawDegraded || !sawRecovered {
		select {
		case event := <-events:
			if event.Type == "ticket.repository_observer" && event.State == "degraded" {
				sawDegraded = true
			}
			if event.Type == "ticket.repository_observer" && event.Code == "observer_recovered" {
				sawRecovered = true
			}
		case <-eventDeadline.C:
			t.Fatalf("timed out waiting for observer degradation/recovery events degraded=%t recovered=%t", sawDegraded, sawRecovered)
		}
	}
	manager.StopObservers()
}

func TestRepositoryWatchManagerReplacesObserversAfterCandidateValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	old := supervisor.RepositoryRegistry{"alpha": testRepositoryRegistry()["alpha"]}
	var starts atomic.Int32
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, old, nil, func(context.Context, supervisor.ConfiguredRepository, func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		starts.Add(1)
		return newFakeRepositoryWatchProcess(nil), nil
	})
	manager.StartObservers()
	deadline := time.Now().Add(time.Second)
	for starts.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	renamed := testRepositoryRegistry()["alpha"]
	renamed.Key = "backend"
	candidate := supervisor.RepositoryRegistry{"backend": renamed}
	if err := manager.Replace(context.Background(), candidate); err != nil {
		t.Fatalf("replace: %v", err)
	}
	deadline = time.Now().Add(time.Second)
	for starts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if starts.Load() != 2 {
		t.Fatalf("observer starts after replacement = %d, want old plus new", starts.Load())
	}
	statuses := runtime.RepositoryStatuses()
	if len(statuses) != 1 || statuses[0].ID != joinTestRepositoryID || statuses[0].Key != "backend" || statuses[0].Path != "/repo/alpha" {
		t.Fatalf("repository statuses after replacement = %#v", statuses)
	}
	events := make(chan supervisor.RuntimeEvent, 1)
	manager.SetEventSink(func(event supervisor.RuntimeEvent) { events <- event })
	manager.TicketChanged("backend", supervisor.RepositoryWatchEvent{Ticket: "20260921-00003", Event: "reviewed", State: "review"})
	select {
	case event := <-events:
		if event.Type != "ticket.repository_changed" || event.RepositoryID != joinTestRepositoryID || event.RepositoryKey != "backend" {
			t.Fatalf("repository event after key rename = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for repository event after key rename")
	}
	manager.StopObservers()
}
