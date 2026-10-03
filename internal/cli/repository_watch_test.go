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

func TestTicketWatchReadyProtocol(t *testing.T) {
	validReady := `{"type":"ready","repository_id":"` + joinTestRepositoryID + `"}`
	validEvent := `{"ticket":"20260921-00002","event":"submitted","state":"review"}`
	tests := []struct {
		name            string
		stream          string
		wantReadyCount  int
		wantEventCount  int
		wantProtocolErr bool
	}{
		{name: "valid ready", stream: validReady + "\n", wantReadyCount: 1},
		{name: "event after ready", stream: validReady + "\n" + validEvent + "\n", wantReadyCount: 1, wantEventCount: 1},
		{name: "malformed ready", stream: `{"type":"ready"` + "\n", wantProtocolErr: true},
		{name: "wrong repository", stream: `{"type":"ready","repository_id":"` + joinOtherRepositoryID + `"}` + "\n", wantProtocolErr: true},
		{name: "missing repository", stream: `{"type":"ready"}` + "\n", wantProtocolErr: true},
		{name: "malformed type", stream: `{"type":null,"repository_id":"` + joinTestRepositoryID + `"}` + "\n", wantProtocolErr: true},
		{name: "duplicate readiness identity", stream: `{"type":"ready","repository_id":"` + joinOtherRepositoryID + `","repository_id":"` + joinTestRepositoryID + `"}` + "\n", wantProtocolErr: true},
		{name: "ordinary event before ready", stream: validEvent + "\n", wantProtocolErr: true},
		{name: "duplicate ready", stream: validReady + "\n" + validReady + "\n", wantReadyCount: 1, wantProtocolErr: true},
		{name: "unknown protocol record", stream: validReady + "\n" + `{"type":"heartbeat"}` + "\n", wantReadyCount: 1, wantProtocolErr: true},
		{name: "child exit before ready", wantProtocolErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			readyCount, eventCount := 0, 0
			err := scanTicketWatchOutput(strings.NewReader(test.stream), joinTestRepositoryID, func(event supervisor.RepositoryWatchEvent) {
				eventCount++
				if event.Event != "submitted" {
					t.Errorf("unexpected repository mutation event: %#v", event)
				}
			}, func() { readyCount++ })
			if (err != nil) != test.wantProtocolErr {
				t.Fatalf("scan error=%v, wantProtocolErr=%t", err, test.wantProtocolErr)
			}
			if test.wantProtocolErr {
				var protocolErr *supervisor.WatchProtocolError
				if !errors.As(err, &protocolErr) {
					t.Fatalf("error type=%T, want WatchProtocolError", err)
				}
			}
			if readyCount != test.wantReadyCount {
				t.Fatalf("READY count=%d, want %d", readyCount, test.wantReadyCount)
			}
			if eventCount != test.wantEventCount {
				t.Fatalf("repository event count=%d, want %d", eventCount, test.wantEventCount)
			}
		})
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

func TestWiredRepositoryChangedEventMarksOnlyItsRepositoryAndPublishesUnchanged(t *testing.T) {
	runtime := NewRuntimeState(nil)
	dirty := newSteerDirtySet()
	events := make(chan supervisor.RuntimeEvent, 1)
	watch := newRepositoryWatchManager(context.Background(), runtime, testRepositoryRegistry(), nil)
	manager := &workerManager{repositoryWatch: watch}
	config := RunConfig{Runtime: runtime, steerDirty: dirty}
	wireSupervisorRuntimeEvents(config, manager, func(event supervisor.RuntimeEvent) { events <- event })

	actor := "reviewer"
	watch.TicketChanged("alpha", supervisor.RepositoryWatchEvent{
		Ticket: "20260921-00001", Event: "submitted", State: "review", Actor: &actor,
	})
	var got supervisor.RuntimeEvent
	select {
	case got = <-events:
	case <-time.After(time.Second):
		t.Fatal("repository watch event was not published")
	}
	want := supervisor.RuntimeEvent{
		Type: "ticket.repository_changed", RepositoryID: joinTestRepositoryID, RepositoryKey: "alpha",
		Ticket: "20260921-00001", Code: "submitted", State: "review", Actor: actor,
	}
	if got != want {
		t.Fatalf("published event = %#v, want unchanged event %#v", got, want)
	}
	snapshot := dirty.Drain()
	if snapshot.all || len(snapshot.repositories) != 1 {
		t.Fatalf("dirty snapshot = %#v, want only alpha repository", snapshot)
	}
	if _, ok := snapshot.repositories[joinTestRepositoryID]; !ok {
		t.Fatalf("repository change dirtied %#v, want %s", snapshot.repositories, joinTestRepositoryID)
	}
	if _, ok := snapshot.repositories[joinOtherRepositoryID]; ok {
		t.Fatalf("repository change dirtied unrelated repository: %#v", snapshot.repositories)
	}
}

func TestRepositoryObserverLifecycleEventsDoNotDuplicateReadyReconciliation(t *testing.T) {
	tests := []struct {
		name  string
		event supervisor.RuntimeEvent
	}{
		{name: "recovered", event: supervisor.RuntimeEvent{Type: "ticket.repository_observer", RepositoryID: joinTestRepositoryID, State: "healthy", Code: "observer_recovered"}},
		{name: "restarted", event: supervisor.RuntimeEvent{Type: "ticket.repository_observer", RepositoryID: joinTestRepositoryID, State: "healthy", Code: "observer_restarted"}},
		{name: "degraded", event: supervisor.RuntimeEvent{Type: "ticket.repository_observer", RepositoryID: joinTestRepositoryID, State: "degraded", Code: "watch_exit"}},
		{name: "unknown healthy code", event: supervisor.RuntimeEvent{Type: "ticket.repository_observer", RepositoryID: joinTestRepositoryID, State: "healthy", Code: "watch_started"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dirty := newSteerDirtySet()
			var published []supervisor.RuntimeEvent
			sink := composeRepositoryWatchEventSink(dirty, func(event supervisor.RuntimeEvent) { published = append(published, event) })
			sink(test.event)
			if len(published) != 1 || published[0] != test.event {
				t.Fatalf("published events = %#v, want original event %#v", published, test.event)
			}
			snapshot := dirty.Drain()
			if !snapshot.empty() {
				t.Fatalf("lifecycle event duplicated READY reconciliation: %#v", snapshot)
			}
		})
	}
}

func TestRepositoryWatchWaitsForReadyThenDirtiesAndRefreshesRepository(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	dirty := newSteerDirtySet()
	started := make(chan struct{})
	releaseReady := make(chan struct{})
	var probedPath atomic.Value
	probes := atomic.Int32{}
	repository := testRepositoryRegistry()["alpha"]
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, supervisor.RepositoryRegistry{"alpha": repository}, func(_ context.Context, _ supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
		probes.Add(1)
		path, _ := probedPath.Load().(string)
		return ticketclient.RepositoryInfo{ID: repository.ID, Path: path}, nil
	}, func(_ context.Context, _ supervisor.ConfiguredRepository, _ func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		close(started)
		<-releaseReady
		return newFakeRepositoryWatchProcess(nil), nil
	})
	ready := make(chan string, 1)
	manager.SetReadySink(func(repositoryID string) {
		dirty.MarkRepository(repositoryID)
		ready <- repositoryID
	})
	manager.StartObservers()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("watch startup did not begin")
	}
	if statuses := runtime.RepositoryStatuses(); len(statuses) != 1 || statuses[0].State != "starting" {
		t.Fatalf("observer status before READY=%#v, want starting", statuses)
	}
	if probes.Load() != 0 {
		t.Fatalf("repository was probed before READY: %d", probes.Load())
	}
	if snapshot := dirty.Drain(); !snapshot.empty() {
		t.Fatalf("pre-READY state caused a reconciliation: %#v", snapshot)
	}
	// The watcher's initial baseline now contains B. Its READY must trigger a
	// fresh authoritative read even if Orc already observed A at startup.
	probedPath.Store("/repo/state-B")
	close(releaseReady)
	select {
	case repositoryID := <-ready:
		if repositoryID != repository.ID {
			t.Fatalf("READY repository=%q, want %q", repositoryID, repository.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("valid READY did not trigger its repository callback")
	}
	deadline := time.Now().Add(time.Second)
	for (probes.Load() != 1 || len(runtime.RepositoryStatuses()) != 1 || runtime.RepositoryStatuses()[0].Path != "/repo/state-B") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if probes.Load() != 1 || runtime.RepositoryStatuses()[0].Path != "/repo/state-B" {
		t.Fatalf("post-READY authoritative refresh: probes=%d status=%#v", probes.Load(), runtime.RepositoryStatuses())
	}
	if snapshot := dirty.Drain(); snapshot.all || len(snapshot.repositories) != 1 {
		t.Fatalf("READY dirty snapshot=%#v, want one targeted repository", snapshot)
	} else if _, ok := snapshot.repositories[repository.ID]; !ok {
		t.Fatalf("READY dirtied %#v, want repository %s", snapshot.repositories, repository.ID)
	}
	manager.StopObservers()
}

func TestRepositoryWatchDoesNotRecoverUntilReplacementReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := NewRuntimeState(nil)
	firstExited := make(chan struct{})
	replacementStarted := make(chan struct{})
	releaseReplacement := make(chan struct{})
	var retiredNotify func(supervisor.RepositoryWatchEvent)
	var starts atomic.Int32
	readySignals := make(chan string, 1)
	manager := newRepositoryWatchManagerWithStarter(ctx, runtime, supervisor.RepositoryRegistry{"alpha": testRepositoryRegistry()["alpha"]}, nil,
		func(_ context.Context, _ supervisor.ConfiguredRepository, notify func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
			switch starts.Add(1) {
			case 1:
				retiredNotify = notify
				close(firstExited)
				return nil, &supervisor.WatchProtocolError{Cause: errors.New("child exited before READY")}
			default:
				close(replacementStarted)
				<-releaseReplacement
				return newFakeRepositoryWatchProcess(nil), nil
			}
		})
	manager.SetReadySink(func(repositoryID string) { readySignals <- repositoryID })
	events := make(chan supervisor.RuntimeEvent, 8)
	manager.SetEventSink(func(event supervisor.RuntimeEvent) { events <- event })
	manager.StartObservers()
	select {
	case <-firstExited:
	case <-time.After(time.Second):
		t.Fatal("initial watcher did not exit before READY")
	}
	select {
	case <-replacementStarted:
	case <-time.After(time.Second):
		t.Fatal("watcher was not restarted after pre-READY exit")
	}
	if statuses := runtime.RepositoryStatuses(); len(statuses) != 1 || statuses[0].State != "degraded" {
		t.Fatalf("replacement status before READY=%#v, want degraded", statuses)
	}
	sawProtocolFailure := false
	for {
		select {
		case event := <-events:
			if event.State == "degraded" && event.Code == "watch_protocol_failed" {
				sawProtocolFailure = true
			}
			if event.State == "healthy" || event.Code == "observer_recovered" || event.Code == "observer_restarted" {
				t.Fatalf("replacement was reported before READY: %#v", event)
			}
		default:
			goto checked
		}
	}
checked:
	if !sawProtocolFailure {
		t.Fatal("pre-READY exit was not classified as a protocol failure")
	}
	close(releaseReplacement)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-events:
			if event.State == "healthy" && event.Code == "observer_recovered" {
				select {
				case repositoryID := <-readySignals:
					if repositoryID != joinTestRepositoryID {
						t.Fatalf("replacement READY repository=%q", repositoryID)
					}
				case <-time.After(time.Second):
					t.Fatal("replacement READY did not trigger repository reconciliation")
				}
				retiredNotify(supervisor.RepositoryWatchEvent{Ticket: "20260930-00001", Event: "late", State: "open"})
				statuses := runtime.RepositoryStatuses()
				if len(statuses) != 1 || !statuses[0].LastEventAt.IsZero() {
					t.Fatalf("late event from a retired watch changed current observer state: %#v", statuses)
				}
				manager.StopObservers()
				return
			}
		case <-deadline.C:
			t.Fatal("replacement did not recover after READY")
		}
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
