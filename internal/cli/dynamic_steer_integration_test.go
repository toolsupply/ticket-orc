package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type integratedSteerClient struct {
	mu          sync.RWMutex
	active      string
	ready       string
	activeCalls atomic.Int32
	readyCalls  atomic.Int32
}

func (c *integratedSteerClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	c.activeCalls.Add(1)
	c.mu.RLock()
	ticket := c.active
	c.mu.RUnlock()
	if ticket != "" {
		return testTicketList(ticket, queue), nil
	}
	return ticketclient.ListResult{}, nil
}

func (c *integratedSteerClient) ReadyFrontier(_ context.Context, queue string, _ ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	c.readyCalls.Add(1)
	c.mu.RLock()
	ticket := c.ready
	c.mu.RUnlock()
	if ticket != "" {
		return testTicketList(ticket, queue), nil
	}
	return ticketclient.ListResult{}, nil
}

func (c *integratedSteerClient) setReady(ticket string) {
	c.mu.Lock()
	c.ready = ticket
	c.mu.Unlock()
}

func (*integratedSteerClient) Close() error { return nil }

type integratedRepositoryWatchProcess struct {
	done chan error
	once sync.Once
}

func newIntegratedRepositoryWatchProcess() *integratedRepositoryWatchProcess {
	return &integratedRepositoryWatchProcess{done: make(chan error, 1)}
}

func (p *integratedRepositoryWatchProcess) Wait() error { return <-p.done }
func (p *integratedRepositoryWatchProcess) Stop() error {
	p.once.Do(func() { p.done <- context.Canceled })
	return nil
}
func (p *integratedRepositoryWatchProcess) exit(err error) { p.done <- err }

type ticketWatchMetadataFixture struct {
	taskPath  string
	ticketID  string
	lastSize  int64
	lastMtime time.Time
	snapshot  ticketWatchFixtureSnapshot
	notify    func(supervisor.RepositoryWatchEvent)
}

type ticketWatchFixtureSnapshot struct {
	title     string
	state     string
	priority  string
	objective string
}

func newTicketWatchMetadataFixture(taskPath, ticketID string, notify func(supervisor.RepositoryWatchEvent)) (*ticketWatchMetadataFixture, error) {
	info, err := os.Stat(taskPath)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(taskPath)
	if err != nil {
		return nil, err
	}
	snapshot, err := parseTicketWatchFixtureTask(data)
	if err != nil {
		return nil, err
	}
	return &ticketWatchMetadataFixture{
		taskPath: taskPath, ticketID: ticketID, lastSize: info.Size(), lastMtime: info.ModTime(),
		snapshot: snapshot, notify: notify,
	}, nil
}

// observe models Ticket's metadata fallback followed by its authoritative
// snapshot/diff: malformed tasks fail the snapshot, and valid semantic edits
// produce the public watch event without a .local/change update.
func (fixture *ticketWatchMetadataFixture) observe() (bool, error) {
	info, err := os.Stat(fixture.taskPath)
	if err != nil {
		return false, err
	}
	metadataChanged := info.Size() != fixture.lastSize || !info.ModTime().Equal(fixture.lastMtime)
	if !metadataChanged {
		return false, nil
	}
	data, err := os.ReadFile(fixture.taskPath)
	if err != nil {
		return false, err
	}
	current, err := parseTicketWatchFixtureTask(data)
	if err != nil {
		return false, err
	}
	fixture.lastSize, fixture.lastMtime = info.Size(), info.ModTime()
	semanticChange := current != fixture.snapshot
	fixture.snapshot = current
	if semanticChange && fixture.notify != nil {
		fixture.notify(supervisor.RepositoryWatchEvent{Ticket: fixture.ticketID, Event: "edited", State: current.state})
	}
	return semanticChange, nil
}

func parseTicketWatchFixtureTask(data []byte) (ticketWatchFixtureSnapshot, error) {
	const header = "# Metadata watch fixture\n\n- State: open\n- Priority: P2\n\n## Objective\n"
	if !strings.HasPrefix(string(data), header) {
		return ticketWatchFixtureSnapshot{}, errors.New("invalid minimal Ticket task fixture")
	}
	objective := strings.TrimSpace(strings.TrimPrefix(string(data), header))
	if objective == "" || strings.Contains(objective, "\n#") {
		return ticketWatchFixtureSnapshot{}, errors.New("invalid minimal Ticket objective")
	}
	return ticketWatchFixtureSnapshot{
		title: "Metadata watch fixture", state: "open", priority: "P2", objective: objective,
	}, nil
}

type integratedSteerCalls struct {
	active int32
	ready  int32
}

type manualSteerElapsedTimer struct {
	now   time.Time
	ticks chan time.Time
}

func newManualSteerElapsedTimer(start time.Time) *manualSteerElapsedTimer {
	return &manualSteerElapsedTimer{now: start, ticks: make(chan time.Time)}
}

func (timer *manualSteerElapsedTimer) Advance(duration time.Duration) {
	timer.now = timer.now.Add(duration)
	timer.ticks <- timer.now
}

func snapshotIntegratedSteerCalls(clients map[string]*integratedSteerClient) map[string]integratedSteerCalls {
	snapshot := make(map[string]integratedSteerCalls, len(clients))
	for actor, client := range clients {
		snapshot[actor] = integratedSteerCalls{active: client.activeCalls.Load(), ready: client.readyCalls.Load()}
	}
	return snapshot
}

func assertIntegratedSteerCallDelta(t *testing.T, clients map[string]*integratedSteerClient, before map[string]integratedSteerCalls, activeDelta, readyDelta map[string]int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		complete := true
		for actor, client := range clients {
			prior := before[actor]
			wantActive := prior.active + activeDelta[actor]
			wantReady := prior.ready + readyDelta[actor]
			if client.activeCalls.Load() < wantActive || client.readyCalls.Load() < wantReady {
				complete = false
				break
			}
		}
		if complete {
			for actor, client := range clients {
				prior := before[actor]
				if got, want := client.activeCalls.Load()-prior.active, activeDelta[actor]; got != want {
					t.Fatalf("actor %s ActiveClaims delta=%d, want exactly %d", actor, got, want)
				}
				if got, want := client.readyCalls.Load()-prior.ready, readyDelta[actor]; got != want {
					t.Fatalf("actor %s ReadyFrontier delta=%d, want exactly %d", actor, got, want)
				}
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	for actor, client := range clients {
		prior := before[actor]
		if got, want := client.activeCalls.Load()-prior.active, activeDelta[actor]; got != want {
			t.Errorf("actor %s ActiveClaims delta=%d, want %d", actor, got, want)
		}
		if got, want := client.readyCalls.Load()-prior.ready, readyDelta[actor]; got != want {
			t.Errorf("actor %s ReadyFrontier delta=%d, want %d", actor, got, want)
		}
	}
	t.Fatal("scheduler did not finish the expected authoritative Ticket observations")
}

func settleIntegratedSteerSchedule(t *testing.T, ticks chan<- time.Time) {
	t.Helper()
	for range 2 {
		select {
		case ticks <- time.Now():
		case <-time.After(2 * time.Second):
			t.Fatal("steer scheduler did not reach a deterministic tick barrier")
		}
	}
}

func TestRepositoryReadyForcesPostStartupTargetedSteerReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stateDir := t.TempDir()
	pathA, pathB := t.TempDir(), t.TempDir()
	store := state.NewRegistrationStore(stateDir)
	registrationFor := func(repositoryID, repositoryPath, actor string) state.SteerRegistration {
		return state.SteerRegistration{
			RepositoryID: repositoryID, RepositoryPath: repositoryPath, RepositoryName: actor,
			Actor: actor, Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(stateDir), SessionID: actor + "-thread",
		}
	}
	registrations := []state.SteerRegistration{
		registrationFor(joinTestRepositoryID, pathA, "a-one"),
		registrationFor(joinOtherRepositoryID, pathB, "b-one"),
	}
	for i := range registrations {
		joined, _, _, err := store.Join(ctx, registrations[i])
		if err != nil {
			t.Fatal(err)
		}
		registrations[i] = joined
	}
	runtimeStore := state.NewSteerRuntimeStore(stateDir)
	seedIdleSteerDeliveries(t, runtimeStore, registrations)
	clients := map[string]*integratedSteerClient{"a-one": {}, "b-one": {}}
	watchStarted := make(chan struct{})
	releaseReady := make(chan struct{})
	watchRuntime := NewRuntimeState(nil)
	watch := newRepositoryWatchManagerWithStarter(ctx, watchRuntime, supervisor.RepositoryRegistry{
		dynamicRepositoryKey(joinTestRepositoryID): {
			Key: dynamicRepositoryKey(joinTestRepositoryID), ID: joinTestRepositoryID,
			Target: supervisor.TicketTarget{Mode: supervisor.TicketTargetRepository, Repository: pathA},
		},
	}, nil, func(context.Context, supervisor.ConfiguredRepository, func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		close(watchStarted)
		<-releaseReady
		return newIntegratedRepositoryWatchProcess(), nil
	})
	defer watch.StopObservers()
	dirty := newSteerDirtySet()
	runtime := NewRuntimeState(nil)
	config := RunConfig{StateDir: stateDir, Runtime: runtime, steerDirty: dirty, control: &daemon.Control{}}
	manager := &workerManager{repositoryWatch: watch}
	published := make(chan supervisor.RuntimeEvent, 8)
	wireSupervisorRuntimeEvents(config, manager, func(event supervisor.RuntimeEvent) { published <- event })
	watch.StartObservers()
	select {
	case <-watchStarted:
	case <-time.After(time.Second):
		t.Fatal("repository watcher did not start")
	}

	gate := newDispatchGate(state.NewDaemonControlStore(stateDir), state.DaemonRunning)
	statuses := &dynamicSteerStatus{}
	queued := make(chan string, 4)
	registrationTicks := make(chan time.Time)
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		runDynamicSteerWithSchedule(ctx, stateDir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime,
			func(registration state.SteerRegistration) (steerClient, error) {
				return clients[registration.Actor], nil
			},
			testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, _ steertransport.Message) error {
				queued <- registration.SessionID
				return nil
			}), statuses, runtimeStore, newRegistrationObserver(store), gate, dirty, registrationTicks)
	}()
	t.Cleanup(func() { cancel(); <-schedulerDone })
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool { return len(items) == 2 })
	initial := snapshotIntegratedSteerCalls(clients)
	assertIntegratedSteerCallDelta(t, clients, map[string]integratedSteerCalls{},
		map[string]int32{"a-one": 2, "b-one": 2}, map[string]int32{"a-one": 1, "b-one": 1})
	if initial["a-one"].active != 2 || initial["b-one"].active != 2 || initial["a-one"].ready != 1 || initial["b-one"].ready != 1 {
		t.Fatalf("initial authoritative reconciliation calls=%#v", initial)
	}

	// Orc has observed state A while the Ticket watch is still establishing its
	// baseline. The repository changes to B before READY, so only the READY
	// trigger can guarantee another authoritative query that sees B.
	clients["a-one"].setReady("20260930-11237")
	close(releaseReady)
	assertIntegratedSteerCallDelta(t, clients, initial,
		map[string]int32{"a-one": 2}, map[string]int32{"a-one": 1})
	select {
	case session := <-queued:
		if session != "a-one-thread" {
			t.Fatalf("post-READY reconciliation queued %q, want repository A worker", session)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-READY authoritative reconciliation did not discover state B")
	}
	for {
		select {
		case event := <-published:
			if event.Type == "ticket.repository_observer" && event.Code == "observer_ready" {
				t.Fatalf("internal READY signal was exposed as a public observer event: %#v", event)
			}
		default:
			return
		}
	}
}

func TestDynamicSteerIdleIntegrationUsesTargetedAuthoritativeObservations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stateDir := t.TempDir()
	pathA, pathB := t.TempDir(), t.TempDir()
	const editedTicketID = "20260929-99999"
	taskPathA := filepath.Join(pathA, editedTicketID, "TASK.md")
	if err := os.MkdirAll(filepath.Dir(taskPathA), 0o700); err != nil {
		t.Fatal(err)
	}
	const originalTask = "# Metadata watch fixture\n\n- State: open\n- Priority: P2\n\n## Objective\nOriginal objective.\n"
	const editedTask = "# Metadata watch fixture\n\n- State: open\n- Priority: P2\n\n## Objective\nModified objective.\n"
	if len(originalTask) != len(editedTask) {
		t.Fatal("fixture semantic edit must preserve TASK.md size")
	}
	if err := os.WriteFile(taskPathA, []byte(originalTask), 0o600); err != nil {
		t.Fatal(err)
	}
	markerPathA := filepath.Join(pathA, ".local", "change")
	if err := os.MkdirAll(filepath.Dir(markerPathA), 0o700); err != nil {
		t.Fatal(err)
	}
	markerValue := []byte("unchanged marker")
	if err := os.WriteFile(markerPathA, markerValue, 0o600); err != nil {
		t.Fatal(err)
	}
	markerBefore, err := os.Stat(markerPathA)
	if err != nil {
		t.Fatal(err)
	}
	registrations := state.NewRegistrationStore(stateDir)
	registrationFor := func(repositoryID, repositoryPath, actor string) state.SteerRegistration {
		return state.SteerRegistration{
			RepositoryID: repositoryID, RepositoryPath: repositoryPath, RepositoryName: "display " + actor,
			Actor: actor, Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(stateDir), SessionID: actor + "-thread",
		}
	}
	initialRegistrations := []state.SteerRegistration{
		registrationFor(joinTestRepositoryID, pathA, "a-one"),
		registrationFor(joinTestRepositoryID, pathA, "a-two"),
		registrationFor(joinOtherRepositoryID, pathB, "b-one"),
		registrationFor(joinOtherRepositoryID, pathB, "b-two"),
	}
	for i := range initialRegistrations {
		joined, _, _, err := registrations.Join(ctx, initialRegistrations[i])
		if err != nil {
			t.Fatal(err)
		}
		initialRegistrations[i] = joined
	}
	runtimeStore := state.NewSteerRuntimeStore(stateDir)
	seedIdleSteerDeliveries(t, runtimeStore, initialRegistrations)

	clients := map[string]*integratedSteerClient{
		"a-one": {}, "a-two": {}, "b-one": {}, "b-two": {},
	}
	watchRuntime := NewRuntimeState(nil)
	var watcherMu sync.Mutex
	watcherStarts := make(map[string][]*integratedRepositoryWatchProcess)
	watchFixtures := make(map[string]*ticketWatchMetadataFixture)
	watch := newRepositoryWatchManagerWithStarter(ctx, watchRuntime, nil, nil,
		func(_ context.Context, repository supervisor.ConfiguredRepository, notify func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
			process := newIntegratedRepositoryWatchProcess()
			var fixture *ticketWatchMetadataFixture
			if repository.ID == joinTestRepositoryID {
				var err error
				fixture, err = newTicketWatchMetadataFixture(taskPathA, editedTicketID, notify)
				if err != nil {
					return nil, err
				}
			}
			watcherMu.Lock()
			watcherStarts[repository.Key] = append(watcherStarts[repository.Key], process)
			if fixture != nil {
				watchFixtures[repository.Key] = fixture
			}
			watcherMu.Unlock()
			return process, nil
		})
	defer watch.StopObservers()

	runtimeState := NewRuntimeState(nil)
	controlStore := state.NewDaemonControlStore(stateDir)
	gate := newDispatchGate(controlStore, state.DaemonRunning)
	config := RunConfig{StateDir: stateDir, Runtime: runtimeState, dispatchGate: gate, steerDirty: newSteerDirtySet(), control: &daemon.Control{}}
	manager := newWorkerManager(ctx, config, "unused", io.Discard, io.Discard, nil)
	manager.repositoryWatch = watch

	registrationObserver := newRegistrationObserver(registrations)
	identityCache := newDynamicRepositoryIdentityCache()
	probeCounts := map[string]int{}
	probe := func(_ context.Context, registration state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		probeCounts[registration.RepositoryID]++
		return ticketclient.RepositoryInfo{ID: registration.RepositoryID, Path: registration.RepositoryPath}, nil
	}
	observed := registrationObserver.Observe(ctx)
	cacheStart := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	applyDynamicRepositoryObservationWithCache(ctx, manager, probe, observed, identityCache, cacheStart)
	applyDynamicRepositoryObservationWithCache(ctx, manager, probe, observed, identityCache, cacheStart.Add(time.Second))
	if probeCounts[joinTestRepositoryID] != 1 || probeCounts[joinOtherRepositoryID] != 1 {
		t.Fatalf("unchanged dynamic identities were probed again: %#v", probeCounts)
	}

	published := make(chan supervisor.RuntimeEvent, 16)
	emitEvent := func(event supervisor.RuntimeEvent) { published <- event }
	wireSupervisorRuntimeEvents(config, manager, emitEvent)
	wireSupervisorControlCallbacks(&config, manager, emitEvent, nil, &sync.Once{})
	watch.StartObservers()
	waitIntegratedObserverStarts(t, &watcherMu, watcherStarts, map[string]int{
		dynamicRepositoryKey(joinTestRepositoryID):  1,
		dynamicRepositoryKey(joinOtherRepositoryID): 1,
	})

	registrationTicks := make(chan time.Time)
	idleTimer := newManualSteerElapsedTimer(time.Date(2026, time.September, 29, 13, 0, 0, 0, time.UTC))
	statuses := &dynamicSteerStatus{}
	aThreeReconciled := make(chan struct{}, 1)
	statuses.setPublisher(func(event daemon.Event) {
		if event.Type == "steer.status" && event.Actor == "a-three" {
			select {
			case aThreeReconciled <- struct{}{}:
			default:
			}
		}
	})
	queued := make(chan string, 8)
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		runDynamicSteerWithElapsedSchedule(ctx, stateDir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtimeState,
			func(registration state.SteerRegistration) (steerClient, error) {
				return clients[registration.Actor], nil
			},
			testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, _ steertransport.Message) error {
				queued <- registration.SessionID
				return nil
			}),
			statuses, runtimeStore, newRegistrationObserver(registrations), gate, config.steerDirty, registrationTicks, idleTimer.ticks)
	}()
	t.Cleanup(func() { cancel(); <-schedulerDone })
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool { return len(items) == 4 })
	initialCalls := snapshotIntegratedSteerCalls(clients)
	wantInitial := map[string]integratedSteerCalls{
		"a-one": {active: 2, ready: 1}, "a-two": {active: 2, ready: 1},
		"b-one": {active: 2, ready: 1}, "b-two": {active: 2, ready: 1},
	}
	for actor, want := range wantInitial {
		if got := initialCalls[actor]; got != want {
			t.Fatalf("initial authoritative calls for %s=%#v, want %#v", actor, got, want)
		}
	}
	settleIntegratedSteerSchedule(t, registrationTicks)
	if got := snapshotIntegratedSteerCalls(clients); !equalIntegratedSteerCalls(got, initialCalls) {
		t.Fatalf("unchanged registration observations caused Ticket calls: before=%#v after=%#v", initialCalls, got)
	}
	observed = registrationObserver.Observe(ctx)
	applyDynamicRepositoryObservationWithCache(ctx, manager, probe, observed, identityCache, cacheStart.Add(2*time.Second))
	if probeCounts[joinTestRepositoryID] != 1 || probeCounts[joinOtherRepositoryID] != 1 {
		t.Fatalf("unchanged repository paths caused dynamic identity probes: %#v", probeCounts)
	}

	beforeAChange := snapshotIntegratedSteerCalls(clients)
	// The event payload deliberately names a ticket that authoritative Ticket
	// state does not contain. Only a-one's fake Ticket client reports ready work.
	clients["a-one"].setReady("20260929-11111")
	watcherMu.Lock()
	fixtureA := watchFixtures[dynamicRepositoryKey(joinTestRepositoryID)]
	watcherMu.Unlock()
	if fixtureA == nil {
		t.Fatal("repository A observer did not install its Ticket watch fixture")
	}
	if err := os.WriteFile(taskPathA, []byte(editedTask), 0o600); err != nil {
		t.Fatal(err)
	}
	changedAt := time.Now().Add(time.Second)
	if err := os.Chtimes(taskPathA, changedAt, changedAt); err != nil {
		t.Fatal(err)
	}
	if detected, err := fixtureA.observe(); err != nil || !detected {
		t.Fatalf("Ticket metadata fallback detected out-of-band edit=%t err=%v", detected, err)
	}
	markerAfter, err := os.Stat(markerPathA)
	if err != nil {
		t.Fatal(err)
	}
	markerContents, err := os.ReadFile(markerPathA)
	if err != nil {
		t.Fatal(err)
	}
	if markerAfter.Size() != markerBefore.Size() || !markerAfter.ModTime().Equal(markerBefore.ModTime()) || string(markerContents) != string(markerValue) {
		t.Fatal("out-of-band edit changed .local/change instead of using metadata detection")
	}
	var changed supervisor.RuntimeEvent
	select {
	case changed = <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("repository A change was not published through the composed runtime sink")
	}
	if changed.Type != "ticket.repository_changed" || changed.RepositoryID != joinTestRepositoryID || changed.Ticket != editedTicketID || changed.Code != "edited" {
		t.Fatalf("published repository change=%#v", changed)
	}
	settleIntegratedSteerSchedule(t, registrationTicks)
	assertIntegratedSteerCallDelta(t, clients, beforeAChange,
		map[string]int32{"a-one": 2, "a-two": 2}, map[string]int32{"a-one": 1, "a-two": 1})
	if got := <-queued; got != "a-one-thread" {
		t.Fatalf("repository change queued thread %q, want a-one from authoritative ready frontier", got)
	}
	for _, status := range statuses.snapshot() {
		if status.Actor == "a-one" && status.Ticket == changed.Ticket {
			t.Fatalf("event payload was treated as authoritative Ticket state: %#v", status)
		}
	}

	newRegistration, _, _, err := registrations.Join(ctx, registrationFor(joinTestRepositoryID, pathA, "a-three"))
	if err != nil {
		t.Fatal(err)
	}
	clients["a-three"] = &integratedSteerClient{}
	clients["a-three"].setReady("20260929-22222")
	allRegistrations := append(append([]state.SteerRegistration(nil), initialRegistrations...), newRegistration)
	observed = registrationObserver.Observe(ctx)
	applyDynamicRepositoryObservationWithCache(ctx, manager, probe, observed, identityCache, cacheStart.Add(3*time.Second))
	if probeCounts[joinTestRepositoryID] != 1 || probeCounts[joinOtherRepositoryID] != 1 {
		t.Fatalf("new registration at a cached path caused identity probe: %#v", probeCounts)
	}
	if err := runtimeStore.Reconcile(ctx, allRegistrations); err != nil {
		t.Fatal(err)
	}
	seedIdleSteerRegistration(t, runtimeStore, newRegistration)
	beforeJoin := snapshotIntegratedSteerCalls(clients)
	select {
	case registrationTicks <- time.Now():
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not observe the new registration")
	}
	select {
	case <-aThreeReconciled:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not publish completed status for actor a-three")
	}
	assertIntegratedSteerCallDelta(t, clients, beforeJoin,
		map[string]int32{"a-one": 2, "a-two": 2, "a-three": 2},
		map[string]int32{"a-two": 1, "a-three": 1})
	if got := <-queued; got != "a-three-thread" {
		t.Fatalf("new registration queued thread %q, want its authoritative ready frontier", got)
	}
	if len(allRegistrations) != 5 {
		t.Fatalf("registrations = %d, want five", len(allRegistrations))
	}

	beforeResume := snapshotIntegratedSteerCalls(clients)
	clients["b-one"].setReady("20260929-33333")
	if _, err := config.control.PauseDaemon(ctx); err != nil {
		t.Fatalf("pause daemon before resume trigger: %v", err)
	}
	if _, err := config.control.ResumeDaemon(ctx); err != nil {
		t.Fatalf("resume daemon: %v", err)
	}
	settleIntegratedSteerSchedule(t, registrationTicks)
	assertIntegratedSteerCallDelta(t, clients, beforeResume,
		map[string]int32{"a-one": 2, "a-two": 2, "a-three": 2, "b-one": 2, "b-two": 2},
		map[string]int32{"a-two": 1, "b-one": 1, "b-two": 1})
	if got := <-queued; got != "b-one-thread" {
		t.Fatalf("daemon resume queued thread %q, want b-one from its current Ticket frontier", got)
	}

	watcherMu.Lock()
	aProcesses := watcherStarts[dynamicRepositoryKey(joinTestRepositoryID)]
	watcherMu.Unlock()
	if len(aProcesses) != 1 {
		t.Fatalf("repository A observer processes=%d, want one before recovery", len(aProcesses))
	}
	beforeRecovery := snapshotIntegratedSteerCalls(clients)
	clients["a-two"].setReady("20260929-44444")
	aProcesses[0].exit(errors.New("watch exited"))
	var sawDegraded, sawRecovered bool
	recoveryDeadline := time.NewTimer(2 * time.Second)
	defer recoveryDeadline.Stop()
	for !sawRecovered {
		select {
		case event := <-published:
			if event.Type != "ticket.repository_observer" || event.RepositoryID != joinTestRepositoryID {
				continue
			}
			if event.State == "degraded" {
				sawDegraded = true
				if got := snapshotIntegratedSteerCalls(clients); !equalIntegratedSteerCalls(got, beforeRecovery) {
					t.Fatalf("degraded observer event caused Ticket work: before=%#v after=%#v", beforeRecovery, got)
				}
			}
			if event.State == "healthy" && event.Code == "observer_recovered" {
				sawRecovered = true
			}
		case <-recoveryDeadline.C:
			t.Fatalf("observer recovery events degraded=%t recovered=%t", sawDegraded, sawRecovered)
		}
	}
	settleIntegratedSteerSchedule(t, registrationTicks)
	assertIntegratedSteerCallDelta(t, clients, beforeRecovery,
		map[string]int32{"a-one": 2, "a-two": 2, "a-three": 2},
		map[string]int32{"a-two": 1})
	if got := <-queued; got != "a-two-thread" {
		t.Fatalf("observer recovery queued thread %q, want a-two from its current Ticket frontier", got)
	}
	if !sawDegraded {
		t.Fatal("observer recovered without a published degradation event")
	}

	beforeLongIdle := snapshotIntegratedSteerCalls(clients)
	// Advance an independent elapsed-time timer beyond the old full-work sweep
	// interval. This reaches the scheduler's elapsed-time case without waiting
	// in wall-clock time or generating a registration/repository trigger.
	idleTimer.Advance(31 * time.Second)
	assertIntegratedSteerCallDelta(t, clients, beforeLongIdle, map[string]int32{}, map[string]int32{})
	// Unchanged registration polls after that simulated interval must remain
	// targeted as well.
	settleIntegratedSteerSchedule(t, registrationTicks)
	assertIntegratedSteerCallDelta(t, clients, beforeLongIdle, map[string]int32{}, map[string]int32{})

	observed = registrationObserver.Observe(ctx)
	applyDynamicRepositoryObservationWithCache(ctx, manager, probe, observed, identityCache, cacheStart.Add(dynamicRepositoryReverifyInterval))
	if probeCounts[joinTestRepositoryID] != 2 || probeCounts[joinOtherRepositoryID] != 2 {
		t.Fatalf("identity revalidation did not re-probe each dynamic identity once: %#v", probeCounts)
	}
}

func seedIdleSteerDeliveries(t *testing.T, runtime *state.SteerRuntimeStore, registrations []state.SteerRegistration) {
	t.Helper()
	ctx := context.Background()
	if err := runtime.Reconcile(ctx, registrations); err != nil {
		t.Fatal(err)
	}
	for _, registration := range registrations {
		seedIdleSteerRegistration(t, runtime, registration)
	}
}

func seedIdleSteerRegistration(t *testing.T, runtime *state.SteerRuntimeStore, registration state.SteerRegistration) {
	t.Helper()
	updated, err := runtime.CompleteDelivery(context.Background(), registration, string(orc.SteerIdle), true, false)
	if err != nil || !updated {
		t.Fatalf("seed idle delivery for %s: updated=%t err=%v", registration.Actor, updated, err)
	}
}

func waitIntegratedObserverStarts(t *testing.T, mu *sync.Mutex, starts map[string][]*integratedRepositoryWatchProcess, want map[string]int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ready := true
		for key, count := range want {
			if len(starts[key]) < count {
				ready = false
				break
			}
		}
		mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("repository observer starts=%#v, want at least %#v", starts, want)
}

func equalIntegratedSteerCalls(left, right map[string]integratedSteerCalls) bool {
	if len(left) != len(right) {
		return false
	}
	for actor, calls := range left {
		if right[actor] != calls {
			return false
		}
	}
	return true
}
