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
	changed     chan<- struct{}
}

func (c *integratedSteerClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	c.mu.RLock()
	ticket := c.active
	c.mu.RUnlock()
	c.activeCalls.Add(1)
	c.signalObservation()
	if ticket != "" {
		return testTicketList(ticket, queue), nil
	}
	return ticketclient.ListResult{}, nil
}

func (c *integratedSteerClient) ReadyFrontier(_ context.Context, queue string, _ ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	c.mu.RLock()
	ticket := c.ready
	c.mu.RUnlock()
	c.readyCalls.Add(1)
	c.signalObservation()
	if ticket != "" {
		return testTicketList(ticket, queue), nil
	}
	return ticketclient.ListResult{}, nil
}

func (c *integratedSteerClient) signalObservation() {
	if c.changed == nil {
		return
	}
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

func (c *integratedSteerClient) setReady(ticket string) {
	c.mu.Lock()
	c.ready = ticket
	c.mu.Unlock()
}

func (*integratedSteerClient) Close() error { return nil }

type integratedRepositoryWatchProcess struct {
	done       chan error
	waitExited chan struct{}
	once       sync.Once
}

func newIntegratedRepositoryWatchProcess() *integratedRepositoryWatchProcess {
	return &integratedRepositoryWatchProcess{done: make(chan error, 1), waitExited: make(chan struct{})}
}

func (p *integratedRepositoryWatchProcess) Wait() error {
	defer close(p.waitExited)
	return <-p.done
}
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

func snapshotIntegratedSteerCalls(clients map[string]*integratedSteerClient) map[string]integratedSteerCalls {
	snapshot := make(map[string]integratedSteerCalls, len(clients))
	for actor, client := range clients {
		snapshot[actor] = integratedSteerCalls{active: client.activeCalls.Load(), ready: client.readyCalls.Load()}
	}
	return snapshot
}

func assertIntegratedSteerCallDelta(t *testing.T, clients map[string]*integratedSteerClient, before map[string]integratedSteerCalls, activeDelta, readyDelta map[string]int32, changed <-chan struct{}) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !integratedSteerCallMinimumReached(clients, before, activeDelta, readyDelta) {
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatalf("scheduler did not finish expected Ticket observations: before=%#v active_delta=%#v ready_delta=%#v after=%#v", before, activeDelta, readyDelta, snapshotIntegratedSteerCalls(clients))
		}
	}
	for actor, client := range clients {
		prior := before[actor]
		if got, want := client.activeCalls.Load()-prior.active, activeDelta[actor]; got != want {
			t.Fatalf("actor %s ActiveClaims delta=%d, want exactly %d", actor, got, want)
		}
		if got, want := client.readyCalls.Load()-prior.ready, readyDelta[actor]; got != want {
			t.Fatalf("actor %s ReadyFrontier delta=%d, want exactly %d", actor, got, want)
		}
	}
}

func integratedSteerCallMinimumReached(clients map[string]*integratedSteerClient, before map[string]integratedSteerCalls, activeDelta, readyDelta map[string]int32) bool {
	for actor, client := range clients {
		prior := before[actor]
		if client.activeCalls.Load() < prior.active+activeDelta[actor] || client.readyCalls.Load() < prior.ready+readyDelta[actor] {
			return false
		}
	}
	return true
}

func settleIntegratedSteerSchedule(t *testing.T, registrationTicks chan<- time.Time, passCompleted <-chan struct{}) {
	t.Helper()
	for range 2 {
		select {
		case registrationTicks <- time.Now():
		case <-time.After(2 * time.Second):
			t.Fatal("steer scheduler did not reach a deterministic tick barrier")
		}
		select {
		case <-passCompleted:
		case <-time.After(2 * time.Second):
			t.Fatal("steer scheduler did not complete the registration reconciliation")
		}
	}
}

func waitIntegratedSteerInitialStatus(t *testing.T, statuses *dynamicSteerStatus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := statuses.waitFirst(ctx); err != nil {
		t.Fatalf("scheduler did not publish initial steer status: %v", err)
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
	callsChanged := make(chan struct{}, 1)
	clients := map[string]*integratedSteerClient{"a-one": {changed: callsChanged}, "b-one": {changed: callsChanged}}
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
	passCompleted := make(chan struct{}, 1)
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		runDynamicSteerWithScheduleCompletion(ctx, stateDir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime,
			func(registration state.SteerRegistration) (steerClient, error) {
				return clients[registration.Actor], nil
			},
			testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, _ steertransport.Message) error {
				queued <- registration.SessionID
				return nil
			}), statuses, runtimeStore, newRegistrationObserver(store), gate, dirty, registrationTicks, passCompleted)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-schedulerDone:
		case <-time.After(10 * time.Second):
			t.Errorf("dynamic steer scheduler did not stop during test cleanup")
		}
	})
	waitIntegratedSteerInitialStatus(t, statuses)
	if items := statuses.snapshot(); len(items) != 2 {
		t.Fatalf("initial steer statuses=%#v, want two", items)
	}
	initial := snapshotIntegratedSteerCalls(clients)
	assertIntegratedSteerCallDelta(t, clients, map[string]integratedSteerCalls{},
		map[string]int32{"a-one": 2, "b-one": 2}, map[string]int32{"a-one": 1, "b-one": 1}, callsChanged)
	if initial["a-one"].active != 2 || initial["b-one"].active != 2 || initial["a-one"].ready != 1 || initial["b-one"].ready != 1 {
		t.Fatalf("initial authoritative reconciliation calls=%#v", initial)
	}

	// Orc has observed state A while the Ticket watch is still establishing its
	// baseline. The repository changes to B before READY, so only the READY
	// trigger can guarantee another authoritative query that sees B.
	clients["a-one"].setReady("20260930-11237")
	close(releaseReady)
	assertIntegratedSteerCallDelta(t, clients, initial,
		map[string]int32{"a-one": 2}, map[string]int32{"a-one": 1}, callsChanged)
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
	for _, readyBeforeStartup := range []bool{true, false} {
		name := "ready_after_startup"
		if readyBeforeStartup {
			name = "ready_before_startup"
		}
		t.Run(name, func(t *testing.T) {
			testDynamicSteerIdleIntegration(t, readyBeforeStartup)
		})
	}
}

func testDynamicSteerIdleIntegration(t *testing.T, readyBeforeStartup bool) {
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

	callsChanged := make(chan struct{}, 1)
	clients := map[string]*integratedSteerClient{
		"a-one": {changed: callsChanged}, "a-two": {changed: callsChanged}, "b-one": {changed: callsChanged}, "b-two": {changed: callsChanged},
	}
	watchRuntime := NewRuntimeState(nil)
	var watcherMu sync.Mutex
	watcherStarts := make(map[string][]*integratedRepositoryWatchProcess)
	watchFixtures := make(map[string]*ticketWatchMetadataFixture)
	releaseReady := make(chan struct{})
	watch := newRepositoryWatchManagerWithStarter(ctx, watchRuntime, nil, nil,
		func(ctx context.Context, repository supervisor.ConfiguredRepository, notify func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
			select {
			case <-releaseReady:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
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
	t.Cleanup(func() {
		cancel() // Unblock any starter still waiting for the test's READY gate.
		watch.StopObservers()
		watcherMu.Lock()
		var processes []*integratedRepositoryWatchProcess
		for _, started := range watcherStarts {
			processes = append(processes, started...)
		}
		watcherMu.Unlock()
		for _, process := range processes {
			select {
			case <-process.waitExited:
			case <-time.After(10 * time.Second):
				t.Error("repository watch process did not exit during test cleanup")
			}
		}
	})

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
	watchReady := make(chan string, 2)
	watch.SetReadySink(func(repositoryID string) {
		config.steerDirty.MarkRepository(repositoryID)
		watchReady <- repositoryID
	})
	waitReady := func() {
		t.Helper()
		readyRepositories := make(map[string]bool, 2)
		for range 2 {
			select {
			case repositoryID := <-watchReady:
				readyRepositories[repositoryID] = true
			case <-time.After(2 * time.Second):
				t.Fatal("repository observers did not report READY")
			}
		}
		if len(readyRepositories) != 2 || !readyRepositories[joinTestRepositoryID] || !readyRepositories[joinOtherRepositoryID] {
			t.Fatalf("repository READY notifications=%#v", readyRepositories)
		}
	}
	watch.StartObservers()
	if readyBeforeStartup {
		close(releaseReady)
		// The READY sink has marked both repositories dirty before bootstrap
		// drains the dirty set, so startup needs exactly one read per actor.
		waitReady()
	}

	registrationTicks := make(chan time.Time)
	passCompleted := make(chan struct{}, 1)
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
		runDynamicSteerWithScheduleCompletion(ctx, stateDir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtimeState,
			func(registration state.SteerRegistration) (steerClient, error) {
				return clients[registration.Actor], nil
			},
			testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, _ steertransport.Message) error {
				queued <- registration.SessionID
				return nil
			}),
			statuses, runtimeStore, newRegistrationObserver(registrations), gate, config.steerDirty, registrationTicks, passCompleted)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-schedulerDone:
		case <-time.After(10 * time.Second):
			t.Errorf("dynamic steer scheduler did not stop during test cleanup")
		}
	})
	waitIntegratedSteerInitialStatus(t, statuses)
	if items := statuses.snapshot(); len(items) != 4 {
		t.Fatalf("initial steer statuses=%#v, want four", items)
	}
	// A status publication alone does not prove that the scheduler pass has
	// finished. No further READY can arrive in either controlled ordering yet.
	settleIntegratedSteerSchedule(t, registrationTicks, passCompleted)
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
	if !readyBeforeStartup {
		close(releaseReady)
		waitReady()
		// READY arrived after the baseline: it must force exactly one more
		// authoritative reconciliation for each repository's actors.
		settleIntegratedSteerSchedule(t, registrationTicks, passCompleted)
		assertIntegratedSteerCallDelta(t, clients, initialCalls,
			map[string]int32{"a-one": 2, "a-two": 2, "b-one": 2, "b-two": 2},
			map[string]int32{"a-one": 1, "a-two": 1, "b-one": 1, "b-two": 1}, callsChanged)
	}
	settledCalls := snapshotIntegratedSteerCalls(clients)
	settleIntegratedSteerSchedule(t, registrationTicks, passCompleted)
	if got := snapshotIntegratedSteerCalls(clients); !equalIntegratedSteerCalls(got, settledCalls) {
		t.Fatalf("unchanged registration observations caused Ticket calls: before=%#v after=%#v", settledCalls, got)
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
	settleIntegratedSteerSchedule(t, registrationTicks, passCompleted)
	assertIntegratedSteerCallDelta(t, clients, beforeAChange,
		map[string]int32{"a-one": 2, "a-two": 2}, map[string]int32{"a-one": 1, "a-two": 1}, callsChanged)
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
	clients["a-three"] = &integratedSteerClient{changed: callsChanged}
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
	case <-passCompleted:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not complete the new-registration reconciliation")
	}
	select {
	case <-aThreeReconciled:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not publish completed status for actor a-three")
	}
	assertIntegratedSteerCallDelta(t, clients, beforeJoin,
		map[string]int32{"a-one": 2, "a-two": 2, "a-three": 2},
		map[string]int32{"a-two": 1, "a-three": 1}, callsChanged)
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
	settleIntegratedSteerSchedule(t, registrationTicks, passCompleted)
	assertIntegratedSteerCallDelta(t, clients, beforeResume,
		map[string]int32{"a-one": 2, "a-two": 2, "a-three": 2, "b-one": 2, "b-two": 2},
		map[string]int32{"a-two": 1, "b-one": 1, "b-two": 1}, callsChanged)
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
	settleIntegratedSteerSchedule(t, registrationTicks, passCompleted)
	assertIntegratedSteerCallDelta(t, clients, beforeRecovery,
		map[string]int32{"a-one": 2, "a-two": 2, "a-three": 2},
		map[string]int32{"a-two": 1}, callsChanged)
	if got := <-queued; got != "a-two-thread" {
		t.Fatalf("observer recovery queued thread %q, want a-two from its current Ticket frontier", got)
	}
	if !sawDegraded {
		t.Fatal("observer recovered without a published degradation event")
	}

	beforeLongIdle := snapshotIntegratedSteerCalls(clients)
	// Unchanged registration polls remain cheap even after the recovery work
	// above; healthy repositories are not swept on a generic elapsed timer.
	settleIntegratedSteerSchedule(t, registrationTicks, passCompleted)
	assertIntegratedSteerCallDelta(t, clients, beforeLongIdle, map[string]int32{}, map[string]int32{}, callsChanged)

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
