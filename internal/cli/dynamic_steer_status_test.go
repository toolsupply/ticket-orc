package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
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
	"github.com/toolsupply/ticket-orc/internal/testutil"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestManagedWorkerOwnershipBlocksDynamicSteer(t *testing.T) {
	dir := t.TempDir()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "shared-actor", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	worker := supervisor.RunWorker{Name: "managed", Config: supervisor.RoleConfig{Actor: registration.Actor, RepositoryIdentity: registration.RepositoryID}}
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	runtime.SetEffectiveWorker(worker)
	runtime.Set(supervisor.WorkerTransition{Worker: worker.Name, State: supervisor.WorkerRunning})
	if !managedOwner(runtime, registration) {
		t.Fatal("matching managed worker did not block dynamic registration")
	}
	var clients atomic.Int32
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime, func(state.SteerRegistration) (steerClient, error) {
		clients.Add(1)
		return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
	}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
		queued.Add(1)
		return nil
	}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
	runner.bootstrap(ctx)
	items := statuses.snapshot()
	if len(items) != 1 || items[0].State != "conflict" || items[0].Code != "managed_owner" || items[0].ManagedOwner != "managed" {
		t.Fatalf("steer conflict status=%#v", items)
	}
	if clients.Load() != 0 || queued.Load() != 0 {
		t.Fatalf("managed conflict created Ticket clients=%d or queued=%d wake(s)", clients.Load(), queued.Load())
	}
	registration.Actor = "another-actor"
	if managedOwner(runtime, registration) {
		t.Fatal("different Ticket actor reported as managed conflict")
	}
}

type dynamicSignoffTestClient struct {
	*sequenceSteerClient
	ticket         ticketclient.Ticket
	frontierTicket *ticketclient.Ticket
	frontierCalls  atomic.Int32
}

func (client *dynamicSignoffTestClient) Show(_ context.Context, id string) (ticketclient.Ticket, error) {
	if id != client.ticket.ID {
		return ticketclient.Ticket{}, ticketclient.ErrProtocol
	}
	return client.ticket, nil
}

func (client *dynamicSignoffTestClient) SignoffFrontier(_ context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	client.frontierCalls.Add(1)
	if offset != 0 || limit != 1 {
		return ticketclient.ListResult{}, ticketclient.ErrProtocol
	}
	client.readyFilters = append(client.readyFilters, filters)
	frontierTicket := client.ticket
	if client.frontierTicket != nil {
		frontierTicket = *client.frontierTicket
	}
	if !orc.EligibleSignoffEntry(frontierTicket, filters) {
		return ticketclient.ListResult{}, nil
	}
	return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: frontierTicket.ID, State: frontierTicket.State}}}, nil
}

func TestPersistedDynamicSignoffRegistrationUsesOneShotExactTargetPath(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	registrations := state.NewRegistrationStore(dir)
	current, _, _, err := registrations.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	runtime := state.NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []state.SteerRegistration{current}); err != nil {
		t.Fatal(err)
	}
	before, err := runtime.Snapshot(ctx)
	if err != nil || len(before.Deliveries) != 1 {
		t.Fatalf("initial steer runtime=%#v err=%v", before, err)
	}
	var clients, wakes atomic.Int32
	client := &dynamicSignoffTestClient{
		sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
		ticket:              ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}},
	}
	var message string
	statuses := &dynamicSteerStatus{}
	roles := map[string]steerRolePolicy{
		"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"blocked"}}, NudgePrompt: "accept"},
		"coder":         {TicketQueue: "open", NudgePrompt: "code"},
		"reviewer":      {TicketQueue: "review", NudgePrompt: "review"},
	}
	clientsFactory := func(state.SteerRegistration) (steerClient, error) {
		clients.Add(1)
		return client, nil
	}
	transport := func(_ context.Context, _ string, _ state.SteerRegistration, queued steertransport.Message) error {
		wakes.Add(1)
		message = queued.Text
		return nil
	}
	newRouter := func() *steertransport.Router { return testSteerRouter(t, transport) }
	runner := newTestDynamicSteerRunner(t, ctx, dir, roles, nil, clientsFactory, newRouter(), statuses, runtime, newRegistrationObserver(registrations), nil, nil)
	runner.bootstrap(ctx)
	items := statuses.snapshot()
	if len(items) != 1 || items[0].State != "queued" || items[0].Code != "awaiting_signoff_outcome" || items[0].EffectiveTicketQueue != "signoff" || items[0].Ticket != client.ticket.ID {
		t.Fatalf("signoff registration status=%#v", items)
	}
	if clients.Load() != 1 || wakes.Load() != 1 {
		t.Fatalf("dynamic signoff created Ticket clients=%d or sent wakes=%d", clients.Load(), wakes.Load())
	}
	runner.policies.Replace(map[string]steerRolePolicy{
		"architect-api": {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"blocked"}}, NudgePrompt: "open now"},
		"coder":         {TicketQueue: "open", NudgePrompt: "code"},
		"reviewer":      {TicketQueue: "review", NudgePrompt: "review"},
	})
	runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
	policyReload, err := runtime.Snapshot(ctx)
	if err != nil || len(policyReload.Deliveries) != 1 || policyReload.Deliveries[0].TargetTicket != client.ticket.ID {
		t.Fatalf("signoff-to-open policy reload lost outstanding target: %#v err=%v", policyReload, err)
	}
	runner.policies.Replace(roles)
	for _, required := range []string{client.ticket.ID, steerSessionBootstrapPrompt, "do not claim", "Do not process a second signoff ticket", "Inspect related Ticket work", "human decision", "ticket query -j id:" + client.ticket.ID, "reassign --to user", "mutation_applied=true", "takes precedence over any conflicting configured role prompt", "accept"} {
		if !strings.Contains(message, required) {
			t.Errorf("signoff message %q missing %q", message, required)
		}
	}
	bootstrapped, err := runtime.Snapshot(ctx)
	if err != nil || len(bootstrapped.Deliveries) != 1 || bootstrapped.Deliveries[0].BootstrapPending {
		t.Fatalf("accepted bootstrap+signoff delivery did not clear bootstrap intent: %#v err=%v", bootstrapped, err)
	}
	client.sequenceSteerClient.activeByQueue["open"] = []bool{true}
	runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
	if wakes.Load() != 1 {
		t.Fatalf("unchanged outstanding signoff target was sent %d times", wakes.Load())
	}
	afterClaim, err := runtime.Snapshot(ctx)
	if err != nil || len(afterClaim.Deliveries) != 1 || afterClaim.Deliveries[0].TargetTicket != client.ticket.ID || afterClaim.Deliveries[0].State != "queued" {
		t.Fatalf("active actor claim consumed outstanding signoff target: %#v err=%v", afterClaim, err)
	}
	client.sequenceSteerClient.activeByQueue["open"] = []bool{false}
	runner.close()
	runner = newTestDynamicSteerRunner(t, ctx, dir, roles, nil, clientsFactory, newRouter(), statuses, runtime, newRegistrationObserver(registrations), nil, nil)
	runner.bootstrap(ctx)
	if wakes.Load() != 1 {
		t.Fatalf("restarted dynamic runner sent unchanged outstanding target %d times", wakes.Load())
	}
	client.ticket.Assignee = "user"
	runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
	items = statuses.snapshot()
	if wakes.Load() != 1 || len(items) != 1 || items[0].Code != "human_handoff" {
		t.Fatalf("user handoff status=%#v wakes=%d", items, wakes.Load())
	}
	client.ticket.Assignee = ""
	runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
	if wakes.Load() != 2 {
		t.Fatalf("explicitly returned signoff target sent %d wakes, want a new opportunity", wakes.Load())
	}
	client.ticket.State = "review"
	runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
	items = statuses.snapshot()
	if wakes.Load() != 2 || len(items) != 1 || items[0].Code != "signoff_resolved" {
		t.Fatalf("resolved signoff target status=%#v wakes=%d", items, wakes.Load())
	}
	client.ticket = ticketclient.Ticket{ID: "20261007-12346", State: "signoff", Tags: []string{"api"}}
	runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
	if wakes.Load() != 3 {
		t.Fatalf("next eligible signoff target sent %d wakes, want 3", wakes.Load())
	}
	client.ticket.Assignee = "reviewer"
	runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
	items = statuses.snapshot()
	if wakes.Load() != 3 || len(items) != 1 || items[0].Code != "assigned_elsewhere" {
		t.Fatalf("other-owner target status=%#v wakes=%d", items, wakes.Load())
	}
	afterRegistrations, err := registrations.Snapshot(ctx)
	if err != nil || len(afterRegistrations.Registrations) != 1 || afterRegistrations.Registrations[0].RegistrationID != current.RegistrationID {
		t.Fatalf("signoff registration was not preserved: %#v err=%v", afterRegistrations, err)
	}
	after, err := runtime.Snapshot(ctx)
	if err != nil || len(after.Deliveries) != 1 || after.Deliveries[0].State != "none" || after.Deliveries[0].TargetTicket != "" || after.Deliveries[0].RegistrationID != before.Deliveries[0].RegistrationID {
		t.Fatalf("signoff delivery changed: before=%#v after=%#v err=%v", before, after, err)
	}
}

func TestDynamicSteerStatusConcurrentSnapshotsAndUpdates(t *testing.T) {
	statuses := &dynamicSteerStatus{}
	var published atomic.Int32
	statuses.setPublisher(func(daemon.Event) { published.Add(1) })
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				statuses.replace([]daemon.SteerStatus{{RepositoryID: "repo", Actor: "actor", Session: fmt.Sprintf("session-%d-%d", worker, i), State: "queued"}})
				_ = statuses.snapshot()
			}
		}()
	}
	workers.Wait()
	if len(statuses.snapshot()) != 1 || published.Load() == 0 {
		t.Fatalf("status snapshot=%#v published=%d", statuses.snapshot(), published.Load())
	}
}

func TestDynamicSteerStatusPublishesActiveTicketID(t *testing.T) {
	var published daemon.Event
	statuses := &dynamicSteerStatus{}
	statuses.setPublisher(func(event daemon.Event) { published = event })
	statuses.replace([]daemon.SteerStatus{{
		RepositoryID: "repo", Role: "coder", Actor: "worker", Harness: "future-harness", Session: steerTestThread,
		State: "consumed", Ticket: "20260926-12345",
	}})
	if published.Type != "steer.status" || published.Ticket != "20260926-12345" || published.Harness != "future-harness" {
		t.Fatalf("published status event=%#v", published)
	}
}

func TestDynamicSteerStatusAndDeliveryEventsExposeHarnessOnly(t *testing.T) {
	registration := state.SteerRegistration{
		RepositoryID: "repo", Actor: "worker", Role: "coder", Harness: "future-harness", SessionID: "opaque-session",
		Transport: state.SteerTransportRoute{Kind: "codex-queue", Params: map[string]string{"home": "/private/orc/path"}},
	}
	var events []daemon.Event
	statuses := &dynamicSteerStatus{publish: func(event daemon.Event) { events = append(events, event) }}
	statuses.replace([]daemon.SteerStatus{{RepositoryID: registration.RepositoryID, Actor: registration.Actor, Role: registration.Role,
		Harness: registration.Harness, Session: registration.SessionID, State: "queued"}})
	statuses.publishDelivery(registration, "sending", "20260929-12345")
	if len(events) != 2 || events[0].Harness != registration.Harness || events[1].Harness != registration.Harness {
		t.Fatalf("harness missing from steer events: %#v", events)
	}
	statusJSON, err := json.Marshal(daemon.Status{Steer: statuses.snapshot()})
	if err != nil {
		t.Fatal(err)
	}
	eventJSON, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, encoded := range []string{string(statusJSON), string(eventJSON)} {
		if !strings.Contains(encoded, `"harness":"future-harness"`) || strings.Contains(encoded, "/private/orc/path") || strings.Contains(encoded, `"transport"`) {
			t.Fatalf("steer status/event JSON exposed incomplete or private identity: %s", encoded)
		}
	}
}

func TestDynamicSteerRunnerDefersDirtyWorkGeneratedDuringPass(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	pathA, pathB := t.TempDir(), t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	for _, registration := range []state.SteerRegistration{
		{RepositoryID: joinTestRepositoryID, RepositoryPath: pathA, Actor: "worker-a", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "thread-a"},
		{RepositoryID: joinOtherRepositoryID, RepositoryPath: pathB, Actor: "worker-b", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "thread-b"},
	} {
		if _, _, _, err := registrations.Join(ctx, registration); err != nil {
			t.Fatal(err)
		}
	}

	dirty := newSteerDirtySet()
	clientA := &dirtyDuringSteerPassClient{markDirty: func() { dirty.MarkRepository(joinOtherRepositoryID) }}
	clientB := &dirtyDuringSteerPassClient{}
	runner := newDynamicSteerRunner(dir, newSteerPolicyStore(map[string]steerRolePolicy{
		"coder": {TicketQueue: "open", NudgePrompt: "wake"},
	}), nil, func(registration state.SteerRegistration) (steerClient, error) {
		if registration.Actor == "worker-a" {
			return clientA, nil
		}
		return clientB, nil
	}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }),
		&dynamicSteerStatus{}, nil, newRegistrationObserver(registrations), nil, dirty)
	if runner == nil {
		t.Fatal("runner construction returned nil")
	}
	defer runner.close()
	runner.bootstrap(ctx)
	beforeA, beforeB := clientA.activeCalls.Load(), clientB.activeCalls.Load()
	clientA.armed.Store(true)

	dirty.MarkRepository(joinTestRepositoryID)
	runner.repositoryChanged(ctx)
	if got := clientA.activeCalls.Load() - beforeA; got == 0 {
		t.Fatal("first trigger did not reconcile repository A")
	}
	if got := clientB.activeCalls.Load() - beforeB; got != 0 {
		t.Fatalf("first trigger processed repository B dirtied during the pass: %d observations", got)
	}

	pending := dirty.Drain()
	if !pending.includes(joinOtherRepositoryID) || pending.includes(joinTestRepositoryID) {
		t.Fatalf("work left pending after first pass = %#v, want only repository B", pending)
	}
	dirty.MarkRepository(joinOtherRepositoryID)
	runner.repositoryChanged(ctx)
	if got := clientB.activeCalls.Load() - beforeB; got == 0 {
		t.Fatal("second trigger did not reconcile deferred repository B")
	}
}

func TestSteerDirtySetCoalescesAndPreservesMarksAcrossDrain(t *testing.T) {
	dirty := newSteerDirtySet()
	dirty.MarkRepository("repo-a")
	dirty.MarkRepository("repo-a")
	dirty.MarkRepository("repo-b")
	first := dirty.Drain()
	if first.all || len(first.repositories) != 2 || !first.includes("repo-a") || !first.includes("repo-b") {
		t.Fatalf("first dirty snapshot=%#v", first)
	}

	// This mark arrives after a reconciliation has drained its input. It must
	// remain available for the next reconciliation instead of being cleared by
	// completion of the current one.
	dirty.MarkRepository("repo-c")
	second := dirty.Drain()
	if second.all || len(second.repositories) != 1 || !second.includes("repo-c") {
		t.Fatalf("mark during reconciliation was lost: %#v", second)
	}

	dirty.MarkRepository("repo-d")
	dirty.MarkAll()
	dirty.MarkRepository("repo-e")
	all := dirty.Drain()
	if !all.all || len(all.repositories) != 0 || !all.includes("repo-e") || !all.includes("repo-unseen") {
		t.Fatalf("MarkAll did not subsume repository marks: %#v", all)
	}
	select {
	case <-dirty.Wake():
		t.Fatal("drain left a stale wake notification pending")
	default:
	}
}

func TestSteerDirtySetConcurrentRepositoryMarksDoNotBlockOrDisappear(t *testing.T) {
	dirty := newSteerDirtySet()
	const marks = 64
	var producers sync.WaitGroup
	for i := 0; i < marks; i++ {
		producers.Add(1)
		go func(i int) {
			defer producers.Done()
			dirty.MarkRepository(fmt.Sprintf("repo-%d", i))
		}(i)
	}
	producers.Wait()
	snapshot := dirty.Drain()
	if snapshot.all || len(snapshot.repositories) != marks {
		t.Fatalf("concurrent dirty snapshot has %d repositories, want %d: %#v", len(snapshot.repositories), marks, snapshot)
	}
}

func TestDynamicSteerReconcilesAndSnapshotsOnceForMultipleRegistrations(t *testing.T) {
	dir := t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	for _, actor := range []string{"worker-a", "worker-b"} {
		if _, _, _, err := registrations.Join(context.Background(), state.SteerRegistration{
			RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: actor, Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: actor,
		}); err != nil {
			t.Fatal(err)
		}
	}
	persistence := &countingSteerRuntime{inner: state.NewSteerRuntimeStore(dir)}
	statuses := &dynamicSteerStatus{}
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {
		TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend"}, WithoutTags: []string{"generic-blocked", "legacy-skip"}}, TicketWithoutTags: []string{"generic-blocked"}, ReviewSkipTags: []string{"legacy-skip"}, NudgePrompt: "wake",
	}}, nil, func(state.SteerRegistration) (steerClient, error) {
		return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
	}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }), statuses, persistence, newRegistrationObserver(registrations), nil, nil)
	runner.bootstrap(ctx)
	items := statuses.snapshot()
	if len(items) != 2 || items[0].State != "none" || items[1].State != "none" ||
		items[0].EffectiveTicketQueue != "open" || !reflect.DeepEqual(items[0].EffectiveTicketTags, []string{"backend"}) || !reflect.DeepEqual(items[0].EffectiveTicketWithoutTags, []string{"generic-blocked"}) || !reflect.DeepEqual(items[0].EffectiveReviewSkipTags, []string{"legacy-skip"}) {
		t.Fatalf("initial reconciliation statuses=%#v", items)
	}
	if got := persistence.reconciles.Load(); got != 1 {
		t.Fatalf("Reconcile called %d times for one tick, want 1", got)
	}
	if got := persistence.snapshots.Load(); got != 1 {
		t.Fatalf("Snapshot called %d times for two registrations in one tick, want 1", got)
	}
}

func TestDynamicSteerScheduleSkipsUnchangedAndScopesRepositoryWork(t *testing.T) {
	dir := t.TempDir()
	store := state.NewRegistrationStore(dir)
	const firstRepository = "11111111-1111-4111-8111-111111111111"
	const secondRepository = "22222222-2222-4222-8222-222222222222"
	for _, registration := range []state.SteerRegistration{
		{RepositoryID: firstRepository, RepositoryPath: dir, Actor: "first", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "first-thread"},
		{RepositoryID: secondRepository, RepositoryPath: dir, Actor: "second", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "second-thread"},
	} {
		if _, _, _, err := store.Join(context.Background(), registration); err != nil {
			t.Fatal(err)
		}
	}

	clients := map[string]*countingSteerClient{
		firstRepository:  {},
		secondRepository: {},
	}
	dirty := newSteerDirtySet()
	statuses := &dynamicSteerStatus{}
	activeGate := testutil.NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(reg state.SteerRegistration) (steerClient, error) { return clients[reg.RepositoryID], nil }, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }),
		statuses, nil, newRegistrationObserver(store), nil, dirty)
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 2 || items[0].State == "checking" || items[1].State == "checking" {
		t.Fatalf("initial statuses=%#v", items)
	}
	assertCalls := func(repositoryID string, active, ready int32) {
		t.Helper()
		if got := clients[repositoryID].activeCalls.Load(); got != active {
			t.Fatalf("repository %s ActiveClaims calls=%d, want %d", repositoryID, got, active)
		}
		if got := clients[repositoryID].readyCalls.Load(); got != ready {
			t.Fatalf("repository %s ReadyFrontier calls=%d, want %d", repositoryID, got, ready)
		}
	}
	assertCalls(firstRepository, 2, 1)
	assertCalls(secondRepository, 2, 1)

	// A dirty repository query must not observe the earlier-sorted repository.
	dirty.MarkRepository(secondRepository)
	runner.repositoryChanged(ctx)
	assertCalls(firstRepository, 2, 1)
	assertCalls(secondRepository, 4, 2)

	// Many unchanged registration observations remain cheap.
	for range 32 {
		runner.registrationTick(ctx, time.Now())
	}
	assertCalls(firstRepository, 2, 1)
	assertCalls(secondRepository, 4, 2)

	// A repository mark that arrives during an in-flight query must survive the
	// current drain and trigger another pass after that query finishes.
	clients[secondRepository].activeGate = activeGate
	clients[secondRepository].blockNext.Store(true)
	dirty.MarkRepository(secondRepository)
	passDone := make(chan struct{})
	t.Cleanup(func() {
		activeGate.Release()
		cancel()
		waitSteerPass(t, passDone, "blocked repository reconciliation")
	})
	go func() {
		runner.repositoryChanged(ctx)
		close(passDone)
	}()
	waitSteerBarrier(t, activeGate, "second repository Ticket query")
	dirty.MarkRepository(firstRepository)
	activeGate.Release()
	waitSteerPass(t, passDone, "first repository reconciliation pass")
	assertCalls(firstRepository, 2, 1)
	assertCalls(secondRepository, 6, 3)
	runner.repositoryChanged(ctx)
	assertCalls(firstRepository, 4, 2)
}
