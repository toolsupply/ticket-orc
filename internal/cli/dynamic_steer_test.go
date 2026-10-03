package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func testSteerCodexTransport(home string) state.SteerTransportRoute {
	return state.SteerTransportRoute{Kind: "codex-queue", Params: map[string]string{"home": home}}
}

type testSteerTransport struct {
	deliver func(context.Context, string, state.SteerRegistration, steertransport.Message) error
}

func (testSteerTransport) Kind() string { return "codex-queue" }
func (testSteerTransport) Prepare(context.Context, string, state.SteerRegistration) (steertransport.Endpoint, error) {
	return steertransport.Endpoint{Kind: "codex-queue"}, nil
}
func (testSteerTransport) Verify(context.Context, string, state.SteerRegistration) (steertransport.Endpoint, error) {
	return steertransport.Endpoint{Kind: "codex-queue"}, nil
}
func (transport testSteerTransport) Deliver(ctx context.Context, root string, registration state.SteerRegistration, message steertransport.Message) error {
	return transport.deliver(ctx, root, registration, message)
}
func (testSteerTransport) Retire(context.Context, string, state.SteerRegistration) error { return nil }

func testSteerRouter(t *testing.T, deliver func(context.Context, string, state.SteerRegistration, steertransport.Message) error) *steertransport.Router {
	t.Helper()
	router, err := steertransport.NewRouter(testSteerTransport{deliver: deliver})
	if err != nil {
		t.Fatal(err)
	}
	return router
}

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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime, func(state.SteerRegistration) (steerClient, error) {
			clients.Add(1)
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			queued.Add(1)
			return nil
		}), statuses)
	}()
	deadline := time.Now().Add(time.Second)
	for len(statuses.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
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

type sequenceSteerClient struct {
	mu             sync.Mutex
	active         []bool
	activeByQueue  map[string][]bool
	activeQueueIdx map[string]int
	ai, ri         int
	ready          []bool
	readyQueues    []string
	readyFilters   []ticketclient.QueueFilters
}

type countingSteerClient struct {
	activeCalls atomic.Int32
	readyCalls  atomic.Int32
	blockNext   atomic.Bool
	activeEnter chan struct{}
	activeLeave <-chan struct{}
}

func (c *countingSteerClient) ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error) {
	c.activeCalls.Add(1)
	if c.blockNext.CompareAndSwap(true, false) {
		close(c.activeEnter)
		<-c.activeLeave
	}
	return ticketclient.ListResult{}, nil
}

func (c *countingSteerClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	c.readyCalls.Add(1)
	return ticketclient.ListResult{}, nil
}

func (c *countingSteerClient) Close() error { return nil }

type readyBarrierSteerClient struct {
	observed chan struct{}
	release  chan struct{}
	once     sync.Once
}

type policyReloadBarrierSteerClient struct {
	mu       sync.Mutex
	observed chan struct{}
	release  chan struct{}
	filters  []ticketclient.QueueFilters
	first    bool
}

func (c *policyReloadBarrierSteerClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}

func (c *policyReloadBarrierSteerClient) ReadyFrontier(_ context.Context, queue string, filters ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	c.mu.Lock()
	c.filters = append(c.filters, filters)
	first := !c.first
	c.first = true
	c.mu.Unlock()
	if first {
		close(c.observed)
		<-c.release
	}
	return testTicketList("20260926-00002", queue), nil
}

func (c *policyReloadBarrierSteerClient) filterSnapshot() []ticketclient.QueueFilters {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ticketclient.QueueFilters(nil), c.filters...)
}

func (c *policyReloadBarrierSteerClient) Close() error { return nil }

func (c *readyBarrierSteerClient) ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}

func (c *readyBarrierSteerClient) ReadyFrontier(_ context.Context, queue string, _ ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	c.once.Do(func() {
		close(c.observed)
		<-c.release
	})
	return testTicketList("20260926-00002", queue), nil
}

func (c *readyBarrierSteerClient) Close() error { return nil }

type writeFailureThenObservationClient struct {
	calls      atomic.Int32
	secondCall chan struct{}
}

func (c *writeFailureThenObservationClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	if queue == "review" {
		return ticketclient.ListResult{}, nil
	}
	if c.calls.Add(1) == 1 {
		return ticketclient.ListResult{}, nil
	}
	select {
	case <-c.secondCall:
	default:
		close(c.secondCall)
	}
	return ticketclient.ListResult{}, errors.New("injected Ticket observation failure")
}
func (c *writeFailureThenObservationClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	return testTicketList("20260926-00002", "open"), nil
}
func (c *writeFailureThenObservationClient) Close() error { return nil }

func (c *sequenceSteerClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if values, ok := c.activeByQueue[queue]; ok {
		if len(values) == 0 {
			return ticketclient.ListResult{}, nil
		}
		if c.activeQueueIdx == nil {
			c.activeQueueIdx = make(map[string]int)
		}
		i := c.activeQueueIdx[queue]
		if i >= len(values) {
			i = len(values) - 1
		}
		c.activeQueueIdx[queue]++
		if values[i] {
			return testTicketList("20260926-00001", queue), nil
		}
		return ticketclient.ListResult{}, nil
	}
	// Existing sequence-based cases model the role's configured queue (open).
	// Tests for claims in another queue provide activeByQueue explicitly.
	if queue != "open" {
		return ticketclient.ListResult{}, nil
	}
	i := c.ai
	if len(c.active) == 0 {
		return ticketclient.ListResult{}, nil
	}
	if i >= len(c.active) {
		i = len(c.active) - 1
	}
	c.ai++
	if c.active[i] {
		return testTicketList("20260926-00001", queue), nil
	}
	return ticketclient.ListResult{}, nil
}
func (c *sequenceSteerClient) ReadyFrontier(_ context.Context, queue string, filters ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readyQueues = append(c.readyQueues, queue)
	c.readyFilters = append(c.readyFilters, filters)
	i := c.ri
	if len(c.ready) == 0 {
		return ticketclient.ListResult{}, nil
	}
	if i >= len(c.ready) {
		i = len(c.ready) - 1
	}
	c.ri++
	if c.ready[i] {
		return testTicketList("20260926-00002", queue), nil
	}
	return ticketclient.ListResult{}, nil
}
func (c *sequenceSteerClient) Close() error { return nil }

func testTicketList(id, state string) ticketclient.ListResult {
	return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: id, State: state, Assignee: "actor"}}}
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

func TestBoundedSteerTicketObservationsReturnTicketIDs(t *testing.T) {
	client := &sequenceSteerClient{
		activeByQueue: map[string][]bool{"open": {false}, "review": {true}},
		ready:         []bool{true},
	}
	active, err := boundedActorActive(context.Background(), client)
	if err != nil || active != "20260926-00001" {
		t.Fatalf("boundedActorActive()=%q, %v", active, err)
	}
	ready, err := boundedReady(context.Background(), client, "open", ticketclient.QueueFilters{})
	if err != nil || ready != "20260926-00002" {
		t.Fatalf("boundedReady()=%q, %v", ready, err)
	}
}

func TestSteerObservationUsesActorWideBusyAndCompleteRoleFilters(t *testing.T) {
	policy := steerRolePolicy{TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"required"}}, NudgePrompt: "work"}
	busy := &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {true}}, ready: []bool{true}}
	active, err := observeSteerActiveTicket(context.Background(), busy, policy)
	if err != nil || active == "" {
		t.Fatalf("cross-queue active claim = %q, err=%v", active, err)
	}
	ready, err := observeSteerReadyTicket(context.Background(), busy, policy, string(orc.SteerIdle), active)
	if err != nil || ready != "" || busy.ri != 0 {
		t.Fatalf("busy actor ready observation = %q, err=%v; ready calls=%d", ready, err, busy.ri)
	}

	free := &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}, ready: []bool{true}}
	active, err = observeSteerActiveTicket(context.Background(), free, policy)
	if err != nil || active != "" {
		t.Fatalf("free actor active observation = %q, err=%v", active, err)
	}
	ready, err = observeSteerReadyTicket(context.Background(), free, policy, string(orc.SteerIdle), active)
	wantFilters := ticketclient.QueueFilters{Tags: []string{"required"}}
	if err != nil || ready == "" || !reflect.DeepEqual(free.readyQueues, []string{"open"}) || !reflect.DeepEqual(free.readyFilters, []ticketclient.QueueFilters{wantFilters}) {
		t.Fatalf("free actor ready observation=%q err=%v queues=%v filters=%#v", ready, err, free.readyQueues, free.readyFilters)
	}
}

func TestSteerObservationSuppressesNudgeWhenFilteredReadyFrontierIsEmpty(t *testing.T) {
	policy := steerRolePolicy{
		TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend", "urgent"}},
		NudgePrompt: "backend urgent work",
	}
	client := &sequenceSteerClient{
		activeByQueue: map[string][]bool{"open": {false}, "review": {false}},
		ready:         []bool{false},
	}
	active, err := observeSteerActiveTicket(context.Background(), client, policy)
	if err != nil || active != "" {
		t.Fatalf("active claim=%q err=%v", active, err)
	}
	ready, err := observeSteerReadyTicket(context.Background(), client, policy, string(orc.SteerIdle), active)
	wantFilters := []ticketclient.QueueFilters{{Tags: []string{"backend", "urgent"}}}
	if err != nil || ready != "" || !reflect.DeepEqual(client.readyFilters, wantFilters) {
		t.Fatalf("filtered ready ticket=%q err=%v filters=%#v, want no eligible work under %#v", ready, err, client.readyFilters, wantFilters)
	}
}

func TestSteerPolicyReplacementWaitsForReservedDispatch(t *testing.T) {
	store := newSteerPolicyStore(map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "old"}})
	snapshot := store.Snapshot()
	started := make(chan struct{})
	release := make(chan struct{})
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		current, err := store.WithGeneration(snapshot.generation, func() error {
			close(started)
			<-release
			return nil
		})
		if !current || err != nil {
			t.Errorf("reserved old-generation dispatch current=%t err=%v", current, err)
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not reserve its policy generation")
	}
	type replacement struct {
		generation uint64
		changed    bool
	}
	replaced := make(chan replacement, 1)
	go func() {
		generation, changed := store.Replace(map[string]steerRolePolicy{"coder": {TicketQueue: "review", NudgePrompt: "new"}})
		replaced <- replacement{generation: generation, changed: changed}
	}()
	select {
	case result := <-replaced:
		t.Fatalf("policy generation %+v committed while old dispatch was reserved", result)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-dispatchDone
	select {
	case result := <-replaced:
		if !result.changed || result.generation != snapshot.generation+1 {
			t.Fatalf("replacement=%+v, want generation %d and changed", result, snapshot.generation+1)
		}
	case <-time.After(time.Second):
		t.Fatal("policy replacement did not finish after dispatch release")
	}
	if current, err := store.WithGeneration(snapshot.generation, func() error { return nil }); current || err != nil {
		t.Fatalf("old policy dispatch remained available after commit: current=%t err=%v", current, err)
	}
}

func TestConcurrentEquivalentSteerPolicyReplacementAdvancesGenerationOnce(t *testing.T) {
	store := newSteerPolicyStore(map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "old"}})
	snapshot := store.Snapshot()
	started := make(chan struct{})
	release := make(chan struct{})
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		current, err := store.WithGeneration(snapshot.generation, func() error {
			close(started)
			<-release
			return nil
		})
		if !current || err != nil {
			t.Errorf("reserved dispatch current=%t err=%v", current, err)
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not reserve its policy generation")
	}

	type replacement struct {
		generation uint64
		changed    bool
	}
	results := make(chan replacement, 2)
	for range 2 {
		go func() {
			generation, changed := store.Replace(map[string]steerRolePolicy{"coder": {TicketQueue: "review", NudgePrompt: "new"}})
			results <- replacement{generation: generation, changed: changed}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-dispatchDone
	first, second := <-results, <-results
	if first.generation != snapshot.generation+1 || second.generation != snapshot.generation+1 || first.changed == second.changed {
		t.Fatalf("replacements=%+v %+v, want one change at generation %d", first, second, snapshot.generation+1)
	}
}

func TestSteerPolicyReplacementIgnoresCanonicalEquivalentPolicies(t *testing.T) {
	initial := map[string]steerRolePolicy{
		"coder":   {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend", "urgent"}}, NudgePrompt: "work"},
		"quality": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"no-review"}}, NudgePrompt: "review", ReviewCompletion: ReviewCompletionSignoff},
	}
	store := newSteerPolicyStore(initial)
	before := store.Snapshot()
	identicalGeneration, identicalChanged := store.Replace(initial)
	if identicalChanged || identicalGeneration != before.generation {
		t.Fatalf("identical replacement changed policy: changed=%t generation=%d before=%#v", identicalChanged, identicalGeneration, before)
	}
	candidate := map[string]steerRolePolicy{
		"coder":   {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"urgent", "backend"}, WithoutTags: []string{}}, NudgePrompt: "work"},
		"quality": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"no-review"}}, NudgePrompt: "review", ReviewCompletion: ReviewCompletionSignoff},
	}
	generation, changed := store.Replace(candidate)
	after := store.Snapshot()
	if changed || generation != before.generation || after.generation != before.generation || !reflect.DeepEqual(after.roles, before.roles) {
		t.Fatalf("canonical-equivalent replacement changed policy: changed=%t generation=%d before=%#v after=%#v", changed, generation, before, after)
	}
}

func TestSteerPolicyReplacementDetectsEveryEffectivePolicyChange(t *testing.T) {
	base := func() map[string]steerRolePolicy {
		return map[string]steerRolePolicy{
			"coder":   {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend"}}, NudgePrompt: "work"},
			"quality": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"no-review"}}, NudgePrompt: "review", ReviewCompletion: ReviewCompletionSignoff},
		}
	}
	tests := []struct {
		name   string
		mutate func(map[string]steerRolePolicy)
	}{
		{name: "ticket queue", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["coder"]
			policy.TicketQueue = "review"
			roles["coder"] = policy
		}},
		{name: "required tag", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["coder"]
			policy.QueueFilters.Tags = []string{"frontend"}
			roles["coder"] = policy
		}},
		{name: "excluded review tag", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["quality"]
			policy.QueueFilters.WithoutTags = []string{"no-security-review"}
			roles["quality"] = policy
		}},
		{name: "nudge prompt", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["coder"]
			policy.NudgePrompt = "new work"
			roles["coder"] = policy
		}},
		{name: "review completion", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["quality"]
			policy.ReviewCompletion = ReviewCompletionClose
			roles["quality"] = policy
		}},
		{name: "role added", mutate: func(roles map[string]steerRolePolicy) {
			roles["triage"] = steerRolePolicy{TicketQueue: "open", NudgePrompt: "triage"}
		}},
		{name: "role removed", mutate: func(roles map[string]steerRolePolicy) { delete(roles, "quality") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSteerPolicyStore(base())
			before := store.Snapshot()
			candidate := base()
			test.mutate(candidate)
			generation, changed := store.Replace(candidate)
			after := store.Snapshot()
			if !changed || generation != before.generation+1 || after.generation != before.generation+1 || !reflect.DeepEqual(after.roles, candidate) {
				t.Fatalf("replacement changed=%t generation=%d snapshot=%#v candidate=%#v", changed, generation, after, candidate)
			}
		})
	}
}

func TestPlanSteerNotificationSeparatesControlAndWork(t *testing.T) {
	tests := []struct {
		name               string
		policy             steerRolePolicy
		state              string
		activeTicket       string
		readyTicket        string
		recoveryPending    bool
		bootstrapPending   bool
		wantSend           bool
		wantWork           bool
		wantCompletion     string
		wantMessageParts   []string
		forbidMessageParts []string
	}{
		{
			name:   "idle with no pending notification",
			policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"},
			state:  string(orc.SteerIdle),
		},
		{
			name:   "bootstrap is control only",
			policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"},
			state:  string(orc.SteerIdle), bootstrapPending: true,
			wantSend: true, wantCompletion: string(orc.SteerIdle),
			wantMessageParts: []string{steerSessionBootstrapPrompt},
		},
		{
			name:   "ready work includes bootstrap and nudge",
			policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"},
			state:  string(orc.SteerIdle), readyTicket: "20260926-00001", bootstrapPending: true,
			wantSend: true, wantWork: true, wantCompletion: string(orc.SteerQueued),
			wantMessageParts: []string{steerSessionBootstrapPrompt, "open work"},
		},
		{
			name:   "recovery with active claim omits ready-only reviewer prose",
			policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review work", ReviewCompletion: ReviewCompletionClose},
			state:  string(orc.SteerIdle), activeTicket: "20260926-00002", recoveryPending: true,
			wantSend: true, wantWork: false, wantCompletion: string(orc.SteerQueued),
			wantMessageParts:   []string{steerClaimRecoveryPrompt},
			forbidMessageParts: []string{"review work", "approve and close"},
		},
		{
			name:   "review ready work includes completion policy",
			policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review work"},
			state:  string(orc.SteerIdle), readyTicket: "20260926-00003",
			wantSend: true, wantWork: true, wantCompletion: string(orc.SteerQueued),
			wantMessageParts: []string{"review work", "approve the ticket to signoff"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := planSteerNotification(test.policy, test.state, test.activeTicket, test.readyTicket, test.recoveryPending, test.bootstrapPending)
			if plan.send != test.wantSend || plan.workBearing != test.wantWork || plan.completionState != test.wantCompletion {
				t.Fatalf("plan send/work/completion = %t/%t/%q, want %t/%t/%q", plan.send, plan.workBearing, plan.completionState, test.wantSend, test.wantWork, test.wantCompletion)
			}
			for _, part := range test.wantMessageParts {
				if !strings.Contains(plan.message, part) {
					t.Fatalf("plan message %q omitted %q", plan.message, part)
				}
			}
			for _, part := range test.forbidMessageParts {
				if strings.Contains(plan.message, part) {
					t.Fatalf("plan message %q unexpectedly included %q", plan.message, part)
				}
			}
			if !test.wantSend && plan.message != "" {
				t.Fatalf("idle plan unexpectedly contains message %q", plan.message)
			}
			if test.name == "bootstrap is control only" && strings.Contains(plan.message, test.policy.NudgePrompt) {
				t.Fatalf("control-only bootstrap included work nudge: %q", plan.message)
			}
		})
	}
}

type countingSteerRuntime struct {
	inner      steerRuntimePersistence
	snapshots  atomic.Int32
	reconciles atomic.Int32
	completed  chan struct{}
}

func (s *countingSteerRuntime) Snapshot(ctx context.Context) (state.SteerRuntimeSnapshot, error) {
	s.snapshots.Add(1)
	return s.inner.Snapshot(ctx)
}
func (s *countingSteerRuntime) Reconcile(ctx context.Context, registrations []state.SteerRegistration) error {
	s.reconciles.Add(1)
	return s.inner.Reconcile(ctx, registrations)
}
func (s *countingSteerRuntime) Update(ctx context.Context, reg state.SteerRegistration, stateName, code string) (bool, error) {
	return s.inner.Update(ctx, reg, stateName, code)
}
func (s *countingSteerRuntime) CompleteDelivery(ctx context.Context, reg state.SteerRegistration, nextState string, bootstrap, recovery bool) (bool, error) {
	current, err := s.inner.CompleteDelivery(ctx, reg, nextState, bootstrap, recovery)
	if current && err == nil && nextState == string(orc.SteerQueued) && s.completed != nil {
		select {
		case s.completed <- struct{}{}:
		default:
		}
	}
	return current, err
}
func (s *countingSteerRuntime) ConfirmNoActiveClaim(ctx context.Context, reg state.SteerRegistration) (bool, error) {
	return s.inner.ConfirmNoActiveClaim(ctx, reg)
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithPersistence(ctx, dir, map[string]steerRolePolicy{"coder": {
			TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend"}}, NudgePrompt: "wake",
		}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
			}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }), statuses, persistence)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 2 && items[0].State == "none" && items[1].State == "none" &&
			items[0].EffectiveTicketQueue == "open" && reflect.DeepEqual(items[0].EffectiveTicketTags, []string{"backend"})
	})
	cancel()
	<-done
	if got := persistence.reconciles.Load(); got != 1 {
		t.Fatalf("Reconcile called %d times for one tick, want 1", got)
	}
	if got := persistence.snapshots.Load(); got != 1 {
		t.Fatalf("Snapshot called %d times for two registrations in one tick, want 1", got)
	}
}

type failingSteerRuntime struct {
	inner *state.SteerRuntimeStore
	fail  string
}

func (s failingSteerRuntime) Snapshot(ctx context.Context) (state.SteerRuntimeSnapshot, error) {
	return s.inner.Snapshot(ctx)
}
func (s failingSteerRuntime) Reconcile(ctx context.Context, registrations []state.SteerRegistration) error {
	return s.inner.Reconcile(ctx, registrations)
}
func (s failingSteerRuntime) Update(ctx context.Context, reg state.SteerRegistration, stateName, code string) (bool, error) {
	if stateName == s.fail {
		return false, errors.New("injected persistence error")
	}
	return s.inner.Update(ctx, reg, stateName, code)
}
func (s failingSteerRuntime) CompleteDelivery(ctx context.Context, reg state.SteerRegistration, nextState string, bootstrap, recovery bool) (bool, error) {
	if s.fail == "queued" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.CompleteDelivery(ctx, reg, nextState, bootstrap, recovery)
}
func (s failingSteerRuntime) ConfirmNoActiveClaim(ctx context.Context, reg state.SteerRegistration) (bool, error) {
	if s.fail == "recovery" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.ConfirmNoActiveClaim(ctx, reg)
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
	registrationTicks := make(chan time.Time)
	dirty := newSteerDirtySet()
	statuses := &dynamicSteerStatus{}
	activeEnter := make(chan struct{})
	activeLeave := make(chan struct{})
	var releaseOnce sync.Once
	releaseBlockedQuery := func() { releaseOnce.Do(func() { close(activeLeave) }) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithSchedule(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
			func(reg state.SteerRegistration) (steerClient, error) { return clients[reg.RepositoryID], nil }, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }),
			statuses, nil, newRegistrationObserver(store), nil, dirty, registrationTicks)
	}()
	t.Cleanup(func() { releaseBlockedQuery(); cancel(); <-done })
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 2 && items[0].State != "checking" && items[1].State != "checking"
	})
	waitSteerCallCount(t, clients[firstRepository], 2, 1)
	waitSteerCallCount(t, clients[secondRepository], 2, 1)

	// A dirty repository query must not observe the earlier-sorted repository.
	dirty.MarkRepository(secondRepository)
	waitSteerCallCount(t, clients[secondRepository], 4, 2)
	if got := clients[firstRepository].activeCalls.Load(); got != 2 {
		t.Fatalf("unrelated repository ActiveClaims calls=%d, want 2", got)
	}
	if got := clients[firstRepository].readyCalls.Load(); got != 1 {
		t.Fatalf("unrelated repository ReadyFrontier calls=%d, want 1", got)
	}

	// Many unchanged registration observations remain cheap. Unbuffered ticks
	// make each scheduler pass complete before the next tick is accepted.
	for range 32 {
		registrationTicks <- time.Now()
	}
	registrationTicks <- time.Now() // barrier after the final observation
	if got := clients[firstRepository].activeCalls.Load(); got != 2 {
		t.Fatalf("unchanged registration observation queried ActiveClaims %d times", got)
	}
	if got := clients[secondRepository].readyCalls.Load(); got != 2 {
		t.Fatalf("unchanged registration observation queried ReadyFrontier %d times, want 2", got)
	}

	// A repository mark that arrives during an in-flight query must survive the
	// current drain and trigger another pass after that query finishes.
	clients[secondRepository].activeEnter = activeEnter
	clients[secondRepository].activeLeave = activeLeave
	clients[secondRepository].blockNext.Store(true)
	dirty.MarkRepository(secondRepository)
	select {
	case <-activeEnter:
	case <-time.After(2 * time.Second):
		t.Fatal("second repository reconcile did not reach the blocking Ticket query")
	}
	dirty.MarkRepository(firstRepository)
	releaseBlockedQuery()
	waitSteerCallCount(t, clients[firstRepository], 4, 2)

}

func TestDynamicSteerDirtyWakeRefreshesRegistrationIncarnation(t *testing.T) {
	dir := t.TempDir()
	registrationStore := state.NewRegistrationStore(dir)
	registration := state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	}
	registration, _, _, err := registrationStore.Join(context.Background(), registration)
	if err != nil {
		t.Fatal(err)
	}
	runtimeStore := state.NewSteerRuntimeStore(dir)
	if err := runtimeStore.Reconcile(context.Background(), []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtimeStore.CompleteDelivery(context.Background(), registration, string(orc.SteerQueued), true, true); err != nil || !updated {
		t.Fatalf("seed prior queued delivery updated=%t err=%v", updated, err)
	}
	oldClient := &countingSteerClient{}
	currentClient := &countingSteerClient{}
	clients := map[string]steerClient{
		registration.SessionID:                 oldClient,
		"01a0dcb8-aa3a-78d3-87ba-b5972a10e2a5": currentClient,
	}
	queued := make(chan string, 1)
	dirty := newSteerDirtySet()
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithSchedule(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
			func(current state.SteerRegistration) (steerClient, error) { return clients[current.SessionID], nil },
			testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, _ steertransport.Message) error {
				queued <- registration.SessionID
				return nil
			}), statuses, runtimeStore,
			newRegistrationObserver(registrationStore), nil, dirty, make(chan time.Time))
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == string(orc.SteerQueued)
	})
	waitSteerCallCount(t, oldClient, 2, 0)

	newRegistration := registration
	newRegistration.SessionID = "01a0dcb8-aa3a-78d3-87ba-b5972a10e2a5"
	newRegistration, previous, changed, err := registrationStore.Join(context.Background(), newRegistration)
	if err != nil || !changed || previous == nil || newRegistration.RegistrationID == registration.RegistrationID {
		t.Fatalf("replace registration=%#v previous=%#v changed=%t err=%v", newRegistration, previous, changed, err)
	}
	// No registration tick is sent: the repository event must refresh the
	// registration snapshot before reconciling or delivering.
	dirty.MarkRepository(registration.RepositoryID)
	select {
	case thread := <-queued:
		if thread != newRegistration.SessionID {
			t.Fatalf("notification thread=%q, want current registration %q", thread, newRegistration.SessionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconcile trigger did not use the replacement registration")
	}
	waitSteerCallCount(t, currentClient, 2, 1)
	if got := oldClient.activeCalls.Load(); got != 2 {
		t.Fatalf("stale registration received %d additional Ticket observations", got-2)
	}
	deliveries, err := runtimeStore.Snapshot(context.Background())
	if err != nil || len(deliveries.Deliveries) != 1 || deliveries.Deliveries[0].RegistrationID != newRegistration.RegistrationID || deliveries.Deliveries[0].SessionID != newRegistration.SessionID {
		t.Fatalf("stale delivery was not retired: snapshot=%#v err=%v", deliveries, err)
	}
}

func TestChangedSteerRepositoriesDetectsJoinRemovalAndReplacement(t *testing.T) {
	previous := []state.SteerRegistration{
		{RepositoryID: "11111111-1111-4111-8111-111111111111", Actor: "replaced", Role: "coder", RepositoryPath: "/repo/old", SessionID: "old-thread"},
		{RepositoryID: "22222222-2222-4222-8222-222222222222", Actor: "removed", Role: "coder"},
	}
	current := []state.SteerRegistration{
		{RepositoryID: "11111111-1111-4111-8111-111111111111", Actor: "replaced", Role: "reviewer", RepositoryPath: "/repo/new", SessionID: "new-thread"},
		{RepositoryID: "33333333-3333-4333-8333-333333333333", Actor: "joined", Role: "coder"},
	}
	changed := changedSteerRepositories(previous, current)
	for _, repositoryID := range []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
	} {
		if _, found := changed[repositoryID]; !found {
			t.Errorf("registration change for repository %s was not marked dirty: %#v", repositoryID, changed)
		}
	}
}

func waitSteerCallCount(t *testing.T, client *countingSteerClient, active, ready int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client.activeCalls.Load() >= active && client.readyCalls.Load() >= ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Ticket calls active=%d ready=%d, want at least %d/%d", client.activeCalls.Load(), client.readyCalls.Load(), active, ready)
}

func TestDynamicSteerPublishesCorruptRuntimeState(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "steer-runtime.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	var clients atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			clients.Add(1)
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			t.Error("corrupt runtime state allowed queue")
			return nil
		}), statuses)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "degraded" && items[0].Code == "runtime_state_unavailable"
	})
	cancel()
	<-done
	if clients.Load() != 0 {
		t.Fatalf("Ticket clients created for corrupt runtime state: %d", clients.Load())
	}
}

func TestDynamicSteerReportsFailedSendingPersistenceWithoutQueue(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	go func() {
		defer close(done)
		runDynamicSteerWithPersistence(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			queued.Add(1)
			return nil
		}), statuses, store)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "runtime_state_write_failed"
	})
	cancel()
	<-done
	if queued.Load() != 0 {
		t.Fatalf("queued %d wake(s) without persisted sending state", queued.Load())
	}
	snapshot, err := store.inner.Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != "none" {
		t.Fatalf("persisted state=%#v err=%v", snapshot, err)
	}
}

func TestDynamicSteerWriteFailureSurvivesTicketObservationFailure(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	client := &writeFailureThenObservationClient{secondCall: make(chan struct{})}
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	go func() {
		defer close(done)
		runDynamicSteerWithDirtySet(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil }, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			t.Error("queue called without persisted sending state")
			return nil
		}), statuses, store, dirty)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "runtime_state_write_failed"
	})
	dirty.MarkAll()
	select {
	case <-client.secondCall:
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatal("second Ticket observation did not run")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "runtime_state_write_failed"
	})
	cancel()
	<-done
}

type terminalObservationSteerClient struct {
	closed atomic.Bool
}

func (c *terminalObservationSteerClient) ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, ticketclient.ErrTransport
}
func (c *terminalObservationSteerClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}
func (c *terminalObservationSteerClient) Close() error {
	c.closed.Store(true)
	return nil
}

func TestDynamicSteerReconnectsAfterTerminalTicketObservationFailure(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	_, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	failedClient := &terminalObservationSteerClient{}
	var clients atomic.Int32
	reconnected := make(chan struct{})
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithDirtySet(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			if clients.Add(1) == 1 {
				return failedClient, nil
			}
			select {
			case <-reconnected:
			default:
				close(reconnected)
			}
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
		}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			if message.Kind != steertransport.MessageSteer {
				t.Errorf("notification kind=%q, want steer", message.Kind)
			}
			if !strings.Contains(message.Text, steerSessionBootstrapPrompt) {
				t.Errorf("initial notification omitted bootstrap: %q", message.Text)
			}
			return nil
		}), statuses, nil, dirty)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "ticket_observation_failed"
	})
	dirty.MarkAll()
	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("dirty reconciliation did not recreate the failed Ticket client")
	}
	if clients.Load() < 2 {
		t.Fatalf("Ticket client factory called %d time(s), want reconnect after terminal failure", clients.Load())
	}
	if !failedClient.closed.Load() {
		t.Fatal("terminal Ticket client was not closed when discarded")
	}
}

func TestDynamicSteerManagedOwnerConflictSurvivesPendingWriteFailure(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "shared-actor", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	runtime := NewRuntimeState([]supervisor.RunWorker{})
	dirty := newSteerDirtySet()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	go func() {
		defer close(done)
		runDynamicSteerWithDirtySet(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			queued.Add(1)
			return nil
		}), statuses, store, dirty)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "runtime_state_write_failed"
	})
	worker := supervisor.RunWorker{Name: "managed", Config: supervisor.RoleConfig{Actor: reg.Actor, RepositoryIdentity: reg.RepositoryID}}
	runtime.SetConfiguredWorkers([]supervisor.RunWorker{worker})
	runtime.SetEffectiveWorker(worker)
	runtime.Set(supervisor.WorkerTransition{Worker: worker.Name, State: supervisor.WorkerRunning})
	dirty.MarkAll()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "conflict" && items[0].Code == "managed_owner" && items[0].PersistenceCode == "runtime_state_write_failed"
	})
	cancel()
	<-done
	if queued.Load() != 0 {
		t.Fatalf("queued %d wake(s) despite managed ownership conflict", queued.Load())
	}
}

func waitSteerStatus(t *testing.T, statuses *dynamicSteerStatus, check func([]daemon.SteerStatus) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if items := statuses.snapshot(); check(items) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scheduler status did not match: %#v", statuses.snapshot())
}

func TestDynamicSteerSendsSecondWakeAfterPositiveClaimEvidence(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5"}
	reg, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	client := &sequenceSteerClient{active: []bool{false, true, false}, ready: []bool{true, true}}
	queued := make(chan string, 3)
	persistence := &countingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), completed: make(chan struct{}, 2)}
	dirty := newSteerDirtySet()
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithDirtySet(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "configured prompt"}}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil }, testSteerRouter(t, func(queueCtx context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			select {
			case queued <- message.Text:
				return nil
			case <-queueCtx.Done():
				return queueCtx.Err()
			}
		}), statuses, persistence, dirty)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		if !strings.Contains(message, "configured prompt") || !strings.Contains(message, steerSessionBootstrapPrompt) {
			t.Fatalf("first queued message=%q", message)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("scheduler did not send initial wake")
	}
	select {
	case <-persistence.completed:
	case <-time.After(4 * time.Second):
		t.Fatal("initial wake was not durably recorded before reconciliation")
	}
	dirty.MarkAll()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == string(orc.SteerConsumed)
	})
	dirty.MarkAll()
	select {
	case message := <-queued:
		if !strings.Contains(message, "configured prompt") || strings.Contains(message, steerSessionBootstrapPrompt) {
			t.Fatalf("second queued message=%q", message)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("scheduler did not send wake after observed claim ended")
	}
	select {
	case <-persistence.completed:
	case <-time.After(4 * time.Second):
		t.Fatal("second wake was not durably recorded as queued before cancellation")
	}
	snapshot, err := persistence.Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != string(orc.SteerQueued) {
		t.Fatalf("delivery was not durably queued before cancellation: snapshot=%#v err=%v", snapshot, err)
	}
	cancel()
	<-done
	select {
	case extra := <-queued:
		t.Fatalf("unexpected duplicate wake %q", extra)
	default:
	}
	snapshot, err = persistence.Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != string(orc.SteerQueued) {
		t.Fatalf("delivery snapshot=%#v err=%v", snapshot, err)
	}
}

func TestDynamicSteerPublishesNotificationDeliveryWithReadyTicketID(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	statuses := &dynamicSteerStatus{}
	events := make(chan daemon.Event, 8)
	statuses.setPublisher(func(event daemon.Event) { events <- event })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
			},
			testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }), statuses)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "queued"
	})
	cancel()
	<-done
	var observed map[string]daemon.Event
	observed = make(map[string]daemon.Event)
	for {
		select {
		case event := <-events:
			if event.Type == "steer.delivery" {
				observed[event.State] = event
			}
		default:
			if sending, ok := observed["sending"]; !ok || sending.Ticket != "20260926-00002" {
				t.Fatalf("sending delivery event=%#v", sending)
			}
			if queued, ok := observed["queued"]; !ok || queued.Ticket != "20260926-00002" {
				t.Fatalf("queued delivery event=%#v", queued)
			}
			return
		}
	}
}

func TestDynamicSteerBootstrapsFirstRegistrationOnce(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "without work"
		if ready {
			name = "with ready work"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			reg, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
				Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
			})
			if err != nil {
				t.Fatal(err)
			}
			queued := make(chan string, 4)
			statuses := &dynamicSteerStatus{}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "role work nudge"}}, nil,
					func(state.SteerRegistration) (steerClient, error) {
						return &sequenceSteerClient{active: []bool{false}, ready: []bool{ready}}, nil
					},
					testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, message steertransport.Message) error {
						if registration.SessionID != reg.SessionID {
							t.Errorf("bootstrap sent to session %q, want %q", registration.SessionID, reg.SessionID)
						}
						queued <- message.Text
						return nil
					}), statuses)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			select {
			case message := <-queued:
				if !strings.Contains(message, steerSessionBootstrapPrompt) || strings.Contains(message, steerClaimRecoveryPrompt) || strings.Contains(message, "role work nudge") != ready {
					t.Fatalf("first-registration message=%q ready=%t", message, ready)
				}
				if !ready && message != steerSessionBootstrapPrompt {
					t.Fatalf("idle new session received content beyond bootstrap: %q", message)
				}
				if ready && (strings.Index(message, steerSessionBootstrapPrompt) > strings.Index(message, "role work nudge")) {
					t.Fatalf("ready work preceded bootstrap: %q", message)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first registration did not receive its bootstrap")
			}
			wantState := "none"
			if ready {
				wantState = "queued"
			}
			waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
				return len(items) == 1 && items[0].State == wantState
			})
			snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].BootstrapPending || snapshot.Deliveries[0].State != wantState {
				t.Fatalf("bootstrap delivery snapshot=%#v err=%v", snapshot, err)
			}
			time.Sleep(steerPollInterval + 25*time.Millisecond)
			if len(queued) != 0 {
				t.Fatalf("scheduler repeated bootstrap: %q", <-queued)
			}
			cancel()
			<-done

			restartedQueue := make(chan string, 1)
			restartStatuses := &dynamicSteerStatus{}
			restartCtx, restartCancel := context.WithCancel(context.Background())
			restartDone := make(chan struct{})
			go func() {
				defer close(restartDone)
				runDynamicSteer(restartCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "role work nudge"}}, nil,
					func(state.SteerRegistration) (steerClient, error) {
						return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
					},
					testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
						restartedQueue <- message.Text
						return nil
					}), restartStatuses)
			}()
			t.Cleanup(func() {
				restartCancel()
				<-restartDone
			})
			waitSteerStatus(t, restartStatuses, func(items []daemon.SteerStatus) bool {
				return len(items) == 1 && items[0].State == wantState
			})
			time.Sleep(steerPollInterval + 25*time.Millisecond)
			if len(restartedQueue) != 0 {
				t.Fatalf("daemon restart repeated bootstrap: %q", <-restartedQueue)
			}
			restartCancel()
			<-restartDone
		})
	}
}

func TestDynamicSteerSuppressionPreservesPendingDeliveryUntilResume(t *testing.T) {
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	controlStore := state.NewDaemonControlStore(dir)
	paused, err := controlStore.PauseDaemon(context.Background())
	if err != nil || paused.Mode != state.DaemonPaused {
		t.Fatalf("persist pause = %#v, %v", paused, err)
	}
	gate := newDispatchGate(controlStore, paused.Mode)
	dirty := newSteerDirtySet()
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithGate(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "work"}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
			},
			testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				queued <- message.Text
				return nil
			}),
			statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), gate, dirty)
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Code == "daemon_paused"
	})
	if len(queued) != 0 {
		t.Fatalf("paused daemon queued %q", <-queued)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("paused runtime snapshot = %#v, %v", snapshot, err)
	}
	delivery := snapshot.Deliveries[0]
	if delivery.State != string(orc.SteerIdle) || !delivery.BootstrapPending || delivery.RecoveryPending {
		t.Fatalf("paused delivery was consumed or mutated: %#v", delivery)
	}
	if _, _, err := gate.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	dirty.MarkAll()
	select {
	case message := <-queued:
		if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, "work") {
			t.Fatalf("resumed message = %q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("resume did not deliver pending bootstrap for %s", registration.SessionID)
	}
}

func TestPauseWhileSteerReadyObservationIsBlockedSuppressesNotification(t *testing.T) {
	dir := t.TempDir()
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	}); err != nil {
		t.Fatal(err)
	}
	controlStore := state.NewDaemonControlStore(dir)
	gate := newDispatchGate(controlStore, state.DaemonRunning)
	client := &readyBarrierSteerClient{observed: make(chan struct{}), release: make(chan struct{})}
	queued := make(chan string, 1)
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithGate(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "work"}}, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				queued <- message.Text
				return nil
			}),
			statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), gate, dirty)
	}()
	released := false
	releaseReady := func() {
		if !released {
			close(client.release)
			released = true
		}
	}
	t.Cleanup(func() { releaseReady(); cancel(); <-done })

	select {
	case <-client.observed:
	case <-time.After(time.Second):
		t.Fatal("Ticket ready frontier was not observed")
	}
	// Pause while the ready-frontier query is in flight. When its ready result
	// returns, the worker must still observe the closed dispatch gate.
	mode, applied, err := gate.Pause(context.Background())
	if err != nil || !applied || mode.Mode != state.DaemonPaused {
		t.Fatalf("pause mode=%#v applied=%t err=%v", mode, applied, err)
	}
	releaseReady()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Code == "daemon_paused"
	})
	if len(queued) != 0 {
		t.Fatalf("ready work queued after pause succeeded: %q", <-queued)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("paused delivery snapshot=%#v err=%v", snapshot, err)
	}
	if delivery := snapshot.Deliveries[0]; delivery.State != string(orc.SteerIdle) || !delivery.BootstrapPending {
		t.Fatalf("paused ready work consumed pending delivery: %#v", delivery)
	}

	if _, _, err := gate.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	dirty.MarkAll()
	select {
	case message := <-queued:
		if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, "work") {
			t.Fatalf("resumed notification=%q", message)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not reconcile the observed ready work without rejoin")
	}
}

func TestPauseAfterReadyObservationBlocksSteerQueueReservation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewDaemonControlStore(dir)
	runtimeStore := state.NewSteerRuntimeStore(dir)
	if err := runtimeStore.Reconcile(ctx, []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	gate := newDispatchGate(store, state.DaemonRunning)
	client := &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}
	policy := steerRolePolicy{TicketQueue: "open", NudgePrompt: "work"}
	active, err := observeSteerActiveTicket(ctx, client, policy)
	if err != nil || active != "" {
		t.Fatalf("active observation=%q err=%v", active, err)
	}
	ready, err := observeSteerReadyTicket(ctx, client, policy, string(orc.SteerIdle), active)
	if err != nil || ready != "20260926-00002" {
		t.Fatalf("ready observation=%q err=%v", ready, err)
	}
	plan := planSteerNotification(policy, string(orc.SteerIdle), active, ready, false, true)
	if !plan.send {
		t.Fatal("observed ready work did not produce a send plan")
	}
	if _, _, err := gate.Pause(ctx); err != nil {
		t.Fatalf("pause after ready observation: %v", err)
	}
	var queueCalls atomic.Int32
	router := testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
		queueCalls.Add(1)
		return nil
	})
	statuses := &dynamicSteerStatus{}
	writeFailures, persisted := map[string]string{}, map[string]string{}
	allowed, err := withSteerDispatch(gate, func() error {
		current, err := currentSteerRegistration(ctx, state.NewRegistrationStore(dir), registration)
		if err != nil {
			return err
		}
		if !current {
			return errors.New("registration was replaced")
		}
		sendSteerNotification(ctx, dir, registration, plan, router, runtimeStore, statuses, writeFailures, persisted)
		return nil
	})
	if err != nil || allowed || queueCalls.Load() != 0 {
		t.Fatalf("dispatch after pause allowed=%t queue calls=%d err=%v", allowed, queueCalls.Load(), err)
	}
	snapshot, err := runtimeStore.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("delivery snapshot=%#v err=%v", snapshot, err)
	}
	if delivery := snapshot.Deliveries[0]; delivery.State != string(orc.SteerIdle) || !delivery.BootstrapPending {
		t.Fatalf("dispatch suppression consumed delivery: %#v", delivery)
	}
}

func TestDynamicSteerIdleBootstrapThenReadyQueuesOneRoleNudge(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	_, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued := make(chan string, 3)
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	steerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithDirtySet(steerCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "configured role nudge"}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{false, true}}, nil
			},
			testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				queued <- message.Text
				return nil
			}), statuses, nil, dirty)
	}()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case message := <-queued:
		if message != steerSessionBootstrapPrompt {
			t.Fatalf("first idle message=%q, want bootstrap only", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle registration did not receive bootstrap")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none"
	})
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != "none" || snapshot.Deliveries[0].BootstrapPending {
		t.Fatalf("bootstrap-only state=%#v err=%v", snapshot.Deliveries, err)
	}
	dirty.MarkAll()
	select {
	case message := <-queued:
		if message != "configured role nudge" {
			t.Fatalf("later ready message=%q, want configured role nudge without bootstrap", message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("later ready work did not queue a role nudge")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "queued"
	})
	time.Sleep(steerPollInterval + 25*time.Millisecond)
	if len(queued) != 0 {
		t.Fatalf("scheduler queued more than one role nudge: %q", <-queued)
	}
}

func TestDynamicSteerSuppressesReadyObservedUnderSupersededPolicy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := &policyReloadBarrierSteerClient{observed: make(chan struct{}), release: make(chan struct{})}
	policies := newSteerPolicyStore(map[string]steerRolePolicy{
		"coder": {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"old"}}, NudgePrompt: "old policy"},
	})
	dirty := newSteerDirtySet()
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	initialStatus := registrationStartupStatuses([]state.SteerRegistration{registration}, "")
	initialStatus[0].State = string(orc.SteerIdle)
	applySteerStatusPolicy(&initialStatus[0], policies.Snapshot().roles[registration.Role])
	statuses.replace(initialStatus)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithPolicyStoreGate(runCtx, dir, policies, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				queued <- message.Text
				return nil
			}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	}()
	released := false
	release := func() {
		if !released {
			close(client.release)
			released = true
		}
	}
	t.Cleanup(func() { release(); cancel(); <-done })
	select {
	case <-client.observed:
	case <-time.After(time.Second):
		t.Fatal("old policy ready query did not start")
	}
	policies.Replace(map[string]steerRolePolicy{
		"coder": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"new"}, WithoutTags: []string{"blocked"}}, NudgePrompt: "new policy"},
	})
	dirty.MarkAll()
	// The old Ticket query is still blocked. Exercise the same projection used
	// by the daemon Status callback before the asynchronous loop can refresh its
	// cached registration status.
	status := runtimeDaemonStatusWithSteerPolicies(nil, statuses.snapshot(), policies)
	if len(status.Steer) != 1 || status.Steer[0].State != string(orc.SteerIdle) || status.Steer[0].EffectiveTicketQueue != "review" ||
		!reflect.DeepEqual(status.Steer[0].EffectiveTicketTags, []string{"new"}) || !reflect.DeepEqual(status.Steer[0].EffectiveReviewSkipTags, []string{"blocked"}) {
		t.Fatalf("status boundary did not project committed policy over blocked old observation: %#v", status.Steer)
	}
	cached := statuses.snapshot()
	if len(cached) != 1 || cached[0].EffectiveTicketQueue != "open" || !reflect.DeepEqual(cached[0].EffectiveTicketTags, []string{"old"}) {
		t.Fatalf("status projection mutated the asynchronous cache: %#v", cached)
	}
	frontierKey := queueForecastFrontierKey(queueForecastOwner{RepositoryID: registration.RepositoryID, Queue: "review", filters: ticketclient.QueueFilters{Tags: []string{"new"}, WithoutTags: []string{"blocked"}}})
	owners, err := queueForecast(ctx, LoadedFileConfig{Instance: InstanceContext{InstanceDir: dir, LocalDir: dir}}, status, func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: map[string]map[string]ticketclient.ListResult{}, frontiers: map[string]ticketclient.ListResult{
			frontierKey: {Items: []ticketclient.Ticket{{ID: "20261002-30001", State: "review"}}},
		}}, nil
	})
	if err != nil || len(owners) != 1 || owners[0].Queue != "review" || owners[0].Next == nil || owners[0].Next.ID != "20261002-30001" {
		t.Fatalf("forecast after committed policy while old query blocked=%#v err=%v", owners, err)
	}
	release()
	select {
	case message := <-queued:
		if !strings.Contains(message, "new policy") || strings.Contains(message, "old policy") {
			t.Fatalf("stale or incomplete policy notification=%q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("current policy did not reconcile and notify ready work")
	}
	select {
	case message := <-queued:
		t.Fatalf("superseded policy dispatched an extra notification: %q", message)
	case <-time.After(25 * time.Millisecond):
	}
	filters := client.filterSnapshot()
	if len(filters) < 2 || !reflect.DeepEqual(filters[0], ticketclient.QueueFilters{Tags: []string{"old"}}) || !reflect.DeepEqual(filters[len(filters)-1], ticketclient.QueueFilters{Tags: []string{"new"}, WithoutTags: []string{"blocked"}}) {
		t.Fatalf("ready filters across policy reload=%#v", filters)
	}
	status = runtimeDaemonStatusWithSteerPolicies(nil, statuses.snapshot(), policies)
	if len(status.Steer) != 1 || status.Steer[0].EffectiveTicketQueue != "review" || !reflect.DeepEqual(status.Steer[0].EffectiveTicketTags, []string{"new"}) || !reflect.DeepEqual(status.Steer[0].EffectiveReviewSkipTags, []string{"blocked"}) {
		t.Fatalf("status boundary exposed stale selector after old query completed: %#v", status.Steer)
	}
	policies.Replace(map[string]steerRolePolicy{})
	status = runtimeDaemonStatusWithSteerPolicies(nil, statuses.snapshot(), policies)
	if len(status.Steer) != 1 || status.Steer[0].EffectiveTicketQueue != "" || len(status.Steer[0].EffectiveTicketTags) != 0 || len(status.Steer[0].EffectiveReviewSkipTags) != 0 {
		t.Fatalf("status boundary retained a selector after committed role removal: %#v", status.Steer)
	}
	var calls []string
	owners, err = queueForecast(ctx, LoadedFileConfig{Instance: InstanceContext{InstanceDir: dir, LocalDir: dir}}, status, func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID,
			active: map[string]map[string]ticketclient.ListResult{"worker": {
				"review": {Items: []ticketclient.Ticket{{ID: "20261002-30002", State: "review"}}},
			}}, calls: &calls}, nil
	})
	if err != nil || len(owners) != 1 || len(owners[0].Active) != 1 || owners[0].Active[0].ID != "20261002-30002" || strings.Contains(strings.Join(calls, " "), ":ready:") {
		t.Fatalf("forecast after committed role removal=%#v calls=%v err=%v", owners, calls, err)
	}
}

func TestDynamicSteerNoOpPolicyReloadDuringReconciliationPreservesDispatch(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "runtime")
	configPath := filepath.Join(root, "config.json")
	configData := func(tags string) string {
		return fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"roles":{"coder":{"ticket_queue":"open","ticket_tags":%s,"nudge_prompt":"coding work"}}}`, dir, tags)
	}
	writeConfigFixture(t, configPath, configData(`["backend","urgent"]`))
	loaded, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := resolveSteerRolePolicies(loaded.Config, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	policies := newSteerPolicyStore(initial)
	before := policies.Snapshot()
	dirty := newSteerDirtySet()
	manager := newWorkerManager(ctx, RunConfig{
		ConfigPath: configPath, StateDir: dir, InstanceID: loaded.Config.ID,
		steerPolicies: policies, steerDirty: dirty,
	}, os.Args[0], io.Discard, io.Discard, nil)
	defer manager.repositoryWatch.StopObservers()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := &policyReloadBarrierSteerClient{observed: make(chan struct{}), release: make(chan struct{})}
	queued := make(chan string, 1)
	statuses := &dynamicSteerStatus{}
	initialStatus := registrationStartupStatuses([]state.SteerRegistration{registration}, "")
	initialStatus[0].State = string(orc.SteerIdle)
	applySteerStatusPolicy(&initialStatus[0], before.roles[registration.Role])
	statuses.replace(initialStatus)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithPolicyStoreGate(runCtx, dir, policies, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				queued <- message.Text
				return nil
			}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	}()
	released := false
	release := func() {
		if !released {
			close(client.release)
			released = true
		}
	}
	t.Cleanup(func() { release(); cancel(); <-done })
	select {
	case <-client.observed:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not start its ready query")
	}
	writeConfigFixture(t, configPath, configData(`["urgent","backend"]`))
	reload, err := manager.reload(ctx)
	if err != nil || !reload.Applied {
		t.Fatalf("canonical-equivalent reload result=%#v err=%v", reload, err)
	}
	if after := policies.Snapshot(); after.generation != before.generation || !reflect.DeepEqual(after.roles, before.roles) {
		t.Fatalf("canonical-equivalent reload changed policy: before=%#v after=%#v", before, after)
	}
	if dirtied := dirty.Drain(); !dirtied.empty() {
		t.Fatalf("canonical-equivalent reload dirtied steering repositories: %#v", dirtied)
	}
	release()
	select {
	case message := <-queued:
		if !strings.Contains(message, "coding work") {
			t.Fatalf("notification=%q, want existing policy nudge", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no-op reload suppressed valid notification")
	}
	if filters := client.filterSnapshot(); len(filters) != 1 {
		t.Fatalf("no-op reload caused another reconciliation query: %#v", filters)
	}
}

func TestDynamicSteerReconcilesMatchingWorkAfterCrossQueueClaimEnds(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, registration, string(orc.SteerQueued), false, false); err != nil || !updated {
		t.Fatalf("seed outstanding delivery updated=%t err=%v", updated, err)
	}
	client := &sequenceSteerClient{
		activeByQueue: map[string][]bool{"open": {true, false}, "review": {false}},
		ready:         []bool{true},
	}
	policy := steerRolePolicy{TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"matching"}}, NudgePrompt: "matching work"}
	policies := newSteerPolicyStore(map[string]steerRolePolicy{"coder": policy})
	dirty := newSteerDirtySet()
	queued := make(chan string, 1)
	statuses := &dynamicSteerStatus{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithPolicyStoreGate(runCtx, dir, policies, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				queued <- message.Text
				return nil
			}), statuses, persistence, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == string(orc.SteerConsumed) && items[0].Ticket == "20260926-00001"
	})
	select {
	case message := <-queued:
		t.Fatalf("active claim caused a new-work notification: %q", message)
	default:
	}
	dirty.MarkRepository(registration.RepositoryID)
	select {
	case message := <-queued:
		if !strings.Contains(message, "matching work") || strings.Contains(message, steerClaimRecoveryPrompt) {
			t.Fatalf("ready notification after claim disappearance=%q", message)
		}
	case <-time.After(time.Second):
		t.Fatal("matching ready work was not dispatched after cross-queue claim disappeared")
	}
	if len(client.readyFilters) != 1 || !reflect.DeepEqual(client.readyFilters[0], policy.QueueFilters) {
		t.Fatalf("ready frontier filters=%#v, want %#v", client.readyFilters, policy.QueueFilters)
	}
}

func TestDynamicSteerEmitsReviewerCompletionPolicyOnlyForReviewQueue(t *testing.T) {
	tests := []struct {
		name             string
		role             string
		policy           steerRolePolicy
		wantPolicyPrompt string
	}{
		{name: "default review signoff", role: "quality", policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review nudge"}, wantPolicyPrompt: "For accepted work, approve the ticket to signoff."},
		{name: "configured signoff", role: "quality", policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review nudge", ReviewCompletion: ReviewCompletionSignoff}, wantPolicyPrompt: "For accepted work, approve the ticket to signoff."},
		{name: "configured close", role: "quality", policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review nudge", ReviewCompletion: ReviewCompletionClose}, wantPolicyPrompt: "For accepted work, approve and close the ticket."},
		{name: "coder has no reviewer policy", role: "builder", policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "code nudge"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: test.role,
				Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
			}); err != nil {
				t.Fatal(err)
			}
			queued := make(chan string, 1)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				runDynamicSteer(ctx, dir, map[string]steerRolePolicy{test.role: test.policy}, nil,
					func(state.SteerRegistration) (steerClient, error) {
						return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
					},
					testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
						queued <- message.Text
						return nil
					}),
					&dynamicSteerStatus{})
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			select {
			case message := <-queued:
				if !strings.Contains(message, test.policy.NudgePrompt) {
					t.Fatalf("notification omitted role nudge: %q", message)
				}
				if strings.Index(message, steerSessionBootstrapPrompt) < 0 || strings.Index(message, steerSessionBootstrapPrompt) > strings.Index(message, test.policy.NudgePrompt) {
					t.Fatalf("new-session notification did not put bootstrap before role nudge: %q", message)
				}
				if test.wantPolicyPrompt == "" {
					if strings.Contains(message, "For accepted work") {
						t.Fatalf("non-review notification included reviewer policy: %q", message)
					}
				} else {
					if !strings.Contains(message, test.wantPolicyPrompt) {
						t.Fatalf("review notification omitted policy %q: %q", test.wantPolicyPrompt, message)
					}
					if strings.Index(message, test.policy.NudgePrompt) > strings.Index(message, test.wantPolicyPrompt) {
						t.Fatalf("review policy preceded role nudge: %q", message)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("dynamic role did not receive its ready-work notification")
			}
		})
	}
}

func TestDynamicSteerRecreatedRegistrationDoesNotInheritQueuedDelivery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("persist old queued delivery updated=%t err=%v", updated, err)
	}
	if err := os.Remove(filepath.Join(dir, "steer.json")); err != nil {
		t.Fatal(err)
	}
	current, previous, changed, err := registrations.Join(ctx, registration)
	if err != nil || previous != nil || !changed || current.RegistrationID == old.RegistrationID {
		t.Fatalf("recreated registration=%#v previous=%#v changed=%t err=%v", current, previous, changed, err)
	}

	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	steerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(steerCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "fresh wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			return nil
		}), statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		if message != "fresh wake" {
			t.Fatalf("queued message=%q", message)
		}
	case <-time.After(time.Second):
		t.Fatal("new registration inherited the old queued delivery instead of scheduling a fresh wake")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "queued"
	})
	snapshot, err := persistence.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != current.RegistrationID || snapshot.Deliveries[0].State != "queued" {
		t.Fatalf("recreated delivery=%#v err=%v", snapshot, err)
	}
}

func TestDynamicSteerJoinRefreshesAndBootstrapsSession(t *testing.T) {
	tests := []struct {
		name           string
		threadChanged  bool
		roleChanged    bool
		active         bool
		activeByQueue  map[string][]bool
		ready          bool
		wantWake       bool
		wantBootstrap  bool
		wantRecovery   bool
		wantRolePrompt string
		wantStatusCode string
	}{
		{name: "same thread active claim", active: true, wantStatusCode: "busy"},
		{name: "same thread exact rejoin ready", ready: true, wantWake: true, wantRolePrompt: "coder normal work"},
		{name: "same thread ready work", ready: true, wantWake: true, wantRolePrompt: "coder normal work"},
		{name: "same thread role change active", roleChanged: true, active: true, wantStatusCode: "busy"},
		{name: "same thread role change preserves claim in prior queue", roleChanged: true, activeByQueue: map[string][]bool{"open": {false}, "review": {true}}, wantStatusCode: "busy"},
		{name: "same thread role change ready", roleChanged: true, ready: true, wantWake: true, wantRolePrompt: "architect normal work"},
		{name: "new thread active claim", threadChanged: true, active: true, wantWake: true, wantBootstrap: true, wantRecovery: true},
		{name: "new thread ready work", threadChanged: true, ready: true, wantWake: true, wantBootstrap: true, wantRolePrompt: "coder normal work"},
		{name: "new thread no work", threadChanged: true, wantWake: true, wantBootstrap: true},
		{name: "new thread role change active", threadChanged: true, roleChanged: true, active: true, wantWake: true, wantBootstrap: true, wantRecovery: true},
		{name: "new thread role change recovers claim in prior queue", threadChanged: true, roleChanged: true, activeByQueue: map[string][]bool{"open": {false}, "review": {true}}, wantWake: true, wantBootstrap: true, wantRecovery: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			registrations := state.NewRegistrationStore(dir)
			old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
				Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
			})
			if err != nil {
				t.Fatal(err)
			}
			persistence := state.NewSteerRuntimeStore(dir)
			if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
				t.Fatal(err)
			}
			if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
				t.Fatalf("seed old queued delivery updated=%t err=%v", updated, err)
			}

			queued := make(chan string, 2)
			statuses := &dynamicSteerStatus{}
			deliveryEvents := make(chan daemon.Event, 8)
			statuses.setPublisher(func(event daemon.Event) {
				if event.Type == "steer.delivery" {
					deliveryEvents <- event
				}
			})
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{
					"coder":     {TicketQueue: "open", NudgePrompt: "coder normal work"},
					"architect": {TicketQueue: "open", NudgePrompt: "architect normal work"},
				}, nil, func(state.SteerRegistration) (steerClient, error) {
					return &sequenceSteerClient{active: []bool{test.active}, activeByQueue: test.activeByQueue, ready: []bool{test.ready}}, nil
				}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
					queued <- message.Text
					return nil
				}), statuses)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			initialState := "queued"
			initiallyActive := test.active
			for _, claims := range test.activeByQueue {
				initiallyActive = initiallyActive || len(claims) > 0 && claims[0]
			}
			if initiallyActive {
				initialState = "consumed"
			}
			waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
				return len(items) == 1 && items[0].Session == old.SessionID && items[0].State == initialState
			})

			next := old
			if test.threadChanged {
				next.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
			}
			if test.roleChanged {
				next.Role = "architect"
			}
			current, _, changed, err := registrations.Join(ctx, next)
			if err != nil || changed != (test.threadChanged || test.roleChanged) {
				t.Fatalf("join changed=%t err=%v registration=%#v", changed, err, current)
			}
			if test.wantWake {
				select {
				case message := <-queued:
					if (test.wantRolePrompt != "" && !strings.Contains(message, test.wantRolePrompt)) || strings.Contains(message, steerClaimRecoveryPrompt) != test.wantRecovery || strings.Contains(message, steerSessionBootstrapPrompt) != test.wantBootstrap {
						t.Fatalf("wake message=%q want role prompt %q recovery=%t bootstrap=%t", message, test.wantRolePrompt, test.wantRecovery, test.wantBootstrap)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("join did not schedule the expected wake")
				}
				wantDeliveryState := "queued"
				if test.wantBootstrap && test.wantRolePrompt == "" && !test.wantRecovery {
					wantDeliveryState = "none"
				}
				waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
					return len(items) == 1 && items[0].Session == current.SessionID && items[0].State == wantDeliveryState
				})
				if test.wantRecovery {
					for _, wantState := range []string{"sending", "queued"} {
						select {
						case event := <-deliveryEvents:
							if event.State != wantState || event.Ticket != "20260926-00001" {
								t.Fatalf("recovery delivery event=%#v, want state=%q and active Ticket ID", event, wantState)
							}
						case <-time.After(time.Second):
							t.Fatalf("missing recovery delivery event state=%q", wantState)
						}
					}
				}
			} else {
				waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
					return len(items) == 1 && items[0].Session == current.SessionID && items[0].State == "none" && items[0].Code == test.wantStatusCode
				})
				select {
				case message := <-queued:
					t.Fatalf("unexpected wake: %q", message)
				default:
				}
			}
			time.Sleep(steerPollInterval + 25*time.Millisecond)
			if len(queued) != 0 {
				t.Fatalf("join queued more than one wake: %q", <-queued)
			}
			snapshot, err := persistence.Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != current.RegistrationID {
				t.Fatalf("join delivery snapshot=%#v err=%v", snapshot, err)
			}
			wantRecoveryPending := false
			if snapshot.Deliveries[0].BootstrapPending || snapshot.Deliveries[0].RecoveryPending != wantRecoveryPending {
				t.Fatalf("accepted join delivery retained incorrect intents: %#v, want recovery_pending=%t", snapshot.Deliveries[0], wantRecoveryPending)
			}
		})
	}
}

func TestDynamicSteerJoinRetriesRejectedRecoveryHandoff(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("seed old delivery updated=%t err=%v", updated, err)
	}
	newRoute := old
	newRoute.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, newRoute)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 3)
	statuses := &dynamicSteerStatus{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "normal work"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{true}, ready: []bool{false}}, nil
		}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			if attempts.Add(1) == 1 {
				return steertransport.Rejected(errors.New("queue rejected"))
			}
			return nil
		}), statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, steerClaimRecoveryPrompt) || strings.Contains(message, "normal work") {
			t.Fatalf("initial handoff omitted generic recovery or included unverified role work: %q", message)
		}
		if !(strings.Index(message, steerSessionBootstrapPrompt) < strings.Index(message, steerClaimRecoveryPrompt)) {
			t.Fatalf("initial handoff components were out of order: %q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement thread did not receive its recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "degraded" && items[0].Code == "queue_rejected"
	})
	afterReject, err := persistence.Snapshot(ctx)
	if err != nil || len(afterReject.Deliveries) != 1 || !afterReject.Deliveries[0].RecoveryPending || !afterReject.Deliveries[0].BootstrapPending {
		t.Fatalf("rejected handoff lost bootstrap/recovery markers: %#v err=%v", afterReject, err)
	}

	refreshed, previous, changed, err := registrations.Join(ctx, current)
	if err != nil || previous == nil || changed || refreshed.IncarnationID == current.IncarnationID {
		t.Fatalf("explicit recovery join=%#v previous=%#v changed=%t err=%v", refreshed, previous, changed, err)
	}
	select {
	case message := <-queued:
		if !strings.Contains(message, steerClaimRecoveryPrompt) {
			t.Fatalf("recovery retry message=%q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("explicit join did not retry the pending recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Session == current.SessionID && items[0].State == "queued"
	})
	time.Sleep(steerPollInterval + 25*time.Millisecond)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("recovery handoff attempts=%d, want one failed attempt and one explicit-join retry", got)
	}
	afterAccept, err := persistence.Snapshot(ctx)
	if err != nil || len(afterAccept.Deliveries) != 1 || afterAccept.Deliveries[0].RecoveryPending || afterAccept.Deliveries[0].BootstrapPending {
		t.Fatalf("accepted handoff retained bootstrap/recovery markers: %#v err=%v", afterAccept, err)
	}
}

func TestDynamicSteerUncertainBootstrapRecoveryHandoffWaitsForJoin(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "reviewer",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("complete prior delivery updated=%t err=%v", updated, err)
	}
	replacement := old
	replacement.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, replacement)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{"reviewer": {
			TicketQueue: "review", NudgePrompt: "review work", ReviewCompletion: ReviewCompletionClose,
		}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{true}}, nil
		}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			if attempts.Add(1) == 1 {
				return errors.New("queue outcome uncertain")
			}
			return nil
		}), statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		parts := []string{steerSessionBootstrapPrompt, steerClaimRecoveryPrompt}
		last := -1
		for _, part := range parts {
			index := strings.Index(message, part)
			if index <= last {
				t.Fatalf("combined reviewer recovery message omitted or reordered %q: %q", part, message)
			}
			last = index
		}
		if strings.Contains(message, "review work") || strings.Contains(message, "approve and close") {
			t.Fatalf("active claim recovery included ready-only reviewer prose: %q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement reviewer did not receive its recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "sending" && items[0].Code == "queue_uncertain"
	})
	afterUncertain, err := persistence.Snapshot(ctx)
	if err != nil || len(afterUncertain.Deliveries) != 1 || !afterUncertain.Deliveries[0].BootstrapPending || !afterUncertain.Deliveries[0].RecoveryPending {
		t.Fatalf("uncertain delivery lost pending flags: %#v err=%v", afterUncertain, err)
	}
	time.Sleep(steerPollInterval + 25*time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("uncertain delivery was retried without join; attempts=%d", attempts.Load())
	}

	refreshed, _, changed, err := registrations.Join(ctx, current)
	if err != nil || changed || refreshed.IncarnationID == current.IncarnationID {
		t.Fatalf("explicit recovery join=%#v changed=%t err=%v", refreshed, changed, err)
	}
	select {
	case <-queued:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit join did not retry uncertain recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Session == current.SessionID && items[0].State == "queued"
	})
	if attempts.Load() != 2 {
		t.Fatalf("explicit-join handoff attempts=%d, want 2", attempts.Load())
	}
	afterAccepted, err := persistence.Snapshot(ctx)
	if err != nil || len(afterAccepted.Deliveries) != 1 || afterAccepted.Deliveries[0].BootstrapPending || afterAccepted.Deliveries[0].RecoveryPending {
		t.Fatalf("accepted combined handoff retained pending flags: %#v err=%v", afterAccepted, err)
	}
}

func TestDynamicSteerRoleChangePreservesPendingRecoveryHandoff(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("seed old delivery updated=%t err=%v", updated, err)
	}
	newRoute := old
	newRoute.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, newRoute)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{
			"coder":     {TicketQueue: "open", NudgePrompt: "coder normal work"},
			"architect": {TicketQueue: "open", NudgePrompt: "architect normal work"},
		}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{true}, ready: []bool{false}}, nil
		}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			if attempts.Add(1) == 1 {
				return steertransport.Rejected(errors.New("queue rejected"))
			}
			return nil
		}), statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		if !strings.Contains(message, steerClaimRecoveryPrompt) {
			t.Fatalf("initial handoff message=%q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement thread did not receive its recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "degraded" && items[0].Code == "queue_rejected"
	})
	afterReject, err := persistence.Snapshot(ctx)
	if err != nil || len(afterReject.Deliveries) != 1 || !afterReject.Deliveries[0].RecoveryPending {
		t.Fatalf("rejected handoff lost recovery marker: %#v err=%v", afterReject, err)
	}

	roleRoute := current
	roleRoute.Role = "architect"
	reassigned, _, changed, err := registrations.Join(ctx, roleRoute)
	if err != nil || !changed || reassigned.RegistrationID == current.RegistrationID {
		t.Fatalf("same-thread role change changed=%t registration=%#v err=%v", changed, reassigned, err)
	}
	select {
	case message := <-queued:
		if !strings.Contains(message, steerClaimRecoveryPrompt) || strings.Contains(message, "architect normal work") {
			t.Fatalf("role-change handoff message=%q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("same-thread role change lost the pending recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Session == reassigned.SessionID && items[0].Role == "architect" && items[0].State == "queued"
	})
	time.Sleep(steerPollInterval + 25*time.Millisecond)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("recovery handoff attempts=%d, want rejected handoff and one role-change retry", got)
	}
	afterAccept, err := persistence.Snapshot(ctx)
	if err != nil || len(afterAccept.Deliveries) != 1 || afterAccept.Deliveries[0].RecoveryPending {
		t.Fatalf("accepted handoff retained recovery marker: %#v err=%v", afterAccept, err)
	}
}
