package cli

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestDynamicSignoffRequiresNoActiveActorClaimAndExactShowEligibility(t *testing.T) {
	tests := []struct {
		name          string
		activeByQueue map[string][]bool
		shown         ticketclient.Ticket
		frontier      ticketclient.Ticket
		wantFrontier  int32
	}{
		{name: "open claim keeps actor busy", activeByQueue: map[string][]bool{"open": {true}, "review": {false}}, shown: ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}}, frontier: ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}}},
		{name: "authoritative Show rejects stale selector result", activeByQueue: map[string][]bool{"open": {false}, "review": {false}}, shown: ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"changed"}}, frontier: ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}}, wantFrontier: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			registrations := state.NewRegistrationStore(dir)
			registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
			if _, _, _, err := registrations.Join(ctx, registration); err != nil {
				t.Fatal(err)
			}
			client := &dynamicSignoffTestClient{
				sequenceSteerClient: &sequenceSteerClient{activeByQueue: test.activeByQueue},
				ticket:              test.shown,
				frontierTicket:      &test.frontier,
			}
			var wakes atomic.Int32
			var message string
			runner := newTestDynamicSteerRunner(t, ctx, dir,
				map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
				func(state.SteerRegistration) (steerClient, error) { return client, nil },
				testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, queued steertransport.Message) error {
					wakes.Add(1)
					message = queued.Text
					return nil
				}),
				&dynamicSteerStatus{}, nil, newRegistrationObserver(registrations), nil, nil)
			runner.bootstrap(ctx)
			if test.name == "open claim keeps actor busy" {
				if wakes.Load() != 1 || !strings.Contains(message, steerSessionBootstrapPrompt) || strings.Contains(message, test.shown.ID) {
					t.Fatalf("busy actor received signoff work instead of bootstrap only: wakes=%d message=%q", wakes.Load(), message)
				}
			} else if wakes.Load() != 0 {
				t.Fatalf("stale Show result dispatched %d wake(s)", wakes.Load())
			}
			if got := client.frontierCalls.Load(); got != test.wantFrontier {
				t.Fatalf("SignoffFrontier calls=%d, want %d", got, test.wantFrontier)
			}
			snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
				t.Fatalf("blocked/ineligible signoff acquired a reservation: %#v err=%v", snapshot, err)
			}
			runner.close()
		})
	}
}

func TestDynamicSignoffRechecksActorClaimBeforeReservation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := registrations.Join(ctx, registration); err != nil {
		t.Fatal(err)
	}
	base := &dynamicSignoffTestClient{
		sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
		ticket:              ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}},
	}
	client := &claimDuringSignoffShowClient{dynamicSignoffTestClient: base}
	var wakes atomic.Int32
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			wakes.Add(1)
			return nil
		}),
		&dynamicSteerStatus{}, nil, newRegistrationObserver(registrations), nil, nil)
	runner.bootstrap(ctx)
	if wakes.Load() != 0 {
		t.Fatalf("actor claim appearing during selection allowed %d signoff wake(s)", wakes.Load())
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
		t.Fatalf("actor-busy selection reserved a signoff target: %#v err=%v", snapshot, err)
	}
	runner.close()
}

func TestDynamicSignoffFailsClosedWhenFinalActorClaimObservationFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := registrations.Join(ctx, registration); err != nil {
		t.Fatal(err)
	}
	client := &finalClaimObservationErrorClient{dynamicSignoffTestClient: &dynamicSignoffTestClient{
		sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
		ticket:              ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}},
	}}
	var wakes atomic.Int32
	statuses := &dynamicSteerStatus{}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			wakes.Add(1)
			return nil
		}),
		statuses, nil, newRegistrationObserver(registrations), nil, nil)
	runner.bootstrap(ctx)
	items := statuses.snapshot()
	if wakes.Load() != 0 || len(items) != 1 || items[0].Code != "ticket_observation_failed" {
		t.Fatalf("failed final actor observation dispatched work or was misreported: wakes=%d status=%#v", wakes.Load(), items)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
		t.Fatalf("failed final actor observation reserved target: %#v err=%v", snapshot, err)
	}
	runner.close()
}

func TestDynamicSignoffSendsBootstrapWhenFrontierIsEmpty(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := registrations.Join(ctx, registration); err != nil {
		t.Fatal(err)
	}
	client := &dynamicSignoffTestClient{
		sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
		ticket:              ticketclient.Ticket{ID: "20261007-12345", State: "review"},
	}
	var messages []string
	runtime := state.NewSteerRuntimeStore(dir)
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff"}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			messages = append(messages, message.Text)
			return nil
		}),
		&dynamicSteerStatus{}, runtime, newRegistrationObserver(registrations), nil, nil)
	runner.bootstrap(ctx)
	if len(messages) != 1 || messages[0] != steerSessionBootstrapPrompt {
		t.Fatalf("no-work bootstrap messages=%q, want bootstrap only", messages)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].BootstrapPending || snapshot.Deliveries[0].TargetTicket != "" {
		t.Fatalf("no-work bootstrap intent was not durably completed: %#v err=%v", snapshot, err)
	}
	runner.close()
}

type signoffBacklogTestClient struct {
	*sequenceSteerClient
	tickets map[string]ticketclient.Ticket
	readyID string
}

func (client *signoffBacklogTestClient) Show(_ context.Context, id string) (ticketclient.Ticket, error) {
	ticket, found := client.tickets[id]
	if !found {
		return ticketclient.Ticket{}, ticketclient.ErrProtocol
	}
	return ticket, nil
}

func (client *signoffBacklogTestClient) SignoffFrontier(_ context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	if limit != 1 || offset != 0 {
		return ticketclient.ListResult{}, ticketclient.ErrProtocol
	}
	ticket, found := client.tickets[client.readyID]
	if !found || ticket.State != "signoff" || ticket.Assignee != "" {
		return ticketclient.ListResult{}, nil
	}
	tags := make(map[string]bool, len(ticket.Tags))
	for _, tag := range ticket.Tags {
		tags[tag] = true
	}
	for _, tag := range filters.Tags {
		if !tags[tag] {
			return ticketclient.ListResult{}, nil
		}
	}
	for _, tag := range filters.WithoutTags {
		if tags[tag] {
			return ticketclient.ListResult{}, nil
		}
	}
	return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: ticket.ID, State: ticket.State}}}, nil
}

func TestDynamicSignoffRetirementAndRecoveryTriggerBacklogPass(t *testing.T) {
	t.Run("retired target schedules one later selection pass", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		registrations := state.NewRegistrationStore(dir)
		registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
		current, _, _, err := registrations.Join(ctx, registration)
		if err != nil {
			t.Fatal(err)
		}
		runtime := state.NewSteerRuntimeStore(dir)
		if err := runtime.Reconcile(ctx, []state.SteerRegistration{current}); err != nil {
			t.Fatal(err)
		}
		if reserved, err := runtime.ReserveSignoffTarget(ctx, current, "20261007-12345", "signoff"); err != nil || !reserved {
			t.Fatalf("seed target reserved=%t err=%v", reserved, err)
		}
		if completed, err := runtime.CompleteSignoffDeliveryWithIntents(ctx, current, "20261007-12345", true, false); err != nil || !completed {
			t.Fatalf("seed target completion=%t err=%v", completed, err)
		}
		client := &signoffBacklogTestClient{
			sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false, false, false}, "review": {false, false, false}}},
			tickets: map[string]ticketclient.Ticket{
				"20261007-12345": {ID: "20261007-12345", State: "closed"},
				"20261007-12346": {ID: "20261007-12346", State: "signoff", Tags: []string{"api"}},
			},
			readyID: "20261007-12346",
		}
		var wakes atomic.Int32
		runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				wakes.Add(1)
				return nil
			}),
			&dynamicSteerStatus{}, runtime, newRegistrationObserver(registrations), nil, nil)
		runner.bootstrap(ctx)
		if wakes.Load() != 0 {
			t.Fatalf("retirement pass selected backlog immediately: wakes=%d", wakes.Load())
		}
		runner.registrationTick(ctx, time.Now())
		if wakes.Load() != 1 {
			t.Fatalf("follow-up registration tick did not select backlog: wakes=%d", wakes.Load())
		}
		runner.registrationTick(ctx, time.Now())
		if wakes.Load() != 1 {
			t.Fatalf("second registration tick duplicated outstanding target: wakes=%d", wakes.Load())
		}
		snapshot, err := runtime.Snapshot(ctx)
		if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "20261007-12346" {
			t.Fatalf("follow-up target not persisted: %#v err=%v", snapshot, err)
		}
		runner.close()
	})

	t.Run("retired generic recovery delivery schedules backlog pass", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		registrations := state.NewRegistrationStore(dir)
		registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
		current, _, _, err := registrations.Join(ctx, registration)
		if err != nil {
			t.Fatal(err)
		}
		runtime := state.NewSteerRuntimeStore(dir)
		if err := runtime.Reconcile(ctx, []state.SteerRegistration{current}); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.Update(ctx, current, "consumed", ""); err != nil {
			t.Fatal(err)
		}
		client := &signoffBacklogTestClient{
			sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false, false, false}, "review": {false, false, false}}},
			tickets:             map[string]ticketclient.Ticket{"20261007-12346": {ID: "20261007-12346", State: "signoff", Tags: []string{"api"}}},
			readyID:             "20261007-12346",
		}
		var wakes atomic.Int32
		runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				wakes.Add(1)
				return nil
			}),
			&dynamicSteerStatus{}, runtime, newRegistrationObserver(registrations), nil, nil)
		runner.bootstrap(ctx)
		if wakes.Load() != 0 {
			t.Fatalf("generic recovery retirement pass selected backlog immediately: %d wake(s)", wakes.Load())
		}
		runner.registrationTick(ctx, time.Now())
		if wakes.Load() != 1 {
			t.Fatalf("generic recovery retirement did not schedule backlog pass: %d wake(s)", wakes.Load())
		}
		runner.close()
	})

	t.Run("owned claim recovery completion schedules follow-up pass", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		registrations := state.NewRegistrationStore(dir)
		registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "old-session"}
		old, _, _, err := registrations.Join(ctx, registration)
		if err != nil {
			t.Fatal(err)
		}
		runtime := state.NewSteerRuntimeStore(dir)
		if err := runtime.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
			t.Fatal(err)
		}
		registration.SessionID = steerTestThread
		current, _, _, err := registrations.Join(ctx, registration)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Reconcile(ctx, []state.SteerRegistration{current}); err != nil {
			t.Fatal(err)
		}
		client := &signoffBacklogTestClient{
			sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {true, false, false}, "review": {false, false, false}}},
			tickets:             map[string]ticketclient.Ticket{"20261007-12346": {ID: "20261007-12346", State: "signoff", Tags: []string{"api"}}},
			readyID:             "20261007-12346",
		}
		var messages []string
		runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				messages = append(messages, message.Text)
				return nil
			}),
			&dynamicSteerStatus{}, runtime, newRegistrationObserver(registrations), nil, nil)
		runner.bootstrap(ctx)
		if len(messages) != 1 || !strings.Contains(messages[0], steerSessionBootstrapPrompt) || !strings.Contains(messages[0], steerClaimRecoveryPrompt) || strings.Contains(messages[0], "Resolve exactly signoff ticket") {
			t.Fatalf("replacement active-claim recovery message=%q", messages)
		}
		afterRecovery, err := runtime.Snapshot(ctx)
		if err != nil || len(afterRecovery.Deliveries) != 1 || afterRecovery.Deliveries[0].BootstrapPending || afterRecovery.Deliveries[0].RecoveryPending {
			t.Fatalf("accepted recovery did not clear included intents: %#v err=%v", afterRecovery, err)
		}
		select {
		case <-runner.dirty.Wake():
		case <-time.After(5 * time.Second):
			t.Fatal("accepted recovery did not queue a follow-up trigger")
		}
		runner.repositoryChanged(ctx)
		if len(messages) != 2 || !strings.Contains(messages[1], "20261007-12346") {
			t.Fatalf("follow-up after claim recovery did not dispatch signoff backlog: messages=%q activeIdx=%#v statuses=%#v", messages, client.activeQueueIdx, runner.statuses.snapshot())
		}
		afterTarget, err := runtime.Snapshot(ctx)
		if err != nil || len(afterTarget.Deliveries) != 1 || afterTarget.Deliveries[0].TargetTicket != "20261007-12346" || afterTarget.Deliveries[0].BootstrapPending || afterTarget.Deliveries[0].RecoveryPending {
			t.Fatalf("post-recovery signoff target state=%#v err=%v", afterTarget, err)
		}
		runner.close()
	})
}

func TestDynamicSignoffSelectionIsSuppressedByPolicyReloadAndPause(t *testing.T) {
	for _, mode := range []string{"policy reload", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			dir := t.TempDir()
			registrations := state.NewRegistrationStore(dir)
			registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
			if _, _, _, err := registrations.Join(ctx, registration); err != nil {
				t.Fatal(err)
			}
			client := &blockedSignoffFrontierClient{
				dynamicSignoffTestClient: &dynamicSignoffTestClient{
					sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
					ticket:              ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}},
				},
				entered: make(chan struct{}), release: make(chan struct{}, 1),
			}
			policies := newSteerPolicyStore(map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}})
			var gate *dispatchGate
			if mode == "pause" {
				gate = newDispatchGate(state.NewDaemonControlStore(dir), state.DaemonRunning)
			}
			var wakes atomic.Int32
			runner := newDynamicSteerRunner(dir, policies, nil,
				func(state.SteerRegistration) (steerClient, error) { return client, nil },
				testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
					wakes.Add(1)
					return nil
				}),
				&dynamicSteerStatus{}, nil, newRegistrationObserver(registrations), gate, nil)
			done := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				select {
				case client.release <- struct{}{}:
				default:
				}
				select {
				case <-done:
					runner.close()
				case <-time.After(5 * time.Second):
					t.Error("blocked signoff bootstrap did not stop before cleanup")
				}
			})
			go func() { defer close(done); runner.bootstrap(ctx) }()
			select {
			case <-client.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("SignoffFrontier did not reach the dispatch barrier")
			}
			if mode == "policy reload" {
				runner.policies.Replace(map[string]steerRolePolicy{"architect-api": {TicketQueue: "review", NudgePrompt: "review"}})
			} else if _, _, err := gate.Pause(ctx); err != nil {
				t.Fatalf("pause during signoff selection: %v", err)
			}
			client.release <- struct{}{}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("signoff reconciliation did not finish after dispatch suppression")
			}
			if wakes.Load() != 0 {
				t.Fatalf("stale policy or paused daemon dispatched %d signoff wake(s)", wakes.Load())
			}
			snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
				t.Fatalf("suppressed selection persisted a target: %#v err=%v", snapshot, err)
			}
		})
	}
}

func TestDynamicSignoffPersistsUncertainOutcomesWithoutResending(t *testing.T) {
	tests := []struct {
		name       string
		fail       string
		transport  error
		wantState  string
		wantCode   string
		wantStored string
		wantTarget bool
	}{
		{name: "uncertain transport", transport: errors.New("queue outcome uncertain"), wantState: "sending", wantCode: "queue_uncertain", wantStored: "sending", wantTarget: true},
		{name: "rejection persistence fails", fail: "signoff_reject", transport: steertransport.Rejected(errors.New("queue rejected")), wantState: "sending", wantCode: "runtime_state_write_failed", wantStored: "sending", wantTarget: true},
		{name: "accepted completion persistence fails", fail: "signoff_complete", wantState: "sending", wantCode: "runtime_state_write_failed", wantStored: "sending", wantTarget: true},
		{name: "definite rejection", transport: steertransport.Rejected(errors.New("queue rejected")), wantState: "degraded", wantCode: "queue_rejected", wantStored: "degraded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			registrations := state.NewRegistrationStore(dir)
			registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
			if _, _, _, err := registrations.Join(ctx, registration); err != nil {
				t.Fatal(err)
			}
			inner := state.NewSteerRuntimeStore(dir)
			persistence := steerRuntimePersistence(inner)
			if test.fail != "" {
				persistence = failingSteerRuntime{inner: inner, fail: test.fail}
			}
			client := &dynamicSignoffTestClient{
				sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
				ticket:              ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}},
			}
			var wakes atomic.Int32
			statuses := &dynamicSteerStatus{}
			roles := map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}, NudgePrompt: "wake"}}
			router := testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				wakes.Add(1)
				return test.transport
			})
			newRunner := func() *dynamicSteerRunner {
				return newTestDynamicSteerRunner(t, ctx, dir, roles, nil, func(state.SteerRegistration) (steerClient, error) {
					return client, nil
				}, router, statuses, persistence, newRegistrationObserver(registrations), nil, nil)
			}
			runner := newRunner()
			runner.bootstrap(ctx)
			items := statuses.snapshot()
			if len(items) != 1 || items[0].State != test.wantState || items[0].Code != test.wantCode {
				t.Fatalf("first delivery status=%#v, want %s/%s", items, test.wantState, test.wantCode)
			}
			runner.reconcile(ctx, steerDirtySnapshot{all: true}, time.Now())
			runner.close()
			runner = newRunner()
			runner.bootstrap(ctx)
			if got := wakes.Load(); got != 1 {
				t.Fatalf("uncertain/signoff target was delivered %d times across reconcile and restart, want once", got)
			}
			snapshot, err := inner.Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 {
				t.Fatalf("runtime snapshot=%#v err=%v", snapshot, err)
			}
			delivery := snapshot.Deliveries[0]
			if (delivery.TargetTicket == client.ticket.ID) != test.wantTarget || delivery.State != test.wantStored {
				t.Fatalf("stored delivery=%#v, want target=%t and state %s", delivery, test.wantTarget, test.wantStored)
			}
			runner.close()
		})
	}
}

func TestDynamicSignoffClearAndReserveFailuresAreReported(t *testing.T) {
	t.Run("clear failure retains target", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		registrations := state.NewRegistrationStore(dir)
		registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
		current, _, _, err := registrations.Join(ctx, registration)
		if err != nil {
			t.Fatal(err)
		}
		inner := state.NewSteerRuntimeStore(dir)
		if err := inner.Reconcile(ctx, []state.SteerRegistration{current}); err != nil {
			t.Fatal(err)
		}
		if _, err := inner.ReserveSignoffTarget(ctx, current, "20261007-12345", "signoff"); err != nil {
			t.Fatal(err)
		}
		if _, err := inner.CompleteSignoffDelivery(ctx, current, "20261007-12345"); err != nil {
			t.Fatal(err)
		}
		client := &dynamicSignoffTestClient{sequenceSteerClient: &sequenceSteerClient{}, ticket: ticketclient.Ticket{ID: "20261007-12345", State: "review"}}
		statuses := &dynamicSteerStatus{}
		runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff"}}, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				t.Error("retiring target sent a wake")
				return nil
			}),
			statuses, failingSteerRuntime{inner: inner, fail: "signoff_clear"}, newRegistrationObserver(registrations), nil, nil)
		runner.bootstrap(ctx)
		items := statuses.snapshot()
		if len(items) != 1 || items[0].Code != "runtime_state_write_failed" {
			t.Fatalf("failed target retirement status=%#v", items)
		}
		snapshot, err := inner.Snapshot(ctx)
		if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "20261007-12345" {
			t.Fatalf("failed target retirement lost durable reservation: %#v err=%v", snapshot, err)
		}
		runner.close()
	})

	t.Run("reserve failure prevents transport", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		registrations := state.NewRegistrationStore(dir)
		registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
		if _, _, _, err := registrations.Join(ctx, registration); err != nil {
			t.Fatal(err)
		}
		inner := state.NewSteerRuntimeStore(dir)
		client := &dynamicSignoffTestClient{sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}}, ticket: ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}}}
		var wakes atomic.Int32
		statuses := &dynamicSteerStatus{}
		runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				wakes.Add(1)
				return nil
			}),
			statuses, failingSteerRuntime{inner: inner, fail: "signoff_reserve"}, newRegistrationObserver(registrations), nil, nil)
		runner.bootstrap(ctx)
		items := statuses.snapshot()
		if len(items) != 1 || items[0].Code != "runtime_state_write_failed" || wakes.Load() != 0 {
			t.Fatalf("failed reserve status=%#v wakes=%d", items, wakes.Load())
		}
		snapshot, err := inner.Snapshot(ctx)
		if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
			t.Fatalf("failed reserve persisted a target: %#v err=%v", snapshot, err)
		}
		runner.close()
	})
}

type blockedSignoffFrontierClient struct {
	*dynamicSignoffTestClient
	entered chan struct{}
	release chan struct{}
}

type claimDuringSignoffShowClient struct {
	*dynamicSignoffTestClient
}

func (client *claimDuringSignoffShowClient) Show(ctx context.Context, id string) (ticketclient.Ticket, error) {
	ticket, err := client.dynamicSignoffTestClient.Show(ctx, id)
	client.sequenceSteerClient.activeByQueue["open"] = []bool{true}
	return ticket, err
}

type finalClaimObservationErrorClient struct {
	*dynamicSignoffTestClient
	calls atomic.Int32
}

func (client *finalClaimObservationErrorClient) ActiveClaims(ctx context.Context, queue string, limit int) (ticketclient.ListResult, error) {
	if client.calls.Add(1) == 3 {
		return ticketclient.ListResult{}, errors.New("injected final actor claim observation failure")
	}
	return client.sequenceSteerClient.ActiveClaims(ctx, queue, limit)
}

func (client *blockedSignoffFrontierClient) SignoffFrontier(ctx context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	close(client.entered)
	select {
	case <-client.release:
	case <-ctx.Done():
		return ticketclient.ListResult{}, ctx.Err()
	}
	return client.dynamicSignoffTestClient.SignoffFrontier(ctx, filters, limit, offset)
}

func TestDynamicSignoffRegistrationReplacementWhileFrontierBlockedDoesNotDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "architect", Role: "architect-api", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	current, _, _, err := registrations.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	client := &blockedSignoffFrontierClient{
		dynamicSignoffTestClient: &dynamicSignoffTestClient{
			sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
			ticket:              ticketclient.Ticket{ID: "20261007-12345", State: "signoff", Tags: []string{"api"}},
		},
		entered: make(chan struct{}), release: make(chan struct{}, 1),
	}
	var wakes atomic.Int32
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect-api": {TicketQueue: "signoff", QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}}}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			wakes.Add(1)
			return nil
		}),
		&dynamicSteerStatus{}, nil, newRegistrationObserver(registrations), nil, nil)
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case client.release <- struct{}{}:
		default:
		}
		select {
		case <-done:
			runner.close()
		case <-time.After(5 * time.Second):
			t.Error("bootstrap goroutine did not stop before runner cleanup")
		}
	})
	go func() { defer close(done); runner.bootstrap(ctx) }()
	select {
	case <-client.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("SignoffFrontier did not reach the blocking barrier")
	}
	refreshed, _, _, err := registrations.Join(ctx, current)
	if err != nil || refreshed.IncarnationID == current.IncarnationID {
		t.Fatalf("replacement registration=%#v err=%v", refreshed, err)
	}
	client.release <- struct{}{}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked reconciliation did not finish")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("stale registration dispatched %d signoff wake(s)", got)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
		t.Fatalf("stale registration reserved target: %#v err=%v", snapshot, err)
	}
}
