package cli

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

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
		activeByQueue: map[string][]bool{"open": {false}, "review": {true, false}},
		ready:         []bool{true},
	}
	policy := steerRolePolicy{TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"matching"}}, NudgePrompt: "matching work"}
	policies := newSteerPolicyStore(map[string]steerRolePolicy{"coder": policy})
	dirty := newSteerDirtySet()
	queued := make(chan string, 1)
	reconciled := make(chan struct{}, 1)
	statuses := &dynamicSteerStatus{}
	statuses.setPublisher(func(event daemon.Event) {
		if event.Type == "steer.status" && event.Actor == registration.Actor && event.State == string(orc.SteerQueued) {
			select {
			case reconciled <- struct{}{}:
			default:
			}
		}
	})
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
	case <-reconciled:
		// Status publication follows authoritative observations and delivery.
	case <-time.After(time.Second):
		t.Fatal("post-claim authoritative reconciliation did not complete")
	}
	select {
	case message := <-queued:
		if !strings.Contains(message, "matching work") || strings.Contains(message, steerClaimRecoveryPrompt) {
			t.Fatalf("ready notification after claim disappearance=%q", message)
		}
	default:
		t.Fatal("completed post-claim reconciliation did not dispatch matching work")
	}
	if len(client.readyFilters) != 1 || !reflect.DeepEqual(client.readyFilters[0], policy.QueueFilters) {
		t.Fatalf("ready frontier filters=%#v, want %#v", client.readyFilters, policy.QueueFilters)
	}
}
