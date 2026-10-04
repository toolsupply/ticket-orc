package orc

import (
	"context"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type loopTicketFixture struct {
	ticket   ticketclient.Ticket
	ready    bool
	race     bool
	actor    string
	holds    int
	releases int
}

func (f *loopTicketFixture) Release(_ context.Context, id string) (ticketclient.Transition, error) {
	f.releases++
	if f.ticket.Assignee != f.actor {
		return ticketclient.Transition{}, &ticketclient.CommandError{Code: "already_claimed"}
	}
	from := f.ticket.State
	f.ticket.Assignee = ""
	return ticketclient.Transition{ID: id, Changed: true, State: from}, nil
}

func (f *loopTicketFixture) Show(_ context.Context, id string) (ticketclient.Ticket, error) {
	result := f.ticket
	result.ID = id
	return result, nil
}

func (f *loopTicketFixture) ShowReadiness(_ context.Context, id string) (ticketclient.TicketDetail, error) {
	return ticketclient.TicketDetail{ID: id, State: f.ticket.State, Assignee: f.ticket.Assignee, Readiness: &ticketclient.TicketReadiness{Ready: f.ready}}, nil
}

func (f *loopTicketFixture) HoldTicket(_ context.Context, id string, _ ticketclient.MutationOptions) (ticketclient.MutationResult, error) {
	f.holds++
	if f.race {
		f.race = false
		f.ticket.Assignee = "concurrent-worker"
	}
	if f.ticket.Assignee != "" {
		return ticketclient.MutationResult{}, &ticketclient.CommandError{Code: "already_claimed"}
	}
	from := f.ticket.State
	f.ticket.State = "hold"
	return ticketclient.MutationResult{ID: id, Changed: true, FromState: from, State: "hold"}, nil
}

func loopTestStore(t *testing.T) *state.Store {
	t.Helper()
	return state.NewForRepository(t.TempDir(), "/repo")
}

func TestTicketLoopCountsObservedSameStateClaimsAndContainsOnSecondStall(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	ticket := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}, ready: true}
	containment := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}}
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	for index, actor := range []string{"coder-a", "coder-b"} {
		if _, err := store.RecordClaim(ctx, "ticket", actor, "open"); err != nil {
			t.Fatal(err)
		}
		loop, err := ReconcileEndedClaim(ctx, store, ticket, containment, "ticket")
		if err != nil {
			t.Fatalf("ReconcileEndedClaim cycle %d: %v", index+1, err)
		}
		if index == 0 && (loop.StallCount != 1 || loop.Phase != state.TicketLoopActive || containment.holds != 0) {
			t.Fatalf("first cycle loop=%#v holds=%d", loop, containment.holds)
		}
		if index == 1 && (loop.StallCount != 2 || loop.Phase != state.TicketLoopHeld || loop.HeldFrom != "open" || containment.holds != 1) {
			t.Fatalf("second cycle loop=%#v holds=%d", loop, containment.holds)
		}
	}
	if got, want := ContainmentActor("1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"), "ticket-orc.1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"; got != want {
		t.Fatalf("containment actor = %q, want %q", got, want)
	}
}

func TestTicketLoopDoesNotCountUnclaimedNudge(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}, ready: true}
	loop, err := ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
	if err != nil || loop.StallCount != 0 {
		t.Fatalf("nudge without claim loop=%#v err=%v", loop, err)
	}
}

func TestConcurrentEndedClaimReconciliationConsumesAttemptOnce(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordClaim(ctx, "ticket", "worker", "open"); err != nil {
		t.Fatal(err)
	}
	reader := &barrierLoopReader{
		loopTicketFixture: &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}, ready: true},
		ready:             make(chan struct{}, 2),
		resume:            make(chan struct{}),
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
			errs <- err
		}()
	}
	<-reader.ready
	<-reader.ready
	close(reader.resume)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	loop, found, err := store.TicketLoop(ctx, "ticket")
	if err != nil || !found || loop.StallCount != 1 || loop.ClaimState != "" {
		t.Fatalf("concurrent reconciliation loop=%#v found=%t err=%v; want one consumed attempt", loop, found, err)
	}
}

type barrierLoopReader struct {
	*loopTicketFixture
	ready  chan struct{}
	resume chan struct{}
}

func (f *barrierLoopReader) ShowReadiness(ctx context.Context, id string) (ticketclient.TicketDetail, error) {
	f.ready <- struct{}{}
	select {
	case <-f.resume:
	case <-ctx.Done():
		return ticketclient.TicketDetail{}, ctx.Err()
	}
	return f.loopTicketFixture.ShowReadiness(ctx, id)
}

func TestDynamicSteerClaimRequiresPriorWorkBearingDispatch(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	loop, recorded, err := store.RecordSteerClaim(ctx, "ticket", "worker", "open")
	if err != nil || recorded || loop.StallCount != 0 {
		t.Fatalf("claim without dispatch loop=%#v recorded=%t err=%v", loop, recorded, err)
	}
	if _, err := store.RecordDispatch(ctx, "ticket"); err != nil {
		t.Fatal(err)
	}
	loop, recorded, err = store.RecordSteerClaim(ctx, "ticket", "worker", "open")
	if err != nil || !recorded || loop.ClaimState != "open" || loop.DispatchPending {
		t.Fatalf("dispatched claim loop=%#v recorded=%t err=%v", loop, recorded, err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}, ready: true}
	loop, err = ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
	if err != nil || loop.StallCount != 1 {
		t.Fatalf("post-claim same-state loop=%#v err=%v", loop, err)
	}
}

func TestEndedClaimInSameStateButNotReadyEndsAttemptWithoutRetroactiveStall(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordClaim(ctx, "ticket", "worker", "open"); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}, ready: false}
	loop, err := ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
	if err != nil || loop.StallCount != 0 || loop.ClaimState != "" || loop.ClaimActor != "" {
		t.Fatalf("blocked same-state observation loop=%#v err=%v", loop, err)
	}
	reader.ready = true
	loop, err = ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
	if err != nil || loop.StallCount != 0 || loop.ClaimState != "" || loop.ClaimActor != "" {
		t.Fatalf("later readiness resurrected ended attempt loop=%#v err=%v", loop, err)
	}
}

func TestEndedReviewerClaimInSameStateButNotReadyEndsAttempt(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordClaim(ctx, "ticket", "reviewer", "review"); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "review"}, ready: false}
	loop, err := ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
	if err != nil || loop.StallCount != 0 || loop.ClaimState != "" || loop.ClaimActor != "" {
		t.Fatalf("blocked reviewer same-state observation loop=%#v err=%v", loop, err)
	}
	reader.ready = true
	loop, err = ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
	if err != nil || loop.StallCount != 0 || loop.ClaimState != "" {
		t.Fatalf("later review readiness resurrected ended attempt loop=%#v err=%v", loop, err)
	}
}

func TestEndedClaimObservingExternalHoldMarksLoopHeld(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordClaim(ctx, "ticket", "worker", "open"); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "hold"}, ready: false}
	loop, err := ReconcileEndedClaim(ctx, store, reader, nil, "ticket")
	if err != nil || loop.Phase != state.TicketLoopHeld || loop.ClaimState != "" || loop.ClaimActor != "" {
		t.Fatalf("external hold did not finish attempt and mark held: loop=%#v err=%v", loop, err)
	}
}

func TestReviewToOpenAfterObservedClaimCountsOneBounce(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordClaim(ctx, "ticket", "reviewer", "review"); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}, ready: true}
	holder := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}}
	loop, err := ReconcileEndedClaim(ctx, store, reader, holder, "ticket")
	if err != nil || loop.BounceCount != 1 || loop.Phase != state.TicketLoopHeld || holder.holds != 1 {
		t.Fatalf("bounce reconciliation loop=%#v holds=%d err=%v", loop, holder.holds, err)
	}
	loop, found, err := store.TicketLoop(ctx, "ticket")
	if err != nil || !found || loop.BounceCount != 1 {
		t.Fatalf("repeat observation double-counted bounce loop=%#v found=%t err=%v", loop, found, err)
	}
}

func TestTicketLoopDefersAssignedContainmentAndRetriesOwnershipRace(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkContainmentPending(ctx, "ticket"); err != nil {
		t.Fatal(err)
	}
	ticket := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open", Assignee: "worker"}}
	holder := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}, race: true}
	loop, found, err := ReconcileTicketLoop(ctx, store, ticket, holder, "ticket")
	if err != nil || !found || loop.Phase != state.TicketLoopContainmentPending || holder.holds != 0 {
		t.Fatalf("assigned pending loop=%#v found=%t holds=%d err=%v", loop, found, holder.holds, err)
	}
	ticket.ticket.Assignee = ""
	if _, found, err := ReconcileTicketLoop(ctx, store, ticket, holder, "ticket"); err != nil || !found {
		t.Fatalf("ownership race should remain a pending loop without aborting reconciliation: found=%t err=%v", found, err)
	}
	loop, found, err = store.TicketLoop(ctx, "ticket")
	if err != nil || !found || loop.Phase != state.TicketLoopContainmentPending || holder.ticket.Assignee != "concurrent-worker" {
		t.Fatalf("race altered ownership or circuit loop=%#v found=%t assignee=%q err=%v", loop, found, holder.ticket.Assignee, err)
	}
	// Releasing the authoritative claim lets the next reconciliation complete
	// ordinary hold as the Orc actor.
	holder.ticket.Assignee = ""
	loop, found, err = ReconcileTicketLoop(ctx, store, ticket, holder, "ticket")
	if err != nil || !found || loop.Phase != state.TicketLoopHeld || holder.ticket.State != "hold" {
		t.Fatalf("retry loop=%#v found=%t ticket=%#v err=%v", loop, found, holder.ticket, err)
	}
}

func TestClaimedWorkerReleasesOnlyItsOwnAssignmentBeforeContainment(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkContainmentPending(ctx, "ticket"); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open", Assignee: "worker"}, actor: "worker"}
	loop, found, err := ReconcileClaimedTicketLoop(ctx, store, reader, reader, reader, "worker", "ticket")
	if err != nil || !found || loop.Phase != state.TicketLoopHeld || reader.ticket.State != "hold" || reader.ticket.Assignee != "" || reader.releases != 1 || reader.holds != 1 {
		t.Fatalf("own claim containment loop=%#v found=%t ticket=%#v releases=%d holds=%d err=%v", loop, found, reader.ticket, reader.releases, reader.holds, err)
	}
}

func TestClaimedWorkerDoesNotReleaseAnotherActorsAssignment(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkContainmentPending(ctx, "ticket"); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open", Assignee: "other"}, actor: "worker"}
	loop, found, err := ReconcileClaimedTicketLoop(ctx, store, reader, reader, reader, "worker", "ticket")
	if err != nil || !found || loop.Phase != state.TicketLoopContainmentPending || reader.ticket.Assignee != "other" || reader.releases != 0 || reader.holds != 0 {
		t.Fatalf("other actor claim changed loop=%#v found=%t ticket=%#v releases=%d holds=%d err=%v", loop, found, reader.ticket, reader.releases, reader.holds, err)
	}
}

func TestHeldTicketLeavingHoldStartsNewGeneration(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	loop, _, err := store.Participate(ctx, "ticket", 6)
	if err != nil {
		t.Fatal(err)
	}
	loop, err = store.RecordClaim(ctx, "ticket", "worker", "review")
	if err != nil {
		t.Fatal(err)
	}
	loop, consumed, err := store.RecordStall(ctx, "ticket", loop.AttemptID)
	if err != nil || !consumed {
		t.Fatal(err)
	}
	if _, err := store.MarkContainmentPending(ctx, "ticket"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkHeld(ctx, "ticket", "review"); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "review"}, ready: true}
	if _, found, err := ReconcileTicketLoop(ctx, store, reader, nil, "ticket"); err != nil || found {
		t.Fatalf("recovered loop found=%t err=%v", found, err)
	}
	loop, _, err = store.Participate(ctx, "ticket", 10)
	if err != nil || loop.StallCount != 0 || loop.BounceCount != 0 || loop.EffectiveBounceLimit != 10 {
		t.Fatalf("new generation loop=%#v err=%v", loop, err)
	}
}

func TestPendingAndHeldTicketLoopsReconcileAfterRestart(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	store := state.NewForRepository(stateDir, "/repo")
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkContainmentPending(ctx, "ticket"); err != nil {
		t.Fatal(err)
	}
	restarted := state.NewForRepository(stateDir, "/repo")
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}}
	holder := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "open"}}
	if err := ReconcileTicketLoops(ctx, restarted, reader, holder); err != nil {
		t.Fatalf("reconcile pending after restart: %v", err)
	}
	loop, found, err := restarted.TicketLoop(ctx, "ticket")
	if err != nil || !found || loop.Phase != state.TicketLoopHeld || loop.HeldFrom != "open" {
		t.Fatalf("held after pending restart=%#v found=%t err=%v", loop, found, err)
	}
	reader.ticket.State = "hold"
	if err := ReconcileTicketLoops(ctx, restarted, reader, holder); err != nil {
		t.Fatalf("reconcile held after restart: %v", err)
	}
	if loop, found, err = restarted.TicketLoop(ctx, "ticket"); err != nil || !found || loop.Phase != state.TicketLoopHeld {
		t.Fatalf("held record lost across restart=%#v found=%t err=%v", loop, found, err)
	}
	reader.ticket.State = "review"
	if err := ReconcileTicketLoops(ctx, restarted, reader, holder); err != nil {
		t.Fatalf("reconcile recovery after restart: %v", err)
	}
	if _, found, err := restarted.TicketLoop(ctx, "ticket"); err != nil || found {
		t.Fatalf("recovered loop remains found=%t err=%v", found, err)
	}
}

func TestTerminalTicketLoopIsRemovedDuringReconciliation(t *testing.T) {
	ctx := context.Background()
	store := loopTestStore(t)
	if _, _, err := store.Participate(ctx, "ticket", 6); err != nil {
		t.Fatal(err)
	}
	reader := &loopTicketFixture{ticket: ticketclient.Ticket{ID: "ticket", State: "closed"}}
	if err := ReconcileTicketLoops(ctx, store, reader, nil); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.TicketLoop(ctx, "ticket"); err != nil || found {
		t.Fatalf("terminal loop remains found=%t err=%v", found, err)
	}
}
