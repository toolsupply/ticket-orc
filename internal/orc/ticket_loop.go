package orc

import (
	"context"
	"errors"
	"fmt"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type TicketLoopState interface {
	Read(context.Context) (state.Snapshot, error)
	TicketLoop(context.Context, string) (state.TicketLoop, bool, error)
	Participate(context.Context, string, int) (state.TicketLoop, bool, error)
	RecordClaim(context.Context, string, string, string) (state.TicketLoop, error)
	RecordDispatch(context.Context, string) (state.TicketLoop, error)
	RecordSteerClaim(context.Context, string, string, string) (state.TicketLoop, bool, error)
	RecordStall(context.Context, string, uint64) (state.TicketLoop, bool, error)
	RecordBounce(context.Context, string, uint64) (state.TicketLoop, bool, error)
	ClearClaim(context.Context, string, uint64) (bool, error)
	MarkContainmentPending(context.Context, string) (state.TicketLoop, error)
	MarkHeld(context.Context, string, string) error
	ResetTicketLoop(context.Context, string) error
}

type TicketLoopReader interface {
	Show(context.Context, string) (ticketclient.Ticket, error)
	ShowReadiness(context.Context, string) (ticketclient.TicketDetail, error)
}

type TicketLoopHolder interface {
	HoldTicket(context.Context, string, ticketclient.MutationOptions) (ticketclient.MutationResult, error)
}

type TicketLoopReleaser interface {
	Release(context.Context, string) (ticketclient.Transition, error)
}

// ContainmentActor derives the audit identity used for Orc-owned Ticket
// lifecycle operations from the durable instance UUID.
func ContainmentActor(instanceID string) string { return "ticket-orc." + instanceID }

// ReconcileTicketLoop applies persisted pending/held state against an
// authoritative Ticket read. The Ticket hold mutation remains the final
// ownership check; assigned tickets stay pending.
func ReconcileTicketLoop(ctx context.Context, store TicketLoopState, reader TicketLoopReader, holder TicketLoopHolder, ticketID string) (state.TicketLoop, bool, error) {
	loop, found, err := store.TicketLoop(ctx, ticketID)
	if err != nil || !found {
		return loop, false, err
	}
	ticket, err := reader.Show(ctx, ticketID)
	if err != nil {
		return loop, true, fmt.Errorf("read ticket %s while reconciling its loop: %w", ticketID, err)
	}
	ticket.State = ticketclient.NormalizeLifecycleState(ticket.State)
	if ticket.State == ticketclient.StateClosed || ticket.State == "rejected" {
		if err := store.ResetTicketLoop(ctx, ticketID); err != nil {
			return loop, true, fmt.Errorf("remove terminal ticket loop %s: %w", ticketID, err)
		}
		return state.TicketLoop{}, false, nil
	}
	if loop.Phase == state.TicketLoopHeld {
		if ticket.State == "hold" {
			return loop, true, nil
		}
		if err := store.ResetTicketLoop(ctx, ticketID); err != nil {
			return loop, true, fmt.Errorf("reset recovered ticket loop %s: %w", ticketID, err)
		}
		return state.TicketLoop{}, false, nil
	}
	if ticket.State == "hold" {
		if err := store.MarkHeld(ctx, ticketID, ""); err != nil {
			return loop, true, fmt.Errorf("record observed hold for %s: %w", ticketID, err)
		}
		loop.Phase, loop.HeldFrom, loop.ClaimState, loop.ClaimActor = state.TicketLoopHeld, "", "", ""
		loop.DispatchPending = false
		return loop, true, nil
	}
	if loop.Phase != state.TicketLoopContainmentPending {
		return loop, true, nil
	}
	if ticket.State != "open" && ticket.State != "review" || ticket.Assignee != "" {
		return loop, true, nil
	}
	if holder == nil {
		return loop, true, fmt.Errorf("Ticket containment client is unavailable")
	}
	message := containmentMessage(loop, ticket.State)
	result, err := holder.HoldTicket(ctx, ticketID, ticketclient.MutationOptions{Message: message})
	if err != nil {
		var commandErr *ticketclient.CommandError
		if errors.As(err, &commandErr) && commandErr.Code == "already_claimed" {
			// Ticket's locked ownership check won a race with the unassigned
			// read. Keep containment pending and let the owning worker continue.
			return loop, true, nil
		}
		// Keep the durable circuit pending. A later authoritative reconciliation
		// determines whether Ticket applied an uncertain mutation.
		return loop, true, fmt.Errorf("hold tripped ticket %s: %w", ticketID, err)
	}
	if result.ID != ticketID || result.State != "hold" || (result.FromState != "open" && result.FromState != "review") {
		return loop, true, fmt.Errorf("hold ticket %s returned inconsistent transition", ticketID)
	}
	if err := store.MarkHeld(ctx, ticketID, result.FromState); err != nil {
		return loop, true, fmt.Errorf("persist held ticket loop %s: %w", ticketID, err)
	}
	loop.Phase, loop.HeldFrom, loop.ClaimState, loop.ClaimActor = state.TicketLoopHeld, result.FromState, "", ""
	return loop, true, nil
}

// ReconcileClaimedTicketLoop handles the one safe exception to assigned-ticket
// deferral: a managed worker may release the claim it just acquired itself so
// deterministic Orc-owned containment can use its own actor. A changed owner
// is never released or impersonated.
func ReconcileClaimedTicketLoop(ctx context.Context, store TicketLoopState, reader TicketLoopReader, holder TicketLoopHolder, releaser TicketLoopReleaser, actor, ticketID string) (state.TicketLoop, bool, error) {
	loop, found, err := ReconcileTicketLoop(ctx, store, reader, holder, ticketID)
	if err != nil || !found || loop.Phase != state.TicketLoopContainmentPending {
		return loop, found, err
	}
	ticket, err := reader.Show(ctx, ticketID)
	if err != nil {
		return loop, true, fmt.Errorf("read just-claimed ticket %s before releasing for containment: %w", ticketID, err)
	}
	if (ticket.State != "open" && ticket.State != "review") || ticket.Assignee != actor {
		return loop, true, nil
	}
	if releaser == nil {
		return loop, true, fmt.Errorf("Ticket release client is unavailable for just-claimed ticket %s", ticketID)
	}
	if _, err := releaser.Release(ctx, ticketID); err != nil {
		var commandErr *ticketclient.CommandError
		if errors.As(err, &commandErr) && commandErr.Code == "already_claimed" {
			return loop, true, nil
		}
		return loop, true, fmt.Errorf("release own claim on tripped ticket %s: %w", ticketID, err)
	}
	return ReconcileTicketLoop(ctx, store, reader, holder, ticketID)
}

func containmentMessage(loop state.TicketLoop, heldFrom string) string {
	if loop.BounceCount > 0 && loop.EffectiveBounceLimit > 0 && loop.BounceCount >= loop.EffectiveBounceLimit {
		return fmt.Sprintf("Orc circuit breaker: automatic processing placed this ticket on hold after repeated review bounces. Observed bounces: %d; effective limit: %d. Held from: %s. Resolve the underlying condition and move the ticket to an appropriate workflow state to resume automation.", loop.BounceCount, loop.EffectiveBounceLimit, heldFrom)
	}
	return fmt.Sprintf("Orc circuit breaker: automatic processing placed this ticket on hold after repeated same-state stalls. Observed stalls: %d. Held from: %s. Resolve the underlying condition and move the ticket to an appropriate workflow state to resume automation.", loop.StallCount, heldFrom)
}

// ReconcileEndedClaim records a stall only when positive claim evidence is
// followed by an unassigned, ready ticket in the same lifecycle state.
func ReconcileEndedClaim(ctx context.Context, store TicketLoopState, reader TicketLoopReader, holder TicketLoopHolder, ticketID string) (state.TicketLoop, error) {
	loop, found, err := store.TicketLoop(ctx, ticketID)
	if err != nil || !found || loop.ClaimState == "" || loop.Phase != state.TicketLoopActive {
		return loop, err
	}
	detail, err := reader.ShowReadiness(ctx, ticketID)
	if err != nil {
		return loop, fmt.Errorf("read ticket readiness after claim %s: %w", ticketID, err)
	}
	detail.State = ticketclient.NormalizeLifecycleState(detail.State)
	if detail.State == ticketclient.StateClosed || detail.State == "rejected" {
		if err := store.ResetTicketLoop(ctx, ticketID); err != nil {
			return loop, fmt.Errorf("remove terminal ticket loop %s: %w", ticketID, err)
		}
		return state.TicketLoop{}, nil
	}
	if detail.State == "hold" {
		if err := store.MarkHeld(ctx, ticketID, ""); err != nil {
			return loop, fmt.Errorf("record observed hold for %s: %w", ticketID, err)
		}
		loop.Phase, loop.HeldFrom, loop.ClaimState, loop.ClaimActor = state.TicketLoopHeld, "", "", ""
		loop.DispatchPending = false
		return loop, nil
	}
	if loop.ClaimState == "review" && detail.State == "open" {
		var consumed bool
		loop, consumed, err = store.RecordBounce(ctx, ticketID, loop.AttemptID)
		if err != nil {
			return loop, fmt.Errorf("record review return for %s: %w", ticketID, err)
		}
		if consumed && loop.Phase == state.TicketLoopContainmentPending {
			loop, _, err = ReconcileTicketLoop(ctx, store, reader, holder, ticketID)
		}
		return loop, err
	}
	if detail.State == loop.ClaimState {
		if detail.Assignee != "" {
			// An assigned ticket still has an active claim.
			return loop, nil
		}
		if detail.State == "open" || detail.State == "review" {
			if detail.Readiness == nil {
				// Without readiness evidence, preserve the attempt until an
				// authoritative reconciliation can decide whether it ended.
				return loop, nil
			}
			if !detail.Readiness.Ready {
				if _, err := store.ClearClaim(ctx, ticketID, loop.AttemptID); err != nil {
					return loop, fmt.Errorf("clear non-ready attempt for %s: %w", ticketID, err)
				}
				loop.ClaimState, loop.ClaimActor = "", ""
				return loop, nil
			}
			var consumed bool
			loop, consumed, err = store.RecordStall(ctx, ticketID, loop.AttemptID)
			if err != nil {
				return loop, fmt.Errorf("record same-state stall for %s: %w", ticketID, err)
			}
			if consumed && loop.Phase == state.TicketLoopContainmentPending {
				loop, _, err = ReconcileTicketLoop(ctx, store, reader, holder, ticketID)
			}
			return loop, err
		}
	}
	if _, err := store.ClearClaim(ctx, ticketID, loop.AttemptID); err != nil {
		return loop, fmt.Errorf("clear ended claim observation for %s: %w", ticketID, err)
	}
	loop.ClaimState, loop.ClaimActor = "", ""
	return loop, nil
}

// ReconcileTicketLoops inspects every durable loop at startup and before
// another managed claim, so pending containment and recovery do not rely on
// repository watch history.
func ReconcileTicketLoops(ctx context.Context, store TicketLoopState, reader TicketLoopReader, holder TicketLoopHolder) error {
	snapshot, err := store.Read(ctx)
	if err != nil {
		return fmt.Errorf("read ticket loops: %w", err)
	}
	for _, loop := range snapshot.Loops {
		if loop.Phase == state.TicketLoopActive && loop.ClaimState != "" {
			if _, err := ReconcileEndedClaim(ctx, store, reader, holder, loop.Ticket); err != nil {
				return err
			}
		}
		if _, _, err := ReconcileTicketLoop(ctx, store, reader, holder, loop.Ticket); err != nil {
			return err
		}
	}
	return nil
}
