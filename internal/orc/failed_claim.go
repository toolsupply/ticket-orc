package orc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// FailedClaimTickets is the existing Ticket read/release boundary. The caller
// binds its client to the configured repository and actor, under execution
// exclusivity, after observing the exact current worker's completed exit.
type FailedClaimTickets interface {
	TicketLoopReader
	ReleaseTicket(context.Context, string, ticketclient.MutationOptions) (ticketclient.MutationResult, error)
}

const CleanupUnconfirmed = "cleanup unconfirmed; preserve the claim and session; reconcile the previous execution before restarting workers in this workspace"

// ReconcileFailedCoderClaim ends an attempt, never starts an execution. Cleanup
// is positive outer-boundary evidence, independent of nested exit diagnostics.
// The supervisor calls this once while serializing conflicting lifecycle work.
// An interrupted daemon must not replay it from persisted actor/attempt strings.
func ReconcileFailedCoderClaim(ctx context.Context, expected FailureContext, failure *Failure, cleanupConfirmed bool, store *state.Store, tickets FailedClaimTickets, holder TicketLoopHolder) (string, error) {
	if !cleanupConfirmed {
		return CleanupUnconfirmed, nil
	}
	if ctx.Err() != nil {
		return "worker stop requested; preserve the claim", ctx.Err()
	}
	if failure == nil || failure.Execution == nil || failure.Classification != "harness_process_failed" || failure.Origin != "codex" || failure.Operation != "managed turn" {
		return "failure category is not eligible for claim release; inspect the failed attempt", nil
	}
	e := failure.Execution.Bounded()
	if e.Phase != "running" || e.Process.Start != "started" || !e.Process.ExitObserved || e.Process.Cancellation != "none" ||
		(e.Process.ExitCode == 0 && e.Process.Signal == 0) || e.ProtocolFailed || e.ProviderFailed || e.OutputFailed {
		return "execution outcome is not eligible for claim release; reconcile the failed attempt", nil
	}
	if expected.RepositoryID == "" || expected.Actor == "" || expected.Worker == "" || expected.Role == "" ||
		failure.RepositoryID != expected.RepositoryID || failure.Actor != expected.Actor || failure.Worker != expected.Worker || failure.Role != expected.Role ||
		failure.Ticket == "" || failure.AttemptID == 0 {
		return "execution identity changed or is incomplete; reconcile the exact claim", nil
	}
	if store == nil || tickets == nil {
		return "claim reconciliation unavailable; inspect the failed attempt", fmt.Errorf("failed claim dependencies unavailable")
	}
	loop, found, err := store.TicketLoop(ctx, failure.Ticket)
	if err != nil {
		return "cannot read recorded attempt; preserve the claim", err
	}
	if !found || loop.Repository != expected.RepositoryID || loop.Ticket != failure.Ticket || loop.AttemptID != failure.AttemptID ||
		loop.ClaimActor != expected.Actor || loop.ClaimState != "open" || loop.Phase != state.TicketLoopActive {
		return "recorded attempt changed or containment is pending; preserve the claim", nil
	}
	detail, err := tickets.ShowReadiness(ctx, failure.Ticket)
	if err != nil {
		return "cannot verify Ticket; preserve the claim", err
	}
	if detail.ID != failure.Ticket || detail.State != "open" || detail.Assignee != expected.Actor {
		// An already-ended claim can be reconciled without another release/note.
		if detail.ID == failure.Ticket && detail.State == "open" && detail.Assignee == "" {
			if err := retireInterruptedCoderSession(ctx, store, failure.Ticket, expected); err != nil {
				return "interrupted session retirement unconfirmed; inspect the ended claim", err
			}
			_, err = reconcileEndedClaim(ctx, store, tickets, holder, failure.Ticket, failure.AttemptID)
		}
		return "Ticket outcome or ownership supersedes the failed attempt; no release", err
	}
	// Ticket's bounded default show includes Handoff. Refuse a truncated or
	// oversized handoff instead of discarding substantive existing context.
	handoff := detail.Sections["handoff"]
	for _, available := range detail.Available {
		if available == "handoff" {
			if _, included := detail.Sections["handoff"]; !included {
				return "existing handoff is unavailable; read and merge it manually before releasing", nil
			}
		}
	}
	if handoff.Truncated || len(handoff.Text) > 8192 {
		return "existing handoff exceeds the safe merge bound; merge it manually before releasing", nil
	}
	note := fmt.Sprintf("Previous managed Codex attempt ended unexpectedly; the cause is unknown. Partial work may remain. Inspect this ticket's history, working tree and previous execution log before continuing. Reconcile already-applied changes and avoid blindly repeating expensive or destructive commands. Attempt: %d.", failure.AttemptID)
	if e.LogID != "" {
		note += " Log reference: " + e.LogID + "."
	}
	merged := strings.TrimSpace(handoff.Text)
	if merged != "" {
		merged += "\n\n"
	}
	merged += note
	if err := retireInterruptedCoderSession(ctx, store, failure.Ticket, expected); err != nil {
		return "interrupted session retirement unconfirmed; preserve the claim", err
	}
	message := fmt.Sprintf("Managed worker %s ended attempt %d unexpectedly (exit %d, signal %d); outer cleanup confirmed. Released unchanged open claim; inspect partial work before continuing.", expected.Worker, failure.AttemptID, e.Process.ExitCode, e.Process.Signal)
	result, releaseErr := tickets.ReleaseTicket(ctx, failure.Ticket, ticketclient.MutationOptions{Handoff: merged, Message: message})
	if releaseErr != nil {
		// A read can expose the resulting claim, but cannot prove delivery of
		// both handoff and audit message. Never replay an ambiguous mutation.
		_, readErr := tickets.Show(ctx, failure.Ticket)
		return "release outcome uncertain; inspect Ticket handoff/history and ownership before any further mutation", fmt.Errorf("release failed claim (do not retry): %w", errors.Join(releaseErr, readErr))
	}
	if result.ID != failure.Ticket || result.State != "open" || result.Assignee != "" || !result.Changed || (result.FromState != "" && result.FromState != "open") {
		observed, readErr := tickets.Show(ctx, failure.Ticket)
		if readErr != nil || observed.ID != failure.Ticket || observed.State != "open" || observed.Assignee != "" {
			return "release receipt unconfirmed; inspect Ticket before any further mutation", fmt.Errorf("inconsistent release receipt for %s: %w", failure.Ticket, errors.Join(fmt.Errorf("receipt mismatch"), readErr))
		}
	}
	_, err = reconcileEndedClaim(ctx, store, tickets, holder, failure.Ticket, failure.AttemptID)
	return "claim released with handoff; inspect the failed attempt before an ordinary worker start", err
}

func retireInterruptedCoderSession(ctx context.Context, store *state.Store, ticket string, expected FailureContext) error {
	current, found, err := store.GetSession(ctx, ticket, "coder", "codex", expected.Worker)
	if err != nil || !found {
		return err
	}
	if !current.IsCurrent() || current.Repository != expected.RepositoryID || current.ID == "" {
		return fmt.Errorf("interrupted session identity is uncertain")
	}
	_, changed, err := store.SupersedeSession(ctx, current, "unexpected_execution_termination")
	if err == nil && !changed {
		err = fmt.Errorf("interrupted session changed before retirement")
	}
	return err
}
