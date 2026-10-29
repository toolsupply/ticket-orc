package orc

import (
	"context"
	"errors"
	"fmt"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type claimReleaseTickets interface {
	Release(context.Context, string) (ticketclient.Transition, error)
}

// releaseFailedStartupClaim releases only a claim whose owner and lifecycle
// still match the ticket immediately after a failed, sessionless harness turn.
// Callers must first successfully reread the exact claimed ticket.
func releaseFailedStartupClaim(
	ctx context.Context,
	tickets claimReleaseTickets,
	ticketID, actor, claimedState string,
	result harness.RunResult,
	harnessErr error,
	observed ticketclient.Ticket,
) (bool, bool, error) {
	if ctx == nil || ctx.Err() != nil || harnessErr == nil || result.SessionID != "" {
		return false, false, nil
	}
	observed.State = ticketclient.NormalizeLifecycleState(observed.State)
	if observed.ID != ticketID || observed.State != claimedState || observed.Assignee != actor {
		return false, false, nil
	}
	transition, err := tickets.Release(ctx, ticketID)
	if err != nil {
		return true, false, errors.Join(harnessErr, fmt.Errorf("automatic claim recovery failed: release %s claim %s: %w", claimedState, ticketID, err))
	}
	if !transition.Changed {
		return true, false, errors.Join(harnessErr, fmt.Errorf("automatic claim recovery failed: Ticket did not release unchanged %s claim %s", claimedState, ticketID))
	}
	return true, true, harnessErr
}

// Codex may execute commands before emitting a session ID. Preserve startup
// release only on positive adapter evidence that execution never began.
// Other harnesses and reviewers retain their existing startup behavior.
func releaseFailedCoderStartupClaim(ctx context.Context, adapter string, tickets claimReleaseTickets, ticketID, actor string, result harness.RunResult, runErr error, observed ticketclient.Ticket) (bool, bool, error) {
	if adapter == "codex" && (result.Evidence == nil || result.Evidence.Bounded().Process.Start != "not_started") {
		return false, false, nil
	}
	return releaseFailedStartupClaim(ctx, tickets, ticketID, actor, "open", result, runErr, observed)
}
