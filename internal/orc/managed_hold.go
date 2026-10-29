package orc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// holdQueueTickets observes unclaimed holds on the worker's existing,
// repository-bound Ticket client and acquires exclusive assessment ownership.
type holdQueueTickets interface {
	EscalateHold(context.Context, string, string, string) (ticketclient.Ticket, error)
	OwnedHold(context.Context) (*ticketclient.Ticket, error)
	AcquireHold(context.Context, string, ticketclient.QueueFilters) (*ticketclient.Ticket, error)
	Show(context.Context, string) (ticketclient.Ticket, error)
	HasActiveClaim(context.Context, string) (bool, error)
	HoldFrontier(context.Context, ticketclient.QueueFilters, int, int) (ticketclient.ListResult, error)
	StartChangeWatch(context.Context, string) (ticketclient.ChangeWatcher, error)
}

// HoldTaskPrompt is the shared boundary for managed and steered holds.
// It is the entire task; ordinary primary completion prompts
// must not leak into it. Authority describes permitted decisions, not identity.
func HoldTaskPrompt(ticketID, primaryQueue string) string {
	authority := "For this hold assessment, exercise reviewer authority and escalate architectural or scope decisions outside approved authority."
	if primaryQueue == "signoff" {
		authority = "For this hold assessment, exercise architectural planning authority within approved scope."
	}
	return HoldTurnPrompt(ticketID) + "\n\n" + authority
}

func selectManagedHold(ctx context.Context, tickets holdQueueTickets, filters ticketclient.QueueFilters, watch ticketclient.ChangeWatcher, cleanup TicketCleanup) (*ticketclient.Ticket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := watch.Check(); err != nil {
		return nil, fmt.Errorf("Ticket change watch failed: %w", err)
	}
	// Ownership is actor-wide, independent of tags. Leave active review work
	// to Ticket's atomic next --claim, and respect an existing open claim.
	active, err := hasManagedActiveClaim(ctx, tickets)
	if err != nil || active {
		return nil, err
	}
	old, err := tickets.OwnedHold(ctx)
	if err != nil {
		return nil, err
	}
	if old != nil {
		return nil, fmt.Errorf("held ticket %s remains assigned to %s without current execution authority; operator reconciliation required", old.ID, old.Assignee)
	}
	for offset := 0; ; {
		frontier, err := tickets.HoldFrontier(ctx, filters, acceptanceFrontierLimit, offset)
		if err != nil {
			return nil, fmt.Errorf("observe Ticket hold frontier: %w", err)
		}
		for _, candidate := range frontier.Items {
			if err := ticketclient.ValidateFullID(candidate.ID); err != nil {
				return nil, fmt.Errorf("hold candidate: %w", err)
			}
			current, err := tickets.Show(ctx, candidate.ID)
			if err != nil {
				return nil, fmt.Errorf("recheck hold ticket %s before dispatch: %w", candidate.ID, err)
			}
			if current.ID != candidate.ID {
				return nil, fmt.Errorf("%w: hold recheck returned another ticket", ticketclient.ErrProtocol)
			}
			current = current.NormalizeState()
			if isTerminalAcceptanceTicket(current) {
				if err := cleanupAcceptanceTicket(ctx, cleanup, current); err != nil {
					return nil, err
				}
				continue
			}
			if current.State != "hold" || current.Assignee != "" || !matchesQueueFilters(current, filters) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := watch.Check(); err != nil {
				return nil, fmt.Errorf("Ticket change watch failed before hold dispatch: %w", err)
			}
			acquired, err := tickets.AcquireHold(ctx, current.ID, filters)
			if err != nil {
				return nil, err
			}
			if acquired == nil {
				// A definite lost race yields this opportunity to primary work.
				return nil, nil
			}
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("owned hold %s not dispatched; operator reconciliation required: %w", acquired.ID, err)
			}
			if err := watch.Check(); err != nil {
				return nil, fmt.Errorf("owned hold %s not dispatched after watch failure; operator reconciliation required: %w", acquired.ID, err)
			}
			return acquired, nil
		}
		if !frontier.More {
			return nil, nil
		}
		if err := advanceAcceptanceOffset(&offset, acceptanceFrontierLimit); err != nil {
			return nil, err
		}
	}
}

// Active ownership takes precedence over either input, independently of tags.
func hasManagedActiveClaim(ctx context.Context, tickets interface {
	HasActiveClaim(context.Context, string) (bool, error)
}) (bool, error) {
	for _, queue := range []string{"open", "review"} {
		active, err := tickets.HasActiveClaim(ctx, queue)
		if err != nil {
			return false, fmt.Errorf("observe active %s claim before dispatch: %w", queue, err)
		}
		if active {
			return true, nil
		}
	}
	return false, nil
}

func finishManagedHold(ctx context.Context, tickets holdQueueTickets, watch ticketclient.ChangeWatcher, cleanup TicketCleanup, ticket ticketclient.Ticket, result harness.RunResult, turnErr error, sink EventSink, worker, role string, diagnostics io.Writer) error {
	if turnErr == nil && (!result.StreamEndedNormally || result.ExitCode != 0) {
		turnErr = &harness.ExecutionError{Harness: "managed", Result: result, Cause: errors.New("hold assessment lacks clean turn completion")}
	}
	observed, err := tickets.Show(ctx, ticket.ID)
	if err != nil {
		return errors.Join(turnErr, fmt.Errorf("reobserve held ticket %s after managed turn: %w", ticket.ID, err))
	}
	if observed.ID != ticket.ID {
		return errors.Join(turnErr, fmt.Errorf("%w: hold outcome returned another ticket", ticketclient.ErrProtocol))
	}
	if err := watch.Check(); err != nil {
		turnErr = errors.Join(turnErr, fmt.Errorf("Ticket change watch failed: %w", err))
	}
	observed = observed.NormalizeState()
	// Escalate only positively clean completion. Execution and observation
	// failures retain ownership and their original evidence for reconciliation.
	if turnErr == nil && observed.State == "hold" && (observed.Assignee == "" || observed.Assignee == ticket.Assignee) {
		observed, err = tickets.EscalateHold(ctx, ticket.ID, observed.Assignee,
			"Automated hold assessment returned unresolved. User action required: review the existing blocker and assessment context, resolve the blocker or provide the missing decision, then explicitly return the ticket to the eligible workflow when ready.")
		if err != nil {
			return fmt.Errorf("escalate unresolved hold %s; operator reconciliation required: %w", ticket.ID, err)
		}
		if observed.ID != ticket.ID {
			return fmt.Errorf("%w: hold escalation returned another ticket", ticketclient.ErrProtocol)
		}
		if observed.State == "hold" && observed.Assignee == "user" {
			logAcceptance(diagnostics, "%s: hold assessment outcome hold_escalated", ticket.ID)
			emitEvent(sink, Event{Type: "ticket.hold", Worker: worker, Role: role, State: "hold_escalated", Ticket: ticket.ID})
			return nil
		}
		if observed.State == "hold" && (observed.Assignee == "" || observed.Assignee == ticket.Assignee) {
			return fmt.Errorf("hold escalation %s left assessment ownership unchanged; operator reconciliation required", ticket.ID)
		}
	}
	switch observed.State {
	case "open", "review", "signoff", "hold", "queued", "closed", "rejected":
	default:
		return errors.Join(turnErr, fmt.Errorf("%w: unknown hold outcome state %q", ticketclient.ErrProtocol, observed.State))
	}
	outcome := observed.State
	if observed.State == "hold" {
		switch observed.Assignee {
		case "", ticket.Assignee:
			outcome = "hold_unresolved"
		case "user":
			outcome = "human_handoff"
		default:
			outcome = "assigned_elsewhere"
		}
	}
	logAcceptance(diagnostics, "%s: hold assessment outcome %s", ticket.ID, outcome)
	emitEvent(sink, Event{Type: "ticket.hold", Worker: worker, Role: role, State: outcome, Ticket: ticket.ID})
	if isTerminalAcceptanceTicket(observed) {
		turnErr = errors.Join(turnErr, cleanupAcceptanceTicket(ctx, cleanup, observed))
	}
	if turnErr != nil {
		return fmt.Errorf("hold turn for %s: %w", ticket.ID, turnErr)
	}
	return nil
}
