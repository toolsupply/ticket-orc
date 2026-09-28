package orc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// ErrNotTerminal prevents orchestration state from being removed before the
// authoritative ticket reaches a terminal state.
var ErrNotTerminal = errors.New("ticket is not terminal")

type stateStore interface {
	Read(context.Context) (state.Snapshot, error)
	RemoveTicket(context.Context, string) ([]state.Session, bool, error)
}

type ticketReader interface {
	Show(context.Context, string) (ticketclient.Ticket, error)
}

// SessionCleaner applies terminal policy to one retained harness session.
type SessionCleaner interface {
	Cleanup(context.Context, string, harness.CleanupPolicy) error
}

// CleanupFailure records a best-effort harness cleanup that could not be
// terminal after its state mapping was durably removed.
type CleanupFailure struct {
	Session state.Session
	Err     error
}

// CleanupResult describes one terminal ticket cleanup.
type CleanupResult struct {
	TicketID string
	State    string
	Changed  bool
	Removed  []state.Session
	Failures []CleanupFailure
}

// SweepResult summarizes a garbage-collection pass.
type SweepResult struct {
	Checked         int
	RemovedTickets  int
	RemovedSessions int
	CleanupFailures int
}

// Cleaner coordinates authoritative terminal checks, atomic state removal,
// and best-effort cleanup of retained harness sessions.
type Cleaner struct {
	store            stateStore
	tickets          ticketReader
	harnesses        map[string]SessionCleaner
	policy           harness.CleanupPolicy
	policyForSession func(state.Session) harness.CleanupPolicy
	diagnostics      io.Writer
}

// NewCleaner constructs terminal cleanup policy around injected state, ticket,
// and harness boundaries.
func NewCleaner(store stateStore, tickets ticketReader, harnesses map[string]SessionCleaner, policy harness.CleanupPolicy, diagnostics io.Writer) (*Cleaner, error) {
	return newCleaner(store, tickets, harnesses, policy, nil, diagnostics)
}

// NewCleanerWithSessionPolicy applies a default policy per retained harness
// when the caller has not supplied an explicit global cleanup policy.
func NewCleanerWithSessionPolicy(store stateStore, tickets ticketReader, harnesses map[string]SessionCleaner, policy harness.CleanupPolicy, policyForSession func(state.Session) harness.CleanupPolicy, diagnostics io.Writer) (*Cleaner, error) {
	return newCleaner(store, tickets, harnesses, policy, policyForSession, diagnostics)
}

func newCleaner(store stateStore, tickets ticketReader, harnesses map[string]SessionCleaner, policy harness.CleanupPolicy, policyForSession func(state.Session) harness.CleanupPolicy, diagnostics io.Writer) (*Cleaner, error) {
	if store == nil || tickets == nil {
		return nil, fmt.Errorf("cleanup state and ticket clients must not be nil")
	}
	if policy != harness.CleanupDelete && policy != harness.CleanupArchive && policy != harness.CleanupKeep {
		return nil, fmt.Errorf("unknown session cleanup policy %q", policy)
	}
	if diagnostics == nil {
		diagnostics = io.Discard
	}
	diagnostics = terminalDiagnostics{dst: diagnostics}
	cleaners := make(map[string]SessionCleaner, len(harnesses))
	for name, cleaner := range harnesses {
		if name == "" || cleaner == nil {
			return nil, fmt.Errorf("cleanup harness names and implementations must not be empty")
		}
		cleaners[name] = cleaner
	}
	return &Cleaner{
		store:            store,
		tickets:          tickets,
		harnesses:        cleaners,
		policy:           policy,
		policyForSession: policyForSession,
		diagnostics:      diagnostics,
	}, nil
}

// terminalDiagnostics is the human-output boundary for cleanup warnings.
// CleanupFailure.Err remains the original error; only bytes sent to the
// operator-facing diagnostics stream are sanitized.
type terminalDiagnostics struct{ dst io.Writer }

func (w terminalDiagnostics) Write(data []byte) (int, error) {
	clean := terminaltext.Sanitize(string(data), true)
	if _, err := io.WriteString(w.dst, clean); err != nil {
		return 0, err
	}
	return len(data), nil
}

// CleanupTicket confirms the ticket's current state and removes terminal
// orchestration state. Harness cleanup failures are reported in the result and
// diagnostics but do not restore mappings or fail the operation.
func (c *Cleaner) CleanupTicket(ctx context.Context, ticketID string) (CleanupResult, error) {
	ticket, err := c.tickets.Show(ctx, ticketID)
	if err != nil {
		return CleanupResult{TicketID: ticketID}, fmt.Errorf("confirm terminal ticket %s: %w", ticketID, err)
	}
	ticket = ticket.NormalizeState()
	if !isTerminal(ticket.State) {
		return CleanupResult{TicketID: ticket.ID, State: ticket.State}, fmt.Errorf("%w: %s is %s", ErrNotTerminal, ticket.ID, ticket.State)
	}
	return c.removeConfirmed(ctx, ticket)
}

// CleanupConfirmed removes terminal state for a ticket whose state was just
// confirmed by the caller through the authoritative ticket client.
func (c *Cleaner) CleanupConfirmed(ctx context.Context, ticket ticketclient.Ticket) (CleanupResult, error) {
	ticket = ticket.NormalizeState()
	if !isTerminal(ticket.State) {
		return CleanupResult{TicketID: ticket.ID, State: ticket.State}, fmt.Errorf("%w: %s is %s", ErrNotTerminal, ticket.ID, ticket.State)
	}
	return c.removeConfirmed(ctx, ticket)
}

// Sweep checks every ticket represented in retained state and removes entries
// whose authoritative state is terminal. It continues past individual ticket
// lookup and state-removal errors, then returns them together.
func (c *Cleaner) Sweep(ctx context.Context) (SweepResult, error) {
	snapshot, err := c.store.Read(ctx)
	if err != nil {
		return SweepResult{}, fmt.Errorf("read orchestration state: %w", err)
	}
	ids := snapshot.TicketIDs()
	result := SweepResult{Checked: len(ids)}
	var sweepErr error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(sweepErr, err)
		}
		ticket, err := c.tickets.Show(ctx, id)
		if err != nil {
			wrapped := fmt.Errorf("inspect ticket %s: %w", id, err)
			c.warn("%v", wrapped)
			sweepErr = errors.Join(sweepErr, wrapped)
			continue
		}
		ticket = ticket.NormalizeState()
		if !isTerminal(ticket.State) {
			continue
		}
		cleanup, err := c.removeConfirmed(ctx, ticket)
		if err != nil {
			wrapped := fmt.Errorf("clean ticket %s: %w", id, err)
			c.warn("%v", wrapped)
			sweepErr = errors.Join(sweepErr, wrapped)
			continue
		}
		if cleanup.Changed {
			result.RemovedTickets++
		}
		result.RemovedSessions += len(cleanup.Removed)
		result.CleanupFailures += len(cleanup.Failures)
	}
	return result, sweepErr
}

func (c *Cleaner) removeConfirmed(ctx context.Context, ticket ticketclient.Ticket) (CleanupResult, error) {
	ticket = ticket.NormalizeState()
	result := CleanupResult{TicketID: ticket.ID, State: ticket.State}
	removed, changed, err := c.store.RemoveTicket(ctx, ticket.ID)
	if err != nil {
		return result, fmt.Errorf("remove orchestration state: %w", err)
	}
	result.Changed = changed
	result.Removed = removed
	for _, session := range removed {
		cleaner, ok := c.harnesses[session.Harness]
		if !ok {
			err := fmt.Errorf("no cleanup implementation for harness %q", session.Harness)
			result.Failures = append(result.Failures, CleanupFailure{Session: session, Err: err})
			c.warn("%s: could not clean %s session %s (%s): %v", ticket.ID, session.Harness, session.ID, session.Role, err)
			continue
		}
		policy := c.policy
		if c.policyForSession != nil {
			policy = c.policyForSession(session)
		}
		if err := cleaner.Cleanup(ctx, session.ID, policy); err != nil {
			result.Failures = append(result.Failures, CleanupFailure{Session: session, Err: err})
			c.warn("%s: could not apply %s to %s session %s (%s): %v", ticket.ID, policy, session.Harness, session.ID, session.Role, err)
		}
	}
	if changed {
		c.log("%s: removed terminal orchestration state (%d session(s))", ticket.ID, len(removed))
	}
	return result, nil
}

func (c *Cleaner) log(format string, args ...any) {
	_, _ = fmt.Fprintf(c.diagnostics, "[ticket-orc] "+format+"\n", args...)
}

func (c *Cleaner) warn(format string, args ...any) {
	_, _ = fmt.Fprintf(c.diagnostics, "[ticket-orc] warning: "+format+"\n", args...)
}

func isTerminal(ticketState string) bool {
	return ticketclient.NormalizeLifecycleState(ticketState) == ticketclient.StateClosed || ticketState == "rejected"
}
