package orc

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
)

type managedSessionState interface {
	TicketState
	SupersedeSession(context.Context, state.Session, string) (state.Session, bool, error)
	RegisterSession(context.Context, state.Session, string) error
	LinkSessionReplacement(context.Context, state.Session, string) error
	LinkLatestSessionReplacement(context.Context, state.Session, string) error
}

type managedTurnOptions struct {
	Ticket, Role, Harness, Owner string
	Policy                       string
	Cleanup                      harness.CleanupPolicy
	MinimumContextPercent        int
	Diagnostics                  io.Writer
}

type managedContextTelemetryState interface {
	SetSessionContextTelemetry(context.Context, state.Session, contextheadroom.Telemetry) (bool, error)
}

// runManagedTurn is the shared coder/reviewer eligibility and replacement
// path. Retained IDs are resumed only while their exact ownership record is
// current; invalidation is recorded before any fresh session is started.
func runManagedTurn(ctx context.Context, options managedTurnOptions, orchestrationState TicketState, agent harness.Harness, request harness.RunRequest) (harness.RunResult, error, bool) {
	if options.Policy != SessionPolicyTicket && options.Policy != SessionPolicyFresh {
		return harness.RunResult{}, fmt.Errorf("unknown managed session policy %q", options.Policy), false
	}
	managed, ok := orchestrationState.(managedSessionState)
	if !ok {
		return harness.RunResult{}, fmt.Errorf("state store does not support safe managed session lifecycle"), false
	}
	key := state.Session{Ticket: options.Ticket, Role: options.Role, Harness: options.Harness, Owner: options.Owner, ID: "lookup"}
	current, found, err := managed.GetSession(ctx, options.Ticket, options.Role, options.Harness, options.Owner)
	if err != nil {
		return harness.RunResult{}, fmt.Errorf("read retained session: %w", err), false
	}
	if found && (!current.IsCurrent() || current.ID == "") {
		found = false
	}
	replacedID := ""
	if found && options.Policy == SessionPolicyTicket {
		known, allowed := contextheadroom.ReuseAllowed(current.ContextTelemetry, options.MinimumContextPercent, time.Now().UTC())
		if known && !allowed {
			retired, changed, err := supersedeManagedSession(ctx, options, managed, agent, current, "context_below_threshold")
			if err != nil {
				return harness.RunResult{}, err, false
			}
			if !changed {
				return harness.RunResult{}, fmt.Errorf("retained session changed before context supersession"), false
			}
			replacedID = retired.ID
			found = false
		} else if !known && current.ContextTelemetry.Known {
			clearSessionContextTelemetry(ctx, options, orchestrationState, current)
		}
	}
	if found && options.Policy == SessionPolicyFresh {
		retired, changed, err := supersedeManagedSession(ctx, options, managed, agent, current, "session_policy_fresh")
		if err != nil {
			return harness.RunResult{}, fmt.Errorf("supersede retained session: %w", err), false
		}
		if !changed {
			return harness.RunResult{}, fmt.Errorf("retained session changed before supersession"), false
		}
		replacedID = retired.ID
		found = false
	}
	var result harness.RunResult
	var runErr error
	if found {
		result, runErr = agent.Resume(ctx, current.ID, request)
		if result.SessionOutcome == harness.SessionInvalidated {
			retired, changed, err := supersedeManagedSession(ctx, options, managed, agent, current, "harness_invalidated")
			if err != nil {
				return result, fmt.Errorf("record invalidated session: %w", err), true
			}
			if changed {
				replacedID = retired.ID
			} else {
				return result, fmt.Errorf("invalidated session changed before supersession"), true
			}
			found = false
			result, runErr = agent.Run(ctx, request)
		}
	} else {
		result, runErr = agent.Run(ctx, request)
	}
	if options.Policy == SessionPolicyTicket && result.SessionID != "" && result.SessionOutcome != harness.SessionInvalidated {
		record := key
		record.ID = result.SessionID
		record.ContextTelemetry = normalizedContextTelemetry(result.ContextTelemetry)
		if err := managed.RegisterSession(ctx, record, replacedID); err != nil {
			cleanupSupersededSession(ctx, options, agent, record)
			return result, fmt.Errorf("register managed session: %w", err), true
		}
		if telemetryState, ok := orchestrationState.(managedContextTelemetryState); ok {
			if _, err := telemetryState.SetSessionContextTelemetry(ctx, record, record.ContextTelemetry); err != nil {
				logCoder(options.Diagnostics, "managed session %s context telemetry could not be recorded: %v", record.ID, err)
			}
		}
	} else if result.SessionID != "" {
		var err error
		if replacedID != "" {
			err = managed.LinkSessionReplacement(ctx, keyWithID(options, replacedID), result.SessionID)
		} else {
			err = managed.LinkLatestSessionReplacement(ctx, keyWithID(options, "lookup"), result.SessionID)
		}
		if err != nil {
			logCoder(options.Diagnostics, "managed session replacement %s could not be recorded: %v", result.SessionID, err)
		}
	}
	return result, runErr, true
}

func supersedeManagedSession(ctx context.Context, options managedTurnOptions, managed managedSessionState, agent harness.Harness, current state.Session, reason string) (state.Session, bool, error) {
	retired, changed, err := managed.SupersedeSession(ctx, current, reason)
	if err != nil || !changed {
		return retired, changed, err
	}
	cleanupSupersededSession(ctx, options, agent, retired)
	return retired, true, nil
}

func normalizedContextTelemetry(telemetry contextheadroom.Telemetry) contextheadroom.Telemetry {
	if !telemetry.Valid() {
		return contextheadroom.Telemetry{}
	}
	return telemetry
}

func clearSessionContextTelemetry(ctx context.Context, options managedTurnOptions, orchestrationState TicketState, current state.Session) {
	if telemetryState, ok := orchestrationState.(managedContextTelemetryState); ok {
		if _, err := telemetryState.SetSessionContextTelemetry(ctx, current, contextheadroom.Telemetry{}); err != nil {
			logCoder(options.Diagnostics, "managed session %s stale context telemetry could not be cleared: %v", current.ID, err)
		}
	}
}

func keyWithID(options managedTurnOptions, id string) state.Session {
	return state.Session{Ticket: options.Ticket, Role: options.Role, Harness: options.Harness, Owner: options.Owner, ID: id}
}

func cleanupSupersededSession(ctx context.Context, options managedTurnOptions, agent harness.Harness, session state.Session) {
	cleaner, ok := agent.(interface {
		Cleanup(context.Context, string, harness.CleanupPolicy) error
	})
	if !ok || options.Cleanup == "" || options.Cleanup == harness.CleanupKeep {
		return
	}
	if err := cleaner.Cleanup(ctx, session.ID, options.Cleanup); err != nil {
		logCoder(options.Diagnostics, "managed session %s superseded (%s); cleanup failed: %v", session.ID, session.SupersededReason, err)
	}
}
