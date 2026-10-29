package orc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	AcceptanceSessionRole   = "architect"
	acceptanceFrontierLimit = 20
)

// AcceptanceConfig contains execution settings for one configured signoff role.
type AcceptanceConfig struct {
	WorkerName                 string
	RoleName                   string
	Actor                      string
	Harness                    string
	Model                      string
	Reasoning                  string
	SessionPolicy              string
	SessionCleanup             string
	MinimumReuseContextPercent int
	StateDir                   string
	OutputMode                 string
	QueueFilters               ticketclient.QueueFilters
	HoldQueue                  bool
	HoldQueueFilters           ticketclient.QueueFilters
	TicketPrompt               string
	RepositoryID               string
	CodexSandbox               string
	CodexServiceTier           string
	PiProvider                 string
	ClaudePermissionMode       string
	WorkingDir                 string
	Operator                   io.Writer
	Diagnostics                io.Writer
	EventSink                  EventSink
}

type AcceptanceTickets interface {
	HasActiveClaim(context.Context, string) (bool, error)
	Show(context.Context, string) (ticketclient.Ticket, error)
	SignoffFrontier(context.Context, ticketclient.QueueFilters, int, int) (ticketclient.ListResult, error)
	StartChangeWatch(context.Context, string) (ticketclient.ChangeWatcher, error)
}

// RunAcceptance handles one unclaimed signoff entry per harness turn. Ticket
// remains authoritative for both eligibility and lifecycle state.
func RunAcceptance(ctx context.Context, config AcceptanceConfig, tickets AcceptanceTickets, agent harness.Harness, orchestrationState TicketState, cleanup TicketCleanup) (runErr error) {
	if config.RoleName == "" {
		config.RoleName = "architect"
	}
	if ctx == nil {
		return fmt.Errorf("acceptance context must not be nil")
	}
	if tickets == nil || agent == nil || orchestrationState == nil {
		return fmt.Errorf("acceptance dependencies must not be nil")
	}
	if strings.TrimSpace(config.Actor) == "" || strings.TrimSpace(config.Harness) == "" {
		return fmt.Errorf("acceptance actor and harness must not be empty")
	}
	if !ticketclient.ValidRepositoryID(config.RepositoryID) {
		return fmt.Errorf("acceptance repository ID is invalid")
	}
	if config.SessionPolicy != SessionPolicyTicket && config.SessionPolicy != SessionPolicyFresh {
		return fmt.Errorf("unknown acceptance session policy %q", config.SessionPolicy)
	}
	if config.MinimumReuseContextPercent < 0 || config.MinimumReuseContextPercent > 100 {
		return fmt.Errorf("acceptance minimum reuse context percent must be from 0 to 100")
	}
	if config.TicketPrompt != "" {
		if err := ValidateTicketPrompt(config.TicketPrompt, "acceptance"); err != nil {
			return fmt.Errorf("acceptance ticket prompt: %w", err)
		}
	}
	if err := harness.PreflightIfSupported(agent, harness.PreflightConfig{
		Model:                config.Model,
		Reasoning:            config.Reasoning,
		SessionPolicy:        config.SessionPolicy,
		SessionCleanup:       harness.CleanupPolicy(config.SessionCleanup),
		OutputMode:           config.OutputMode,
		CodexSandbox:         config.CodexSandbox,
		CodexServiceTier:     config.CodexServiceTier,
		PiProvider:           config.PiProvider,
		ClaudePermissionMode: config.ClaudePermissionMode,
	}); err != nil {
		return err
	}
	if config.Operator == nil {
		config.Operator = io.Discard
	}
	if config.Diagnostics == nil {
		config.Diagnostics = io.Discard
	}
	if strings.TrimSpace(config.WorkingDir) == "" {
		workingDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("resolve acceptance working directory: %w", err)
		}
		config.WorkingDir = workingDir
	}
	filters, err := ticketclient.CanonicalQueueFilters(config.QueueFilters)
	if err != nil {
		return fmt.Errorf("acceptance selector: %w", err)
	}
	var holds holdQueueTickets
	if config.HoldQueue {
		var ok bool
		holds, ok = tickets.(holdQueueTickets)
		if !ok {
			return fmt.Errorf("acceptance Ticket client does not support hold observation")
		}
		config.HoldQueueFilters, err = ticketclient.CanonicalQueueFilters(config.HoldQueueFilters)
		if err != nil {
			return fmt.Errorf("acceptance hold selector: %w", err)
		}
	}
	watch, err := tickets.StartChangeWatch(ctx, config.RepositoryID)
	if err != nil {
		return fmt.Errorf("start Ticket change watch: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, watch.Close()) }()
	emitEvent(config.EventSink, Event{Type: WorkerReadyEventType, Worker: config.WorkerName, Role: config.RoleName, State: "ready", Phase: "startup"})

	// The cursor is local to this worker invocation, never durable state.
	preferPrimary := false
	dispatchHold := func() (bool, error) {
		if holds == nil {
			return false, nil
		}

		candidate, err := selectManagedHold(ctx, holds, config.HoldQueueFilters, watch, cleanup)
		if err != nil {
			return false, err
		}
		if candidate != nil {
			emitEvent(config.EventSink, Event{Type: "ticket.hold", Worker: config.WorkerName, Role: config.RoleName, State: "selected", Ticket: candidate.ID})
			result, turnErr := runAcceptanceTask(ctx, config, *candidate, HoldTaskPrompt(candidate.ID, "signoff"), agent, orchestrationState)
			if err := finishManagedHold(ctx, holds, watch, cleanup, *candidate, result, turnErr, config.EventSink, config.WorkerName, config.RoleName, config.Diagnostics); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil
	}
	offset := 0
	staleTargets := make(map[string]struct{})
	waitForChange := func() error {
		for {
			material, err := watch.WaitForChange(ctx)
			if err != nil {
				return fmt.Errorf("wait for Ticket change: %w", err)
			}
			if material {
				if err := refreshStaleAcceptanceTargets(ctx, tickets, cleanup, staleTargets); err != nil {
					return err
				}
				offset = 0
				return nil
			}
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := watch.Check(); err != nil {
			return fmt.Errorf("Ticket change watch failed: %w", err)
		}
		// An architect must wait for its actor's existing work to finish before
		// selecting either input, including on primary-preferred boundaries.
		active, err := hasManagedActiveClaim(ctx, tickets)
		if err != nil {
			return err
		}
		if active {
			if err := waitForChange(); err != nil {
				return err
			}
			continue
		}
		if !preferPrimary {
			selected, err := dispatchHold()
			if err != nil {
				return err
			}
			if selected {
				preferPrimary = true
				offset = 0
				continue
			}
		}
		frontier, err := tickets.SignoffFrontier(ctx, filters, acceptanceFrontierLimit, offset)
		if err != nil {
			return fmt.Errorf("observe Ticket signoff frontier: %w", err)
		}
		if len(frontier.Items) == 0 {
			if frontier.More {
				if err := advanceAcceptanceOffset(&offset, acceptanceFrontierLimit); err != nil {
					return err
				}
				continue
			}
			if preferPrimary {
				selected, err := dispatchHold()
				if err != nil {
					return err
				}
				if selected {
					offset = 0
					continue
				}
			}
			if err := waitForChange(); err != nil {
				return err
			}
			continue
		}
		candidate := frontier.Items[0]
		if _, stale := staleTargets[candidate.ID]; stale {
			if err := advanceAcceptanceOffset(&offset, 1); err != nil {
				return err
			}
			continue
		}
		current, err := tickets.Show(ctx, candidate.ID)
		if err != nil {
			return fmt.Errorf("recheck signoff ticket %s before acceptance: %w", candidate.ID, err)
		}
		current.State = ticketclient.NormalizeLifecycleState(current.State)
		if isTerminalAcceptanceTicket(current) {
			if err := cleanupAcceptanceTicket(ctx, cleanup, current); err != nil {
				return fmt.Errorf("clean terminal ticket %s before acceptance: %w", candidate.ID, err)
			}
			continue
		}
		if !eligibleAcceptanceEntry(current, filters) {
			continue
		}
		if err := watch.Check(); err != nil {
			return fmt.Errorf("Ticket change watch failed before dispatch: %w", err)
		}
		logAcceptance(config.Diagnostics, "%s: selected unassigned signoff work", candidate.ID)
		emitEvent(config.EventSink, Event{Type: "ticket.acceptance", Worker: config.WorkerName, Role: config.RoleName, State: "selected", Ticket: candidate.ID})
		preferPrimary = false
		turnErr := runAcceptanceTurn(ctx, config, current, agent, orchestrationState)
		observed, observeErr := tickets.Show(ctx, candidate.ID)
		if observeErr != nil {
			return errors.Join(turnErr, fmt.Errorf("reobserve signoff ticket %s after acceptance turn: %w", candidate.ID, observeErr))
		}
		if watchErr := watch.Check(); watchErr != nil {
			turnErr = errors.Join(turnErr, fmt.Errorf("Ticket change watch failed: %w", watchErr))
		}
		observed.State = ticketclient.NormalizeLifecycleState(observed.State)
		if observed.State == ticketclient.StateClosed || observed.State == "rejected" {
			if cleanup == nil {
				return errors.Join(turnErr, fmt.Errorf("terminal ticket %s requires cleanup", candidate.ID))
			}
			if _, cleanupErr := cleanup.CleanupConfirmed(ctx, observed); cleanupErr != nil {
				return errors.Join(turnErr, fmt.Errorf("clean terminal ticket %s: %w", candidate.ID, cleanupErr))
			}
		}
		if observed.State == "signoff" {
			switch {
			case observed.Assignee == "":
				staleTargets[candidate.ID] = struct{}{}
				logAcceptance(config.Diagnostics, "%s: acceptance returned without lifecycle or ownership progress", candidate.ID)
				emitEvent(config.EventSink, Event{Type: "ticket.acceptance", Worker: config.WorkerName, Role: config.RoleName, State: "acceptance_stale", Ticket: candidate.ID})
			case observed.Assignee == "user":
				logAcceptance(config.Diagnostics, "%s: acceptance handed to user", candidate.ID)
				emitEvent(config.EventSink, Event{Type: "ticket.acceptance", Worker: config.WorkerName, Role: config.RoleName, State: "human_handoff", Ticket: candidate.ID})
			default:
				logAcceptance(config.Diagnostics, "%s: acceptance ticket assigned to %s", candidate.ID, observed.Assignee)
				emitEvent(config.EventSink, Event{Type: "ticket.acceptance", Worker: config.WorkerName, Role: config.RoleName, State: "assigned_elsewhere", Ticket: candidate.ID})
			}
		} else {
			emitEvent(config.EventSink, Event{Type: "ticket.acceptance", Worker: config.WorkerName, Role: config.RoleName, State: observed.State, Ticket: candidate.ID})
		}
		if turnErr != nil {
			return fmt.Errorf("acceptance turn for %s: %w", candidate.ID, turnErr)
		}
		if observed.State == "signoff" && observed.Assignee == "" {
			if err := advanceAcceptanceOffset(&offset, 1); err != nil {
				return err
			}
		}
	}
}

func refreshStaleAcceptanceTargets(ctx context.Context, tickets AcceptanceTickets, cleanup TicketCleanup, stale map[string]struct{}) error {
	ids := make([]string, 0, len(stale))
	for id := range stale {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		observed, err := tickets.Show(ctx, id)
		if err != nil {
			return fmt.Errorf("recheck stale signoff ticket %s after Ticket change: %w", id, err)
		}
		observed.State = ticketclient.NormalizeLifecycleState(observed.State)
		if isTerminalAcceptanceTicket(observed) {
			if err := cleanupAcceptanceTicket(ctx, cleanup, observed); err != nil {
				return fmt.Errorf("clean terminal stale ticket %s: %w", id, err)
			}
			delete(stale, id)
			continue
		}
		if observed.State != "signoff" || observed.Assignee != "" {
			delete(stale, id)
		}
	}
	return nil
}

func isTerminalAcceptanceTicket(ticket ticketclient.Ticket) bool {
	state := ticketclient.NormalizeLifecycleState(ticket.State)
	return state == ticketclient.StateClosed || state == "rejected"
}

func cleanupAcceptanceTicket(ctx context.Context, cleanup TicketCleanup, ticket ticketclient.Ticket) error {
	if cleanup == nil {
		return fmt.Errorf("terminal ticket %s requires cleanup", ticket.ID)
	}
	if _, err := cleanup.CleanupConfirmed(ctx, ticket); err != nil {
		return fmt.Errorf("clean terminal ticket %s: %w", ticket.ID, err)
	}
	return nil
}

func eligibleAcceptanceEntry(ticket ticketclient.Ticket, filters ticketclient.QueueFilters) bool {
	if ticket.State != "signoff" || ticket.Assignee != "" {
		return false
	}
	return matchesQueueFilters(ticket, filters)
}

func matchesQueueFilters(ticket ticketclient.Ticket, filters ticketclient.QueueFilters) bool {
	tags := make(map[string]struct{}, len(ticket.Tags))
	for _, tag := range ticket.Tags {
		tags[tag] = struct{}{}
	}
	for _, tag := range filters.Tags {
		if _, ok := tags[tag]; !ok {
			return false
		}
	}
	for _, tag := range filters.WithoutTags {
		if _, ok := tags[tag]; ok {
			return false
		}
	}
	return true
}

func advanceAcceptanceOffset(offset *int, amount int) error {
	if offset == nil || amount < 0 || *offset > int(^uint(0)>>1)-amount {
		return fmt.Errorf("signoff frontier offset overflow")
	}
	*offset += amount
	return nil
}

func runAcceptanceTurn(ctx context.Context, config AcceptanceConfig, ticket ticketclient.Ticket, agent harness.Harness, orchestrationState TicketState) error {
	template := config.TicketPrompt
	if template == "" {
		template = AcceptanceTicketPromptTemplate
	}
	prompt, err := composeAcceptancePrompt(template, ticket.ID)
	if err != nil {
		return err
	}
	_, err = runAcceptanceTask(ctx, config, ticket, prompt, agent, orchestrationState)
	return err
}

func runAcceptanceTask(ctx context.Context, config AcceptanceConfig, ticket ticketclient.Ticket, prompt string, agent harness.Harness, orchestrationState TicketState) (harness.RunResult, error) {
	request := harness.RunRequest{
		WorkerName:           config.WorkerName,
		WorkingDir:           config.WorkingDir,
		Role:                 config.RoleName,
		Actor:                config.Actor,
		TicketID:             ticket.ID,
		Prompt:               prompt,
		Model:                config.Model,
		Reasoning:            config.Reasoning,
		RequireSession:       config.SessionPolicy == SessionPolicyTicket,
		StateDir:             config.StateDir,
		OutputMode:           config.OutputMode,
		CodexSandbox:         config.CodexSandbox,
		CodexServiceTier:     config.CodexServiceTier,
		PiProvider:           config.PiProvider,
		ClaudePermissionMode: config.ClaudePermissionMode,
		Operator:             config.Operator,
	}
	options := managedTurnOptions{
		Ticket: ticket.ID, Role: AcceptanceSessionRole, Harness: config.Harness,
		Owner: sessionOwner(config.WorkerName, config.Actor), Policy: config.SessionPolicy,
		Cleanup:               harness.CleanupPolicy(config.SessionCleanup),
		MinimumContextPercent: config.MinimumReuseContextPercent,
		Diagnostics:           config.Diagnostics,
	}
	result, runErr, _ := runManagedTurn(ctx, options, orchestrationState, agent, request)
	return result, runErr
}

func resolvedAcceptancePrompt(template, ticketID string) string {
	prompt, err := composeAcceptancePrompt(template, ticketID)
	if err != nil {
		return ""
	}
	return prompt
}

func composeAcceptancePrompt(template, ticketID string) (string, error) {
	if template == "" {
		template = AcceptanceTicketPromptTemplate
	}
	taskPrompt, err := ExpandTicketPrompt(template, ticketID, "acceptance", "")
	if err != nil {
		return "", err
	}
	return taskPrompt + "\n\n" + SignoffTurnPrompt(ticketID), nil
}

func AcceptancePrompt(ticketID string) string { return resolvedAcceptancePrompt("", ticketID) }

func logAcceptance(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, "[ticket-orc] "+format+"\n", args...)
}
