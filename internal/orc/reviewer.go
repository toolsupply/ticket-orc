package orc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type ReviewerTagFilter interface {
	WaitAndClaimReviewWithoutTags(context.Context, []string) (ticketclient.Ticket, error)
}

// ReviewerConfig contains policy and process settings for one reviewer loop.
type ReviewerConfig struct {
	WorkerName                 string
	Actor                      string
	Harness                    string
	Model                      string
	Reasoning                  string
	MaxBounces                 int
	SessionPolicy              string
	SessionCleanup             string
	MinimumReuseContextPercent int
	StateDir                   string
	OutputMode                 string
	ReviewCompletion           string
	ReviewSkipTags             []string
	CodexSandbox               string
	PiProvider                 string
	ClaudePermissionMode       string
	TicketPrompt               string
	WorkingDir                 string
	Operator                   io.Writer
	Diagnostics                io.Writer
	EventSink                  EventSink
}

type ReviewerTickets interface {
	WaitAndClaimReview(context.Context) (ticketclient.Ticket, error)
	Show(context.Context, string) (ticketclient.Ticket, error)
	CloseTicket(context.Context, string) (ticketclient.Transition, error)
}

// RunReviewer continuously claims review tickets until cancellation or a
// state, harness, or lifecycle error occurs.
func RunReviewer(ctx context.Context, config ReviewerConfig, tickets ReviewerTickets, agent harness.Harness, orchestrationState TicketState, cleanup TicketCleanup) error {
	if ctx == nil {
		return fmt.Errorf("reviewer context must not be nil")
	}
	if tickets == nil || agent == nil || orchestrationState == nil {
		return fmt.Errorf("reviewer dependencies must not be nil")
	}
	if strings.TrimSpace(config.Actor) == "" || strings.TrimSpace(config.Harness) == "" {
		return fmt.Errorf("reviewer actor and harness must not be empty")
	}
	if config.MinimumReuseContextPercent < 0 || config.MinimumReuseContextPercent > 100 {
		return fmt.Errorf("reviewer minimum reuse context percent must be from 0 to 100")
	}
	if config.SessionPolicy != SessionPolicyTicket && config.SessionPolicy != SessionPolicyFresh {
		return fmt.Errorf("unknown reviewer session policy %q", config.SessionPolicy)
	}
	if config.TicketPrompt != "" {
		if err := ValidateTicketPrompt(config.TicketPrompt, "reviewer"); err != nil {
			return fmt.Errorf("reviewer ticket prompt: %w", err)
		}
	}
	if err := harness.PreflightIfSupported(agent, harness.PreflightConfig{
		Model:                config.Model,
		Reasoning:            config.Reasoning,
		SessionPolicy:        config.SessionPolicy,
		SessionCleanup:       harness.CleanupPolicy(config.SessionCleanup),
		OutputMode:           config.OutputMode,
		CodexSandbox:         config.CodexSandbox,
		PiProvider:           config.PiProvider,
		ClaudePermissionMode: config.ClaudePermissionMode,
	}); err != nil {
		return err
	}
	if config.ReviewCompletion == "" {
		config.ReviewCompletion = "signoff"
	}
	if config.ReviewCompletion != "signoff" && config.ReviewCompletion != "close" {
		return fmt.Errorf("unknown reviewer completion policy %q", config.ReviewCompletion)
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
			return fmt.Errorf("resolve reviewer working directory: %w", err)
		}
		config.WorkingDir = workingDir
	}
	emitEvent(config.EventSink, Event{Type: WorkerReadyEventType, Worker: config.WorkerName, Role: "reviewer", State: "ready", Phase: "startup"})

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var claimed ticketclient.Ticket
		var err error
		if filtered, ok := tickets.(ReviewerTagFilter); ok && len(config.ReviewSkipTags) > 0 {
			claimed, err = filtered.WaitAndClaimReviewWithoutTags(ctx, config.ReviewSkipTags)
		} else if len(config.ReviewSkipTags) > 0 {
			return fmt.Errorf("reviewer Ticket client does not support review skip tags")
		} else {
			claimed, err = tickets.WaitAndClaimReview(ctx)
		}
		if err != nil {
			return &WorkWaitError{Role: "reviewer", Cause: err}
		}
		if err := validateReviewClaim(claimed); err != nil {
			return err
		}
		// Do not start a reviewer turn if cancellation raced with the claim
		// response. The claimed ticket remains authoritative for later repair.
		if err := ctx.Err(); err != nil {
			return err
		}
		logReviewer(config.Diagnostics, "%s: claimed review work", claimed.ID)
		emitEvent(config.EventSink, Event{Type: "ticket.claim", Worker: config.WorkerName, Role: "reviewer", State: "claimed", Ticket: claimed.ID})

		turn, prepareErr := runReviewerTurn(ctx, config, claimed.ID, agent, orchestrationState)
		if prepareErr != nil {
			return fmt.Errorf("prepare reviewer turn for %s: %w", claimed.ID, prepareErr)
		}
		observed, runErr := tickets.Show(ctx, claimed.ID)
		turnErr := errors.Join(turn.err, runErr)
		if runErr != nil {
			return turnErr
		}
		emitEvent(config.EventSink, Event{Type: "ticket.lifecycle", Worker: config.WorkerName, Role: "reviewer", State: observed.State, Ticket: observed.ID})
		lifecycleErr := finishReviewerLifecycle(ctx, config, tickets, cleanup, orchestrationState, observed)
		if lifecycleErr != nil {
			return errors.Join(turnErr, lifecycleErr)
		}
		if turnErr != nil {
			return fmt.Errorf("reviewer turn for %s: %w", claimed.ID, turnErr)
		}
	}
}

func finishReviewerLifecycle(ctx context.Context, config ReviewerConfig, tickets ReviewerTickets, cleanup TicketCleanup, orchestrationState TicketState, observed ticketclient.Ticket) error {
	observed.State = ticketclient.NormalizeLifecycleState(observed.State)
	switch observed.State {
	case "signoff":
		if config.ReviewCompletion == "signoff" {
			logReviewer(config.Diagnostics, "%s: reviewer approved; awaiting human signoff", observed.ID)
			return nil
		}
		if _, closeErr := tickets.CloseTicket(ctx, observed.ID); closeErr != nil {
			return fmt.Errorf("close signed-off ticket %s: %w", observed.ID, closeErr)
		}
		closed, showErr := tickets.Show(ctx, observed.ID)
		if showErr != nil {
			return fmt.Errorf("read state after closing ticket %s: %w", observed.ID, showErr)
		}
		closed.State = ticketclient.NormalizeLifecycleState(closed.State)
		if closed.State != ticketclient.StateClosed && closed.State != "rejected" {
			return fmt.Errorf("close succeeded but ticket %s is %q, expected terminal state", observed.ID, closed.State)
		}
		if cleanup == nil {
			return fmt.Errorf("terminal ticket %s requires cleanup", observed.ID)
		}
		if _, cleanupErr := cleanup.CleanupConfirmed(ctx, closed); cleanupErr != nil {
			return fmt.Errorf("clean terminal ticket %s: %w", observed.ID, cleanupErr)
		}
		logReviewer(config.Diagnostics, "%s: reviewer approved and closed ticket", observed.ID)
	case "open":
		count, incrementErr := orchestrationState.IncrementBounces(ctx, observed.ID)
		if incrementErr != nil {
			return fmt.Errorf("increment review bounces for %s: %w", observed.ID, incrementErr)
		}
		logReviewer(config.Diagnostics, "%s: reviewer returned to open (bounce %d)", observed.ID, count)
	case ticketclient.StateClosed, "rejected":
		if cleanup == nil {
			return fmt.Errorf("terminal ticket %s requires cleanup", observed.ID)
		}
		if _, cleanupErr := cleanup.CleanupConfirmed(ctx, observed); cleanupErr != nil {
			return fmt.Errorf("clean terminal ticket %s: %w", observed.ID, cleanupErr)
		}
	default:
		return fmt.Errorf("reviewer exited successfully but ticket %s is %q, expected signoff, open, or terminal state", observed.ID, observed.State)
	}
	return nil
}

type reviewerTurnResult struct {
	result harness.RunResult
	err    error
}

func runReviewerTurn(ctx context.Context, config ReviewerConfig, ticketID string, agent harness.Harness, orchestrationState TicketState) (reviewerTurnResult, error) {
	prompt := resolvedReviewerPrompt(config.TicketPrompt, ticketID, config.ReviewCompletion)
	request := harness.RunRequest{
		WorkerName:           config.WorkerName,
		WorkingDir:           config.WorkingDir,
		Role:                 "reviewer",
		Actor:                config.Actor,
		TicketID:             ticketID,
		Prompt:               prompt,
		Model:                config.Model,
		Reasoning:            config.Reasoning,
		RequireSession:       config.SessionPolicy == SessionPolicyTicket,
		StateDir:             config.StateDir,
		OutputMode:           config.OutputMode,
		CodexSandbox:         config.CodexSandbox,
		PiProvider:           config.PiProvider,
		ClaudePermissionMode: config.ClaudePermissionMode,
		Operator:             config.Operator,
	}

	result, runErr, started := runManagedTurn(ctx, managedTurnOptions{Ticket: ticketID, Role: "reviewer", Harness: config.Harness, Owner: sessionOwner(config.WorkerName, config.Actor), Policy: config.SessionPolicy, Cleanup: harness.CleanupPolicy(config.SessionCleanup), MinimumContextPercent: config.MinimumReuseContextPercent, Diagnostics: config.Diagnostics}, orchestrationState, agent, request)
	if !started {
		return reviewerTurnResult{}, runErr
	}
	return reviewerTurnResult{result: result, err: runErr}, nil
}

// ReviewerPrompt returns the independent-review instruction for one claimed
// ticket. It explicitly prohibits implementation fixes in the reviewer turn.
func ReviewerPrompt(ticketID string) string {
	return resolvedReviewerPrompt("", ticketID, "signoff")
}

func resolvedReviewerPrompt(template, ticketID, completion string) string {
	if template == "" {
		template = ReviewerTicketPromptTemplate
	}
	prompt, err := ExpandTicketPrompt(template, ticketID, "reviewer", completion)
	if err != nil {
		return ""
	}
	return prompt
}

func validateReviewClaim(ticket ticketclient.Ticket) error {
	if strings.TrimSpace(ticket.ID) == "" {
		return fmt.Errorf("claimed review ticket has no ID")
	}
	if err := ticketclient.ValidateFullID(ticket.ID); err != nil {
		return fmt.Errorf("claimed review ticket ID: %w", err)
	}
	if ticket.State != "review" {
		return fmt.Errorf("claimed review ticket %s has state %q, expected review", ticket.ID, ticket.State)
	}
	return nil
}

func logReviewer(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, "[ticket-orc] "+format+"\n", args...)
}
