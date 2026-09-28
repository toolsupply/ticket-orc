package orc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	SessionPolicyTicket = "ticket"
	SessionPolicyFresh  = "fresh"
)

// CoderConfig contains policy and process settings for one coder loop.
type CoderConfig struct {
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
	CodexSandbox               string
	PiProvider                 string
	ClaudePermissionMode       string
	ReviewCompletion           string
	ReviewSkipTags             []string
	TicketPrompt               string
	WorkingDir                 string
	Operator                   io.Writer
	Diagnostics                io.Writer
	EventSink                  EventSink
}

type CoderTickets interface {
	WaitAndClaimImplementation(context.Context) (ticketclient.Ticket, error)
	Show(context.Context, string) (ticketclient.Ticket, error)
	Release(context.Context, string) (ticketclient.Transition, error)
}

type TicketState interface {
	GetSession(context.Context, string, string, string, ...string) (state.Session, bool, error)
	SetSession(context.Context, state.Session) error
	BounceCount(context.Context, string) (int, error)
	IncrementBounces(context.Context, string) (int, error)
}

type TicketCleanup interface {
	CleanupConfirmed(context.Context, ticketclient.Ticket) (CleanupResult, error)
}

// BounceLimitError reports that automation stopped after a ticket reached its
// configured review-return limit.
type BounceLimitError struct {
	Ticket string
	Count  int
	Limit  int
}

func (e *BounceLimitError) Error() string {
	return fmt.Sprintf("coder bounce limit reached for %s (%d/%d); released claim for human intervention", e.Ticket, e.Count, e.Limit)
}

// RunCoder continuously claims implementation tickets until cancellation or a
// state, harness, or lifecycle error occurs. A successful review or
// terminal cleanup returns to Ticket work without spawning an extra harness turn.
func RunCoder(ctx context.Context, config CoderConfig, tickets CoderTickets, agent harness.Harness, orchestrationState TicketState, cleanup TicketCleanup) error {
	if ctx == nil {
		return fmt.Errorf("coder context must not be nil")
	}
	if tickets == nil || agent == nil || orchestrationState == nil {
		return fmt.Errorf("coder dependencies must not be nil")
	}
	if strings.TrimSpace(config.Actor) == "" || strings.TrimSpace(config.Harness) == "" {
		return fmt.Errorf("coder actor and harness must not be empty")
	}
	if config.MaxBounces < 1 {
		return fmt.Errorf("coder max bounces must be positive")
	}
	if config.MinimumReuseContextPercent < 0 || config.MinimumReuseContextPercent > 100 {
		return fmt.Errorf("coder minimum reuse context percent must be from 0 to 100")
	}
	if config.TicketPrompt != "" {
		if err := ValidateTicketPrompt(config.TicketPrompt, "coder"); err != nil {
			return fmt.Errorf("coder ticket prompt: %w", err)
		}
	}
	if config.SessionPolicy != SessionPolicyTicket && config.SessionPolicy != SessionPolicyFresh {
		return fmt.Errorf("unknown coder session policy %q", config.SessionPolicy)
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
	if config.Operator == nil {
		config.Operator = io.Discard
	}
	if config.Diagnostics == nil {
		config.Diagnostics = io.Discard
	}
	if strings.TrimSpace(config.WorkingDir) == "" {
		workingDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("resolve coder working directory: %w", err)
		}
		config.WorkingDir = workingDir
	}
	emitEvent(config.EventSink, Event{Type: WorkerReadyEventType, Worker: config.WorkerName, Role: "coder", State: "ready", Phase: "startup"})

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		claimed, err := tickets.WaitAndClaimImplementation(ctx)
		if err != nil {
			return &WorkWaitError{Role: "coder", Cause: err}
		}
		if err := validateClaimedTicket(claimed); err != nil {
			return err
		}
		// A signal may arrive after ticket wait returns but before state or
		// harness work starts. Check again so cancellation cannot launch a
		// newly claimed agent turn.
		if err := ctx.Err(); err != nil {
			return err
		}

		bounces, err := orchestrationState.BounceCount(ctx, claimed.ID)
		if err != nil {
			return fmt.Errorf("read bounce count for %s: %w", claimed.ID, err)
		}
		if bounces >= config.MaxBounces {
			if _, releaseErr := tickets.Release(ctx, claimed.ID); releaseErr != nil {
				return errors.Join(
					&BounceLimitError{Ticket: claimed.ID, Count: bounces, Limit: config.MaxBounces},
					fmt.Errorf("release over-limit claim %s: %w", claimed.ID, releaseErr),
				)
			}
			return &BounceLimitError{Ticket: claimed.ID, Count: bounces, Limit: config.MaxBounces}
		}

		logCoder(config.Diagnostics, "%s: claimed implementation work (review bounces: %d/%d)", claimed.ID, bounces, config.MaxBounces)
		emitEvent(config.EventSink, Event{Type: "ticket.claim", Worker: config.WorkerName, Role: "coder", State: "claimed", Ticket: claimed.ID})
		turn, prepareErr := runCoderTurn(ctx, config, claimed.ID, agent, orchestrationState)
		if prepareErr != nil {
			return fmt.Errorf("prepare coder turn for %s: %w", claimed.ID, prepareErr)
		}
		observed, runErr := tickets.Show(ctx, claimed.ID)
		turnErr := errors.Join(turn.err, runErr)
		if runErr != nil {
			return turnErr
		}
		observed.State = ticketclient.NormalizeLifecycleState(observed.State)
		emitEvent(config.EventSink, Event{Type: "ticket.lifecycle", Worker: config.WorkerName, Role: "coder", State: observed.State, Ticket: observed.ID})
		lifecycleErr := finishCoderLifecycle(ctx, config, tickets, cleanup, observed, turnErr)
		if lifecycleErr != nil {
			return errors.Join(turnErr, lifecycleErr)
		}
		if turnErr != nil {
			return fmt.Errorf("coder turn for %s: %w", claimed.ID, turnErr)
		}
	}
}

func finishCoderLifecycle(ctx context.Context, config CoderConfig, tickets CoderTickets, cleanup TicketCleanup, observed ticketclient.Ticket, turnErr error) error {
	observed.State = ticketclient.NormalizeLifecycleState(observed.State)
	switch observed.State {
	case "review":
		if hasReviewSkipTag(observed.Tags, config.ReviewSkipTags) {
			if turnErr != nil {
				return fmt.Errorf("skip-review ticket %s has unsuccessful coder turn: %w", observed.ID, turnErr)
			}
			return completeSkippedReview(ctx, config, tickets, cleanup, observed)
		}
		logCoder(config.Diagnostics, "%s: coder submitted to review", observed.ID)
	case ticketclient.StateClosed, "rejected":
		if cleanup == nil {
			return fmt.Errorf("terminal ticket %s requires cleanup", observed.ID)
		}
		if _, cleanupErr := cleanup.CleanupConfirmed(ctx, observed); cleanupErr != nil {
			return fmt.Errorf("clean terminal ticket %s: %w", observed.ID, cleanupErr)
		}
	default:
		return fmt.Errorf("coder exited successfully but ticket %s is %q, expected review or terminal state", observed.ID, observed.State)
	}
	return nil
}

func hasReviewSkipTag(ticketTags, skipTags []string) bool {
	if len(ticketTags) == 0 || len(skipTags) == 0 {
		return false
	}
	configured := make(map[string]struct{}, len(skipTags))
	for _, tag := range skipTags {
		configured[tag] = struct{}{}
	}
	for _, tag := range ticketTags {
		if _, ok := configured[tag]; ok {
			return true
		}
	}
	return false
}

func completeSkippedReview(ctx context.Context, config CoderConfig, tickets CoderTickets, cleanup TicketCleanup, observed ticketclient.Ticket) error {
	if observed.Assignee != config.Actor {
		return fmt.Errorf("skip-review ticket %s is owned by %q, expected coder actor %q", observed.ID, observed.Assignee, config.Actor)
	}
	if config.ReviewCompletion == "signoff" {
		approver, ok := tickets.(interface {
			ApproveTicket(context.Context, string) (ticketclient.MutationResult, error)
		})
		if !ok {
			return fmt.Errorf("coder Ticket client does not support review approval")
		}
		if _, err := approver.ApproveTicket(ctx, observed.ID); err != nil {
			return fmt.Errorf("approve skip-review ticket %s: %w", observed.ID, err)
		}
		approved, err := tickets.Show(ctx, observed.ID)
		if err != nil {
			return fmt.Errorf("read state after approving skip-review ticket %s: %w", observed.ID, err)
		}
		approved.State = ticketclient.NormalizeLifecycleState(approved.State)
		if approved.State != "signoff" {
			return fmt.Errorf("approval succeeded but skip-review ticket %s is %q, expected signoff", observed.ID, approved.State)
		}
		logCoder(config.Diagnostics, "%s: review skipped by tag and ticket advanced to signoff", observed.ID)
		return nil
	}
	closer, ok := tickets.(interface {
		CloseTicket(context.Context, string) (ticketclient.Transition, error)
	})
	if !ok {
		return fmt.Errorf("coder Ticket client does not support direct review completion")
	}
	if _, err := closer.CloseTicket(ctx, observed.ID); err != nil {
		return fmt.Errorf("close skip-review ticket %s: %w", observed.ID, err)
	}
	closed, err := tickets.Show(ctx, observed.ID)
	if err != nil {
		return fmt.Errorf("read state after closing skip-review ticket %s: %w", observed.ID, err)
	}
	closed.State = ticketclient.NormalizeLifecycleState(closed.State)
	if closed.State != ticketclient.StateClosed {
		return fmt.Errorf("close succeeded but skip-review ticket %s is %q, expected closed", observed.ID, closed.State)
	}
	if cleanup == nil {
		return fmt.Errorf("terminal ticket %s requires cleanup", observed.ID)
	}
	if _, err := cleanup.CleanupConfirmed(ctx, closed); err != nil {
		return fmt.Errorf("clean terminal ticket %s: %w", observed.ID, err)
	}
	logCoder(config.Diagnostics, "%s: review skipped by tag and ticket closed", observed.ID)
	return nil
}

type coderTurnResult struct {
	result harness.RunResult
	err    error
}

func runCoderTurn(ctx context.Context, config CoderConfig, ticketID string, agent harness.Harness, orchestrationState TicketState) (coderTurnResult, error) {
	prompt := resolvedCoderPrompt(config.TicketPrompt, ticketID)
	request := harness.RunRequest{
		WorkerName:           config.WorkerName,
		WorkingDir:           config.WorkingDir,
		Role:                 "coder",
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

	result, runErr, started := runManagedTurn(ctx, managedTurnOptions{Ticket: ticketID, Role: "coder", Harness: config.Harness, Owner: sessionOwner(config.WorkerName, config.Actor), Policy: config.SessionPolicy, Cleanup: harness.CleanupPolicy(config.SessionCleanup), MinimumContextPercent: config.MinimumReuseContextPercent, Diagnostics: config.Diagnostics}, orchestrationState, agent, request)
	if !started {
		return coderTurnResult{}, runErr
	}
	return coderTurnResult{result: result, err: runErr}, nil
}

// sessionOwner uses the configured worker name or Ticket actor. Execution
// providers and model selection do not define managed session ownership.
func sessionOwner(worker, actor string) string {
	if strings.TrimSpace(worker) != "" {
		return worker
	}
	return actor
}

// CoderPrompt returns the short operational instruction supplied to a coder
// harness for one already-claimed ticket.
func CoderPrompt(ticketID string) string {
	return resolvedCoderPrompt("", ticketID)
}

func resolvedCoderPrompt(template, ticketID string) string {
	if template == "" {
		template = CoderTicketPromptTemplate
	}
	prompt, err := ExpandTicketPrompt(template, ticketID, "coder", "")
	if err != nil {
		return ""
	}
	return prompt
}

func validateClaimedTicket(ticket ticketclient.Ticket) error {
	if strings.TrimSpace(ticket.ID) == "" {
		return fmt.Errorf("claimed implementation ticket has no ID")
	}
	if ticket.State != "open" {
		return fmt.Errorf("claimed implementation ticket %s has state %q, expected open", ticket.ID, ticket.State)
	}
	return nil
}

func logCoder(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, "[ticket-orc] "+format+"\n", args...)
}
