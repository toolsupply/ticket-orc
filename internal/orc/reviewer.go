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

// ReviewerConfig contains policy and process settings for one reviewer loop.
type ReviewerConfig struct {
	WorkerName                 string
	RoleName                   string
	TicketQueue                string
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
	QueueFilters               ticketclient.QueueFilters
	HoldQueue                  bool
	HoldQueueFilters           ticketclient.QueueFilters
	RepositoryID               string
	CodexSandbox               string
	CodexServiceTier           string
	PiProvider                 string
	ClaudePermissionMode       string
	TicketPrompt               string
	WorkingDir                 string
	Operator                   io.Writer
	Diagnostics                io.Writer
	EventSink                  EventSink
	ContainmentTickets         TicketLoopHolder
}

type ReviewerTickets interface {
	WaitAndClaimReview(context.Context, ticketclient.QueueFilters) (ticketclient.Ticket, error)
	Show(context.Context, string) (ticketclient.Ticket, error)
	Release(context.Context, string) (ticketclient.Transition, error)
	CloseTicket(context.Context, string) (ticketclient.Transition, error)
}

// RunReviewer continuously claims review tickets until cancellation or a
// state, harness, or lifecycle error occurs. A confirmed hold after a successful
// turn ends the ticket attempt and returns to Ticket queue selection.
func RunReviewer(ctx context.Context, config ReviewerConfig, tickets ReviewerTickets, agent harness.Harness, orchestrationState TicketState, cleanup TicketCleanup) (runErr error) {
	if config.RoleName == "" {
		config.RoleName = "reviewer"
	}
	if config.TicketQueue == "" {
		config.TicketQueue = "review"
	}
	if config.TicketQueue != "review" {
		return fmt.Errorf("review executor requires the review queue, got %q", config.TicketQueue)
	}
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
		CodexServiceTier:     config.CodexServiceTier,
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
	var holds interface {
		holdQueueTickets
		NextAndClaimReview(context.Context, ticketclient.QueueFilters) (*ticketclient.Ticket, error)
	}
	var watch ticketclient.ChangeWatcher
	if config.HoldQueue {
		var ok bool
		holds, ok = tickets.(interface {
			holdQueueTickets
			NextAndClaimReview(context.Context, ticketclient.QueueFilters) (*ticketclient.Ticket, error)
		})
		if !ok {
			return fmt.Errorf("reviewer Ticket client does not support hold observation and nonblocking review claims")
		}
		if !ticketclient.ValidRepositoryID(config.RepositoryID) {
			return fmt.Errorf("reviewer repository ID is invalid")
		}
		var err error
		config.HoldQueueFilters, err = ticketclient.CanonicalQueueFilters(config.HoldQueueFilters)
		if err != nil {
			return fmt.Errorf("reviewer hold selector: %w", err)
		}
		watch, err = holds.StartChangeWatch(ctx, config.RepositoryID)
		if err != nil {
			return fmt.Errorf("start Ticket change watch: %w", err)
		}
		defer func() { runErr = errors.Join(runErr, watch.Close()) }()
	}
	emitEvent(config.EventSink, Event{Type: WorkerReadyEventType, Worker: config.WorkerName, Role: config.RoleName, State: "ready", Phase: "startup"})

	preferPrimary := false // Idle loops start with a hold opportunity.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		loopState, hasLoops := orchestrationState.(TicketLoopState)
		loopReader, hasReader := tickets.(TicketLoopReader)
		if hasLoops && hasReader {
			if err := ReconcileTicketLoops(ctx, loopState, loopReader, config.ContainmentTickets); err != nil {
				return err
			}
		}
		var claimed ticketclient.Ticket
		if holds != nil {
			var item *ticketclient.Ticket
			var err error
			if preferPrimary {
				item, err = holds.NextAndClaimReview(ctx, config.QueueFilters)
				if err != nil {
					return &WorkWaitError{Role: "reviewer", Cause: err}
				}
			}
			if item == nil {
				candidate, err := selectManagedHold(ctx, holds, config.HoldQueueFilters, watch, cleanup)
				if err != nil {
					return err
				}
				if candidate != nil {
					emitEvent(config.EventSink, Event{Type: "ticket.hold", Worker: config.WorkerName, Role: config.RoleName, State: "selected", Ticket: candidate.ID})
					turn, prepareErr := runReviewerTask(ctx, config, candidate.ID, HoldTaskPrompt(candidate.ID, "review"), agent, orchestrationState)
					if err := finishManagedHold(ctx, holds, watch, cleanup, *candidate, turn.result, errors.Join(prepareErr, turn.err), config.EventSink, config.WorkerName, config.RoleName, config.Diagnostics); err != nil {
						return err
					}
					preferPrimary = true
					continue
				}
			}
			if item == nil && !preferPrimary {
				item, err = holds.NextAndClaimReview(ctx, config.QueueFilters)
				if err != nil {
					return &WorkWaitError{Role: "reviewer", Cause: err}
				}
			}
			if item == nil {
				for {
					material, err := watch.WaitForChange(ctx)
					if err != nil {
						return fmt.Errorf("wait for Ticket change: %w", err)
					}
					if material {
						break
					}
				}
				continue
			}
			preferPrimary = false
			claimed = *item
		} else {
			var err error
			claimed, err = tickets.WaitAndClaimReview(ctx, config.QueueFilters)
			if err != nil {
				return &WorkWaitError{Role: "reviewer", Cause: err}
			}
		}
		if err := validateReviewClaim(claimed); err != nil {
			return err
		}
		// Do not start a reviewer turn if cancellation raced with the claim
		// response. The claimed ticket remains authoritative for later repair.
		if err := ctx.Err(); err != nil {
			return err
		}
		if watch != nil {
			if err := watch.Check(); err != nil {
				return fmt.Errorf("Ticket change watch failed before review dispatch: %w", err)
			}
		}
		var attempt state.TicketLoop
		if loopState, ok := orchestrationState.(TicketLoopState); ok && config.MaxBounces > 0 {
			loop, participationTripped, err := loopState.Participate(ctx, claimed.ID, config.MaxBounces)
			if err != nil {
				return fmt.Errorf("record reviewer participation for %s: %w", claimed.ID, err)
			}
			loop, err = loopState.RecordClaim(ctx, claimed.ID, config.Actor, "review")
			if err != nil {
				return fmt.Errorf("record reviewer claim for %s: %w", claimed.ID, err)
			}
			attempt = loop
			if loop.Phase != state.TicketLoopActive {
				loopReader, ok := tickets.(TicketLoopReader)
				if !ok {
					return fmt.Errorf("Ticket reader is unavailable to reconcile tripped ticket %s", claimed.ID)
				}
				var reconcileErr error
				if participationTripped {
					_, _, reconcileErr = ReconcileClaimedTicketLoop(ctx, loopState, loopReader, config.ContainmentTickets, tickets, config.Actor, claimed.ID)
				} else {
					_, _, reconcileErr = ReconcileTicketLoop(ctx, loopState, loopReader, config.ContainmentTickets, claimed.ID)
				}
				if reconcileErr != nil {
					return reconcileErr
				}
				continue
			}
		}
		logReviewer(config.Diagnostics, "%s: claimed review work", claimed.ID)
		emitEvent(config.EventSink, Event{Type: "ticket.claim", Worker: config.WorkerName, Role: config.RoleName, State: "claimed", Ticket: claimed.ID})

		turn, prepareErr := runReviewerTurn(ctx, config, claimed.ID, agent, orchestrationState)
		if prepareErr != nil {
			return fmt.Errorf("prepare reviewer turn for %s: %w", claimed.ID, prepareErr)
		}
		postTurnError := func(err error) error {
			return NewFailureContextError(FailureContext{Origin: "orc", Operation: "post-turn reconciliation", Phase: "post-turn", Ticket: claimed.ID, Worker: config.WorkerName, Role: config.RoleName, Actor: config.Actor, Execution: turn.result.Evidence, RepositoryID: config.RepositoryID, AttemptID: attempt.AttemptID}, err)
		}
		observed, runErr := tickets.Show(ctx, claimed.ID)
		turnErr := errors.Join(turn.err, runErr)
		if runErr != nil {
			if turn.err != nil && turn.result.SessionID == "" && ctx.Err() == nil {
				logReviewer(config.Diagnostics, "%s: automatic claim recovery skipped because Ticket state could not be verified", claimed.ID)
			}
			return postTurnError(turnErr)
		}
		if attempted, released, recoveryErr := releaseFailedStartupClaim(ctx, tickets, claimed.ID, config.Actor, "review", turn.result, turn.err, observed); attempted {
			if released {
				logReviewer(config.Diagnostics, "%s: failed before session startup; released unchanged review claim", claimed.ID)
			} else {
				logReviewer(config.Diagnostics, "%s: automatic claim recovery failed after pre-session harness failure", claimed.ID)
			}
			return postTurnError(fmt.Errorf("reviewer turn for %s: %w", claimed.ID, recoveryErr))
		}
		emitEvent(config.EventSink, Event{Type: "ticket.lifecycle", Worker: config.WorkerName, Role: config.RoleName, State: observed.State, Ticket: observed.ID})
		if loopState, ok := orchestrationState.(TicketLoopState); ok {
			if loopReader, ok := tickets.(TicketLoopReader); ok && observed.State != ticketclient.StateClosed && observed.State != "rejected" {
				loop, reconcileErr := ReconcileEndedClaim(ctx, loopState, loopReader, config.ContainmentTickets, claimed.ID)
				if reconcileErr != nil {
					return postTurnError(errors.Join(turnErr, reconcileErr))
				}
				if loop.Phase == state.TicketLoopContainmentPending || loop.Phase == state.TicketLoopHeld {
					if turnErr != nil {
						return postTurnError(fmt.Errorf("reviewer turn for %s: %w", claimed.ID, turnErr))
					}
					// A confirmed hold ends this ticket attempt, not the worker.
					continue
				}
			}
		}
		lifecycleErr := finishReviewerLifecycle(ctx, config, tickets, cleanup, orchestrationState, observed)
		if lifecycleErr != nil {
			return postTurnError(errors.Join(turnErr, lifecycleErr))
		}
		if turnErr != nil {
			return postTurnError(fmt.Errorf("reviewer turn for %s: %w", claimed.ID, turnErr))
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
		count, incrementErr := 0, error(nil)
		if _, usesTicketLoops := orchestrationState.(TicketLoopState); usesTicketLoops {
			count, incrementErr = orchestrationState.BounceCount(ctx, observed.ID)
		} else {
			count, incrementErr = orchestrationState.IncrementBounces(ctx, observed.ID)
		}
		if incrementErr != nil {
			return fmt.Errorf("record review bounces for %s: %w", observed.ID, incrementErr)
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
	return runReviewerTask(ctx, config, ticketID, resolvedReviewerPrompt(config.TicketPrompt, ticketID, config.ReviewCompletion), agent, orchestrationState)
}

func runReviewerTask(ctx context.Context, config ReviewerConfig, ticketID, prompt string, agent harness.Harness, orchestrationState TicketState) (reviewerTurnResult, error) {
	roleName := config.RoleName
	if roleName == "" {
		roleName = "reviewer"
	}
	request := harness.RunRequest{
		WorkerName:           config.WorkerName,
		WorkingDir:           config.WorkingDir,
		Role:                 roleName,
		Actor:                config.Actor,
		TicketID:             ticketID,
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
