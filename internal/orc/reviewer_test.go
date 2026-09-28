package orc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func reviewerConfig() ReviewerConfig {
	return ReviewerConfig{
		Actor:         "reviewer-1",
		Harness:       "codex",
		SessionPolicy: SessionPolicyTicket,
		StateDir:      ".ticket-orc",
		OutputMode:    "quiet",
		WorkingDir:    "/work/project",
	}
}

func TestRunReviewerTurnPromptDoesNotNarrateTicketRouting(t *testing.T) {
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{}
	config := reviewerConfig()
	if _, err := runReviewerTurn(context.Background(), config, "20260921-77092", agent, stateStore); err != nil {
		t.Fatalf("runReviewerTurn: %v", err)
	}
	if len(agent.runs) != 1 || strings.Contains(agent.runs[0].Prompt, "--config") || strings.Contains(agent.runs[0].Prompt, "--scope") {
		t.Fatalf("managed prompt = %#v, want no explicit routing instructions", agent.runs)
	}
}

func TestReviewerUsesManagedSessionEligibilityPath(t *testing.T) {
	ticketID := "20260923-43205-reviewer"
	stateStore := newFakeCoderState()
	owner := "reviewer-1"
	stateStore.sessions[coderSessionKey(ticketID, "reviewer", "codex", owner)] = state.Session{Ticket: ticketID, Role: "reviewer", Harness: "codex", Owner: owner, ID: "review-thread"}
	agent := &fakeCoderHarness{resumeRes: harness.RunResult{SessionID: "review-thread"}}
	if _, err := runReviewerTurn(context.Background(), reviewerConfig(), ticketID, agent, stateStore); err != nil {
		t.Fatal(err)
	}
	if len(agent.resumes) != 1 || agent.resumes[0] != "review-thread" || len(agent.runs) != 1 {
		t.Fatalf("reviewer session calls: resumes=%v runs=%d", agent.resumes, len(agent.runs))
	}
	if len(stateStore.history) != 0 {
		t.Fatalf("healthy reviewer session superseded: %#v", stateStore.history)
	}
}

func TestRunReviewerApprovesAndReturnsToQueue(t *testing.T) {
	ticketID := "20260919-40001"
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review", Assignee: "reviewer-1"}},
		shows:        map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "signoff"}},
	}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "review-thread"}}
	ctx, cancel := context.WithCancel(context.Background())
	tickets.onShow = cancel
	err := RunReviewer(ctx, reviewerConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunReviewer = %v, want queue cancellation", err)
	}
	if len(agent.runs) != 1 || agent.runs[0].Role != "reviewer" {
		t.Fatalf("harness calls = %#v", agent.runs)
	}
	for _, want := range []string{ticketID, "in review", "assigned to you", "inspect and test it independently", "Approve it", "return it to open", "review_completion=signoff"} {
		if !strings.Contains(agent.runs[0].Prompt, want) {
			t.Errorf("prompt %q does not contain %q", agent.runs[0].Prompt, want)
		}
	}
	for _, unwanted := range []string{"reviewer-1", "actor", "role", "--config", "--scope"} {
		if strings.Contains(agent.runs[0].Prompt, unwanted) {
			t.Errorf("prompt %q contains routing or identity text %q", agent.runs[0].Prompt, unwanted)
		}
	}
	if len(stateStore.sets) != 1 || stateStore.sets[0].Role != "reviewer" {
		t.Fatalf("retained sessions = %#v", stateStore.sets)
	}
}

func TestRunReviewerClosePolicyClosesAndCleans(t *testing.T) {
	ticketID := "20260919-40010"
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review", Assignee: "reviewer-1"}},
		shows:        map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "signoff"}},
	}
	tickets.onClose = func() { tickets.shows[ticketID] = ticketclient.Ticket{ID: ticketID, State: ticketclient.StateClosed} }
	cleanup := &fakeCoderCleanup{}
	ctx, cancel := context.WithCancel(context.Background())
	cleanup.onCleanup = cancel
	config := reviewerConfig()
	config.ReviewCompletion = "close"
	err := RunReviewer(ctx, config, tickets, &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}, newFakeCoderState(), cleanup)
	if !errors.Is(err, context.Canceled) || len(tickets.closes) != 1 || tickets.closes[0] != ticketID {
		t.Fatalf("RunReviewer = %v; closes=%#v", err, tickets.closes)
	}
	if len(cleanup.tickets) != 1 || cleanup.tickets[0].State != ticketclient.StateClosed {
		t.Fatalf("cleanup = %#v", cleanup.tickets)
	}
	if len(tickets.showIDs) != 2 {
		t.Fatalf("show IDs = %#v, want signoff and post-close reread", tickets.showIDs)
	}
}

func TestRunReviewerCloseFailureStopsWithoutRetry(t *testing.T) {
	ticketID := "20260919-40011"
	closeErr := errors.New("close mutation uncertain")
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review", Assignee: "reviewer-1"}},
		shows:        map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "signoff"}},
		closeErr:     closeErr,
	}
	config := reviewerConfig()
	config.ReviewCompletion = "close"
	err := RunReviewer(context.Background(), config, tickets, &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, closeErr) || len(tickets.closes) != 1 || len(tickets.showIDs) != 1 {
		t.Fatalf("RunReviewer = %v; closes=%#v shows=%#v", err, tickets.closes, tickets.showIDs)
	}
}

func TestRunReviewerCancellationAfterClaimDoesNotLaunchHarness(t *testing.T) {
	ticketID := "20260919-40009"
	ctx, cancel := context.WithCancel(context.Background())
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}},
		onClaim:      cancel,
	}
	agent := &fakeCoderHarness{}
	err := RunReviewer(ctx, reviewerConfig(), tickets, agent, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunReviewer = %v, want context canceled", err)
	}
	if len(agent.runs) != 0 {
		t.Fatalf("harness runs = %#v, want none", agent.runs)
	}
}

func TestRunReviewerReturnIncrementsBounceAtomically(t *testing.T) {
	ticketID := "20260919-40002"
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}},
		shows:        map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "open"}},
	}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	ctx, cancel := context.WithCancel(context.Background())
	tickets.onShow = cancel
	err := RunReviewer(ctx, reviewerConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if !errors.Is(err, context.Canceled) || stateStore.bounces[ticketID] != 1 {
		t.Fatalf("RunReviewer = %v; bounces=%#v", err, stateStore.bounces)
	}
}

func TestRunReviewerRereadsAfterHarnessFailureAndCountsOpenBounce(t *testing.T) {
	ticketID := "20260919-40006"
	tickets := &fakeCoderTickets{reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "open"}}}
	stateStore := newFakeCoderState()
	harnessErr := errors.New("reviewer exited after returning ticket")
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}, runErr: harnessErr}
	err := RunReviewer(context.Background(), reviewerConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if !errors.Is(err, harnessErr) || stateStore.bounces[ticketID] != 1 || len(tickets.showIDs) != 1 {
		t.Fatalf("RunReviewer = %v; bounces=%#v shows=%#v", err, stateStore.bounces, tickets.showIDs)
	}
}

func TestRunReviewerUnchangedReviewStops(t *testing.T) {
	ticketID := "20260919-40007"
	tickets := &fakeCoderTickets{reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review"}}}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	err := RunReviewer(context.Background(), reviewerConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if err == nil || !strings.Contains(err.Error(), "expected signoff, open, or terminal") {
		t.Fatalf("RunReviewer = %v, want lifecycle error", err)
	}
}

func TestRunReviewerRereadsAfterSessionSaveFailure(t *testing.T) {
	ticketID := "20260919-40008"
	tickets := &fakeCoderTickets{reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "signoff"}}}
	stateStore := newFakeCoderState()
	stateStore.setErr = errors.New("state write failed")
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	err := RunReviewer(context.Background(), reviewerConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if err == nil || !strings.Contains(err.Error(), "state write failed") || len(tickets.showIDs) != 1 {
		t.Fatalf("RunReviewer = %v; shows=%#v", err, tickets.showIDs)
	}
}

func TestRunReviewerWrongStateStops(t *testing.T) {
	ticketID := "20260919-40003"
	tickets := &fakeCoderTickets{reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "openish"}}}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	err := RunReviewer(context.Background(), reviewerConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if err == nil || !strings.Contains(err.Error(), "expected signoff, open, or terminal") {
		t.Fatalf("RunReviewer = %v, want lifecycle error", err)
	}
}

func TestRunReviewerFailureStopsAndSavesSession(t *testing.T) {
	ticketID := "20260919-40004"
	tickets := &fakeCoderTickets{reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}}, shows: map[string]ticketclient.Ticket{}}
	stateStore := newFakeCoderState()
	harnessErr := errors.New("review failed")
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "failed-review"}, runErr: harnessErr}
	err := RunReviewer(context.Background(), reviewerConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if !errors.Is(err, harnessErr) || len(stateStore.sets) != 1 {
		t.Fatalf("RunReviewer = %v; sessions=%#v", err, stateStore.sets)
	}
}

func TestRunReviewerTerminalStateCleans(t *testing.T) {
	ticketID := "20260919-40005"
	tickets := &fakeCoderTickets{reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "rejected"}}}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	cleanup := &fakeCoderCleanup{}
	ctx, cancel := context.WithCancel(context.Background())
	tickets.onShow = cancel
	err := RunReviewer(ctx, reviewerConfig(), tickets, agent, stateStore, cleanup)
	if !errors.Is(err, context.Canceled) || len(cleanup.tickets) != 1 || cleanup.tickets[0].State != "rejected" {
		t.Fatalf("RunReviewer = %v; cleanup=%#v", err, cleanup.tickets)
	}
}
