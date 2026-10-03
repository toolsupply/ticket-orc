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

func TestRunReviewerReleasesUnchangedReviewClaimAfterPreSessionFailure(t *testing.T) {
	ticketID := "20260919-40012"
	harnessErr := errors.New("Codex could not start")
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review", Assignee: "reviewer-1"}},
		shows:        map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "reviewer-1"}},
	}
	var diagnostics strings.Builder
	config := reviewerConfig()
	config.Diagnostics = &diagnostics
	err := RunReviewer(context.Background(), config, tickets, &fakeCoderHarness{runErr: harnessErr}, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, harnessErr) || !strings.Contains(diagnostics.String(), "released unchanged review claim") || len(tickets.releases) != 1 || tickets.releases[0] != ticketID {
		t.Fatalf("RunReviewer=%v diagnostics=%q releases=%#v", err, diagnostics.String(), tickets.releases)
	}
}

func TestRunReviewerPreSessionRecoveryPreservesHarnessAndTicketErrors(t *testing.T) {
	harnessErr := errors.New("Codex startup failed")
	showErr := errors.New("Ticket show failed")
	releaseErr := errors.New("Ticket release failed")
	for _, test := range []struct {
		name        string
		showErr     error
		releaseErr  error
		wantRelease bool
		wantRecover bool
	}{
		{name: "show failure", showErr: showErr},
		{name: "release failure", releaseErr: releaseErr, wantRelease: true, wantRecover: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ticketID := "20260919-40013"
			var diagnostics strings.Builder
			config := reviewerConfig()
			config.Diagnostics = &diagnostics
			tickets := &fakeCoderTickets{
				reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review", Assignee: "reviewer-1"}},
				shows:        map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "reviewer-1"}},
				showErr:      test.showErr, releaseErr: test.releaseErr,
			}
			err := RunReviewer(context.Background(), config, tickets, &fakeCoderHarness{runErr: harnessErr}, newFakeCoderState(), &fakeCoderCleanup{})
			if !errors.Is(err, harnessErr) {
				t.Fatalf("RunReviewer=%v, want original harness error", err)
			}
			if test.showErr != nil && !errors.Is(err, showErr) {
				t.Fatalf("RunReviewer=%v, want Show error", err)
			}
			if test.releaseErr != nil && !errors.Is(err, releaseErr) {
				t.Fatalf("RunReviewer=%v, want Release error", err)
			}
			if test.wantRecover && !strings.Contains(err.Error(), "automatic claim recovery failed") {
				t.Fatalf("RunReviewer=%v, want explicit recovery failure", err)
			}
			if test.wantRecover && !strings.Contains(diagnostics.String(), "automatic claim recovery failed") {
				t.Fatalf("diagnostics=%q, want operator-visible recovery failure", diagnostics.String())
			}
			if (len(tickets.releases) == 1) != test.wantRelease {
				t.Fatalf("release calls=%#v, wantRelease=%t", tickets.releases, test.wantRelease)
			}
			if test.showErr != nil && !strings.Contains(diagnostics.String(), "automatic claim recovery skipped because Ticket state could not be verified") {
				t.Fatalf("diagnostics=%q, want unverified-state recovery diagnostic", diagnostics.String())
			}
		})
	}
}

func TestRunReviewerDoesNotReleaseClaimAfterSessionLifecycleOrOwnershipChange(t *testing.T) {
	harnessErr := errors.New("Codex startup failed")
	for _, test := range []struct {
		name       string
		result     harness.RunResult
		observed   ticketclient.Ticket
		wantBounce bool
		wantClean  bool
	}{
		{name: "session established", result: harness.RunResult{SessionID: "thread"}, observed: ticketclient.Ticket{State: "review", Assignee: "reviewer-1"}},
		{name: "returned to open", observed: ticketclient.Ticket{State: "open", Assignee: "reviewer-1"}, wantBounce: true},
		{name: "advanced to signoff", observed: ticketclient.Ticket{State: "signoff", Assignee: "reviewer-1"}},
		{name: "terminal", observed: ticketclient.Ticket{State: ticketclient.StateClosed, Assignee: "reviewer-1"}, wantClean: true},
		{name: "unassigned", observed: ticketclient.Ticket{State: "review"}},
		{name: "other owner", observed: ticketclient.Ticket{State: "review", Assignee: "someone-else"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ticketID := "20260919-40014"
			observed := test.observed
			observed.ID = ticketID
			tickets := &fakeCoderTickets{
				reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review", Assignee: "reviewer-1"}},
				shows:        map[string]ticketclient.Ticket{ticketID: observed},
			}
			stateStore := newFakeCoderState()
			cleanup := &fakeCoderCleanup{}
			err := RunReviewer(context.Background(), reviewerConfig(), tickets, &fakeCoderHarness{runRes: test.result, runErr: harnessErr}, stateStore, cleanup)
			if !errors.Is(err, harnessErr) || len(tickets.releases) != 0 {
				t.Fatalf("RunReviewer=%v releases=%#v, want harness error and no rollback", err, tickets.releases)
			}
			if (stateStore.bounces[ticketID] == 1) != test.wantBounce {
				t.Fatalf("bounce count=%d wantBounce=%t", stateStore.bounces[ticketID], test.wantBounce)
			}
			if (len(cleanup.tickets) == 1) != test.wantClean {
				t.Fatalf("cleanup=%#v wantClean=%t", cleanup.tickets, test.wantClean)
			}
		})
	}
}

func TestRunReviewerCanReclaimTicketAfterPreSessionRecovery(t *testing.T) {
	ticketID := "20260919-40015"
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{
			{ID: ticketID, State: "review", Assignee: "reviewer-1"},
			{ID: ticketID, State: "review", Assignee: "reviewer-1"},
		},
		shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "reviewer-1"}},
	}
	stateStore := newFakeCoderState()
	harnessErr := errors.New("pre-session failure")
	if err := RunReviewer(context.Background(), reviewerConfig(), tickets, &fakeCoderHarness{runErr: harnessErr}, stateStore, &fakeCoderCleanup{}); !errors.Is(err, harnessErr) {
		t.Fatalf("first RunReviewer=%v, want pre-session harness failure", err)
	}
	if len(tickets.releases) != 1 {
		t.Fatalf("release calls=%#v, want one startup rollback", tickets.releases)
	}
	second := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "retry-session"}}
	if err := RunReviewer(context.Background(), reviewerConfig(), tickets, second, stateStore, &fakeCoderCleanup{}); err == nil || !strings.Contains(err.Error(), "expected signoff, open, or terminal") {
		t.Fatalf("retry RunReviewer=%v, want existing unchanged-review lifecycle result", err)
	}
	if len(second.runs) != 1 || len(tickets.reviewClaims) != 0 {
		t.Fatalf("retry did not reclaim ticket: harness runs=%d remaining claims=%#v", len(second.runs), tickets.reviewClaims)
	}
}

func TestRunReviewerCancellationDuringHarnessDoesNotReleaseClaim(t *testing.T) {
	ticketID := "20260919-40016"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tickets := &fakeCoderTickets{
		reviewClaims: []ticketclient.Ticket{{ID: ticketID, State: "review", Assignee: "reviewer-1"}},
		shows:        map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "reviewer-1"}},
	}
	agent := &fakeCoderHarness{runErr: context.Canceled, onRun: cancel}
	err := RunReviewer(ctx, reviewerConfig(), tickets, agent, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, context.Canceled) || len(tickets.releases) != 0 {
		t.Fatalf("RunReviewer=%v releases=%#v, want cancellation without startup rollback", err, tickets.releases)
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
