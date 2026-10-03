package orc

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type fakeCoderTickets struct {
	mu           sync.Mutex
	claims       []ticketclient.Ticket
	reviewClaims []ticketclient.Ticket
	shows        map[string]ticketclient.Ticket
	showErr      error
	showIDs      []string
	releases     []string
	releaseErr   error
	releaseNoop  bool
	claimHeld    map[string]bool
	closes       []string
	approves     []string
	waitCalls    int
	onClaim      func()
	onShow       func()
	onClose      func()
	closeErr     error
}

func (f *fakeCoderTickets) WaitAndClaimReview(ctx context.Context, _ ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waitCalls++
	if len(f.reviewClaims) == 0 {
		return ticketclient.Ticket{}, ctx.Err()
	}
	ticket := f.reviewClaims[0]
	if f.claimHeld == nil {
		f.claimHeld = make(map[string]bool)
	}
	if f.claimHeld[ticket.ID] {
		return ticketclient.Ticket{}, errors.New("ticket is already claimed")
	}
	f.reviewClaims = f.reviewClaims[1:]
	f.claimHeld[ticket.ID] = true
	if f.onClaim != nil {
		f.onClaim()
	}
	return ticket, nil
}

func (f *fakeCoderTickets) WaitAndClaimImplementation(ctx context.Context, _ ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waitCalls++
	if len(f.claims) == 0 {
		return ticketclient.Ticket{}, ctx.Err()
	}
	ticket := f.claims[0]
	if f.claimHeld == nil {
		f.claimHeld = make(map[string]bool)
	}
	if f.claimHeld[ticket.ID] {
		return ticketclient.Ticket{}, errors.New("ticket is already claimed")
	}
	f.claims = f.claims[1:]
	f.claimHeld[ticket.ID] = true
	if f.onClaim != nil {
		f.onClaim()
	}
	return ticket, nil
}

func (f *fakeCoderTickets) Show(_ context.Context, id string) (ticketclient.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.showIDs = append(f.showIDs, id)
	if f.showErr != nil {
		return ticketclient.Ticket{}, f.showErr
	}
	ticket, ok := f.shows[id]
	if !ok {
		return ticketclient.Ticket{}, errors.New("ticket not found")
	}
	if f.onShow != nil {
		f.onShow()
	}
	return ticket, nil
}

func (f *fakeCoderTickets) Release(_ context.Context, id string) (ticketclient.Transition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases = append(f.releases, id)
	if f.releaseErr != nil {
		return ticketclient.Transition{}, f.releaseErr
	}
	if !f.releaseNoop && f.claimHeld != nil {
		delete(f.claimHeld, id)
	}
	return ticketclient.Transition{ID: id, Changed: !f.releaseNoop, State: f.shows[id].State}, nil
}

type claimRecoveryTestError struct{ message string }

func (e *claimRecoveryTestError) Error() string { return e.message }

type claimRecoveryReleaseTestError struct{ message string }

func (e *claimRecoveryReleaseTestError) Error() string { return e.message }

func (f *fakeCoderTickets) CloseTicket(_ context.Context, id string) (ticketclient.Transition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes = append(f.closes, id)
	if f.closeErr != nil {
		return ticketclient.Transition{}, f.closeErr
	}
	if f.onClose != nil {
		f.onClose()
	}
	return ticketclient.Transition{ID: id, Changed: true, State: ticketclient.StateClosed}, nil
}

func (f *fakeCoderTickets) ApproveTicket(_ context.Context, id string) (ticketclient.MutationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.approves = append(f.approves, id)
	if ticket, ok := f.shows[id]; ok {
		ticket.State = "signoff"
		ticket.Assignee = ""
		f.shows[id] = ticket
	}
	return ticketclient.MutationResult{ID: id, Changed: true, FromState: "review", State: "signoff"}, nil
}

type fakeCoderState struct {
	mu       sync.Mutex
	sessions map[string]state.Session
	bounces  map[string]int
	sets     []state.Session
	history  []state.Session
	err      error
	setErr   error
}

func newFakeCoderState() *fakeCoderState {
	return &fakeCoderState{sessions: make(map[string]state.Session), bounces: make(map[string]int)}
}

func coderSessionKey(ticket, role, harnessName string, owner ...string) string {
	value := ""
	if len(owner) > 0 {
		value = owner[0]
	}
	return ticket + "\x00" + role + "\x00" + harnessName + "\x00" + value
}

func (f *fakeCoderState) GetSession(_ context.Context, ticket, role, harnessName string, owner ...string) (state.Session, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return state.Session{}, false, f.err
	}
	session, ok := f.sessions[coderSessionKey(ticket, role, harnessName, owner...)]
	if ok && !session.IsCurrent() {
		ok = false
	}
	return session, ok, nil
}

func (f *fakeCoderState) SetSession(_ context.Context, session state.Session) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setErr != nil {
		return f.setErr
	}
	if f.err != nil {
		return f.err
	}
	f.sessions[coderSessionKey(session.Ticket, session.Role, session.Harness, session.Owner)] = session
	f.sets = append(f.sets, session)
	return nil
}

func (f *fakeCoderState) SupersedeSession(_ context.Context, expected state.Session, reason string) (state.Session, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := coderSessionKey(expected.Ticket, expected.Role, expected.Harness, expected.Owner)
	current, ok := f.sessions[key]
	if !ok || current.ID != expected.ID || !current.IsCurrent() {
		return state.Session{}, false, nil
	}
	supersededAt := time.Now().UTC()
	current.SupersededAt = &supersededAt
	current.SupersededReason = reason
	f.sessions[key] = current
	f.history = append(f.history, current)
	return current, true, nil
}

func (f *fakeCoderState) RegisterSession(_ context.Context, session state.Session, replaces string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setErr != nil {
		return f.setErr
	}
	key := coderSessionKey(session.Ticket, session.Role, session.Harness, session.Owner)
	if current, ok := f.sessions[key]; ok {
		if current.IsCurrent() && current.ID != session.ID {
			return state.ErrSessionConflict
		}
		if !current.IsCurrent() && current.ID == session.ID {
			return state.ErrSessionSuperseded
		}
	}
	if replaces != "" {
		for i := range f.history {
			if f.history[i].ID == replaces {
				f.history[i].ReplacedBy = session.ID
			}
		}
	}
	f.sessions[key] = session
	f.sets = append(f.sets, session)
	return nil
}

func (f *fakeCoderState) LinkSessionReplacement(_ context.Context, expected state.Session, replacement string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.history {
		if f.history[i].ID == expected.ID {
			f.history[i].ReplacedBy = replacement
			return nil
		}
	}
	return errors.New("superseded session missing")
}

func (f *fakeCoderState) LinkLatestSessionReplacement(ctx context.Context, key state.Session, replacement string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.history {
		if f.history[i].Ticket == key.Ticket && f.history[i].Role == key.Role && f.history[i].Harness == key.Harness && f.history[i].Owner == key.Owner && f.history[i].ReplacedBy == "" {
			f.history[i].ReplacedBy = replacement
			return nil
		}
	}
	return nil
}

func (f *fakeCoderState) BounceCount(_ context.Context, ticket string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	return f.bounces[ticket], nil
}

func (f *fakeCoderState) IncrementBounces(_ context.Context, ticket string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	f.bounces[ticket]++
	return f.bounces[ticket], nil
}

type fakeCoderHarness struct {
	mu              sync.Mutex
	runs            []harness.RunRequest
	resumes         []string
	runRes          harness.RunResult
	runErr          error
	resumeRes       harness.RunResult
	resumeErr       error
	onRun           func()
	cleanups        []string
	cleanupPolicies []harness.CleanupPolicy
}

func (f *fakeCoderHarness) Run(_ context.Context, request harness.RunRequest) (harness.RunResult, error) {
	f.mu.Lock()
	f.runs = append(f.runs, request)
	result, err := f.runRes, f.runErr
	f.mu.Unlock()
	if f.onRun != nil {
		f.onRun()
	}
	return result, err
}

func (f *fakeCoderHarness) Resume(_ context.Context, session string, request harness.RunRequest) (harness.RunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumes = append(f.resumes, session)
	f.runs = append(f.runs, request)
	return f.resumeRes, f.resumeErr
}

func (f *fakeCoderHarness) Cleanup(_ context.Context, id string, policy harness.CleanupPolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanups = append(f.cleanups, id)
	f.cleanupPolicies = append(f.cleanupPolicies, policy)
	return nil
}

type fakeCoderCleanup struct {
	mu        sync.Mutex
	tickets   []ticketclient.Ticket
	onCleanup func()
	err       error
}

func (f *fakeCoderCleanup) CleanupConfirmed(_ context.Context, ticket ticketclient.Ticket) (CleanupResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tickets = append(f.tickets, ticket)
	if f.onCleanup != nil {
		f.onCleanup()
	}
	if f.err != nil {
		return CleanupResult{}, f.err
	}
	return CleanupResult{TicketID: ticket.ID, State: ticket.State, Changed: true}, nil
}

func coderConfig() CoderConfig {
	return CoderConfig{
		Actor:         "coder-1",
		Harness:       "codex",
		MaxBounces:    6,
		SessionPolicy: SessionPolicyTicket,
		StateDir:      ".ticket-orc",
		OutputMode:    "compact",
		WorkingDir:    "/work/project",
		Operator:      io.Discard,
		Diagnostics:   io.Discard,
	}
}

func TestRunCoderClaimsRunsRereadsAndReturnsToQueue(t *testing.T) {
	ticketID := "20260919-30001"
	tickets := &fakeCoderTickets{
		claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
		shows:  map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "coder-1"}},
	}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread-1", StreamEndedNormally: true}}
	ctx, cancel := context.WithCancel(context.Background())
	tickets.onShow = cancel
	stateErr := RunCoder(ctx, coderConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if !errors.Is(stateErr, context.Canceled) {
		t.Fatalf("RunCoder error = %v, want queue cancellation", stateErr)
	}
	if len(agent.runs) != 1 || len(agent.resumes) != 0 {
		t.Fatalf("harness calls = runs %#v resumes %#v", agent.runs, agent.resumes)
	}
	request := agent.runs[0]
	for _, want := range []string{ticketID, "Follow repository instructions", "Ticket Skill", "implement and verify", "submit it to review", "Do not select or work another ticket"} {
		if !strings.Contains(request.Prompt, want) {
			t.Errorf("prompt %q does not contain %q", request.Prompt, want)
		}
	}
	for _, unwanted := range []string{"coder-1", "actor", "role", "--config", "--scope"} {
		if strings.Contains(request.Prompt, unwanted) {
			t.Errorf("prompt %q contains routing or identity text %q", request.Prompt, unwanted)
		}
	}
	if len(stateStore.sets) != 1 || stateStore.sets[0].ID != "thread-1" {
		t.Fatalf("retained sessions = %#v", stateStore.sets)
	}
}

func TestRunCoderCancellationAfterClaimDoesNotLaunchHarness(t *testing.T) {
	ticketID := "20260919-30007"
	ctx, cancel := context.WithCancel(context.Background())
	tickets := &fakeCoderTickets{
		claims:  []ticketclient.Ticket{{ID: ticketID, State: "open"}},
		onClaim: cancel,
	}
	agent := &fakeCoderHarness{}
	err := RunCoder(ctx, coderConfig(), tickets, agent, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCoder = %v, want context canceled", err)
	}
	if len(agent.runs) != 0 {
		t.Fatalf("harness runs = %#v, want none", agent.runs)
	}
}

func TestRunCoderResumesRetainedSession(t *testing.T) {
	ticketID := "20260919-30002"
	stateStore := newFakeCoderState()
	stateStore.sessions[coderSessionKey(ticketID, "coder", "codex", "coder-1")] = state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", Owner: "coder-1", ID: "existing"}
	agent := &fakeCoderHarness{resumeRes: harness.RunResult{SessionID: "existing"}}
	// Exercise the one-turn helper directly so the canceled queue does not hide
	// the resume decision.
	if _, err := runCoderTurn(context.Background(), coderConfig(), ticketID, agent, stateStore); err != nil {
		t.Fatalf("runCoderTurn: %v", err)
	}
	if !reflect.DeepEqual(agent.resumes, []string{"existing"}) {
		t.Fatalf("resume calls = %#v", agent.resumes)
	}
}

func TestManagedSessionInvalidationSupersedesBeforeReplacement(t *testing.T) {
	ticketID := "20260923-43205-test"
	for _, policy := range []harness.CleanupPolicy{harness.CleanupDelete, harness.CleanupArchive, harness.CleanupKeep} {
		t.Run(string(policy), func(t *testing.T) {
			stateStore := newFakeCoderState()
			owner := "coder-1"
			old := state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", Owner: owner, ID: "old-session"}
			stateStore.sessions[coderSessionKey(ticketID, "coder", "codex", owner)] = old
			agent := &fakeCoderHarness{resumeRes: harness.RunResult{SessionID: "old-session", SessionOutcome: harness.SessionInvalidated}, resumeErr: errors.New("typed invalidation"), runRes: harness.RunResult{SessionID: "replacement"}}
			config := coderConfig()
			config.SessionCleanup = string(policy)
			turn, err := runCoderTurn(context.Background(), config, ticketID, agent, stateStore)
			if err != nil || turn.err != nil {
				t.Fatalf("runCoderTurn = %#v, %v", turn, err)
			}
			if !reflect.DeepEqual(agent.resumes, []string{"old-session"}) || len(agent.runs) != 2 {
				t.Fatalf("calls resume=%v runs=%d", agent.resumes, len(agent.runs))
			}
			current := stateStore.sessions[coderSessionKey(ticketID, "coder", "codex", owner)]
			if current.ID != "replacement" {
				t.Fatalf("current session = %#v", current)
			}
			if len(stateStore.history) != 1 || stateStore.history[0].SupersededReason != "harness_invalidated" || stateStore.history[0].ReplacedBy != "replacement" {
				t.Fatalf("supersession history = %#v", stateStore.history)
			}
			if policy == harness.CleanupKeep && len(agent.cleanups) != 0 {
				t.Fatalf("keep cleanup calls = %v", agent.cleanups)
			}
			if policy != harness.CleanupKeep && (!reflect.DeepEqual(agent.cleanups, []string{"old-session"}) || agent.cleanupPolicies[0] != policy) {
				t.Fatalf("cleanup = %v policies=%v", agent.cleanups, agent.cleanupPolicies)
			}
		})
	}
}

func TestManagedSessionReusableFailureDoesNotReplaceAndWrongOwnerIsNeverReused(t *testing.T) {
	ticketID := "20260923-43205-reusable"
	owner := "coder-1"
	stateStore := newFakeCoderState()
	old := state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", Owner: owner, ID: "kept-session"}
	stateStore.sessions[coderSessionKey(ticketID, "coder", "codex", owner)] = old
	agent := &fakeCoderHarness{resumeRes: harness.RunResult{SessionID: old.ID, SessionOutcome: harness.SessionReusable}, resumeErr: errors.New("turn failed but session is reusable")}
	turn, err := runCoderTurn(context.Background(), coderConfig(), ticketID, agent, stateStore)
	if err != nil || turn.err == nil || len(agent.runs) != 1 || len(agent.resumes) != 1 {
		t.Fatalf("reusable failure = %#v, %v; runs=%d resumes=%v", turn, err, len(agent.runs), agent.resumes)
	}
	if got := stateStore.sessions[coderSessionKey(ticketID, "coder", "codex", owner)]; got != old || len(stateStore.history) != 0 {
		t.Fatalf("reusable session changed: current=%#v history=%#v", got, stateStore.history)
	}

	otherOwnerState := newFakeCoderState()
	other := state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", Owner: "different-worker", ID: "other-worker-session"}
	otherOwnerState.sessions[coderSessionKey(ticketID, "coder", "codex", other.Owner)] = other
	fresh := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "owned-session"}}
	if _, err := runCoderTurn(context.Background(), coderConfig(), ticketID, fresh, otherOwnerState); err != nil {
		t.Fatal(err)
	}
	if len(fresh.resumes) != 0 || len(fresh.cleanups) != 0 {
		t.Fatalf("wrong-owner session used or cleaned: resumes=%v cleanup=%v", fresh.resumes, fresh.cleanups)
	}
}

func TestManagedSessionContextThresholdUsesSupersessionPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		remaining int64
		wantRun   bool
	}{
		{name: "below threshold supersedes", remaining: 199, wantRun: true},
		{name: "at threshold resumes", remaining: 200, wantRun: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ticketID := "20260923-context-" + strings.ReplaceAll(test.name, " ", "-")
			owner := "coder-1"
			stateDir := t.TempDir()
			store := newTestStateStoreAt(t, stateDir)
			old := state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", Owner: owner, ID: "old-session"}
			if err := store.RegisterSession(ctx, old, ""); err != nil {
				t.Fatal(err)
			}
			telemetry := contextheadroom.Telemetry{Known: true, Used: 1000 - test.remaining, Window: 1000, Remaining: test.remaining, ObservedAt: time.Now().UTC()}
			if changed, err := store.SetSessionContextTelemetry(ctx, old, telemetry); err != nil || !changed {
				t.Fatalf("set telemetry = %v, %v", changed, err)
			}
			agent := &fakeCoderHarness{resumeRes: harness.RunResult{SessionID: old.ID}, runRes: harness.RunResult{SessionID: "replacement"}}
			config := coderConfig()
			config.MinimumReuseContextPercent = 20
			config.SessionCleanup = string(harness.CleanupArchive)
			turn, err := runCoderTurn(ctx, config, ticketID, agent, store)
			if err != nil || turn.err != nil {
				t.Fatalf("runCoderTurn = %#v, %v", turn, err)
			}
			if test.wantRun {
				if len(agent.runs) != 1 || len(agent.resumes) != 0 {
					t.Fatalf("low-context calls = runs %d resumes %v", len(agent.runs), agent.resumes)
				}
				current, found, err := store.GetSession(ctx, ticketID, "coder", "codex", owner)
				if err != nil || !found || current.ID != "replacement" {
					t.Fatalf("replacement session = %#v found=%v err=%v", current, found, err)
				}
				if !reflect.DeepEqual(agent.cleanups, []string{old.ID}) || !reflect.DeepEqual(agent.cleanupPolicies, []harness.CleanupPolicy{harness.CleanupArchive}) {
					t.Fatalf("low-context cleanup = sessions %v policies %v", agent.cleanups, agent.cleanupPolicies)
				}
				snapshot, err := store.Read(ctx)
				if err != nil || len(snapshot.Sessions) != 2 || snapshot.Sessions[0].SupersededReason != "context_below_threshold" || snapshot.Sessions[0].ReplacedBy != "replacement" {
					t.Fatalf("context supersession history = %#v, err=%v", snapshot.Sessions, err)
				}
				restarted := newTestStateStoreAt(t, stateDir)
				current, found, err = restarted.GetSession(ctx, ticketID, "coder", "codex", owner)
				if err != nil || !found || current.ID != "replacement" {
					t.Fatalf("restarted replacement session = %#v found=%v err=%v", current, found, err)
				}
			} else if len(agent.runs) != 1 || !reflect.DeepEqual(agent.resumes, []string{old.ID}) {
				t.Fatalf("sufficient-context calls = runs %d resumes %v", len(agent.runs), agent.resumes)
			}
		})
	}
}

func TestManagedSessionFailedReplacementStaysRetired(t *testing.T) {
	ticketID := "20260923-43205-failed"
	owner := "coder-1"
	stateStore := newFakeCoderState()
	stateStore.sessions[coderSessionKey(ticketID, "coder", "codex", owner)] = state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", Owner: owner, ID: "old-session"}
	agent := &fakeCoderHarness{
		resumeRes: harness.RunResult{SessionID: "old-session", SessionOutcome: harness.SessionInvalidated},
		resumeErr: errors.New("session rejected"),
		runErr:    errors.New("replacement failed"),
	}
	turn, err := runCoderTurn(context.Background(), coderConfig(), ticketID, agent, stateStore)
	if err != nil || turn.err == nil || !strings.Contains(turn.err.Error(), "replacement failed") {
		t.Fatalf("runCoderTurn = %#v, %v", turn, err)
	}
	if current, found, err := stateStore.GetSession(context.Background(), ticketID, "coder", "codex", owner); err != nil || found {
		t.Fatalf("invalidated session remains current: %#v found=%v err=%v", current, found, err)
	}
	if len(stateStore.history) != 1 || stateStore.history[0].ID != "old-session" || stateStore.history[0].ReplacedBy != "" {
		t.Fatalf("failed replacement history = %#v", stateStore.history)
	}
}

func TestRunCoderFreshPolicyStartsNewAndDoesNotResume(t *testing.T) {
	ticketID := "20260919-30003"
	stateStore := newFakeCoderState()
	stateStore.sessions[coderSessionKey(ticketID, "coder", "codex", "coder-1")] = state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", Owner: "coder-1", ID: "existing"}
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "fresh"}}
	config := coderConfig()
	config.SessionPolicy = SessionPolicyFresh
	result, err := runCoderTurn(context.Background(), config, ticketID, agent, stateStore)
	if err != nil || len(agent.runs) != 1 || len(agent.resumes) != 0 {
		t.Fatalf("fresh run = %#v, %v; runs=%#v resumes=%#v", result, err, agent.runs, agent.resumes)
	}
	if len(stateStore.sets) != 0 {
		t.Fatalf("fresh policy retained session: %#v", stateStore.sets)
	}
}

func TestRunCoderTurnPromptDoesNotNarrateTicketRouting(t *testing.T) {
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{}
	config := coderConfig()
	if _, err := runCoderTurn(context.Background(), config, "20260921-77092", agent, stateStore); err != nil {
		t.Fatalf("runCoderTurn: %v", err)
	}
	if len(agent.runs) != 1 || strings.Contains(agent.runs[0].Prompt, "--config") || strings.Contains(agent.runs[0].Prompt, "--scope") {
		t.Fatalf("managed prompt = %#v, want no explicit routing instructions", agent.runs)
	}
}

func TestRunCoderBounceLimitReleasesWithoutHarness(t *testing.T) {
	ticketID := "20260919-30004"
	tickets := &fakeCoderTickets{claims: []ticketclient.Ticket{{ID: ticketID, State: "open"}}, shows: map[string]ticketclient.Ticket{}}
	stateStore := newFakeCoderState()
	stateStore.bounces[ticketID] = 6
	agent := &fakeCoderHarness{}
	config := coderConfig()
	config.MaxBounces = 6
	err := RunCoder(context.Background(), config, tickets, agent, stateStore, &fakeCoderCleanup{})
	var limitErr *BounceLimitError
	if !errors.As(err, &limitErr) || len(tickets.releases) != 1 || len(agent.runs) != 0 {
		t.Fatalf("RunCoder = %v; releases=%#v runs=%#v", err, tickets.releases, agent.runs)
	}
}

func TestRunCoderHarnessFailureStopsWithoutRespawnAndSavesSession(t *testing.T) {
	ticketID := "20260919-30005"
	tickets := &fakeCoderTickets{claims: []ticketclient.Ticket{{ID: ticketID, State: "open"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review"}}}
	stateStore := newFakeCoderState()
	harnessErr := errors.New("codex failed")
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "failed-thread"}, runErr: harnessErr}
	err := RunCoder(context.Background(), coderConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if !errors.Is(err, harnessErr) || len(agent.runs) != 1 || len(stateStore.sets) != 1 || len(tickets.showIDs) != 1 {
		t.Fatalf("RunCoder = %v; runs=%d sessions=%#v shows=%#v", err, len(agent.runs), stateStore.sets, tickets.showIDs)
	}
}

func TestRunCoderReleasesUnchangedOpenClaimAfterPreSessionFailure(t *testing.T) {
	ticketID := "20260919-30010"
	harnessErr := errors.New("Codex could not start")
	tickets := &fakeCoderTickets{
		claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
		shows:  map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "open", Assignee: "coder-1"}},
	}
	var diagnostics strings.Builder
	config := coderConfig()
	config.Diagnostics = &diagnostics
	err := RunCoder(context.Background(), config, tickets, &fakeCoderHarness{runErr: harnessErr}, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, harnessErr) || !strings.Contains(diagnostics.String(), "released unchanged open claim") || !reflect.DeepEqual(tickets.releases, []string{ticketID}) {
		t.Fatalf("RunCoder=%v diagnostics=%q releases=%#v", err, diagnostics.String(), tickets.releases)
	}
}

func TestRunCoderPreSessionRecoveryPreservesHarnessAndTicketErrors(t *testing.T) {
	harnessErr := &claimRecoveryTestError{message: "Codex startup failed"}
	showErr := errors.New("Ticket show failed")
	releaseErr := &claimRecoveryReleaseTestError{message: "Ticket release failed"}
	for _, test := range []struct {
		name        string
		showErr     error
		releaseErr  error
		releaseNoop bool
		wantRelease bool
		wantRecover bool
	}{
		{name: "show failure", showErr: showErr},
		{name: "release failure", releaseErr: releaseErr, wantRelease: true, wantRecover: true},
		{name: "release did not change claim", releaseNoop: true, wantRelease: true, wantRecover: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ticketID := "20260919-30011"
			var diagnostics strings.Builder
			config := coderConfig()
			config.Diagnostics = &diagnostics
			tickets := &fakeCoderTickets{
				claims:  []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
				shows:   map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "open", Assignee: "coder-1"}},
				showErr: test.showErr, releaseErr: test.releaseErr, releaseNoop: test.releaseNoop,
			}
			err := RunCoder(context.Background(), config, tickets, &fakeCoderHarness{runErr: harnessErr}, newFakeCoderState(), &fakeCoderCleanup{})
			if !errors.Is(err, harnessErr) {
				t.Fatalf("RunCoder=%v, want original harness error", err)
			}
			if test.showErr != nil && !errors.Is(err, showErr) {
				t.Fatalf("RunCoder=%v, want Show error", err)
			}
			if test.releaseErr != nil && !errors.Is(err, releaseErr) {
				t.Fatalf("RunCoder=%v, want Release error", err)
			}
			if test.releaseErr != nil {
				var original *claimRecoveryTestError
				if !errors.As(err, &original) || original != harnessErr {
					t.Fatalf("RunCoder=%v, errors.As did not preserve harness cause", err)
				}
				var release *claimRecoveryReleaseTestError
				if !errors.As(err, &release) || release != releaseErr {
					t.Fatalf("RunCoder=%v, errors.As did not preserve release cause", err)
				}
			}
			if test.showErr != nil && !strings.Contains(diagnostics.String(), "automatic claim recovery skipped because Ticket state could not be verified") {
				t.Fatalf("diagnostics=%q, want unverified-state recovery diagnostic", diagnostics.String())
			}
			if test.wantRecover && !strings.Contains(err.Error(), "automatic claim recovery failed") {
				t.Fatalf("RunCoder=%v, want explicit recovery failure", err)
			}
			if test.wantRecover && !strings.Contains(diagnostics.String(), "automatic claim recovery failed") {
				t.Fatalf("diagnostics=%q, want operator-visible recovery failure", diagnostics.String())
			}
			if got := len(tickets.releases); (got == 1) != test.wantRelease {
				t.Fatalf("release calls=%#v, wantRelease=%t", tickets.releases, test.wantRelease)
			}
		})
	}
}

func TestRunCoderDoesNotReleaseClaimAfterSessionLifecycleOrOwnershipChange(t *testing.T) {
	harnessErr := errors.New("Codex startup failed")
	for _, test := range []struct {
		name      string
		result    harness.RunResult
		observed  ticketclient.Ticket
		wantClean bool
	}{
		{name: "session established", result: harness.RunResult{SessionID: "thread"}, observed: ticketclient.Ticket{State: "open", Assignee: "coder-1"}},
		{name: "moved to review", observed: ticketclient.Ticket{State: "review", Assignee: "coder-1"}},
		{name: "terminal", observed: ticketclient.Ticket{State: ticketclient.StateClosed, Assignee: "coder-1"}, wantClean: true},
		{name: "unassigned", observed: ticketclient.Ticket{State: "open"}},
		{name: "other owner", observed: ticketclient.Ticket{State: "open", Assignee: "someone-else"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ticketID := "20260919-30012"
			observed := test.observed
			observed.ID = ticketID
			tickets := &fakeCoderTickets{
				claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
				shows:  map[string]ticketclient.Ticket{ticketID: observed},
			}
			cleanup := &fakeCoderCleanup{}
			err := RunCoder(context.Background(), coderConfig(), tickets, &fakeCoderHarness{runRes: test.result, runErr: harnessErr}, newFakeCoderState(), cleanup)
			if !errors.Is(err, harnessErr) || len(tickets.releases) != 0 {
				t.Fatalf("RunCoder=%v releases=%#v, want harness error and no rollback", err, tickets.releases)
			}
			if (len(cleanup.tickets) == 1) != test.wantClean {
				t.Fatalf("cleanup=%#v wantClean=%t", cleanup.tickets, test.wantClean)
			}
		})
	}
}

func TestRunCoderCancellationDuringHarnessDoesNotReleaseClaim(t *testing.T) {
	ticketID := "20260919-30013"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tickets := &fakeCoderTickets{
		claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
		shows:  map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "open", Assignee: "coder-1"}},
	}
	agent := &fakeCoderHarness{runErr: context.Canceled, onRun: cancel}
	err := RunCoder(ctx, coderConfig(), tickets, agent, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, context.Canceled) || len(tickets.releases) != 0 {
		t.Fatalf("RunCoder=%v releases=%#v, want cancellation without startup rollback", err, tickets.releases)
	}
}

func TestRunCoderRereadsAfterHarnessFailureWithOpenState(t *testing.T) {
	ticketID := "20260919-30008"
	tickets := &fakeCoderTickets{claims: []ticketclient.Ticket{{ID: ticketID, State: "open"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "open"}}}
	stateStore := newFakeCoderState()
	harnessErr := errors.New("agent exited after submitting nothing")
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}, runErr: harnessErr}
	err := RunCoder(context.Background(), coderConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if !errors.Is(err, harnessErr) || !strings.Contains(err.Error(), "expected review or terminal") || len(tickets.showIDs) != 1 {
		t.Fatalf("RunCoder = %v; shows=%#v", err, tickets.showIDs)
	}
}

func TestRunCoderRereadsAfterSessionSaveFailure(t *testing.T) {
	ticketID := "20260919-30009"
	tickets := &fakeCoderTickets{claims: []ticketclient.Ticket{{ID: ticketID, State: "open"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review"}}}
	stateStore := newFakeCoderState()
	stateStore.setErr = errors.New("state write failed")
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	err := RunCoder(context.Background(), coderConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if err == nil || !strings.Contains(err.Error(), "state write failed") || len(tickets.showIDs) != 1 {
		t.Fatalf("RunCoder = %v; shows=%#v", err, tickets.showIDs)
	}
}

func TestRunCoderWrongPostTurnStateStops(t *testing.T) {
	ticketID := "20260919-30006"
	tickets := &fakeCoderTickets{claims: []ticketclient.Ticket{{ID: ticketID, State: "open"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "open"}}}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	err := RunCoder(context.Background(), coderConfig(), tickets, agent, stateStore, &fakeCoderCleanup{})
	if err == nil || !strings.Contains(err.Error(), "expected review or terminal") {
		t.Fatalf("RunCoder = %v, want lifecycle error", err)
	}
}

func TestRunCoderTerminalStateCleansAndContinues(t *testing.T) {
	ticketID := "20260919-30007"
	// Legacy Ticket output remains accepted and is normalized before cleanup.
	tickets := &fakeCoderTickets{claims: []ticketclient.Ticket{{ID: ticketID, State: "open"}}, shows: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: ticketclient.StateLegacyCompleted}}}
	stateStore := newFakeCoderState()
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	cleanup := &fakeCoderCleanup{}
	ctx, cancel := context.WithCancel(context.Background())
	// Cancellation is returned on the next queue poll after terminal cleanup.
	tickets.onShow = cancel
	err := RunCoder(ctx, coderConfig(), tickets, agent, stateStore, cleanup)
	if !errors.Is(err, context.Canceled) || len(cleanup.tickets) != 1 || cleanup.tickets[0].State != ticketclient.StateClosed {
		t.Fatalf("RunCoder = %v; cleanup=%#v", err, cleanup.tickets)
	}
}

func TestRunCoderSkipTagClosesAndCleansAfterSuccessfulTurn(t *testing.T) {
	ticketID := "20260923-42951"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tickets := &fakeCoderTickets{
		claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
		shows:  map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "coder-1", Tags: []string{"trivial", "backend"}}},
	}
	tickets.onClose = func() {
		tickets.shows[ticketID] = ticketclient.Ticket{ID: ticketID, State: ticketclient.StateClosed, Assignee: "coder-1", Tags: []string{"trivial", "backend"}}
	}
	cleanup := &fakeCoderCleanup{onCleanup: cancel}
	config := coderConfig()
	config.ReviewSkipTags = []string{"trivial", "no-review"}
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	err := RunCoder(ctx, config, tickets, agent, newFakeCoderState(), cleanup)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCoder = %v, want queue cancellation", err)
	}
	if !reflect.DeepEqual(tickets.closes, []string{ticketID}) || len(cleanup.tickets) != 1 || cleanup.tickets[0].State != ticketclient.StateClosed {
		t.Fatalf("close=%#v cleanup=%#v", tickets.closes, cleanup.tickets)
	}
}

func TestRunCoderSkipTagPreservesExplicitSignoffGate(t *testing.T) {
	ticketID := "20260923-42952"
	ctx, cancel := context.WithCancel(context.Background())
	tickets := &fakeCoderTickets{
		claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
		shows:  map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "coder-1", Tags: []string{"no-review"}}},
		onShow: cancel,
	}
	config := coderConfig()
	config.ReviewSkipTags = []string{"trivial", "no-review"}
	config.ReviewCompletion = "signoff"
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
	err := RunCoder(ctx, config, tickets, agent, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, context.Canceled) || len(tickets.closes) != 0 || !reflect.DeepEqual(tickets.approves, []string{ticketID}) || tickets.shows[ticketID].State != "signoff" {
		t.Fatalf("RunCoder = %v, approves=%#v closes=%#v state=%q; want signoff approval without close", err, tickets.approves, tickets.closes, tickets.shows[ticketID].State)
	}
}

func TestRunCoderSkipTagDoesNotCloseUnrelatedOrUnownedReview(t *testing.T) {
	for _, test := range []struct {
		name     string
		assignee string
		tags     []string
	}{
		{name: "unrelated", assignee: "coder-1", tags: []string{"backend"}},
		{name: "unowned", assignee: "other", tags: []string{"trivial"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ticketID := "20260923-42953"
			ctx, cancel := context.WithCancel(context.Background())
			tickets := &fakeCoderTickets{
				claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
				shows:  map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: test.assignee, Tags: test.tags}},
				onShow: cancel,
			}
			config := coderConfig()
			config.ReviewSkipTags = []string{"trivial", "no-review"}
			agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}}
			err := RunCoder(ctx, config, tickets, agent, newFakeCoderState(), &fakeCoderCleanup{})
			if test.assignee == "coder-1" && !errors.Is(err, context.Canceled) {
				t.Fatalf("RunCoder = %v, want cancellation", err)
			}
			if test.assignee != "coder-1" && (err == nil || !strings.Contains(err.Error(), "owned by")) {
				t.Fatalf("RunCoder = %v, want ownership error", err)
			}
			if len(tickets.closes) != 0 {
				t.Fatalf("RunCoder = %v, closes=%#v", err, tickets.closes)
			}
		})
	}
}

func TestRunCoderSkipTagNeverClosesFailedTurn(t *testing.T) {
	ticketID := "20260923-42954"
	ctx, cancel := context.WithCancel(context.Background())
	tickets := &fakeCoderTickets{
		claims: []ticketclient.Ticket{{ID: ticketID, State: "open", Assignee: "coder-1"}},
		shows:  map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: "review", Assignee: "coder-1", Tags: []string{"trivial"}}},
		onShow: cancel,
	}
	config := coderConfig()
	config.ReviewSkipTags = []string{"trivial"}
	harnessErr := errors.New("coder failed")
	agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread"}, runErr: harnessErr}
	err := RunCoder(ctx, config, tickets, agent, newFakeCoderState(), &fakeCoderCleanup{})
	if !errors.Is(err, harnessErr) || len(tickets.closes) != 0 {
		t.Fatalf("RunCoder = %v, closes=%#v", err, tickets.closes)
	}
}
