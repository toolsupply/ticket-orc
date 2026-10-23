package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
)

func conversationFixture(t *testing.T, hooks ConversationProcessHooks) (*ConversationManager, *orc.Conversations, string, string) {
	t.Helper()
	dir := t.TempDir()
	policy, err := orc.NewConversations(state.NewConversationStore(dir), orc.DefaultConversationLimits())
	if err != nil {
		t.Fatal(err)
	}
	r, err := policy.Create(t.Context(), orc.ConversationCreate{RequestID: orc.NewConversationRequestID(time.Now()), Profile: "planning", Operator: "human", Execution: state.ConversationExecution{Harness: "codex", Actor: "planner", MaxDuration: time.Minute}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	m := NewConversationManager(policy, hooks)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	return m, policy, r.ConversationID, dir
}
func conversationSubmitRequest() orc.ConversationSubmit {
	return orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: "plan", Operator: "human"}
}
func awaitConversation(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation barrier timed out")
	}
}
func conversationWaitContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestConversationCancellationWaitsForLaunchAndChildExit(t *testing.T) {
	started := make(chan struct{})
	stopping := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	m, p, id, _ := conversationFixture(t, ConversationProcessHooks{Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, _ func(state.ConversationRecord) error) (harness.RunResult, error) {
		close(started)
		<-ctx.Done()
		close(stopping)
		<-release
		return harness.RunResult{}, ctx.Err()
	}})
	t.Cleanup(func() { once.Do(func() { close(release) }); m.Shutdown(conversationWaitContext(t)) })
	// The cancelled acceptance context does not own the running turn lifetime.
	acceptance, cancelAcceptance := context.WithCancel(t.Context())
	req := conversationSubmitRequest()
	r, err := m.Submit(acceptance, id, req, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cancelAcceptance()
	awaitConversation(t, started)
	canceled := make(chan struct{})
	var confirmed bool
	var cancelErr error
	go func() { confirmed, cancelErr = m.Cancel(context.Background(), id, r.TurnID, "human"); close(canceled) }()
	awaitConversation(t, stopping)
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "running" || !c.Turns[0].CancelRequested {
		t.Fatalf("cancel declared child stopped: %+v", c.Turns[0])
	}
	// stopping is the explicit barrier proving Cancel has requested stop; the
	// fake child cannot return until release, so terminal state is impossible.
	select {
	case <-canceled:
		t.Fatal("cancellation returned before child exit")
	default:
	}
	once.Do(func() { close(release) })
	awaitConversation(t, canceled)
	if !confirmed || cancelErr != nil {
		t.Fatalf("confirmed=%t err=%v", confirmed, cancelErr)
	}
	c, err = p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "cancelled" {
		t.Fatal(c.Turns[0])
	}
	// A retry after execution retains the original acceptance receipt.
	duplicate, err := m.Submit(t.Context(), id, req, time.Now())
	if err != nil || duplicate != r {
		t.Fatalf("%+v %v", duplicate, err)
	}
}

func TestConversationAcceptedCancellationNeverLaunches(t *testing.T) {
	var launches atomic.Int32
	m, p, id, _ := conversationFixture(t, ConversationProcessHooks{Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
		launches.Add(1)
		return harness.RunResult{}, nil
	}})
	r, _, err := p.Accept(t.Context(), id, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.RequestCancel(t.Context(), id, r.TurnID, "human"); err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	child := &conversationChild{cancel: cancel, done: make(chan struct{})}
	m.run(lifetime, id, r.TurnID, "plan", child)
	if child.err != nil || launches.Load() != 0 {
		t.Fatalf("launches=%d err=%v", launches.Load(), child.err)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "cancelled" {
		t.Fatal(c.Turns[0])
	}
}

func TestConversationRestartReconcilesBeforeAdmissionAndNeverReplays(t *testing.T) {
	var launches atomic.Int32
	_, p, id, dir := conversationFixture(t, ConversationProcessHooks{})
	req := conversationSubmitRequest()
	r, _, err := p.Accept(t.Context(), id, req, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Running(t.Context(), id, r.TurnID); err != nil {
		t.Fatal(err)
	}
	if err := p.Append(t.Context(), id, r.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: "partial output"}}); err != nil {
		t.Fatal(err)
	}
	restartedPolicy, err := orc.NewConversations(state.NewConversationStore(dir), p.Limits)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	hooks := ConversationProcessHooks{
		Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
			launches.Add(1)
			return harness.RunResult{}, nil
		},
		ReconcileStopped: func(context.Context, state.Conversation, state.ConversationTurn) error {
			if !stopped {
				return errors.New("owned child still alive")
			}
			return nil
		},
	}
	restarted := NewConversationManager(restartedPolicy, hooks)
	if err := restarted.Reconcile(t.Context()); !orc.IsConversationError(err, "execution_unreconciled") {
		t.Fatal(err)
	}
	_, err = restarted.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if !orc.IsConversationError(err, "execution_unreconciled") {
		t.Fatal(err)
	}
	duplicate, err := restarted.Submit(t.Context(), id, req, time.Now())
	if err != nil || duplicate != r {
		t.Fatalf("%+v %v", duplicate, err)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "running" {
		t.Fatal("restart asserted stop without evidence")
	}
	stopped = true
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err = p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "interrupted" || c.Records[3].Message.Text != "partial output" || launches.Load() != 0 {
		t.Fatalf("recovery lost state: %+v", c)
	}
	_, err = restarted.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if !orc.IsConversationError(err, "session_unavailable") {
		t.Fatal(err)
	}
}

func TestConversationRestartInterruptsAcceptedWithoutProcessProbe(t *testing.T) {
	_, p, id, _ := conversationFixture(t, ConversationProcessHooks{})
	r, _, err := p.Accept(t.Context(), id, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewConversationManager(p, ConversationProcessHooks{})
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].ID != r.TurnID || c.Turns[0].State != "interrupted" {
		t.Fatal(c.Turns)
	}
}

func TestConversationInvalidationNeverReplacesOrReplays(t *testing.T) {
	var launches atomic.Int32
	m, p, id, _ := conversationFixture(t, ConversationProcessHooks{Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
		launches.Add(1)
		return harness.RunResult{SessionID: "lost", SessionOutcome: harness.SessionInvalidated}, nil
	}})
	r, err := m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Wait(conversationWaitContext(t), r.TurnID); err != nil {
		t.Fatal(err)
	}
	_, err = m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if !orc.IsConversationError(err, "session_unavailable") {
		t.Fatal(err)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if launches.Load() != 1 || c.Turns[0].State != "failed" || c.Turns[0].Code != "session_unavailable" {
		t.Fatalf("%+v launches=%d", c.Turns, launches.Load())
	}
}

func TestConversationTerminalPersistenceFailureRemainsActiveUntilReconciled(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var launches atomic.Int32
	m, p, id, dir := conversationFixture(t, ConversationProcessHooks{Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
		if launches.Add(1) == 1 {
			close(started)
		}
		<-release
		return harness.RunResult{SessionID: "session"}, nil
	}})
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})
	r, err := m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	awaitConversation(t, started)
	path := filepath.Join(dir, "conversations.json")
	saved := filepath.Join(dir, "saved.json")
	if err := os.Rename(path, saved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	// Restore before any failure aborts the test, and release before waiting.
	t.Cleanup(func() {
		if _, err := os.Stat(saved); err == nil {
			_ = os.Remove(path)
			_ = os.Rename(saved, path)
		}
	})
	once.Do(func() { close(release) })
	if err := m.Wait(conversationWaitContext(t), r.TurnID); err == nil {
		t.Fatal("terminal persistence failure hidden")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(saved, path); err != nil {
		t.Fatal(err)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "running" {
		t.Fatal("failed terminal write changed durable state")
	}
	// An unrelated submission must not discard the only positive stopped-child
	// evidence for the first turn while its durable state is still active.
	other, err := p.Create(t.Context(), orc.ConversationCreate{RequestID: orc.NewConversationRequestID(time.Now()), Profile: "plan", Operator: "human", Execution: c.Execution}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	next, err := m.Submit(t.Context(), other.ConversationID, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Wait(conversationWaitContext(t), next.TurnID); err != nil {
		t.Fatal(err)
	}
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err = p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "interrupted" {
		t.Fatal(c.Turns[0])
	}
	if launches.Load() != 2 || c.SessionID != "" {
		t.Fatalf("replayed/replaced execution: launches=%d session=%q", launches.Load(), c.SessionID)
	}
}

func TestConversationCloseCleanupFailureCannotReopen(t *testing.T) {
	failure := errors.New("cleanup failed")
	m, p, id, _ := conversationFixture(t, ConversationProcessHooks{Cleanup: func(context.Context, state.Conversation) error { return failure }})
	closeErr := m.Close(t.Context(), id, "human", time.Now())
	var lifecycle *LifecycleError
	if !errors.Is(closeErr, failure) || !errors.As(closeErr, &lifecycle) || !lifecycle.Applied {
		t.Fatalf("committed closure evidence lost: %v", closeErr)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.State != "closed" {
		t.Fatal("cleanup reopened conversation")
	}
	_, _, err = p.Accept(t.Context(), id, conversationSubmitRequest(), time.Now())
	if !orc.IsConversationError(err, "conversation_closed") {
		t.Fatal(err)
	}
}

func TestConversationAbortResumeAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "shutdown"}[shutdown], func(t *testing.T) {
			started := make(chan struct{})
			var once sync.Once
			m, p, id, _ := conversationFixture(t, ConversationProcessHooks{Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, _ func(state.ConversationRecord) error) (harness.RunResult, error) {
				once.Do(func() { close(started) })
				<-ctx.Done()
				return harness.RunResult{}, ctx.Err()
			}})
			t.Cleanup(func() { m.Shutdown(conversationWaitContext(t)) })
			_, err := m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			awaitConversation(t, started)
			var results []ConversationTermination
			if shutdown {
				results = m.Shutdown(conversationWaitContext(t))
			} else {
				results = m.Abort(conversationWaitContext(t))
			}
			if len(results) != 1 || results[0].ConversationID != id || results[0].Outcome != "terminated" {
				t.Fatalf("%+v", results)
			}
			_, err = m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
			if !orc.IsConversationError(err, "execution_blocked") {
				t.Fatal(err)
			}
			c, err := p.Get(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if c.Turns[0].State != "interrupted" {
				t.Fatal(c.Turns[0])
			}
			err = m.Resume(t.Context())
			if shutdown {
				if !orc.IsConversationError(err, "execution_blocked") {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConversationAbortWithExpiredWaitBudgetStillCancelsChild(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	m, _, id, _ := conversationFixture(t, ConversationProcessHooks{Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, _ func(state.ConversationRecord) error) (harness.RunResult, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return harness.RunResult{}, ctx.Err()
	}})
	t.Cleanup(func() { m.Shutdown(conversationWaitContext(t)) })
	r, err := m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	awaitConversation(t, started)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	m.Abort(ctx)
	awaitConversation(t, stopped)
	if err := m.Wait(conversationWaitContext(t), r.TurnID); err != nil {
		t.Fatal(err)
	}
}

func TestConversationOutputLimitStopsExecutionAndPreservesTerminalRecord(t *testing.T) {
	m, p, id, _ := conversationFixture(t, ConversationProcessHooks{Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, emit func(state.ConversationRecord) error) (harness.RunResult, error) {
		for i := 0; i < 100; i++ {
			err := emit(state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: strings.Repeat("x", 1000)}})
			if err != nil {
				<-ctx.Done()
				return harness.RunResult{SessionID: "session"}, err
			}
		}
		return harness.RunResult{SessionID: "session"}, nil
	}})
	r, err := m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Wait(conversationWaitContext(t), r.TurnID); err != nil {
		t.Fatal(err)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	last := c.Records[len(c.Records)-1]
	if c.Turns[0].State != "failed" || c.Turns[0].Code != "transcript_limit" || last.Kind != "turn" || last.Turn.State != "failed" {
		t.Fatalf("%+v", c.Turns)
	}
}

func TestConversationPreflightDoesNotBlockAbortOrAcceptAfterIt(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	m, p, id, _ := conversationFixture(t, ConversationProcessHooks{
		Validate: func(context.Context, state.Conversation) error { close(entered); <-release; return nil },
		Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
			t.Error("aborted preflight launched a child")
			return harness.RunResult{}, nil
		},
	})
	done := make(chan struct{})
	var submitErr error
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("submission did not finish")
		}
	})
	go func() {
		defer close(done)
		_, submitErr = m.Submit(context.Background(), id, conversationSubmitRequest(), time.Now())
	}()
	awaitConversation(t, entered)
	// Completion of Abort proves it did not wait on the Ticket preflight gate.
	m.Abort(conversationWaitContext(t))
	once.Do(func() { close(release) })
	awaitConversation(t, done)
	if !orc.IsConversationError(submitErr, "execution_blocked") {
		t.Fatalf("submission: %v", submitErr)
	}
	c, err := p.Get(t.Context(), id)
	if err != nil || len(c.Turns) != 0 {
		t.Fatalf("preflight accepted work: %#v %v", c, err)
	}
}

func TestConversationSubmitRechecksReceiptAfterPreflight(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		for _, invalid := range []bool{false, true} {
			for _, conflict := range []bool{false, true} {
				t.Run(fmt.Sprintf("blocked=%t/invalid=%t/conflict=%t", blocked, invalid, conflict), func(t *testing.T) {
					entered, release, started, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
					var validations, runs atomic.Int32
					m, policy, id, _ := conversationFixture(t, ConversationProcessHooks{
						Validate: func(ctx context.Context, _ state.Conversation) error {
							if validations.Add(1) == 1 {
								close(entered)
								select {
								case <-release:
								case <-ctx.Done():
									return ctx.Err()
								}
								if invalid {
									return &orc.ConversationError{Code: "profile_unavailable"}
								}
							}
							return nil
						},
						Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, _ func(state.ConversationRecord) error) (harness.RunResult, error) {
							runs.Add(1)
							close(started)
							<-ctx.Done()
							return harness.RunResult{}, ctx.Err()
						},
					})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					ctx := conversationWaitContext(t)
					var first state.ConversationReceipt
					var firstErr error
					t.Cleanup(func() { unblock(); awaitConversation(t, done); m.Shutdown(conversationWaitContext(t)) })
					req := conversationSubmitRequest()
					go func() { defer close(done); first, firstErr = m.Submit(ctx, id, req, time.Now()) }()
					awaitConversation(t, entered)
					secondReq := req
					if conflict {
						secondReq.Text = "different accepted payload"
					}
					accepted, err := m.Submit(ctx, id, secondReq, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					awaitConversation(t, started)
					if blocked {
						m.BlockExecution()
					}
					unblock()
					awaitConversation(t, done)
					if conflict {
						if !orc.IsConversationError(firstErr, "request_id_conflict") {
							t.Fatalf("conflict hidden by preflight/admission: %v", firstErr)
						}
					} else if firstErr != nil || first != accepted {
						t.Fatalf("accepted duplicate lost: receipt=%+v err=%v want=%+v", first, firstErr, accepted)
					}
					c, err := policy.Get(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if runs.Load() != 1 || len(c.Turns) != 1 {
						t.Fatalf("duplicate execution: runs=%d turns=%d", runs.Load(), len(c.Turns))
					}
				})
			}
		}
	}
}

func TestConversationCancelWaitFailureKeepsCommittedIntent(t *testing.T) {
	started, stopping, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	m, p, id, _ := conversationFixture(t, ConversationProcessHooks{Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, _ func(state.ConversationRecord) error) (harness.RunResult, error) {
		close(started)
		<-ctx.Done()
		close(stopping)
		<-release
		return harness.RunResult{}, ctx.Err()
	}})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	wait, cancelWait := context.WithCancel(context.Background())
	t.Cleanup(func() { cancelWait(); unblock(); m.Shutdown(conversationWaitContext(t)) })
	receipt, err := m.Submit(t.Context(), id, conversationSubmitRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	awaitConversation(t, started)
	var stopped bool
	var cancelErr error
	t.Cleanup(func() { cancelWait(); unblock(); awaitConversation(t, done) })
	go func() { defer close(done); stopped, cancelErr = m.Cancel(wait, id, receipt.TurnID, "human") }()
	awaitConversation(t, stopping)
	c, err := p.Get(t.Context(), id)
	if err != nil || !c.Turns[0].CancelRequested || c.Turns[0].State != "running" {
		t.Fatalf("cancel intent not committed: %+v %v", c, err)
	}
	cancelWait()
	awaitConversation(t, done)
	var lifecycle *LifecycleError
	if stopped || !errors.Is(cancelErr, context.Canceled) || !errors.As(cancelErr, &lifecycle) || !lifecycle.Applied {
		t.Fatalf("committed intent lost on wait cancellation: stopped=%t err=%v", stopped, cancelErr)
	}
	unblock()
	if err := m.Wait(conversationWaitContext(t), receipt.TurnID); err != nil {
		t.Fatal(err)
	}
	// Genuine rejection before intent persistence remains distinguishable.
	_, err = m.Cancel(t.Context(), id, receipt.TurnID, "")
	lifecycle = nil
	if !orc.IsConversationError(err, "invalid_operator") || errors.As(err, &lifecycle) {
		t.Fatalf("pre-mutation rejection reported applied: %v", err)
	}
	if err = m.Close(t.Context(), id, "", time.Now()); !orc.IsConversationError(err, "invalid_operator") || errors.As(err, &lifecycle) {
		t.Fatalf("pre-close rejection reported applied: %v", err)
	}
}
