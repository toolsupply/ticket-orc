package supervisor

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
)

// ConversationProcessHooks is the process adapter boundary. Run must not return
// until the owned child and its output callbacks have stopped, even on cancellation. Recovery
// must reconcile surviving owned children, not merely test whether a PID exists.
// Missing recovery evidence fails closed. Only Codex is supported.
type ConversationProcessHooks struct {
	Validate         func(context.Context, state.Conversation) error
	Run              func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error)
	ReconcileStopped func(context.Context, state.Conversation, state.ConversationTurn) error
	Cleanup          func(context.Context, state.Conversation) error
}

type conversationChild struct {
	conversationID string
	cancel         context.CancelFunc
	done           chan struct{}
	err            error // published by closing done
	interrupted    bool  // protected by gate
}

// ConversationManager owns cancellable lifetimes independently of requests and
// worker dispatch. gate serializes acceptance/launch registration with abort and
// shutdown; it is never held across execution, child waits or output callbacks.
type ConversationManager struct {
	gate                          sync.Mutex
	policy                        *orc.Conversations
	hooks                         ConversationProcessHooks
	children                      map[string]*conversationChild
	reconciled, aborted, stopping bool
}

func NewConversationManager(policy *orc.Conversations, hooks ConversationProcessHooks) *ConversationManager {
	return &ConversationManager{policy: policy, hooks: hooks, children: map[string]*conversationChild{}}
}

// Reconcile never launches accepted work. A running intent can precede process
// launch, so even a missing provider session is not evidence of a stopped child.
func (m *ConversationManager) Reconcile(ctx context.Context) error {
	m.gate.Lock()
	defer m.gate.Unlock()
	return m.reconcile(ctx)
}

func (m *ConversationManager) reconcile(ctx context.Context) error {
	m.reconciled = false
	snapshot, err := m.policy.Store.Read(ctx)
	if err != nil {
		return err
	}
	for _, c := range snapshot.Conversations {
		for _, t := range c.Turns {
			if !t.Active() {
				continue
			}
			if child := m.children[t.ID]; child != nil {
				select {
				case <-child.done:
				default:
					continue
				}
			} else if t.State == "running" {
				if m.hooks.ReconcileStopped == nil {
					return &orc.ConversationError{Code: "execution_unreconciled"}
				}
				// This lifecycle gate may span recovery work, just like worker lifecycle
				// transactions. No state/store mutex is held while probing/stopping children.
				if err := m.hooks.ReconcileStopped(ctx, c, t); err != nil {
					return &orc.ConversationError{Code: "execution_unreconciled"}
				}
			}
			if err := m.policy.FinishStopped(ctx, c.ID, t.ID, "interrupted", "daemon_restart", "", false); err != nil {
				return err
			}
			delete(m.children, t.ID)
		}
	}
	m.reconciled = true
	return nil
}

func (m *ConversationManager) Submit(ctx context.Context, id string, req orc.ConversationSubmit, now time.Time) (state.ConversationReceipt, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	// Deduplication precedes admission checks, even while aborted or recovering.
	// Policy.Accept performs the durable check, but must not newly accept when
	// execution is blocked. Read the exact receipt here without any mutation.
	snapshot, err := m.policy.Store.Read(ctx)
	if err != nil {
		return state.ConversationReceipt{}, err
	}
	if _, exists := snapshot.Receipts[id+"\x00"+req.RequestID]; exists {
		receipt, _, err := m.policy.Accept(ctx, id, req, now)
		return receipt, err
	}
	if !m.reconciled {
		return state.ConversationReceipt{}, &orc.ConversationError{Code: "execution_unreconciled"}
	}
	if m.aborted || m.stopping {
		return state.ConversationReceipt{}, &orc.ConversationError{Code: "execution_blocked"}
	}
	if m.hooks.Run == nil {
		return state.ConversationReceipt{}, &orc.ConversationError{Code: "capability_unavailable"}
	}
	if m.hooks.Validate != nil {
		m.gate.Unlock()
		c, err := m.policy.Get(ctx, id)
		if err == nil {
			err = m.hooks.Validate(ctx, c)
		}
		m.gate.Lock()
		// Preflight releases admission ownership. Another submission may have
		// accepted this request while validation ran, even if validation failed
		// or execution was blocked meanwhile. Durable receipts take precedence
		// over those errors, including conflicting reuse of the request ID.
		var readErr error
		snapshot, readErr = m.policy.Store.Read(ctx)
		if readErr != nil {
			return state.ConversationReceipt{}, readErr
		}
		if _, exists := snapshot.Receipts[id+"\x00"+req.RequestID]; exists {
			receipt, _, err := m.policy.Accept(ctx, id, req, now)
			return receipt, err
		}
		if err != nil {
			return state.ConversationReceipt{}, err
		}
		if !m.reconciled {
			return state.ConversationReceipt{}, &orc.ConversationError{Code: "execution_unreconciled"}
		}
		if m.aborted || m.stopping {
			return state.ConversationReceipt{}, &orc.ConversationError{Code: "execution_blocked"}
		}
	}
	receipt, fresh, err := m.policy.Accept(ctx, id, req, now)
	if err != nil || !fresh {
		return receipt, err
	}
	// Stopped handles remain positive child-stop evidence until durable state
	// is terminal. Retention stays bounded by the durable active-turn limits;
	// unrelated submissions cannot erase evidence needed after a failed write.
	for oldTurn, old := range m.children {
		select {
		case <-old.done:
			c := snapshot.Conversations[old.conversationID]
			active := false
			for _, t := range c.Turns {
				if t.ID == oldTurn && t.Active() {
					active = true
					break
				}
			}
			if !active {
				delete(m.children, oldTurn)
			}
		default:
		}
	}
	// Acceptance is durable before registration. A crash here is reconciled as
	// interrupted, never replayed. Registration precedes releasing the launch gate.
	lifetime, cancel := context.WithCancel(context.Background())
	child := &conversationChild{conversationID: id, cancel: cancel, done: make(chan struct{})}
	m.children[receipt.TurnID] = child
	go m.run(lifetime, id, receipt.TurnID, req.Text, child)
	return receipt, nil
}

func (m *ConversationManager) run(lifetime context.Context, id, turn, text string, child *conversationChild) {
	defer close(child.done)
	defer child.cancel()
	c, err := m.policy.Get(context.Background(), id)
	if err != nil {
		child.err = err
		return
	}
	ctx, cancel := context.WithTimeout(lifetime, c.Execution.MaxDuration)
	defer cancel()
	// Cancellation can win before Running. In that case no process is launched.
	err = m.policy.Running(context.Background(), id, turn)
	if err != nil {
		if orc.IsConversationError(err, "turn_not_launchable") {
			child.err = m.policy.FinishStopped(context.Background(), id, turn, "cancelled", "cancel_requested", c.SessionID, !c.SessionUnavailable)
		} else {
			child.err = err
		}
		return
	}
	if ctx.Err() != nil {
		m.gate.Lock()
		child.err = m.policy.FinishExecutionStopped(context.Background(), id, turn, orc.ConversationRunOutcome{ContextError: ctx.Err(), Interrupted: child.interrupted})
		m.gate.Unlock()
		return
	}
	var current state.ConversationTurn
	for _, t := range c.Turns {
		if t.ID == turn {
			current = t
			current.State = "running"
		}
	}
	var outputMu sync.Mutex
	var outputErr error
	emit := func(record state.ConversationRecord) error {
		outputMu.Lock()
		defer outputMu.Unlock()
		if outputErr != nil {
			return outputErr
		}
		outputErr = m.policy.Append(context.Background(), id, turn, record)
		if outputErr != nil {
			cancel()
		}
		return outputErr
	}
	result, runErr := m.hooks.Run(ctx, c, current, text, emit)
	// Run's return is the authoritative child-stop barrier. A cancel request
	// during launch cannot publish terminal cancellation before this point.
	outputMu.Lock()
	run := orc.ConversationRunOutcome{Result: result, RunError: runErr, OutputError: outputErr, ContextError: ctx.Err()}
	outputMu.Unlock()
	m.gate.Lock()
	run.Interrupted = child.interrupted
	child.err = m.policy.FinishExecutionStopped(context.Background(), id, turn, run)
	m.gate.Unlock()
}

// Cancel persists intent before requesting stop. The caller context bounds only
// its wait; cancellation continues independently if that caller disconnects.
func (m *ConversationManager) Cancel(ctx context.Context, id, turn, operator string) (stopped bool, err error) {
	m.gate.Lock()
	if err := m.policy.RequestCancel(ctx, id, turn, operator); err != nil {
		m.gate.Unlock()
		return false, err
	}
	// From here on, cancellation intent is durable even if child-stop or
	// response-state lookup fails. Preserve that evidence across typed errors.
	defer func() {
		if err != nil {
			err = &LifecycleError{Code: "conversation_cancel_incomplete", Applied: true, Cause: err}
		}
	}()
	child := m.children[turn]
	if child != nil {
		child.cancel()
	}
	m.gate.Unlock()
	if child == nil {
		c, err := m.policy.Get(ctx, id)
		if err != nil {
			return false, err
		}
		for _, t := range c.Turns {
			if t.ID == turn {
				return !t.Active(), nil
			}
		}
		return false, &orc.ConversationError{Code: "turn_not_found"}
	}
	select {
	case <-child.done:
		return child.err == nil, child.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

type ConversationTermination struct{ ConversationID, TurnID, Outcome string }

// BlockExecution closes admission before daemon control waits on other targets.
func (m *ConversationManager) BlockExecution() { m.gate.Lock(); m.aborted = true; m.gate.Unlock() }

// Abort blocks launches before cancelling every child, then waits outside the
// gate. No worker dispatch pause is consulted by this manager.
func (m *ConversationManager) Abort(ctx context.Context) []ConversationTermination {
	return m.stop(ctx, false)
}
func (m *ConversationManager) Shutdown(ctx context.Context) []ConversationTermination {
	return m.stop(ctx, true)
}
func (m *ConversationManager) stop(ctx context.Context, shutdown bool) []ConversationTermination {
	m.gate.Lock()
	m.aborted = true
	m.stopping = m.stopping || shutdown
	// Stop intent must reach children even if an earlier daemon abort target
	// consumed the caller's wait budget or durable state cannot be read.
	for _, child := range m.children {
		child.interrupted = true
		child.cancel()
	}
	snapshot, err := m.policy.Store.Read(ctx)
	if err != nil {
		results := []ConversationTermination{{Outcome: "termination_failed"}}
		for turn, child := range m.children {
			results = append(results, ConversationTermination{child.conversationID, turn, "termination_failed"})
		}
		m.gate.Unlock()
		return results
	}
	type target struct {
		id, turn string
		child    *conversationChild
	}
	var targets []target
	for id, c := range snapshot.Conversations {
		for _, t := range c.Turns {
			if t.Active() {
				child := m.children[t.ID]
				targets = append(targets, target{id, t.ID, child})
			}
		}
	}
	m.gate.Unlock()
	results := make([]ConversationTermination, 0, len(targets))
	for _, t := range targets {
		outcome := "termination_failed"
		if t.child != nil {
			select {
			case <-t.child.done:
				if t.child.err == nil {
					outcome = "terminated"
				}
			case <-ctx.Done():
			}
		}
		results = append(results, ConversationTermination{t.id, t.turn, outcome})
	}
	return results
}

func (m *ConversationManager) Resume(ctx context.Context) error {
	m.gate.Lock()
	defer m.gate.Unlock()
	if err := m.reconcile(ctx); err != nil {
		return err
	}
	if m.stopping {
		return &orc.ConversationError{Code: "execution_blocked"}
	}
	m.aborted = false
	return nil
}

func (m *ConversationManager) Close(ctx context.Context, id, operator string, now time.Time) (err error) {
	if err := m.policy.Close(ctx, id, operator, now); err != nil {
		return err
	}
	// Closure cannot be undone by readback/cleanup failures or retention expiry.
	defer func() {
		if err != nil {
			err = &LifecycleError{Code: "conversation_close_incomplete", Applied: true, Cause: err}
		}
	}()
	c, err := m.policy.Get(ctx, id)
	if err != nil {
		return err
	}
	if m.hooks.Cleanup != nil {
		return m.hooks.Cleanup(ctx, c)
	}
	return nil
}

// Wait is an internal completion barrier, also used by deterministic lifecycle
// tests. It confirms durable terminal persistence as well as process exit.
func (m *ConversationManager) Wait(ctx context.Context, turn string) error {
	m.gate.Lock()
	child := m.children[turn]
	m.gate.Unlock()
	if child == nil {
		return errors.New("conversation child unavailable")
	}
	select {
	case <-child.done:
		return child.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Policy exposes the conversation domain service for transport composition.
func (m *ConversationManager) Policy() *orc.Conversations { return m.policy }
