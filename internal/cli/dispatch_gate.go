package cli

import (
	"context"
	"sync"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// dispatchGate serializes each work-initiation boundary with durable mode
// changes so a successful pause cannot race a later queue/start operation.
type dispatchGate struct {
	mu       sync.Mutex
	changed  *sync.Cond
	mode     state.DaemonControlMode
	store    *state.DaemonControlStore
	active   int
	changing bool
}

func newDispatchGate(store *state.DaemonControlStore, mode state.DaemonControlMode) *dispatchGate {
	gate := &dispatchGate{store: store, mode: mode}
	gate.changed = sync.NewCond(&gate.mu)
	return gate
}

func (g *dispatchGate) Mode() state.DaemonControlMode {
	if g == nil {
		return state.DaemonRunning
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mode
}

// WithDispatch reserves a dispatch while running, then releases the mode lock
// before invoking work that may call Ticket or the harness. A pause blocks new
// reservations and waits for existing ones to finish before it persists.
func (g *dispatchGate) WithDispatch(fn func() error) (bool, error) {
	if g == nil {
		return true, fn()
	}
	g.mu.Lock()
	if g.mode != state.DaemonRunning || g.changing {
		g.mu.Unlock()
		return false, nil
	}
	g.active++
	g.mu.Unlock()
	err := func() (err error) {
		defer func() {
			g.mu.Lock()
			g.active--
			if g.active == 0 {
				g.changed.Broadcast()
			}
			g.mu.Unlock()
		}()
		return fn()
	}()
	return true, err
}

func (g *dispatchGate) Pause(ctx context.Context) (state.DaemonControlState, bool, error) {
	return g.change(ctx, state.DaemonPaused)
}

func (g *dispatchGate) Abort(ctx context.Context) (state.DaemonControlState, bool, error) {
	return g.change(ctx, state.DaemonAborted)
}

func (g *dispatchGate) Resume(ctx context.Context) (state.DaemonControlState, bool, error) {
	return g.change(ctx, state.DaemonRunning)
}

func (g *dispatchGate) change(ctx context.Context, target state.DaemonControlMode) (state.DaemonControlState, bool, error) {
	if g == nil || g.store == nil {
		return state.DaemonControlState{}, false, &supervisor.LifecycleError{Code: "capability_unavailable", Message: "daemon control state is unavailable"}
	}
	g.mu.Lock()
	for g.changing {
		g.changed.Wait()
	}
	g.changing = true
	g.changed.Broadcast()
	for g.active > 0 {
		g.changed.Wait()
	}
	previous := g.mode
	var result state.DaemonControlState
	var err error
	switch target {
	case state.DaemonPaused:
		result, err = g.store.PauseDaemon(ctx)
	case state.DaemonAborted:
		result, err = g.store.AbortDaemon(ctx)
	case state.DaemonRunning:
		result, err = g.store.ResumeDaemon(ctx)
	default:
		g.changing = false
		g.changed.Broadcast()
		g.mu.Unlock()
		return state.DaemonControlState{}, false, &supervisor.LifecycleError{Code: "invalid_control_mode", Message: "unsupported daemon control mode"}
	}
	if err != nil {
		g.changing = false
		g.changed.Broadcast()
		g.mu.Unlock()
		return state.DaemonControlState{}, false, err
	}
	g.mode = result.Mode
	g.changing = false
	g.changed.Broadcast()
	g.mu.Unlock()
	return result, previous != result.Mode, nil
}

func (g *dispatchGate) suppressedError() error {
	mode := g.Mode()
	if mode == state.DaemonAborted {
		return &supervisor.LifecycleError{Code: "daemon_aborted", Message: "daemon is aborted; resume it before starting work"}
	}
	return &supervisor.LifecycleError{Code: "daemon_paused", Message: "daemon is paused; resume it before starting work"}
}
