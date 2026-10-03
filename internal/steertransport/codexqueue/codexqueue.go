// Package codexqueue adapts the existing Codex CLI queue command to the
// steering transport boundary.
package codexqueue

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
)

const Kind = "codex-queue"

type CodexQueue struct {
	queue func(context.Context, string, string, string) error
}

func New() *CodexQueue {
	adapter := codex.New()
	return newWithQueue(adapter.Queue)
}

func newWithQueue(queue func(context.Context, string, string, string) error) *CodexQueue {
	return &CodexQueue{queue: queue}
}

func (*CodexQueue) Kind() string { return Kind }

func (*CodexQueue) Prepare(_ context.Context, _ string, registration state.SteerRegistration) (steertransport.Endpoint, error) {
	if registration.Transport.Kind != Kind {
		return steertransport.Endpoint{}, fmt.Errorf("Codex queue requires %q transport", Kind)
	}
	home, ok := registration.Transport.Params["home"]
	if !ok || len(registration.Transport.Params) != 1 || home == "" || !filepath.IsAbs(home) || filepath.Clean(home) != home {
		return steertransport.Endpoint{}, errors.New("Codex queue requires one absolute canonical home parameter")
	}
	if err := codex.ValidateSessionTarget(registration.SessionID); err != nil {
		return steertransport.Endpoint{}, fmt.Errorf("invalid Codex queue session: %w", err)
	}
	return steertransport.Endpoint{Kind: Kind}, nil
}

func (queue *CodexQueue) Verify(ctx context.Context, localRoot string, registration state.SteerRegistration) (steertransport.Endpoint, error) {
	return queue.Prepare(ctx, localRoot, registration)
}

func (c *CodexQueue) Deliver(ctx context.Context, localRoot string, registration state.SteerRegistration, message steertransport.Message) error {
	if _, err := c.Prepare(ctx, localRoot, registration); err != nil {
		return err
	}
	err := c.queue(ctx, registration.Transport.Params["home"], registration.SessionID, message.Text)
	var processErr *codex.ProcessError
	if errors.As(err, &processErr) {
		return steertransport.Rejected(err)
	}
	return err
}

func (*CodexQueue) Retire(context.Context, string, state.SteerRegistration) error { return nil }
