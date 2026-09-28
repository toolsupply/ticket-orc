package harness

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type preflightTestHarness struct {
	validateErr error
	caps        Capabilities
}

func (preflightTestHarness) Run(context.Context, RunRequest) (RunResult, error) {
	return RunResult{}, nil
}
func (preflightTestHarness) Resume(context.Context, string, RunRequest) (RunResult, error) {
	return RunResult{}, nil
}
func (preflightTestHarness) Cleanup(context.Context, string, CleanupPolicy) error { return nil }
func (h preflightTestHarness) Capabilities() Capabilities                         { return h.caps }
func (h preflightTestHarness) Validate(PreflightConfig) error                     { return h.validateErr }

func TestPreflightReturnsTypedFailureAndPreservesCause(t *testing.T) {
	cause := errors.New("unsupported model setting")
	err := Preflight(preflightTestHarness{validateErr: cause, caps: Capabilities{Resume: true, Fresh: true, CleanupDelete: true, CleanupArchive: true}}, PreflightConfig{
		SessionPolicy: "ticket", SessionCleanup: CleanupDelete,
	})
	var preflightErr *PreflightError
	if !errors.As(err, &preflightErr) || !errors.Is(err, cause) {
		t.Fatalf("Preflight error = %#v; want typed error wrapping original cause", err)
	}
	if !strings.Contains(err.Error(), "unsupported model setting") || !strings.HasPrefix(err.Error(), "harness preflight:") {
		t.Fatalf("Preflight message = %q; want useful prefixed cause", err)
	}
}

func TestPreflightCapabilityFailureIsTyped(t *testing.T) {
	err := Preflight(preflightTestHarness{}, PreflightConfig{SessionPolicy: "ticket", SessionCleanup: CleanupDelete})
	var preflightErr *PreflightError
	if !errors.As(err, &preflightErr) || !strings.Contains(err.Error(), "retained ticket sessions") {
		t.Fatalf("Preflight error = %#v; want typed capability failure", err)
	}
}
