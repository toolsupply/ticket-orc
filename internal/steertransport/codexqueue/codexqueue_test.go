package codexqueue

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
)

func TestCodexQueueUsesRegistrationRouteAndClassifiesProcessFailure(t *testing.T) {
	home := filepath.Join(t.TempDir(), "codex-home")
	var gotHome, gotSession, gotMessage string
	queue := newWithQueue(func(_ context.Context, home, session, message string) error {
		gotHome, gotSession, gotMessage = home, session, message
		return &codex.ProcessError{ExitCode: 1, Stderr: "queue rejected"}
	})
	registration := state.SteerRegistration{
		SessionID: "target-session",
		Transport: state.SteerTransportRoute{Kind: Kind, Params: map[string]string{"home": home}},
	}
	endpoint, err := queue.Prepare(context.Background(), "/orc", registration)
	if err != nil || endpoint.Kind != Kind {
		t.Fatalf("Prepare endpoint=%#v err=%v", endpoint, err)
	}
	message := steertransport.Message{Kind: steertransport.MessageStop, Text: "stop"}
	err = queue.Deliver(context.Background(), "/orc", registration, message)
	if !steertransport.IsRejected(err) || gotHome != home || gotSession != registration.SessionID || gotMessage != message.Text {
		t.Fatalf("route home=%q session=%q message=%q error=%v", gotHome, gotSession, gotMessage, err)
	}
	if err := queue.Retire(context.Background(), "/orc", registration); err != nil {
		t.Fatalf("Codex Retire error=%v", err)
	}
}

func TestCodexQueueKeepsAmbiguousFailuresUncertain(t *testing.T) {
	unknown := errors.New("process outcome is unknown")
	queue := newWithQueue(func(context.Context, string, string, string) error { return unknown })
	registration := state.SteerRegistration{
		SessionID: "target-session",
		Transport: state.SteerTransportRoute{Kind: Kind, Params: map[string]string{"home": filepath.Join(t.TempDir(), "codex-home")}},
	}
	err := queue.Deliver(context.Background(), "/orc", registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "wake"})
	if steertransport.IsRejected(err) || !steertransport.IsUncertain(err) || !errors.Is(err, unknown) {
		t.Fatalf("ambiguous failure classification=%v", err)
	}
}

func TestCodexQueueValidatesRouteBeforeExecuting(t *testing.T) {
	called := false
	queue := newWithQueue(func(context.Context, string, string, string) error { called = true; return nil })
	registration := state.SteerRegistration{
		SessionID: "invalid target",
		Transport: state.SteerTransportRoute{Kind: Kind, Params: map[string]string{"home": "/codex"}},
	}
	if _, err := queue.Prepare(context.Background(), "/orc", registration); err == nil {
		t.Fatal("invalid Codex target was accepted")
	}
	if err := queue.Deliver(context.Background(), "/orc", registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "wake"}); err == nil {
		t.Fatal("invalid Codex target was delivered")
	}
	if called {
		t.Fatal("Codex queue ran before route validation")
	}
}
