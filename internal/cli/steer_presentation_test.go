package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestSignoffRetirementRendersKnownOutcomeThenIdle(t *testing.T) {
	for _, ticketState := range []string{"closed", "open", "hold"} {
		t.Run(ticketState, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			registrations := state.NewRegistrationStore(dir)
			reg, _, _, err := registrations.Join(ctx, state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryName: "project", RepositoryPath: dir,
				Actor: "planner", Role: "architect", Harness: "codex",
				Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
			})
			if err != nil {
				t.Fatal(err)
			}
			runtime := state.NewSteerRuntimeStore(dir)
			if err := runtime.Reconcile(ctx, []state.SteerRegistration{reg}); err != nil {
				t.Fatal(err)
			}
			const ticketID = "20261007-12345"
			if reserved, err := runtime.ReserveSignoffTarget(ctx, reg, ticketID, "signoff"); err != nil || !reserved {
				t.Fatalf("reserve=%t err=%v", reserved, err)
			}
			if completed, err := runtime.CompleteSignoffDeliveryWithIntents(ctx, reg, ticketID, true, false); err != nil || !completed {
				t.Fatalf("complete=%t err=%v", completed, err)
			}
			client := &dynamicSignoffTestClient{
				sequenceSteerClient: &sequenceSteerClient{activeByQueue: map[string][]bool{"open": {false}, "review": {false}}},
				ticket:              ticketclient.Ticket{ID: ticketID, State: "signoff"},
			}
			var events []daemon.Event
			statuses := &dynamicSteerStatus{publish: func(event daemon.Event) { events = append(events, event) }}
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"architect": {TicketQueue: "signoff"}}, nil,
				func(state.SteerRegistration) (steerClient, error) { return client, nil },
				testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
					t.Error("outstanding or retired target must not send a notification")
					return nil
				}), statuses, runtime, newRegistrationObserver(registrations), nil, nil)
			runner.bootstrap(ctx)
			if len(events) != 1 || events[0].Code != "awaiting_signoff_outcome" {
				t.Fatalf("pending events=%#v", events)
			}
			client.ticket.State = ticketState
			// A repository observation can discover a target that left signoff.
			runner.dirty.MarkRepository(reg.RepositoryID)
			runner.repositoryChanged(ctx)
			runner.registrationTick(ctx, time.Now())
			if len(events) != 3 || events[1].Code != "signoff_resolved" || events[1].Ticket != ticketID || events[2].Code != "idle" {
				t.Fatalf("retirement and idle events=%#v", events)
			}
			for _, unified := range []bool{false, true} {
				var out bytes.Buffer
				renderer := &consoleWatchRenderer{unified: unified}
				tracker := newConsoleEventTracker()
				tracker.prepare(events[0])
				for _, event := range events[1:] {
					if err := renderer.render(&out, tracker.prepare(event), time.Now()); err != nil {
						t.Fatal(err)
					}
				}
				got := out.String()
				for _, want := range []string{"project", "architect", "planner", shortIdentity(reg.SessionID), "ticket left signoff", ticketID, "no work available"} {
					if !strings.Contains(got, want) {
						t.Errorf("unified=%t missing %q: %s", unified, want, got)
					}
				}
				if strings.Contains(got, "delivery issue") || strings.Contains(got, "closed") || strings.Contains(got, "accepted") {
					t.Errorf("unified=%t misleading outcome: %s", unified, got)
				}
			}
		})
	}
}

func TestSteerPresentationDistinguishesControlAndErrors(t *testing.T) {
	for _, test := range []struct{ code, state, want string }{
		{"signoff_resolved", "none", "ticket left signoff"},
		{"hold_resolved", "none", "hold resolved"},
		{"awaiting_hold_resolution", "queued", "waiting for hold resolution"},
		{"human_handoff", "none", "assigned to user; human action required"},
		{"assigned_elsewhere", "none", "ticket assigned to another actor"},
		{"registration_replaced", "none", "session registration was replaced"},
		{"registration_removed", "left", "left"},
		{"policy_changed", "none", "session policy changed"},
		{"target_changed", "none", "target ticket changed before notification"},
		{"daemon_paused", "none", "Orc is paused"},
		{"daemon_aborted", "none", "Orc was aborted"},
		{"queue_rejected", "degraded", "rejected"},
		{"queue_uncertain", "sending", "uncertain"},
		{"ticket_observation_failed", "queued", "Ticket check failed"},
		{"ticket_client_failed", "none", "Ticket check failed"},
		{"runtime_state_write_failed", "queued", "could not save notification status"},
		{"registration_state_malformed", "degraded", "repair steer.json"},
		{"signoff_observation_unavailable", "degraded", "signoff observation unavailable"},
		{"hold_observation_unavailable", "degraded", "hold observation unavailable"},
	} {
		t.Run(test.code, func(t *testing.T) {
			event := daemon.Event{Type: "steer.status", State: test.state, Code: test.code}
			session := daemon.SteerStatus{State: test.state, Code: test.code}
			var table, detail, local bytes.Buffer
			renderConsoleSteerStatus(&table, []daemon.SteerStatus{session}, nil, false)
			renderConsoleSteerStatus(&detail, []daemon.SteerStatus{session}, nil, true)
			if err := renderCurrentSessionState(&local, currentSessionState{Joined: true, Delivery: test.state, StatusCode: test.code, ManagedOwnership: "clear"}); err != nil {
				t.Fatal(err)
			}
			for _, got := range []string{consoleEventMessage(event), unifiedEventMessage(event), table.String(), detail.String(), local.String()} {
				if !strings.Contains(got, test.want) || strings.Contains(got, "delivery issue") {
					t.Errorf("want %q, got %q", test.want, got)
				}
			}
		})
	}
}

func TestSteerUnknownCodeRemainsInspectableWithStateAndFailure(t *testing.T) {
	const unsafeCode = "future_code\x1b\n\r\u009b"
	for _, status := range []string{"none", "queued", "degraded", "failed", "uncertain"} {
		t.Run(status, func(t *testing.T) {
			event := daemon.Event{Type: "steer.status", State: status, Code: unsafeCode}
			session := daemon.SteerStatus{State: status, Code: unsafeCode}
			for _, got := range []string{consoleEventMessage(event), unifiedEventMessage(event), consoleSteerSessionActivity(session)} {
				if !strings.Contains(got, "future_code") || !strings.Contains(got, status) || strings.ContainsAny(got, "\x1b\n\r\u009b") || strings.Contains(got, "delivery issue") {
					t.Errorf("unknown state/code lost or unsafe: %q", got)
				}
			}
		})
	}
	// Structured evidence takes precedence even if the code is unfamiliar or normal.
	for _, code := range []string{unsafeCode, "signoff_resolved"} {
		failure := &daemon.WorkerFailure{Classification: "ticket_transport_failed", Operation: "show signoff target", TransportCategory: "pipe_error", ExitCode: 7}
		event := daemon.Event{Type: "steer.status", State: "none", Code: code, Failure: failure}
		session := daemon.SteerStatus{State: event.State, Code: code, Failure: failure}
		for _, got := range []string{consoleEventMessage(event), unifiedEventMessage(event), consoleSteerSessionActivity(session)} {
			for _, want := range []string{"Ticket process or transport is unavailable", "show signoff target", "pipe_error", "exit_code=7"} {
				if !strings.Contains(got, want) {
					t.Errorf("failure detail %q missing: %q", want, got)
				}
			}
		}
	}
	longCode := strings.Repeat("界", 1000)
	got := consoleEventMessage(daemon.Event{Type: "steer.status", State: "none", Code: longCode})
	if !strings.Contains(got, strings.Repeat("界", 128)+"…") || len([]rune(got)) > 200 {
		t.Fatalf("unknown code not bounded: %q", got)
	}
}
