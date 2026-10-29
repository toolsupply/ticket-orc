package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Runner steps complete synchronously; callbacks control the exact read that
// fails without timers, polling, or background test goroutines.
type diagnosticSteerClient struct {
	active   func(context.Context, string) (ticketclient.ListResult, error)
	ready    func(context.Context) (ticketclient.ListResult, error)
	show     func(context.Context, string) (ticketclient.Ticket, error)
	frontier func(context.Context) (ticketclient.ListResult, error)
}

func (c *diagnosticSteerClient) ActiveClaims(ctx context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	if c.active != nil {
		return c.active(ctx, queue)
	}
	return ticketclient.ListResult{}, nil
}
func (c *diagnosticSteerClient) ReadyFrontier(ctx context.Context, _ string, _ ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	if c.ready != nil {
		return c.ready(ctx)
	}
	return ticketclient.ListResult{}, nil
}
func (c *diagnosticSteerClient) Show(ctx context.Context, id string) (ticketclient.Ticket, error) {
	return c.show(ctx, id)
}
func (c *diagnosticSteerClient) ShowReadiness(context.Context, string) (ticketclient.TicketDetail, error) {
	return ticketclient.TicketDetail{}, errors.New("unexpected ShowReadiness")
}
func (c *diagnosticSteerClient) SignoffFrontier(ctx context.Context, _ ticketclient.QueueFilters, _, _ int) (ticketclient.ListResult, error) {
	return c.frontier(ctx)
}
func (*diagnosticSteerClient) Close() error { return nil }

func joinDiagnosticSteer(t *testing.T, dir, actor, repository string) state.SteerRegistration {
	t.Helper()
	reg, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
		RepositoryID: repository, RepositoryPath: dir, Actor: actor, Role: "worker", Harness: "codex",
		Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestSteerDiagnosticExitHelper(t *testing.T) {
	if os.Getenv("TICKET_ORC_DIAGNOSTIC_EXIT_HELPER") == "1" {
		os.Exit(7)
	}
}

func TestDynamicSteerObservationDiagnostics(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestSteerDiagnosticExitHelper$")
	command.Env = append(os.Environ(), "TICKET_ORC_DIAGNOSTIC_EXIT_HELPER=1")
	processErr := command.Run()
	if processErr == nil {
		t.Fatal("expected process exit")
	}
	for _, test := range []struct {
		name, stage, classification, code, category string
		err                                         error
		exit                                        int
	}{
		{"startup command", "client startup", "ticket_command_failed", "repository_unavailable", "", &ticketclient.CommandError{Code: "repository_unavailable", ExitCode: 2, Message: "SECRET prompt", Stderr: "SECRET credentials"}, 2},
		{"open pipe", "active claims open", "ticket_transport_failed", "", "pipe_error", &ticketclient.TransportError{Category: "pipe_error", ExitCode: -1}, 0},
		{"review protocol", "active claims review", "ticket_protocol_failed", "", "", fmt.Errorf("SECRET JSON: %w", ticketclient.ErrProtocol), 0},
		{"frontier timeout", "ready frontier", "ticket_observation_timeout", "", "", context.DeadlineExceeded, 0},
		{"closed client", "active claims open", "ticket_client_closed", "", "", ticketclient.ErrClosed, 0},
		{"untyped transport", "active claims open", "ticket_transport_failed", "", "", ticketclient.ErrTransport, 0},
		{"process exit", "client startup", "child_exit", "", "", processErr, 7},
		{"process signal", "active claims open", "ticket_transport_failed", "", "process_exit", &ticketclient.TransportError{Category: "process_exit", ExitCode: -1, Signal: "terminated"}, 0},
		{"unknown", "ready frontier", "ticket_observation_failed", "", "", errors.New("SECRET private error"), 0},
		{"unsafe command fields", "active claims open", "ticket_command_failed", "unknown", "", &ticketclient.CommandError{Code: "SECRET\x1b[31m", TicketID: "SECRET invalid id", ExitCode: -1, Message: "SECRET", Stderr: "SECRET"}, -1},
		{"oversized transport fields", "active claims open", "ticket_transport_failed", "", "unknown", &ticketclient.TransportError{Category: strings.Repeat("SECRET", 100), Signal: "SEC\nRET", ExitCode: -1}, 0},
		{"active ticket show", "show active ticket", "ticket_protocol_failed", "", "", ticketclient.ErrProtocol, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			client := &diagnosticSteerClient{}
			client.active = func(_ context.Context, queue string) (ticketclient.ListResult, error) {
				if test.stage == "active claims "+queue {
					return ticketclient.ListResult{}, test.err
				}
				if test.stage == "show active ticket" {
					return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20261007-12345"}}}, nil
				}
				return ticketclient.ListResult{}, nil
			}
			client.ready = func(context.Context) (ticketclient.ListResult, error) { return ticketclient.ListResult{}, test.err }
			client.show = func(context.Context, string) (ticketclient.Ticket, error) { return ticketclient.Ticket{}, test.err }
			policy := steerRolePolicy{TicketQueue: "open"}
			if test.stage == "show active ticket" {
				policy.MaxBounces, policy.ContainmentActor = 2, "containment"
			}
			var events []daemon.Event
			statuses := &dynamicSteerStatus{publish: func(e daemon.Event) { events = append(events, e) }}
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": policy}, nil,
				func(state.SteerRegistration) (steerClient, error) {
					if test.stage == "client startup" {
						return nil, test.err
					}
					return client, nil
				}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
					t.Fatal("failed read dispatched a wake")
					return nil
				}), statuses, nil, nil, nil, nil)
			runner.bootstrap(ctx)
			items := statuses.snapshot()
			if len(items) != 1 || items[0].Failure == nil || len(events) != 1 {
				t.Fatalf("status=%#v events=%#v", items, events)
			}
			f := items[0].Failure
			if f.Operation != test.stage || f.Classification != test.classification || f.TicketCode != test.code || f.TransportCategory != test.category || f.ExitCode != test.exit || f.Phase != "Ticket observation" || f.Origin != "ticket" {
				t.Fatalf("failure=%#v", f)
			}
			if !reflect.DeepEqual(events[0].Failure, f) {
				t.Fatalf("event lost failure: %#v", events[0])
			}
			var output bytes.Buffer
			renderConsoleSteerStatus(&output, items, nil, true)
			output.WriteString(consoleEventMessage(events[0]))
			if !strings.Contains(output.String(), test.stage) {
				t.Fatalf("missing operation: %q", output.String())
			}
			if test.name == "unknown" && !strings.Contains(output.String(), "no safe structured details") {
				t.Fatal(output.String())
			}
			encoded, err := json.Marshal(struct {
				Status []daemon.SteerStatus
				Events []daemon.Event
			}{items, events})
			if err != nil {
				t.Fatal(err)
			}
			if len(encoded) > 4000 || strings.Contains(string(encoded)+output.String(), "SECRET") || strings.Contains(string(encoded), dir) {
				t.Fatalf("unsafe/unbounded projection: %s %s", encoded, output.String())
			}
			stored, err := runner.persistence.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(stored)
			if err != nil || bytes.Contains(data, []byte("classification")) || bytes.Contains(data, []byte("SECRET")) {
				t.Fatalf("diagnostic persisted: %s err=%v", data, err)
			}
		})
	}
}

func TestDynamicSteerDiagnosticTransitionsPreserveQueuedWake(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
	var observationErr error
	activeTicket := ""
	client := &diagnosticSteerClient{active: func(context.Context, string) (ticketclient.ListResult, error) {
		if observationErr != nil {
			return ticketclient.ListResult{}, observationErr
		}
		if activeTicket != "" {
			return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: activeTicket}}}, nil
		}
		return ticketclient.ListResult{}, nil
	}, ready: func(context.Context) (ticketclient.ListResult, error) {
		return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20261007-12345"}}}, nil
	}}
	var events []daemon.Event
	statuses := &dynamicSteerStatus{publish: func(e daemon.Event) { events = append(events, e) }}
	wakes := 0
	dirty := newSteerDirtySet()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": {TicketQueue: "open"}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			wakes++
			return nil
		}), statuses, nil, nil, nil, dirty)
	step := func() { dirty.MarkAll(); runner.repositoryChanged(ctx) }
	runner.bootstrap(ctx)
	if wakes != 1 {
		t.Fatalf("initial wakes=%d", wakes)
	}
	events = nil
	observationErr = &ticketclient.CommandError{Code: "unavailable", ExitCode: -1, Message: "private first cause"}
	step()
	step()
	observationErr = &ticketclient.CommandError{Code: "unavailable", ExitCode: -1, Message: "different private detail"}
	step()
	if len(events) != 1 {
		t.Fatalf("repeated safe cause emitted %d events", len(events))
	}
	observationErr = &ticketclient.TransportError{Category: "pipe_error", ExitCode: -1}
	step()
	if len(events) != 2 {
		t.Fatal("changed cause was hidden")
	}
	observationErr = nil
	step()
	step()
	if len(events) != 3 || events[2].Failure != nil || events[2].Phase != "ticket_observation_recovered" || statuses.snapshot()[0].Failure != nil {
		t.Fatalf("recovery=%#v status=%#v", events, statuses.snapshot())
	}
	if message := consoleEventMessage(events[2]); !strings.Contains(message, "recovered") || strings.Contains(message, "joined") {
		t.Fatal(message)
	}
	observationErr = context.DeadlineExceeded
	step()
	if len(events) != 4 || events[3].Failure.Classification != "ticket_observation_timeout" {
		t.Fatalf("recurring failure hidden: %#v", events)
	}
	if wakes != 1 {
		t.Fatalf("observation recovery replayed queued wake: %d", wakes)
	}
	observationErr, activeTicket = nil, "20261007-12345"
	step()
	if statuses.snapshot()[0].State != "consumed" || events[len(events)-1].Ticket != activeTicket {
		t.Fatalf("claim transition lost: %#v", statuses.snapshot())
	}
	if message := consoleEventMessage(events[len(events)-1]); !strings.Contains(message, "claimed "+activeTicket) {
		t.Fatalf("recovery hid claim context: %s", message)
	}
	activeTicket = ""
	step()
	if wakes != 2 {
		t.Fatalf("consumed claim did not allow one fresh wake: %d", wakes)
	}
}

func TestDynamicSteerDiagnosticSessionsAreIndependent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const otherRepository = "22222222-2222-4222-8222-222222222222"
	for _, reg := range []struct{ actor, repo string }{{"actor", joinTestRepositoryID}, {"second", joinTestRepositoryID}, {"actor", otherRepository}} {
		joinDiagnosticSteer(t, dir, reg.actor, reg.repo)
	}
	var events []daemon.Event
	statuses := &dynamicSteerStatus{publish: func(e daemon.Event) {
		if e.Type == "steer.status" {
			events = append(events, e)
		}
	}}
	failure := true
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": {TicketQueue: "open"}}, nil,
		func(reg state.SteerRegistration) (steerClient, error) {
			return &diagnosticSteerClient{active: func(context.Context, string) (ticketclient.ListResult, error) {
				if reg.Actor == "actor" && reg.RepositoryID == joinTestRepositoryID && failure {
					return ticketclient.ListResult{}, ticketclient.ErrProtocol
				}
				return ticketclient.ListResult{}, nil
			}}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }), statuses, nil, nil, nil, nil)
	runner.bootstrap(ctx)
	if len(statuses.snapshot()) != 3 {
		t.Fatal(statuses.snapshot())
	}
	for _, item := range statuses.snapshot() {
		wantFailure := item.Actor == "actor" && item.RepositoryID == joinTestRepositoryID
		if (item.Failure != nil) != wantFailure {
			t.Fatalf("cross-session failure: %#v", item)
		}
	}
	// Finish the bootstrap-to-idle transition in healthy sessions first.
	runner.dirty.MarkAll()
	runner.repositoryChanged(ctx)
	events = nil
	failure = false
	runner.dirty.MarkRepository(joinTestRepositoryID)
	runner.repositoryChanged(ctx)
	if len(events) != 1 || events[0].Actor != "actor" || events[0].RepositoryID != joinTestRepositoryID || events[0].Failure != nil {
		t.Fatalf("cross-session recovery: %#v", events)
	}
}

func TestDynamicSteerParentCancellationKeepsCompletedStatus(t *testing.T) {
	for _, stage := range []string{"client startup", "active claims", "ready frontier"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			shuttingDown := false
			canceled := func() error { cancel(); return ctx.Err() }
			client := &diagnosticSteerClient{active: func(context.Context, string) (ticketclient.ListResult, error) {
				if shuttingDown && stage == "active claims" {
					return ticketclient.ListResult{}, canceled()
				}
				return ticketclient.ListResult{}, nil
			}, ready: func(context.Context) (ticketclient.ListResult, error) {
				if shuttingDown && stage == "ready frontier" {
					return ticketclient.ListResult{}, canceled()
				}
				return ticketclient.ListResult{}, nil
			}}
			events := 0
			statuses := &dynamicSteerStatus{publish: func(daemon.Event) { events++ }}
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": {TicketQueue: "open"}}, nil,
				func(state.SteerRegistration) (steerClient, error) {
					if shuttingDown && stage == "client startup" {
						return nil, canceled()
					}
					return client, nil
				}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }), statuses, nil, nil, nil, nil)
			runner.bootstrap(ctx)
			previous := statuses.snapshot()
			events = 0
			shuttingDown = true
			if stage == "client startup" {
				closeSteerClients(runner.connections)
			}
			runner.dirty.MarkAll()
			runner.repositoryChanged(ctx)
			if ctx.Err() == nil {
				t.Fatal("test did not reach canceled operation")
			}
			if events != 0 || !reflect.DeepEqual(previous, statuses.snapshot()) || len(runner.retries.repositories) != 0 {
				t.Fatalf("shutdown emitted failure/retry: %d %#v", events, statuses.snapshot())
			}
		})
	}
}

func TestDynamicSteerIdleRecoveryIsNotAJoin(t *testing.T) {
	tracker := newConsoleEventTracker()
	joined := daemon.Event{Type: "steer.status", RepositoryID: joinTestRepositoryID, Actor: "actor", Role: "worker", Harness: "codex", Session: "session", State: "none", Code: "idle"}
	first := tracker.prepare(joined)
	if !strings.Contains(consoleEventMessage(first), "session joined") {
		t.Fatalf("legitimate join lost: %#v", first)
	}
	failed := joined
	failed.Code = "ticket_observation_failed"
	failed.Failure = steerObservationFailure(steerObservationError("ready frontier", context.DeadlineExceeded))
	tracker.prepare(failed)
	if message := consoleEventMessage(tracker.prepare(joined)); strings.Contains(message, "joined") || !strings.Contains(message, "no work available") {
		t.Fatal(message)
	}
}

func TestDynamicSignoffObservationDiagnostics(t *testing.T) {
	for _, stage := range []string{"active claims", "signoff frontier", "initial show", "final show", "final claim", "outstanding target"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			reg := joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			const target = "20261007-12345"
			activeCalls, showCalls := 0, 0
			cause := &ticketclient.CommandError{Code: "unavailable", ExitCode: -1, Stderr: "PRIVATE signoff stderr"}
			client := &diagnosticSteerClient{
				active: func(_ context.Context, queue string) (ticketclient.ListResult, error) {
					if queue == "open" {
						activeCalls++
					}
					if stage == "active claims" || (stage == "final claim" && activeCalls == 2) {
						return ticketclient.ListResult{}, cause
					}
					return ticketclient.ListResult{}, nil
				},
				frontier: func(context.Context) (ticketclient.ListResult, error) {
					if stage == "signoff frontier" {
						return ticketclient.ListResult{}, cause
					}
					return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: target}}}, nil
				},
				show: func(context.Context, string) (ticketclient.Ticket, error) {
					showCalls++
					if stage == "outstanding target" || stage == "initial show" || (stage == "final show" && showCalls == 2) {
						return ticketclient.Ticket{}, cause
					}
					return ticketclient.Ticket{ID: target, State: "signoff"}, nil
				},
			}
			persistence := state.NewSteerRuntimeStore(dir)
			if stage == "outstanding target" {
				if err := persistence.Reconcile(ctx, []state.SteerRegistration{reg}); err != nil {
					t.Fatal(err)
				}
				if ok, err := persistence.ReserveSignoffTarget(ctx, reg, target, "signoff"); err != nil || !ok {
					t.Fatalf("reserve=%v err=%v", ok, err)
				}
			}
			statuses := &dynamicSteerStatus{}
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": {TicketQueue: "signoff"}}, nil,
				func(state.SteerRegistration) (steerClient, error) { return client, nil },
				testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
					t.Fatal("signoff observation failure dispatched wake")
					return nil
				}), statuses, persistence, nil, nil, nil)
			runner.bootstrap(ctx)
			items := statuses.snapshot()
			wantOperation := "show exact target"
			if stage == "active claims" || stage == "final claim" {
				wantOperation = "active claims open"
			}
			if stage == "signoff frontier" {
				wantOperation = stage
			}
			if len(items) != 1 || items[0].Failure == nil || items[0].Failure.Operation != wantOperation || items[0].Failure.TicketCode != "unavailable" {
				t.Fatalf("signoff status=%#v", items)
			}
		})
	}
}

func TestDynamicSteerObservationOperationChangeIsVisible(t *testing.T) {
	status := &dynamicSteerStatus{}
	var events []daemon.Event
	status.setPublisher(func(e daemon.Event) { events = append(events, e) })
	item := daemon.SteerStatus{RepositoryID: joinTestRepositoryID, Actor: "actor", Session: "session", Code: "ticket_observation_failed"}
	for _, operation := range []string{"active claims open", "active claims open", "ready frontier", "ready frontier"} {
		item.Failure = steerObservationFailure(steerObservationError(operation, context.DeadlineExceeded))
		status.replace([]daemon.SteerStatus{item})
	}
	if len(events) != 2 {
		t.Fatalf("operation changes emitted %d events", len(events))
	}
	if len(status.snapshot()) != 1 {
		t.Fatal("failure history retained")
	}
}

func TestSteerObservationTimeoutWithLiveParent(t *testing.T) {
	parent := context.Background()
	expired, cancel := context.WithDeadline(parent, time.Unix(1, 0))
	defer cancel()
	client := &diagnosticSteerClient{active: func(context.Context, string) (ticketclient.ListResult, error) {
		<-expired.Done()
		return ticketclient.ListResult{}, expired.Err()
	}}
	_, err := boundedActorActive(parent, client)
	if parent.Err() != nil || !errors.Is(err, context.DeadlineExceeded) || steerObservationFailure(err).Classification != "ticket_observation_timeout" {
		t.Fatalf("active-service timeout lost: %v", err)
	}
}
