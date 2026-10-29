package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestHoldVisibilityUsesCommittedAndEffectivePolicies(t *testing.T) {
	worker := supervisor.RunWorker{Name: "quality", Config: supervisor.RoleConfig{RoleName: "reviewer", TicketQueue: "review", HoldQueue: true}}
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	desired := worker
	desired.Config.HoldQueue = false
	runtime.SetConfiguredWorkers([]supervisor.RunWorker{desired})
	runtime.Observe(orc.Event{Worker: "quality", Type: "ticket.hold", Ticket: steerHoldID, State: "human_handoff"})
	runtime.Observe(orc.Event{Worker: "quality", Type: "ticket.claim", Ticket: steerPrimaryID})
	policies := newSteerPolicyStore(map[string]steerRolePolicy{"reviewer": {TicketQueue: "review", HoldQueue: true}})
	cached := []daemon.SteerStatus{{Role: "reviewer", EffectiveTicketQueue: "review", EffectiveHoldQueue: true, TargetQueue: "hold", Ticket: steerHoldID, State: "sending", Code: "queue_uncertain"}}
	policies.Replace(map[string]steerRolePolicy{"reviewer": {TicketQueue: "review"}, "architect": {TicketQueue: "signoff", HoldQueue: true}})
	status := runtimeDaemonStatusWithSteerPolicies(runtime, cached, policies)
	if !reflect.DeepEqual(status.HoldQueueRoles, []string{"architect"}) || !status.Workers[0].EffectiveHoldQueue || status.Steer[0].EffectiveHoldQueue || !cached[0].EffectiveHoldQueue {
		t.Fatalf("effective hold policy: %+v", status)
	}
	if status.Workers[0].HoldTicket != steerHoldID || status.Workers[0].HoldOutcome != "human_handoff" || status.Workers[0].TicketActivityTicket != steerPrimaryID {
		t.Fatalf("hold outcome lost during review: %+v", status.Workers[0])
	}
	if status.Steer[0].TargetQueue != "hold" || status.Steer[0].EffectiveTicketQueue != "review" {
		t.Fatalf("target origin lost: %+v", status.Steer)
	}
	var out bytes.Buffer
	renderConsoleDetailStatus(&out, status)
	for _, want := range []string{"Hold roles: architect", "unassigned holds: true", "Last hold assessment: " + steerHoldID, "human_handoff", "delivery uncertain", "hold " + steerHoldID} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q: %s", want, out.String())
		}
	}
	policies.Replace(nil)
	status = runtimeDaemonStatusWithSteerPolicies(nil, cached, policies)
	if len(status.HoldQueueRoles) != 0 || status.Steer[0].EffectiveHoldQueue {
		t.Fatalf("removed hold policy retained: %+v", status)
	}
	out.Reset()
	renderConsoleStatus(&out, status)
	if !strings.Contains(out.String(), "Hold roles: none configured") {
		t.Fatal(out.String())
	}
}

func TestHoldQueuePresentationKeepsPrimaryForecastSeparate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	reg := joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
	loaded := LoadedFileConfig{Instance: InstanceContext{InstanceDir: dir, LocalDir: dir}}
	status := daemon.Status{HoldQueueRoles: []string{"worker"}, Steer: []daemon.SteerStatus{{RepositoryID: reg.RepositoryID, Actor: reg.Actor, Harness: reg.Harness, Session: reg.SessionID, Role: reg.Role, EffectiveTicketQueue: "review", EffectiveHoldQueue: true, TargetQueue: "hold", Ticket: steerHoldID, State: "queued", Code: "awaiting_hold_resolution"}}}
	open := func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: map[string]map[string]ticketclient.ListResult{}, ready: map[string]ticketclient.ListResult{"review": {Items: []ticketclient.Ticket{{ID: steerPrimaryID, State: "review"}}}}}, nil
	}
	owners, err := queueForecast(ctx, loaded, status, open)
	if err != nil || len(owners) != 1 || owners[0].Queue != "review" || !owners[0].HoldQueue || owners[0].HoldTicket != steerHoldID || owners[0].Next != nil {
		t.Fatalf("hold forecast=%+v err=%v", owners, err)
	}
	var out bytes.Buffer
	if err := writeQueueForecast(ctx, &out, false, loaded, status, open); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Hold roles: worker", "primary review", steerHoldID, "awaiting_hold_resolution"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q: %s", want, out.String())
		}
	}
	if strings.Contains(out.String(), steerPrimaryID) || strings.Contains(out.String(), "signoff eligible") {
		t.Fatal(out.String())
	}
}

func TestLocalHoldViewsPreserveExactTargetOrigin(t *testing.T) {
	dir := t.TempDir()
	writeConfigFixture(t, filepath.Join(dir, "config.json"), `{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"review","hold_queue":true}}}`)
	runner, _ := fakeTicketRunner("")
	lookup := localIdentityEnv(dir)
	var joined bytes.Buffer
	if err := joinWithRunner(context.Background(), "reviewer", &joined, lookup, runner); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadSteerConfig(lookup)
	if err != nil {
		t.Fatal(err)
	}
	reg, found, err := state.NewRegistrationStore(loaded.Instance.LocalDir).Find(context.Background(), localRepoID, "reviewer")
	if err != nil || !found {
		t.Fatalf("registration=%+v %v", reg, err)
	}
	runtime := state.NewSteerRuntimeStore(loaded.Instance.LocalDir)
	if err := runtime.Reconcile(context.Background(), []state.SteerRegistration{reg}); err != nil {
		t.Fatal(err)
	}
	if ok, err := runtime.ReserveSignoffTarget(context.Background(), reg, steerHoldID, "hold"); err != nil || !ok {
		t.Fatalf("reserve=%t %v", ok, err)
	}
	var out bytes.Buffer
	if err := nextWithRunner(context.Background(), true, &out, lookup, runner, localQueueFactory(&localQueueStub{})); err != nil {
		t.Fatal(err)
	}
	var next map[string]any
	if err := json.Unmarshal(out.Bytes(), &next); err != nil {
		t.Fatal(err)
	}
	if next["queue"] != "review" || next["hold_target"] != steerHoldID || next["hold_outcome"] != "queue_uncertain" || next["signoff_target"] != nil {
		t.Fatalf("next=%+v", next)
	}
	result, err := currentSessionStateWithConfig(context.Background(), loaded.Instance.LocalDir, loaded, lookup, runner, localQueueFactory(&localQueueStub{}), nil)
	if err != nil || result.RoleQueue != "review" || !result.HoldQueue || result.HoldTarget != steerHoldID || result.SignoffTarget != "" {
		t.Fatalf("session=%+v %v", result, err)
	}
}

func TestHelpExplainsSecondaryHoldInput(t *testing.T) {
	for _, want := range []string{"roles.<name>.hold_queue=true", "secondary input", "Holds assigned", "human action", "does not change ticket_queue", "does not transfer holds"} {
		if !strings.Contains(configCheckHelp, want) {
			t.Errorf("config help missing %q", want)
		}
	}
	if !strings.Contains(consoleEventMessage(daemon.Event{Type: "ticket.hold", State: "human_handoff", Ticket: steerHoldID}), "human_handoff") {
		t.Fatal("hold event hidden")
	}
}

func TestManagedHoldConsumerBlocksSameRepositorySteerActor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	reg := joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
	worker := supervisor.RunWorker{Name: "managed-quality", Config: supervisor.RoleConfig{Actor: reg.Actor, TicketQueue: "review", HoldQueue: true, RepositoryIdentity: reg.RepositoryID}}
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	runtime.Set(supervisor.WorkerTransition{Worker: worker.Name, State: supervisor.WorkerRunning})
	calls := 0
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("review")}, runtime,
		func(state.SteerRegistration) (steerClient, error) { calls++; return newHoldSteerClient(), nil },
		testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			t.Fatal("hold routed through managed ownership conflict")
			return nil
		}), &dynamicSteerStatus{}, nil, nil, nil, nil)
	runner.bootstrap(ctx)
	status := runner.statuses.snapshot()[0]
	if calls != 0 || status.State != "conflict" || status.ManagedOwner != worker.Name || !status.EffectiveHoldQueue {
		t.Fatalf("hold ownership conflict: %+v calls=%d", status, calls)
	}
}
