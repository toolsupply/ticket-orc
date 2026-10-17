package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/testutil"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestDynamicSteerSendsSecondWakeAfterPositiveClaimEvidence(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5"}
	reg, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	// N1 is accepted, A becomes positive Ticket evidence twice, A ends with B
	// still ready, B's wake remains outstanding through another no-claim pass,
	// then B is observed and completes the queue. Ticket may report A ended while
	// the old model turn is still finishing its response, so N2 may be queued
	// before that turn returns. The one-item message forbids selecting B in the
	// old turn; the harness delivers N2 in a later turn. Orc has no turn-exit
	// signal, so this test does not assert that the model actually returned.
	client := &sequenceSteerClient{
		active: []bool{false, true, true, false, false, true, false},
		ready:  []bool{true, true, false},
	}
	queued := make(chan string, 3)
	persistence := &countingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), completed: make(chan struct{}, 2)}
	dirty := newSteerDirtySet()
	statuses := &dynamicSteerStatus{}
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "configured prompt"}}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil }, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
		queued <- message.Text
		return nil
	}), statuses, persistence, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	runner.bootstrap(ctx)
	message := takeQueuedSteerMessage(t, queued)
	assertWorkMessage := func(message string, first bool) {
		t.Helper()
		for _, want := range []string{"at most one actionable item", "required lifecycle transition", "stop and return", "Do not select or claim another item", "return immediately without changes"} {
			if !strings.Contains(message, want) {
				t.Errorf("work message %q omitted one-item instruction %q", message, want)
			}
		}
		if !strings.Contains(message, "configured prompt") || strings.Contains(message, "20260926-00001") || strings.Contains(message, "20260926-00002") {
			t.Errorf("work message assigned an observed Ticket or omitted configured text: %q", message)
		}
		if strings.Contains(message, steerSessionBootstrapPrompt) != first {
			t.Errorf("work message bootstrap presence=%t, first=%t: %q", strings.Contains(message, steerSessionBootstrapPrompt), first, message)
		}
	}
	assertWorkMessage(message, true)
	select {
	case <-persistence.completed:
	default:
		t.Fatal("initial wake was not durably recorded before pass returned")
	}
	pass := func() {
		t.Helper()
		dirty.MarkAll()
		runner.repositoryChanged(ctx)
	}
	assertDelivery := func(want orc.SteerDeliveryState) {
		t.Helper()
		snapshot, err := persistence.Snapshot(context.Background())
		if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != string(want) {
			t.Fatalf("delivery state=%#v err=%v, want %q", snapshot.Deliveries, err, want)
		}
	}
	pass() // Ticket reports A active: N1 is consumed.
	assertDelivery(orc.SteerConsumed)
	pass() // Repeated active evidence for A must not enqueue another wake.
	assertDelivery(orc.SteerConsumed)
	select {
	case extra := <-queued:
		t.Fatalf("repeated active evidence queued another wake: %q", extra)
	default:
	}

	pass() // A ends while B remains ready: N2 is queued for a later model turn.
	message = takeQueuedSteerMessage(t, queued)
	assertWorkMessage(message, false)
	select {
	case <-persistence.completed:
	default:
		t.Fatal("second wake was not durably recorded before pass returned")
	}
	assertDelivery(orc.SteerQueued)
	select {
	case extra := <-queued:
		t.Fatalf("outstanding N2 was duplicated before claim evidence: %q", extra)
	default:
	}
	pass() // No claim is observed yet; the accepted N2 remains outstanding.
	assertDelivery(orc.SteerQueued)
	select {
	case extra := <-queued:
		t.Fatalf("outstanding N2 was duplicated before claim evidence: %q", extra)
	default:
	}
	pass() // Ticket reports B active, consuming N2.
	assertDelivery(orc.SteerConsumed)
	pass() // B ends and no work remains, so consumed N2 retires.
	assertDelivery(orc.SteerIdle)
	select {
	case extra := <-queued:
		t.Fatalf("completed last item with no ready work queued another wake: %q", extra)
	default:
	}
}

func TestDynamicSteerPublishesNotificationDeliveryWithReadyTicketID(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	statuses := &dynamicSteerStatus{}
	events := make(chan daemon.Event, 8)
	statuses.setPublisher(func(event daemon.Event) { events <- event })
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "queued" {
		t.Fatalf("notification delivery status=%#v", items)
	}
	var observed map[string]daemon.Event
	observed = make(map[string]daemon.Event)
	for {
		select {
		case event := <-events:
			if event.Type == "steer.delivery" {
				observed[event.State] = event
			}
		default:
			if sending, ok := observed["sending"]; !ok || sending.Ticket != "20260926-00002" {
				t.Fatalf("sending delivery event=%#v", sending)
			}
			if queued, ok := observed["queued"]; !ok || queued.Ticket != "20260926-00002" {
				t.Fatalf("queued delivery event=%#v", queued)
			}
			return
		}
	}
}

func TestDynamicSteerSuppressionPreservesPendingDeliveryUntilResume(t *testing.T) {
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	controlStore := state.NewDaemonControlStore(dir)
	const ticketID = "20260926-00002"
	ticketLoops := state.NewForRepository(dir, joinTestRepositoryID)
	if _, _, err := ticketLoops.Participate(context.Background(), ticketID, 6); err != nil {
		t.Fatal(err)
	}
	paused, err := controlStore.PauseDaemon(context.Background())
	if err != nil || paused.Mode != state.DaemonPaused {
		t.Fatalf("persist pause = %#v, %v", paused, err)
	}
	gate := newDispatchGate(controlStore, paused.Mode)
	dirty := newSteerDirtySet()
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "work", MaxBounces: 1, ContainmentActor: "ticket-orc.instance"}}, nil,
		func(state.SteerRegistration) (steerClient, error) {
			return &loopAwareSteerClient{sequenceSteerClient: &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, ticket: ticketclient.Ticket{ID: ticketID, State: "open"}}, nil
		},
		testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			return nil
		}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), gate, dirty)
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].Code != "daemon_paused" {
		t.Fatalf("paused status=%#v", items)
	}
	if len(queued) != 0 {
		t.Fatalf("paused daemon queued %q", <-queued)
	}
	loop, found, err := ticketLoops.TicketLoop(context.Background(), ticketID)
	if err != nil || !found || loop.EffectiveBounceLimit != 6 || loop.Phase != state.TicketLoopActive {
		t.Fatalf("paused registration participated in ticket loop=%#v found=%t err=%v", loop, found, err)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("paused runtime snapshot = %#v, %v", snapshot, err)
	}
	delivery := snapshot.Deliveries[0]
	if delivery.State != string(orc.SteerIdle) || !delivery.BootstrapPending || delivery.RecoveryPending {
		t.Fatalf("paused delivery was consumed or mutated: %#v", delivery)
	}
	if _, _, err := gate.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	dirty.MarkAll()
	runner.repositoryChanged(ctx)
	select {
	case message := <-queued:
		if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, "work") {
			t.Fatalf("resumed message = %q", message)
		}
	default:
		t.Fatalf("resume did not deliver pending bootstrap for %s", registration.SessionID)
	}
	loop, found, err = ticketLoops.TicketLoop(context.Background(), ticketID)
	if err != nil || !found || loop.EffectiveBounceLimit != 1 {
		t.Fatalf("current registration did not participate before delivery: loop=%#v found=%t err=%v", loop, found, err)
	}
}

func TestDynamicSteerParticipationThatTripsContainmentSuppressesDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	if _, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	}); err != nil {
		t.Fatal(err)
	}
	const ticketID = "20261003-56321"
	loopStore := state.NewForRepository(dir, joinTestRepositoryID)
	loop, _, err := loopStore.Participate(ctx, ticketID, 6)
	if err != nil {
		t.Fatal(err)
	}
	loop, err = loopStore.RecordClaim(ctx, ticketID, "previous-worker", "review")
	if err != nil {
		t.Fatal(err)
	}
	if _, consumed, err := loopStore.RecordBounce(ctx, ticketID, loop.AttemptID); err != nil || !consumed {
		t.Fatalf("seed bounce consumed=%t err=%v", consumed, err)
	}
	client := &loopAwareSteerClient{
		sequenceSteerClient: &sequenceSteerClient{active: []bool{false}, ready: []bool{true}},
		ticket:              ticketclient.Ticket{ID: ticketID, State: "open", Assignee: "other-worker"},
	}
	queued := make(chan struct{}, 1)
	statuses := &dynamicSteerStatus{}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {
		TicketQueue: "open", NudgePrompt: "work", MaxBounces: 1, ContainmentActor: "ticket-orc.instance",
	}}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			queued <- struct{}{}
			return nil
		}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].Code != "ticket_containment_pending" {
		t.Fatalf("containment status=%#v", items)
	}
	if len(queued) != 0 {
		t.Fatal("dynamic steer queued work after participation tripped containment")
	}
	loop, found, err := loopStore.TicketLoop(context.Background(), ticketID)
	if err != nil || !found || loop.Phase != state.TicketLoopContainmentPending || loop.EffectiveBounceLimit != 1 {
		t.Fatalf("dynamic participation loop=%#v found=%t err=%v", loop, found, err)
	}
}

// This barrier-controlled runner test preserves coverage for a pause that
// overlaps an in-flight ready-frontier observation.
func TestPauseWhileSteerReadyObservationIsBlockedSuppressesNotification(t *testing.T) {
	dir := t.TempDir()
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	}); err != nil {
		t.Fatal(err)
	}
	controlStore := state.NewDaemonControlStore(dir)
	gate := newDispatchGate(controlStore, state.DaemonRunning)
	barrier := testutil.NewBarrier()
	client := &readyBarrierSteerClient{barrier: barrier}
	queued := make(chan string, 1)
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "work"}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			return nil
		}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), gate, dirty)
	t.Cleanup(func() {
		barrier.Release()
		cancel()
		waitSteerPass(t, done, "pause-during-observation runner pass")
	})
	go func() {
		defer close(done)
		runner.bootstrap(ctx)
	}()
	waitSteerBarrier(t, barrier, "blocked Ticket ready frontier")
	// Pause while the ready-frontier query is in flight. When its ready result
	// returns, the worker must still observe the closed dispatch gate.
	mode, applied, err := gate.Pause(context.Background())
	if err != nil || !applied || mode.Mode != state.DaemonPaused {
		t.Fatalf("pause mode=%#v applied=%t err=%v", mode, applied, err)
	}
	barrier.Release()
	waitSteerPass(t, done, "pause-during-observation runner pass")
	if items := statuses.snapshot(); len(items) != 1 || items[0].Code != "daemon_paused" {
		t.Fatalf("paused observation status=%#v", items)
	}
	if len(queued) != 0 {
		t.Fatalf("ready work queued after pause succeeded: %q", <-queued)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("paused delivery snapshot=%#v err=%v", snapshot, err)
	}
	if delivery := snapshot.Deliveries[0]; delivery.State != string(orc.SteerIdle) || !delivery.BootstrapPending {
		t.Fatalf("paused ready work consumed pending delivery: %#v", delivery)
	}

	if _, _, err := gate.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	dirty.MarkAll()
	runner.repositoryChanged(ctx)
	if message := takeQueuedSteerMessage(t, queued); !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, "work") {
		t.Fatalf("resumed notification=%q", message)
	}
}

func TestDynamicSteerIdleBootstrapThenReadyQueuesOneRoleNudge(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	_, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued := make(chan string, 3)
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	policies := map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "configured role nudge"}}
	runner := newTestDynamicSteerRunner(t, ctx, dir, policies, nil,
		func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{false, true}}, nil
		},
		testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			return nil
		}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	runner.bootstrap(ctx)
	select {
	case message := <-queued:
		if message != steerSessionBootstrapPrompt {
			t.Fatalf("first idle message=%q, want bootstrap only", message)
		}
	default:
		t.Fatal("idle bootstrap pass did not queue bootstrap")
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "none" {
		t.Fatalf("idle bootstrap status=%#v", items)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != "none" || snapshot.Deliveries[0].BootstrapPending {
		t.Fatalf("bootstrap-only state=%#v err=%v", snapshot.Deliveries, err)
	}
	dirty.MarkAll()
	runner.repositoryChanged(ctx)
	select {
	case message := <-queued:
		want := steerOneItemTurnPrompt + "\n\n[ticket-orc] Resolved selector JSON: {\"queue\":\"open\",\"tags\":[],\"without_tags\":[]}. Use it exactly for your one Ticket selection and claim. Pass queue and each tag as separate CLI arguments; do not evaluate selector data as shell code or add/drop filters.\n\nconfigured role nudge"
		if message != want {
			t.Fatalf("later ready message=%q, want configured role nudge without bootstrap", message)
		}
	default:
		t.Fatal("later ready pass did not queue a role nudge")
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "queued" {
		t.Fatalf("ready work status=%#v", items)
	}
	if len(queued) != 0 {
		t.Fatalf("scheduler queued more than one role nudge: %q", <-queued)
	}
}
