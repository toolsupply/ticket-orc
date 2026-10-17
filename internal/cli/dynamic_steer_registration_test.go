package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/testutil"
)

func TestDynamicSteerDirtyWakeRefreshesRegistrationIncarnation(t *testing.T) {
	dir := t.TempDir()
	registrationStore := state.NewRegistrationStore(dir)
	registration := state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	}
	registration, _, _, err := registrationStore.Join(context.Background(), registration)
	if err != nil {
		t.Fatal(err)
	}
	runtimeStore := state.NewSteerRuntimeStore(dir)
	if err := runtimeStore.Reconcile(context.Background(), []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtimeStore.CompleteDelivery(context.Background(), registration, string(orc.SteerQueued), true, true); err != nil || !updated {
		t.Fatalf("seed prior queued delivery updated=%t err=%v", updated, err)
	}
	oldClient := &countingSteerClient{}
	currentClient := &countingSteerClient{}
	clients := map[string]steerClient{
		registration.SessionID:                 oldClient,
		"01a0dcb8-aa3a-78d3-87ba-b5972a10e2a5": currentClient,
	}
	queued := make(chan string, 1)
	dirty := newSteerDirtySet()
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(current state.SteerRegistration) (steerClient, error) { return clients[current.SessionID], nil },
		testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, _ steertransport.Message) error {
			queued <- registration.SessionID
			return nil
		}), statuses, runtimeStore, newRegistrationObserver(registrationStore), nil, dirty)
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != string(orc.SteerQueued) || oldClient.activeCalls.Load() != 2 {
		t.Fatalf("initial reconciliation status=%#v old client calls=%d", items, oldClient.activeCalls.Load())
	}

	newRegistration := registration
	newRegistration.SessionID = "01a0dcb8-aa3a-78d3-87ba-b5972a10e2a5"
	newRegistration, previous, changed, err := registrationStore.Join(context.Background(), newRegistration)
	if err != nil || !changed || previous == nil || newRegistration.RegistrationID == registration.RegistrationID {
		t.Fatalf("replace registration=%#v previous=%#v changed=%t err=%v", newRegistration, previous, changed, err)
	}
	// No registration tick is sent: the repository event must refresh the
	// registration snapshot before reconciling or delivering.
	dirty.MarkRepository(registration.RepositoryID)
	runner.repositoryChanged(ctx)
	select {
	case thread := <-queued:
		if thread != newRegistration.SessionID {
			t.Fatalf("notification thread=%q, want current registration %q", thread, newRegistration.SessionID)
		}
	default:
		t.Fatal("reconcile trigger did not use the replacement registration")
	}
	if currentClient.activeCalls.Load() != 2 || currentClient.readyCalls.Load() != 1 {
		t.Fatalf("replacement client calls active=%d ready=%d", currentClient.activeCalls.Load(), currentClient.readyCalls.Load())
	}
	if got := oldClient.activeCalls.Load(); got != 2 {
		t.Fatalf("stale registration received %d additional Ticket observations", got-2)
	}
	deliveries, err := runtimeStore.Snapshot(context.Background())
	if err != nil || len(deliveries.Deliveries) != 1 || deliveries.Deliveries[0].RegistrationID != newRegistration.RegistrationID || deliveries.Deliveries[0].SessionID != newRegistration.SessionID {
		t.Fatalf("stale delivery was not retired: snapshot=%#v err=%v", deliveries, err)
	}
}

func TestDynamicSteerRegistrationAndClientReplacementDuringObservation(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	registrations := state.NewRegistrationStore(dir)
	registration, _, _, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	barrier := testutil.NewBarrier()
	oldClient := &readyBarrierSteerClient{barrier: barrier}
	newClient := &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}
	var newClientRequests atomic.Int32
	queued := make(chan state.SteerRegistration, 1)
	statuses := &dynamicSteerStatus{}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(current state.SteerRegistration) (steerClient, error) {
			if current.SessionID == registration.SessionID {
				return oldClient, nil
			}
			newClientRequests.Add(1)
			return newClient, nil
		}, testSteerRouter(t, func(_ context.Context, _ string, current state.SteerRegistration, _ steertransport.Message) error {
			queued <- current
			return nil
		}), statuses, nil, newRegistrationObserver(registrations), nil, nil)
	done := make(chan struct{})
	t.Cleanup(func() {
		barrier.Release()
		cancel()
		waitSteerPass(t, done, "registration-replacement runner pass")
	})
	go func() {
		defer close(done)
		runner.bootstrap(ctx)
	}()
	waitSteerBarrier(t, barrier, "old registration's ready observation")

	replacement := registration
	replacement.SessionID = "01a0dcb8-aa3a-78d3-87ba-b5972a10e2a5"
	current, _, changed, err := registrations.Join(ctx, replacement)
	if err != nil || !changed || current.RegistrationID == registration.RegistrationID {
		t.Fatalf("replace registration=%#v changed=%t err=%v", current, changed, err)
	}
	runner.dirty.MarkRepository(current.RepositoryID)
	barrier.Release()
	waitSteerPass(t, done, "old registration observation")
	select {
	case stale := <-queued:
		t.Fatalf("replaced registration received stale notification: %#v", stale)
	default:
	}

	runner.repositoryChanged(ctx)
	select {
	case delivered := <-queued:
		if delivered.RegistrationID != current.RegistrationID || delivered.SessionID != current.SessionID {
			t.Fatalf("notification used stale registration %#v, want %#v", delivered, current)
		}
	default:
		t.Fatal("replacement client did not dispatch current registration")
	}
	if newClientRequests.Load() == 0 {
		t.Fatal("replacement registration reused the old session's Ticket client")
	}
	items := statuses.snapshot()
	if len(items) != 1 || items[0].Session != current.SessionID || items[0].State != string(orc.SteerQueued) {
		t.Fatalf("replacement status=%#v", items)
	}
}

func TestDynamicSteerCancellationUnblocksBlockedObservation(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	if _, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	}); err != nil {
		cancel()
		t.Fatal(err)
	}
	barrier := testutil.NewBarrier()
	client := &readyBarrierSteerClient{barrier: barrier}
	queued := make(chan string, 1)
	statuses := &dynamicSteerStatus{}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			return nil
		}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
	done := make(chan struct{})
	t.Cleanup(func() {
		barrier.Release()
		cancel()
		waitSteerPass(t, done, "canceled observation runner pass")
	})
	go func() {
		defer close(done)
		runner.bootstrap(ctx)
	}()
	waitSteerBarrier(t, barrier, "canceled Ticket ready observation")
	cancel()
	waitSteerPass(t, done, "canceled observation runner pass")
	select {
	case message := <-queued:
		t.Fatalf("canceled observation queued a message: %q", message)
	default:
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 || !snapshot.Deliveries[0].BootstrapPending || snapshot.Deliveries[0].State == string(orc.SteerQueued) {
		t.Fatalf("canceled observation consumed delivery state: %#v err=%v", snapshot, err)
	}
}

func TestChangedSteerRepositoriesDetectsJoinRemovalAndReplacement(t *testing.T) {
	previous := []state.SteerRegistration{
		{RepositoryID: "11111111-1111-4111-8111-111111111111", Actor: "replaced", Role: "coder", RepositoryPath: "/repo/old", SessionID: "old-thread"},
		{RepositoryID: "22222222-2222-4222-8222-222222222222", Actor: "removed", Role: "coder"},
	}
	current := []state.SteerRegistration{
		{RepositoryID: "11111111-1111-4111-8111-111111111111", Actor: "replaced", Role: "reviewer", RepositoryPath: "/repo/new", SessionID: "new-thread"},
		{RepositoryID: "33333333-3333-4333-8333-333333333333", Actor: "joined", Role: "coder"},
	}
	changed := changedSteerRepositories(previous, current)
	for _, repositoryID := range []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
	} {
		if _, found := changed[repositoryID]; !found {
			t.Errorf("registration change for repository %s was not marked dirty: %#v", repositoryID, changed)
		}
	}
}

func TestDynamicSteerBootstrapsFirstRegistrationOnce(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "without work"
		if ready {
			name = "with ready work"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			reg, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
				Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
			})
			if err != nil {
				t.Fatal(err)
			}
			queued := make(chan string, 4)
			statuses := &dynamicSteerStatus{}
			ctx := context.Background()
			policies := map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "role work nudge"}}
			newRunner := func(statuses *dynamicSteerStatus) *dynamicSteerRunner {
				return newTestDynamicSteerRunner(t, ctx, dir, policies, nil,
					func(state.SteerRegistration) (steerClient, error) {
						return &sequenceSteerClient{active: []bool{false}, ready: []bool{ready}}, nil
					},
					testSteerRouter(t, func(_ context.Context, _ string, registration state.SteerRegistration, message steertransport.Message) error {
						if registration.SessionID != reg.SessionID {
							t.Errorf("bootstrap sent to session %q, want %q", registration.SessionID, reg.SessionID)
						}
						queued <- message.Text
						return nil
					}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
			}
			runner := newRunner(statuses)
			runner.bootstrap(ctx)
			select {
			case message := <-queued:
				if !strings.Contains(message, steerSessionBootstrapPrompt) || strings.Contains(message, steerClaimRecoveryPrompt) || strings.Contains(message, "role work nudge") != ready {
					t.Fatalf("first-registration message=%q ready=%t", message, ready)
				}
				if !ready && message != steerSessionBootstrapPrompt {
					t.Fatalf("idle new session received content beyond bootstrap: %q", message)
				}
				if ready && (strings.Index(message, steerSessionBootstrapPrompt) > strings.Index(message, "role work nudge")) {
					t.Fatalf("ready work preceded bootstrap: %q", message)
				}
			default:
				t.Fatal("first registration did not receive its bootstrap")
			}
			wantState := "none"
			if ready {
				wantState = "queued"
			}
			if items := statuses.snapshot(); len(items) != 1 || items[0].State != wantState {
				t.Fatalf("bootstrap status=%#v, want %q", items, wantState)
			}
			snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].BootstrapPending || snapshot.Deliveries[0].State != wantState {
				t.Fatalf("bootstrap delivery snapshot=%#v err=%v", snapshot, err)
			}
			if len(queued) != 0 {
				t.Fatalf("bootstrap pass repeated notification: %q", <-queued)
			}

			restartedQueue := make(chan string, 1)
			restartStatuses := &dynamicSteerStatus{}
			restarted := newTestDynamicSteerRunner(t, ctx, dir, policies, nil,
				func(state.SteerRegistration) (steerClient, error) {
					return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
				},
				testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
					restartedQueue <- message.Text
					return nil
				}), restartStatuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
			restarted.bootstrap(ctx)
			if items := restartStatuses.snapshot(); len(items) != 1 || items[0].State != wantState {
				t.Fatalf("restarted bootstrap status=%#v, want %q", items, wantState)
			}
			if len(restartedQueue) != 0 {
				t.Fatalf("runner restart repeated bootstrap: %q", <-restartedQueue)
			}
		})
	}
}

func TestDynamicSteerRecreatedRegistrationDoesNotInheritQueuedDelivery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("persist old queued delivery updated=%t err=%v", updated, err)
	}
	if err := os.Remove(filepath.Join(dir, "steer.json")); err != nil {
		t.Fatal(err)
	}
	current, previous, changed, err := registrations.Join(ctx, registration)
	if err != nil || previous != nil || !changed || current.RegistrationID == old.RegistrationID {
		t.Fatalf("recreated registration=%#v previous=%#v changed=%t err=%v", current, previous, changed, err)
	}

	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "fresh wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
		return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
	}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
		queued <- message.Text
		return nil
	}), statuses, persistence, newRegistrationObserver(registrations), nil, nil)
	runner.bootstrap(ctx)
	select {
	case message := <-queued:
		want := steerOneItemTurnPrompt + "\n\n[ticket-orc] Resolved selector JSON: {\"queue\":\"open\",\"tags\":[],\"without_tags\":[]}. Use it exactly for your one Ticket selection and claim. Pass queue and each tag as separate CLI arguments; do not evaluate selector data as shell code or add/drop filters.\n\nfresh wake"
		if message != want {
			t.Fatalf("queued message=%q", message)
		}
	default:
		t.Fatal("new registration inherited the old queued delivery instead of scheduling a fresh wake")
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "queued" {
		t.Fatalf("recreated registration status=%#v", items)
	}
	snapshot, err := persistence.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != current.RegistrationID || snapshot.Deliveries[0].State != "queued" {
		t.Fatalf("recreated delivery=%#v err=%v", snapshot, err)
	}
}

func TestDynamicSteerJoinRefreshesAndBootstrapsSession(t *testing.T) {
	tests := []struct {
		name           string
		threadChanged  bool
		roleChanged    bool
		active         bool
		activeByQueue  map[string][]bool
		ready          bool
		wantWake       bool
		wantBootstrap  bool
		wantRecovery   bool
		wantRolePrompt string
		wantStatusCode string
	}{
		{name: "same thread active claim", active: true, wantStatusCode: "busy"},
		{name: "same thread exact rejoin ready", ready: true, wantWake: true, wantRolePrompt: "coder normal work"},
		{name: "same thread ready work", ready: true, wantWake: true, wantRolePrompt: "coder normal work"},
		{name: "same thread role change active", roleChanged: true, active: true, wantStatusCode: "busy"},
		{name: "same thread role change preserves claim in prior queue", roleChanged: true, activeByQueue: map[string][]bool{"open": {false}, "review": {true}}, wantStatusCode: "busy"},
		{name: "same thread role change ready", roleChanged: true, ready: true, wantWake: true, wantRolePrompt: "architect normal work"},
		{name: "new thread active claim", threadChanged: true, active: true, wantWake: true, wantBootstrap: true, wantRecovery: true},
		{name: "new thread ready work", threadChanged: true, ready: true, wantWake: true, wantBootstrap: true, wantRolePrompt: "coder normal work"},
		{name: "new thread no work", threadChanged: true, wantWake: true, wantBootstrap: true},
		{name: "new thread role change active", threadChanged: true, roleChanged: true, active: true, wantWake: true, wantBootstrap: true, wantRecovery: true},
		{name: "new thread role change recovers claim in prior queue", threadChanged: true, roleChanged: true, activeByQueue: map[string][]bool{"open": {false}, "review": {true}}, wantWake: true, wantBootstrap: true, wantRecovery: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			registrations := state.NewRegistrationStore(dir)
			old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
				Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
			})
			if err != nil {
				t.Fatal(err)
			}
			persistence := state.NewSteerRuntimeStore(dir)
			if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
				t.Fatal(err)
			}
			if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
				t.Fatalf("seed old queued delivery updated=%t err=%v", updated, err)
			}

			queued := make(chan string, 2)
			statuses := &dynamicSteerStatus{}
			deliveryEvents := make(chan daemon.Event, 8)
			statuses.setPublisher(func(event daemon.Event) {
				if event.Type == "steer.delivery" {
					deliveryEvents <- event
				}
			})
			dirty := newSteerDirtySet()
			policies := map[string]steerRolePolicy{
				"coder":     {TicketQueue: "open", NudgePrompt: "coder normal work"},
				"architect": {TicketQueue: "open", NudgePrompt: "architect normal work"},
			}
			runner := newTestDynamicSteerRunner(t, ctx, dir, policies, nil, func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{test.active}, activeByQueue: test.activeByQueue, ready: []bool{test.ready}}, nil
			}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
				queued <- message.Text
				return nil
			}), statuses, persistence, newRegistrationObserver(registrations), nil, dirty)
			runner.bootstrap(ctx)
			initialState := "queued"
			initiallyActive := test.active
			for _, claims := range test.activeByQueue {
				initiallyActive = initiallyActive || len(claims) > 0 && claims[0]
			}
			if initiallyActive {
				initialState = "consumed"
			}
			if items := statuses.snapshot(); len(items) != 1 || items[0].Session != old.SessionID || items[0].State != initialState {
				t.Fatalf("initial status=%#v, want session=%s state=%s", items, old.SessionID, initialState)
			}

			next := old
			if test.threadChanged {
				next.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
			}
			if test.roleChanged {
				next.Role = "architect"
			}
			current, _, changed, err := registrations.Join(ctx, next)
			if err != nil || changed != (test.threadChanged || test.roleChanged) {
				t.Fatalf("join changed=%t err=%v registration=%#v", changed, err, current)
			}
			// A successful join refreshes the observed registration incarnation
			// even when its routing fields are unchanged.
			dirty.MarkAll()
			runner.repositoryChanged(ctx)
			if test.wantWake {
				select {
				case message := <-queued:
					if (test.wantRolePrompt != "" && !strings.Contains(message, test.wantRolePrompt)) || strings.Contains(message, steerClaimRecoveryPrompt) != test.wantRecovery || strings.Contains(message, steerSessionBootstrapPrompt) != test.wantBootstrap {
						t.Fatalf("wake message=%q want role prompt %q recovery=%t bootstrap=%t", message, test.wantRolePrompt, test.wantRecovery, test.wantBootstrap)
					}
				default:
					t.Fatal("reconciliation did not produce the expected wake")
				}
				wantDeliveryState := "queued"
				if test.wantBootstrap && test.wantRolePrompt == "" && !test.wantRecovery {
					wantDeliveryState = "none"
				}
				if items := statuses.snapshot(); len(items) != 1 || items[0].Session != current.SessionID || items[0].State != wantDeliveryState {
					t.Fatalf("delivery status=%#v, want session=%s state=%s", items, current.SessionID, wantDeliveryState)
				}
				if test.wantRecovery {
					for _, wantState := range []string{"sending", "queued"} {
						select {
						case event := <-deliveryEvents:
							if event.State != wantState || event.Ticket != "20260926-00001" {
								t.Fatalf("recovery delivery event=%#v, want state=%q and active Ticket ID", event, wantState)
							}
						default:
							t.Fatalf("missing recovery delivery event state=%q", wantState)
						}
					}
				}
			} else {
				items := statuses.snapshot()
				if len(items) != 1 || items[0].Session != current.SessionID || items[0].State != "none" || items[0].Code != test.wantStatusCode {
					t.Fatalf("inactive status=%#v, want session=%s code=%s", items, current.SessionID, test.wantStatusCode)
				}
				select {
				case message := <-queued:
					t.Fatalf("unexpected wake: %q", message)
				default:
				}
			}
			if len(queued) != 0 {
				t.Fatalf("single reconciliation queued more than one wake: %q", <-queued)
			}
			snapshot, err := persistence.Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != current.RegistrationID {
				t.Fatalf("join delivery snapshot=%#v err=%v", snapshot, err)
			}
			wantRecoveryPending := false
			if snapshot.Deliveries[0].BootstrapPending || snapshot.Deliveries[0].RecoveryPending != wantRecoveryPending {
				t.Fatalf("accepted join delivery retained incorrect intents: %#v, want recovery_pending=%t", snapshot.Deliveries[0], wantRecoveryPending)
			}
		})
	}
}

func TestDynamicSteerJoinRetriesRejectedRecoveryHandoff(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("seed old delivery updated=%t err=%v", updated, err)
	}
	newRoute := old
	newRoute.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, newRoute)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 3)
	statuses := &dynamicSteerStatus{}
	router := testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
		queued <- message.Text
		if attempts.Add(1) == 1 {
			return steertransport.Rejected(errors.New("queue rejected"))
		}
		return nil
	})
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "normal work"}}, nil, func(state.SteerRegistration) (steerClient, error) {
		return &sequenceSteerClient{active: []bool{true}, ready: []bool{false}}, nil
	}, router, statuses, persistence, newRegistrationObserver(registrations), nil, newSteerDirtySet())
	runner.bootstrap(ctx)
	message := takeQueuedSteerMessage(t, queued)
	if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, steerClaimRecoveryPrompt) || strings.Contains(message, "normal work") {
		t.Fatalf("initial handoff omitted generic recovery or included unverified role work: %q", message)
	}
	if !(strings.Index(message, steerSessionBootstrapPrompt) < strings.Index(message, steerClaimRecoveryPrompt)) {
		t.Fatalf("initial handoff components were out of order: %q", message)
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "degraded" || items[0].Code != "queue_rejected" {
		t.Fatalf("rejected handoff status=%#v", items)
	}
	afterReject, err := persistence.Snapshot(ctx)
	if err != nil || len(afterReject.Deliveries) != 1 || !afterReject.Deliveries[0].RecoveryPending || !afterReject.Deliveries[0].BootstrapPending {
		t.Fatalf("rejected handoff lost bootstrap/recovery markers: %#v err=%v", afterReject, err)
	}

	refreshed, previous, changed, err := registrations.Join(ctx, current)
	if err != nil || previous == nil || changed || refreshed.IncarnationID == current.IncarnationID {
		t.Fatalf("explicit recovery join=%#v previous=%#v changed=%t err=%v", refreshed, previous, changed, err)
	}
	runner.repositoryChanged(ctx)
	message = takeQueuedSteerMessage(t, queued)
	if !strings.Contains(message, steerClaimRecoveryPrompt) {
		t.Fatalf("recovery retry message=%q", message)
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].Session != current.SessionID || items[0].State != "queued" {
		t.Fatalf("accepted retry status=%#v", items)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("recovery handoff attempts=%d, want one failed attempt and one explicit-join retry", got)
	}
	afterAccept, err := persistence.Snapshot(ctx)
	if err != nil || len(afterAccept.Deliveries) != 1 || afterAccept.Deliveries[0].RecoveryPending || afterAccept.Deliveries[0].BootstrapPending {
		t.Fatalf("accepted handoff retained bootstrap/recovery markers: %#v err=%v", afterAccept, err)
	}
}

func TestDynamicSteerUncertainBootstrapRecoveryHandoffWaitsForJoin(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "reviewer",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("complete prior delivery updated=%t err=%v", updated, err)
	}
	replacement := old
	replacement.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, replacement)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	router := testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
		queued <- message.Text
		if attempts.Add(1) == 1 {
			return errors.New("queue outcome uncertain")
		}
		return nil
	})
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"reviewer": {
		TicketQueue: "review", NudgePrompt: "review work", ReviewCompletion: ReviewCompletionClose,
	}}, nil, func(state.SteerRegistration) (steerClient, error) {
		return &sequenceSteerClient{active: []bool{true}}, nil
	}, router, statuses, persistence, newRegistrationObserver(registrations), nil, newSteerDirtySet())
	runner.bootstrap(ctx)
	message := takeQueuedSteerMessage(t, queued)
	parts := []string{steerSessionBootstrapPrompt, steerClaimRecoveryPrompt}
	last := -1
	for _, part := range parts {
		index := strings.Index(message, part)
		if index <= last {
			t.Fatalf("combined reviewer recovery message omitted or reordered %q: %q", part, message)
		}
		last = index
	}
	if strings.Contains(message, "review work") || strings.Contains(message, "approve and close") {
		t.Fatalf("active claim recovery included ready-only reviewer prose: %q", message)
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "sending" || items[0].Code != "queue_uncertain" {
		t.Fatalf("uncertain handoff status=%#v", items)
	}
	afterUncertain, err := persistence.Snapshot(ctx)
	if err != nil || len(afterUncertain.Deliveries) != 1 || !afterUncertain.Deliveries[0].BootstrapPending || !afterUncertain.Deliveries[0].RecoveryPending {
		t.Fatalf("uncertain delivery lost pending flags: %#v err=%v", afterUncertain, err)
	}
	runner.registrationTick(ctx, time.Now())
	if attempts.Load() != 1 {
		t.Fatalf("uncertain delivery was retried without join; attempts=%d", attempts.Load())
	}

	refreshed, _, changed, err := registrations.Join(ctx, current)
	if err != nil || changed || refreshed.IncarnationID == current.IncarnationID {
		t.Fatalf("explicit recovery join=%#v changed=%t err=%v", refreshed, changed, err)
	}
	runner.repositoryChanged(ctx)
	select {
	case <-queued:
	default:
		t.Fatal("explicit join did not retry uncertain recovery handoff")
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].Session != current.SessionID || items[0].State != "queued" {
		t.Fatalf("accepted recovery status=%#v", items)
	}
	if attempts.Load() != 2 {
		t.Fatalf("explicit-join handoff attempts=%d, want 2", attempts.Load())
	}
	afterAccepted, err := persistence.Snapshot(ctx)
	if err != nil || len(afterAccepted.Deliveries) != 1 || afterAccepted.Deliveries[0].BootstrapPending || afterAccepted.Deliveries[0].RecoveryPending {
		t.Fatalf("accepted combined handoff retained pending flags: %#v err=%v", afterAccepted, err)
	}
}

func TestDynamicSteerRoleChangePreservesPendingRecoveryHandoff(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := state.NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	persistence := state.NewSteerRuntimeStore(dir)
	if err := persistence.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := persistence.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("seed old delivery updated=%t err=%v", updated, err)
	}
	newRoute := old
	newRoute.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, newRoute)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	router := testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
		queued <- message.Text
		if attempts.Add(1) == 1 {
			return steertransport.Rejected(errors.New("queue rejected"))
		}
		return nil
	})
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{
		"coder":     {TicketQueue: "open", NudgePrompt: "coder normal work"},
		"architect": {TicketQueue: "open", NudgePrompt: "architect normal work"},
	}, nil, func(state.SteerRegistration) (steerClient, error) {
		return &sequenceSteerClient{active: []bool{true}, ready: []bool{false}}, nil
	}, router, statuses, persistence, newRegistrationObserver(registrations), nil, newSteerDirtySet())
	runner.bootstrap(ctx)
	message := takeQueuedSteerMessage(t, queued)
	if !strings.Contains(message, steerClaimRecoveryPrompt) {
		t.Fatalf("initial handoff message=%q", message)
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "degraded" || items[0].Code != "queue_rejected" {
		t.Fatalf("rejected handoff status=%#v", items)
	}
	afterReject, err := persistence.Snapshot(ctx)
	if err != nil || len(afterReject.Deliveries) != 1 || !afterReject.Deliveries[0].RecoveryPending {
		t.Fatalf("rejected handoff lost recovery marker: %#v err=%v", afterReject, err)
	}

	roleRoute := current
	roleRoute.Role = "architect"
	reassigned, _, changed, err := registrations.Join(ctx, roleRoute)
	if err != nil || !changed || reassigned.RegistrationID == current.RegistrationID {
		t.Fatalf("same-thread role change changed=%t registration=%#v err=%v", changed, reassigned, err)
	}
	runner.repositoryChanged(ctx)
	message = takeQueuedSteerMessage(t, queued)
	if !strings.Contains(message, steerClaimRecoveryPrompt) || strings.Contains(message, "architect normal work") {
		t.Fatalf("role-change handoff message=%q", message)
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].Session != reassigned.SessionID || items[0].Role != "architect" || items[0].State != "queued" {
		t.Fatalf("role-change accepted status=%#v", items)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("recovery handoff attempts=%d, want rejected handoff and one role-change retry", got)
	}
	afterAccept, err := persistence.Snapshot(ctx)
	if err != nil || len(afterAccept.Deliveries) != 1 || afterAccept.Deliveries[0].RecoveryPending {
		t.Fatalf("accepted handoff retained recovery marker: %#v err=%v", afterAccept, err)
	}
}
