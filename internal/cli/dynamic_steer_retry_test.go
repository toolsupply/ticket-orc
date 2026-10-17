package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestDynamicSteerPublishesCorruptRuntimeState(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "steer-runtime.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	var clients atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
		clients.Add(1)
		return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
	}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
		t.Error("corrupt runtime state allowed queue")
		return nil
	}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
	runner.bootstrap(ctx)
	items := statuses.snapshot()
	if len(items) != 1 || items[0].State != "degraded" || items[0].Code != "runtime_state_unavailable" {
		t.Fatalf("corrupt runtime status=%#v", items)
	}
	if clients.Load() != 0 {
		t.Fatalf("Ticket clients created for corrupt runtime state: %d", clients.Load())
	}
}

func TestDynamicSteerReportsFailedSendingPersistenceWithoutQueue(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx := context.Background()
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
		return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
	}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
		queued.Add(1)
		return nil
	}), statuses, store, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
	runner.bootstrap(ctx)
	items := statuses.snapshot()
	if len(items) != 1 || items[0].State != "none" || items[0].Code != "runtime_state_write_failed" {
		t.Fatalf("failed sending persistence status=%#v", items)
	}
	if queued.Load() != 0 {
		t.Fatalf("queued %d wake(s) without persisted sending state", queued.Load())
	}
	snapshot, err := store.inner.Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != "none" {
		t.Fatalf("persisted state=%#v err=%v", snapshot, err)
	}
}

func TestDynamicSteerWriteFailureSurvivesTicketObservationFailure(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	client := &writeFailureThenObservationClient{secondCall: make(chan struct{})}
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	ctx := context.Background()
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			t.Error("queue called without persisted sending state")
			return nil
		}), statuses, store, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	runner.bootstrap(ctx)
	assertWriteFailureStatus := func() {
		t.Helper()
		items := statuses.snapshot()
		if len(items) != 1 || items[0].State != "none" || items[0].Code != "runtime_state_write_failed" {
			t.Fatalf("write failure status=%#v", items)
		}
	}
	assertWriteFailureStatus()
	dirty.MarkAll()
	runner.repositoryChanged(ctx)
	select {
	case <-client.secondCall:
	default:
		t.Fatal("second Ticket observation did not run")
	}
	assertWriteFailureStatus()
}

func TestDynamicSteerReconnectsAfterTerminalTicketObservationFailure(t *testing.T) {
	dir := t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	const secondRepositoryID = "22222222-2222-4222-8222-222222222222"
	for _, reg := range []state.SteerRegistration{
		{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker-a", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "worker-a-thread"},
		{RepositoryID: secondRepositoryID, RepositoryPath: dir, Actor: "worker-b", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "worker-b-thread"},
	} {
		if _, _, _, err := registrations.Join(context.Background(), reg); err != nil {
			t.Fatal(err)
		}
	}
	failedClient := &terminalObservationSteerClient{}
	var firstRepositoryClients atomic.Int32
	var secondRepositoryClients atomic.Int32
	secondRepositoryClient := &successfulObservationSteerClient{}
	reconnected := make(chan struct{})
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(reg state.SteerRegistration) (steerClient, error) {
		if reg.RepositoryID == joinTestRepositoryID {
			if firstRepositoryClients.Add(1) == 1 {
				return failedClient, nil
			}
			select {
			case <-reconnected:
			default:
				close(reconnected)
			}
			return &successfulObservationSteerClient{}, nil
		}
		secondRepositoryClients.Add(1)
		return secondRepositoryClient, nil
	}, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
		if message.Kind != steertransport.MessageSteer {
			t.Errorf("notification kind=%q, want steer", message.Kind)
		}
		if !strings.Contains(message.Text, steerSessionBootstrapPrompt) {
			t.Errorf("initial notification omitted bootstrap: %q", message.Text)
		}
		return nil
	}), statuses, nil, newRegistrationObserver(registrations), nil, dirty)
	triggeredAt := time.Now()
	runner.bootstrap(ctx)
	items := statuses.snapshot()
	if len(items) != 2 || steerStatusForActor(items, "worker-a").Code != "ticket_observation_failed" || steerStatusForActor(items, "worker-b").Code != "" {
		t.Fatalf("initial statuses=%#v", items)
	}
	if got := secondRepositoryClients.Load(); got != 1 {
		t.Fatalf("healthy repository Ticket clients=%d, want initial observation only", got)
	}
	if got := secondRepositoryClient.activeCalls.Load(); got != 2 {
		t.Fatalf("healthy repository ActiveClaims calls=%d, want two initial queue checks", got)
	}
	if got := secondRepositoryClient.readyCalls.Load(); got != 1 {
		t.Fatalf("healthy repository ReadyFrontier calls=%d, want one initial check", got)
	}
	// The manual registration tick advances the retry clock without producing
	// a repository event. Only the repository with the failed observation is
	// reconciled.
	runner.registrationTick(ctx, triggeredAt.Add(time.Hour))
	select {
	case <-reconnected:
	default:
		t.Fatal("retry deadline did not recreate the failed Ticket client")
	}
	if got := firstRepositoryClients.Load(); got != 2 {
		t.Fatalf("failed repository Ticket clients=%d, want a replacement client", got)
	}
	if !failedClient.closed.Load() {
		t.Fatal("terminal Ticket client was not closed when discarded")
	}
	if got := secondRepositoryClients.Load(); got != 1 {
		t.Fatalf("unaffected repository Ticket clients=%d, want no retry", got)
	}
	if got := secondRepositoryClient.activeCalls.Load(); got != 2 {
		t.Fatalf("unaffected repository ActiveClaims calls=%d, want no retry", got)
	}
	if got := secondRepositoryClient.readyCalls.Load(); got != 1 {
		t.Fatalf("unaffected repository ReadyFrontier calls=%d, want no retry", got)
	}
	items = statuses.snapshot()
	if steerStatusForActor(items, "worker-a").Code != "" || steerStatusForActor(items, "worker-b").Code != "" {
		t.Fatalf("reconnected statuses=%#v", items)
	}
}

func TestSteerRetryScheduleUsesBoundedBackoffAndClearsAfterObservation(t *testing.T) {
	const repositoryID = "11111111-1111-4111-8111-111111111111"
	registration := state.SteerRegistration{RepositoryID: repositoryID, Actor: "worker", RegistrationID: "incarnation"}
	key := steerRegistrationKey(registration)
	work := steerDirtySnapshot{repositories: map[string]struct{}{repositoryID: {}}}
	schedule := newSteerRetrySchedule()
	now := time.Date(2026, time.October, 3, 0, 0, 0, 0, time.UTC)
	wantDelays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second}
	for index, delay := range wantDelays {
		schedule.Reconcile(work, []state.SteerRegistration{registration}, map[string]bool{key: true}, nil, now)
		if got := schedule.Due(now.Add(delay - time.Nanosecond)); !got.empty() {
			t.Fatalf("retry %d became due early: %#v", index+1, got)
		}
		due := schedule.Due(now.Add(delay))
		if len(due.repositories) != 1 {
			t.Fatalf("retry %d due repositories=%#v, want only %s", index+1, due.repositories, repositoryID)
		}
		now = now.Add(delay)
	}
	schedule.Reconcile(work, []state.SteerRegistration{registration}, nil, map[string]bool{key: true}, now)
	if got := schedule.Due(now.Add(time.Hour)); !got.empty() {
		t.Fatalf("successful authoritative observation retained retry: %#v", got)
	}
}

func TestSteerRetryBaseTimeUsesLaterTimestamp(t *testing.T) {
	triggeredAt := time.Date(2026, time.October, 3, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name        string
		completedAt time.Time
		want        time.Time
	}{
		{name: "completion before trigger", completedAt: triggeredAt.Add(-time.Second), want: triggeredAt},
		{name: "equal timestamps", completedAt: triggeredAt, want: triggeredAt},
		{name: "completion after trigger", completedAt: triggeredAt.Add(time.Second), want: triggeredAt.Add(time.Second)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := steerRetryBaseTime(triggeredAt, test.completedAt); !got.Equal(test.want) {
				t.Fatalf("retry base = %s, want %s", got, test.want)
			}
		})
	}
}

func TestSteerRetryDeadlineStartsAfterFailedReconciliationCompletion(t *testing.T) {
	const repositoryID = "11111111-1111-4111-8111-111111111111"
	registration := state.SteerRegistration{RepositoryID: repositoryID, Actor: "worker", RegistrationID: "incarnation"}
	key := steerRegistrationKey(registration)
	work := steerDirtySnapshot{repositories: map[string]struct{}{repositoryID: {}}}
	triggeredAt := time.Date(2026, time.October, 3, 0, 0, 0, 0, time.UTC)
	retryDelay := steerRetryDelay(1)

	for _, test := range []struct {
		name              string
		operationDuration time.Duration
	}{
		{name: "operation shorter than retry delay", operationDuration: retryDelay / 2},
		{name: "operation longer than retry delay", operationDuration: retryDelay + time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			completedAt := triggeredAt.Add(test.operationDuration)
			dueAt := steerRetryBaseTime(triggeredAt, completedAt).Add(retryDelay)
			schedule := newSteerRetrySchedule()
			schedule.Reconcile(work, []state.SteerRegistration{registration}, map[string]bool{key: true}, nil, dueAt.Add(-retryDelay))

			if got := schedule.Due(completedAt); !got.empty() {
				t.Fatalf("retry became due at completion %s before deadline %s: %#v", completedAt, dueAt, got)
			}
			if got := schedule.Due(dueAt.Add(-time.Nanosecond)); !got.empty() {
				t.Fatalf("retry became due before exact deadline %s: %#v", dueAt, got)
			}
			if got := schedule.Due(dueAt); len(got.repositories) != 1 {
				t.Fatalf("retry at exact deadline repositories=%#v, want only %s", got.repositories, repositoryID)
			}
		})
	}
}

func TestDynamicSteerPersistentObservationFailureWaitsForBackoff(t *testing.T) {
	dir := t.TempDir()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	registrations := state.NewRegistrationStore(dir)
	if _, _, _, err := registrations.Join(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	var clients atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(state.SteerRegistration) (steerClient, error) {
			clients.Add(1)
			return &terminalObservationSteerClient{}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
			t.Error("unexpected wake after failed Ticket observation")
			return nil
		}), statuses, nil, newRegistrationObserver(registrations), nil, newSteerDirtySet())
	triggeredAt := time.Now()
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].Code != "ticket_observation_failed" {
		t.Fatalf("initial observation status=%#v", items)
	}
	if got := clients.Load(); got != 1 {
		t.Fatalf("initial Ticket clients=%d, want one", got)
	}

	firstRetry := triggeredAt.Add(time.Hour)
	runner.registrationTick(ctx, firstRetry)
	if got := clients.Load(); got != 2 {
		t.Fatalf("Ticket clients at first retry=%d, want two", got)
	}
	// The second failure is delayed by two seconds, measured from the retry
	// reconciliation's logical time rather than wall-clock test duration.
	runner.registrationTick(ctx, firstRetry.Add(time.Second))
	if got := clients.Load(); got != 2 {
		t.Fatalf("Ticket clients before second deadline=%d, want two", got)
	}
	runner.registrationTick(ctx, firstRetry.Add(2*time.Second))
	if got := clients.Load(); got != 3 {
		t.Fatalf("Ticket clients at second retry=%d, want three", got)
	}
}

func TestDynamicSteerRepositoryEventImmediatelyRetriesFailedObservation(t *testing.T) {
	dir := t.TempDir()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	registrations := state.NewRegistrationStore(dir)
	if _, _, _, err := registrations.Join(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	var clients atomic.Int32
	statuses := &dynamicSteerStatus{}
	dirty := newSteerDirtySet()
	ctx := context.Background()
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
		func(state.SteerRegistration) (steerClient, error) {
			if clients.Add(1) == 1 {
				return &terminalObservationSteerClient{}, nil
			}
			return &successfulObservationSteerClient{}, nil
		}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error { return nil }),
		statuses, nil, newRegistrationObserver(registrations), nil, dirty)
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].Code != "ticket_observation_failed" {
		t.Fatalf("initial observation status=%#v", items)
	}
	dirty.MarkRepository(joinTestRepositoryID)
	runner.repositoryChanged(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].Code != "" {
		t.Fatalf("event retry status=%#v", items)
	}
	if got := clients.Load(); got != 2 {
		t.Fatalf("Ticket client factory calls=%d, want immediate event-driven retry", got)
	}
}

func TestDynamicSteerManagedOwnerConflictSurvivesPendingWriteFailure(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "shared-actor", Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	runtime := NewRuntimeState([]supervisor.RunWorker{})
	dirty := newSteerDirtySet()
	ctx := context.Background()
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime, func(state.SteerRegistration) (steerClient, error) {
		return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
	}, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
		queued.Add(1)
		return nil
	}), statuses, store, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	runner.bootstrap(ctx)
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != "none" || items[0].Code != "runtime_state_write_failed" {
		t.Fatalf("initial status=%#v", items)
	}
	worker := supervisor.RunWorker{Name: "managed", Config: supervisor.RoleConfig{Actor: reg.Actor, RepositoryIdentity: reg.RepositoryID}}
	runtime.SetConfiguredWorkers([]supervisor.RunWorker{worker})
	runtime.SetEffectiveWorker(worker)
	runtime.Set(supervisor.WorkerTransition{Worker: worker.Name, State: supervisor.WorkerRunning})
	dirty.MarkAll()
	runner.repositoryChanged(ctx)
	items := statuses.snapshot()
	if len(items) != 1 || items[0].State != "conflict" || items[0].Code != "managed_owner" || items[0].PersistenceCode != "runtime_state_write_failed" {
		t.Fatalf("managed owner status=%#v", items)
	}
	if queued.Load() != 0 {
		t.Fatalf("queued %d wake(s) despite managed ownership conflict", queued.Load())
	}
}
