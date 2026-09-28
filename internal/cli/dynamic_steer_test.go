package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestManagedWorkerOwnershipBlocksDynamicSteer(t *testing.T) {
	dir := t.TempDir()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "shared-actor", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	worker := supervisor.RunWorker{Name: "managed", Config: supervisor.RoleConfig{Actor: registration.Actor, RepositoryIdentity: registration.RepositoryID}}
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	runtime.SetEffectiveWorker(worker)
	runtime.Set(supervisor.WorkerTransition{Worker: worker.Name, State: supervisor.WorkerRunning})
	if !managedOwner(runtime, registration) {
		t.Fatal("matching managed worker did not block dynamic registration")
	}
	var clients atomic.Int32
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime, func(state.SteerRegistration) (steerClient, error) {
			clients.Add(1)
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, func(context.Context, string, string, string) error { queued.Add(1); return nil }, statuses)
	}()
	deadline := time.Now().Add(time.Second)
	for len(statuses.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	items := statuses.snapshot()
	if len(items) != 1 || items[0].State != "conflict" || items[0].Code != "managed_owner" || items[0].ManagedOwner != "managed" {
		t.Fatalf("steer conflict status=%#v", items)
	}
	if clients.Load() != 0 || queued.Load() != 0 {
		t.Fatalf("managed conflict created Ticket clients=%d or queued=%d wake(s)", clients.Load(), queued.Load())
	}
	registration.Actor = "another-actor"
	if managedOwner(runtime, registration) {
		t.Fatal("different Ticket actor reported as managed conflict")
	}
}

func TestDynamicSteerStatusConcurrentSnapshotsAndUpdates(t *testing.T) {
	statuses := &dynamicSteerStatus{}
	var published atomic.Int32
	statuses.setPublisher(func(daemon.Event) { published.Add(1) })
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				statuses.replace([]daemon.SteerStatus{{RepositoryID: "repo", Actor: "actor", Session: fmt.Sprintf("session-%d-%d", worker, i), State: "queued"}})
				_ = statuses.snapshot()
			}
		}()
	}
	workers.Wait()
	if len(statuses.snapshot()) != 1 || published.Load() == 0 {
		t.Fatalf("status snapshot=%#v published=%d", statuses.snapshot(), published.Load())
	}
}

func TestDynamicSteerStatusPublishesActiveTicketID(t *testing.T) {
	var published daemon.Event
	statuses := &dynamicSteerStatus{}
	statuses.setPublisher(func(event daemon.Event) { published = event })
	statuses.replace([]daemon.SteerStatus{{
		RepositoryID: "repo", Role: "coder", Actor: "worker", Session: steerTestThread,
		State: "consumed", Ticket: "20260926-12345",
	}})
	if published.Type != "steer.status" || published.Ticket != "20260926-12345" {
		t.Fatalf("published status event=%#v", published)
	}
}

type sequenceSteerClient struct {
	mu             sync.Mutex
	active         []bool
	activeByQueue  map[string][]bool
	activeQueueIdx map[string]int
	ai, ri         int
	ready          []bool
}

type readyBarrierSteerClient struct {
	observed chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (c *readyBarrierSteerClient) ActiveClaims(context.Context, string, []string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}

func (c *readyBarrierSteerClient) ReadyFrontier(_ context.Context, queue string, _ []string, _ int) (ticketclient.ListResult, error) {
	c.once.Do(func() {
		close(c.observed)
		<-c.release
	})
	return testTicketList("20260926-00002", queue), nil
}

func (c *readyBarrierSteerClient) Close() error { return nil }

type writeFailureThenObservationClient struct {
	calls      atomic.Int32
	secondCall chan struct{}
}

func (c *writeFailureThenObservationClient) ActiveClaims(_ context.Context, queue string, _ []string, _ int) (ticketclient.ListResult, error) {
	if queue == "review" {
		return ticketclient.ListResult{}, nil
	}
	if c.calls.Add(1) == 1 {
		return ticketclient.ListResult{}, nil
	}
	select {
	case <-c.secondCall:
	default:
		close(c.secondCall)
	}
	return ticketclient.ListResult{}, errors.New("injected Ticket observation failure")
}
func (c *writeFailureThenObservationClient) ReadyFrontier(context.Context, string, []string, int) (ticketclient.ListResult, error) {
	return testTicketList("20260926-00002", "open"), nil
}
func (c *writeFailureThenObservationClient) Close() error { return nil }

func (c *sequenceSteerClient) ActiveClaims(_ context.Context, queue string, _ []string, _ int) (ticketclient.ListResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if values, ok := c.activeByQueue[queue]; ok {
		if len(values) == 0 {
			return ticketclient.ListResult{}, nil
		}
		if c.activeQueueIdx == nil {
			c.activeQueueIdx = make(map[string]int)
		}
		i := c.activeQueueIdx[queue]
		if i >= len(values) {
			i = len(values) - 1
		}
		c.activeQueueIdx[queue]++
		if values[i] {
			return testTicketList("20260926-00001", queue), nil
		}
		return ticketclient.ListResult{}, nil
	}
	// Existing sequence-based cases model the role's configured queue (open).
	// Tests for claims in another queue provide activeByQueue explicitly.
	if queue != "open" {
		return ticketclient.ListResult{}, nil
	}
	i := c.ai
	if len(c.active) == 0 {
		return ticketclient.ListResult{}, nil
	}
	if i >= len(c.active) {
		i = len(c.active) - 1
	}
	c.ai++
	if c.active[i] {
		return testTicketList("20260926-00001", queue), nil
	}
	return ticketclient.ListResult{}, nil
}
func (c *sequenceSteerClient) ReadyFrontier(_ context.Context, queue string, _ []string, _ int) (ticketclient.ListResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.ri
	if len(c.ready) == 0 {
		return ticketclient.ListResult{}, nil
	}
	if i >= len(c.ready) {
		i = len(c.ready) - 1
	}
	c.ri++
	if c.ready[i] {
		return testTicketList("20260926-00002", queue), nil
	}
	return ticketclient.ListResult{}, nil
}
func (c *sequenceSteerClient) Close() error { return nil }

func testTicketList(id, state string) ticketclient.ListResult {
	return ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: id, State: state, Assignee: "actor"}}}
}

func TestBoundedSteerTicketObservationsReturnTicketIDs(t *testing.T) {
	client := &sequenceSteerClient{
		activeByQueue: map[string][]bool{"open": {false}, "review": {true}},
		ready:         []bool{true},
	}
	active, err := boundedActorActive(context.Background(), client, "open")
	if err != nil || active != "20260926-00001" {
		t.Fatalf("boundedActorActive()=%q, %v", active, err)
	}
	ready, err := boundedReady(context.Background(), client, "open")
	if err != nil || ready != "20260926-00002" {
		t.Fatalf("boundedReady()=%q, %v", ready, err)
	}
}

func TestPlanSteerNotificationSeparatesControlAndWork(t *testing.T) {
	tests := []struct {
		name             string
		policy           steerRolePolicy
		state            string
		activeTicket     string
		readyTicket      string
		recoveryPending  bool
		bootstrapPending bool
		wantSend         bool
		wantWork         bool
		wantCompletion   string
		wantMessageParts []string
	}{
		{
			name:   "idle with no pending notification",
			policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"},
			state:  string(orc.SteerIdle),
		},
		{
			name:   "bootstrap is control only",
			policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"},
			state:  string(orc.SteerIdle), bootstrapPending: true,
			wantSend: true, wantCompletion: string(orc.SteerIdle),
			wantMessageParts: []string{steerSessionBootstrapPrompt},
		},
		{
			name:   "ready work includes bootstrap and nudge",
			policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"},
			state:  string(orc.SteerIdle), readyTicket: "20260926-00001", bootstrapPending: true,
			wantSend: true, wantWork: true, wantCompletion: string(orc.SteerQueued),
			wantMessageParts: []string{steerSessionBootstrapPrompt, "open work"},
		},
		{
			name:   "recovery with active claim",
			policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"},
			state:  string(orc.SteerIdle), activeTicket: "20260926-00002", recoveryPending: true,
			wantSend: true, wantWork: true, wantCompletion: string(orc.SteerQueued),
			wantMessageParts: []string{steerClaimRecoveryPrompt, "open work"},
		},
		{
			name:   "review ready work includes completion policy",
			policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review work"},
			state:  string(orc.SteerIdle), readyTicket: "20260926-00003",
			wantSend: true, wantWork: true, wantCompletion: string(orc.SteerQueued),
			wantMessageParts: []string{"review work", "approve the ticket to signoff"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := planSteerNotification(test.policy, test.state, test.activeTicket, test.readyTicket, test.recoveryPending, test.bootstrapPending)
			if plan.send != test.wantSend || plan.workBearing != test.wantWork || plan.completionState != test.wantCompletion {
				t.Fatalf("plan send/work/completion = %t/%t/%q, want %t/%t/%q", plan.send, plan.workBearing, plan.completionState, test.wantSend, test.wantWork, test.wantCompletion)
			}
			for _, part := range test.wantMessageParts {
				if !strings.Contains(plan.message, part) {
					t.Fatalf("plan message %q omitted %q", plan.message, part)
				}
			}
			if !test.wantSend && plan.message != "" {
				t.Fatalf("idle plan unexpectedly contains message %q", plan.message)
			}
			if test.name == "bootstrap is control only" && strings.Contains(plan.message, test.policy.NudgePrompt) {
				t.Fatalf("control-only bootstrap included work nudge: %q", plan.message)
			}
		})
	}
}

type countingSteerRuntime struct {
	inner      steerRuntimePersistence
	snapshots  atomic.Int32
	reconciles atomic.Int32
	completed  chan struct{}
}

func (s *countingSteerRuntime) Snapshot(ctx context.Context) (state.SteerRuntimeSnapshot, error) {
	s.snapshots.Add(1)
	return s.inner.Snapshot(ctx)
}
func (s *countingSteerRuntime) Reconcile(ctx context.Context, registrations []state.SteerRegistration) error {
	s.reconciles.Add(1)
	return s.inner.Reconcile(ctx, registrations)
}
func (s *countingSteerRuntime) Update(ctx context.Context, reg state.SteerRegistration, stateName, code string) (bool, error) {
	return s.inner.Update(ctx, reg, stateName, code)
}
func (s *countingSteerRuntime) CompleteDelivery(ctx context.Context, reg state.SteerRegistration, nextState string, bootstrap, recovery bool) (bool, error) {
	current, err := s.inner.CompleteDelivery(ctx, reg, nextState, bootstrap, recovery)
	if current && err == nil && nextState == string(orc.SteerQueued) && s.completed != nil {
		select {
		case s.completed <- struct{}{}:
		default:
		}
	}
	return current, err
}
func (s *countingSteerRuntime) ConfirmNoActiveClaim(ctx context.Context, reg state.SteerRegistration) (bool, error) {
	return s.inner.ConfirmNoActiveClaim(ctx, reg)
}

func TestDynamicSteerReconcilesAndSnapshotsOnceForMultipleRegistrations(t *testing.T) {
	dir := t.TempDir()
	registrations := state.NewRegistrationStore(dir)
	for _, actor := range []string{"worker-a", "worker-b"} {
		if _, _, _, err := registrations.Join(context.Background(), state.SteerRegistration{
			RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: actor, Role: "coder", CodexHome: dir, ThreadID: actor,
		}); err != nil {
			t.Fatal(err)
		}
	}
	persistence := &countingSteerRuntime{inner: state.NewSteerRuntimeStore(dir)}
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithPersistence(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
			}, func(context.Context, string, string, string) error { return nil }, statuses, persistence)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 2 && items[0].State == "none" && items[1].State == "none"
	})
	cancel()
	<-done
	if got := persistence.reconciles.Load(); got != 1 {
		t.Fatalf("Reconcile called %d times for one tick, want 1", got)
	}
	if got := persistence.snapshots.Load(); got != 1 {
		t.Fatalf("Snapshot called %d times for two registrations in one tick, want 1", got)
	}
}

type failingSteerRuntime struct {
	inner *state.SteerRuntimeStore
	fail  string
}

func (s failingSteerRuntime) Snapshot(ctx context.Context) (state.SteerRuntimeSnapshot, error) {
	return s.inner.Snapshot(ctx)
}
func (s failingSteerRuntime) Reconcile(ctx context.Context, registrations []state.SteerRegistration) error {
	return s.inner.Reconcile(ctx, registrations)
}
func (s failingSteerRuntime) Update(ctx context.Context, reg state.SteerRegistration, stateName, code string) (bool, error) {
	if stateName == s.fail {
		return false, errors.New("injected persistence error")
	}
	return s.inner.Update(ctx, reg, stateName, code)
}
func (s failingSteerRuntime) CompleteDelivery(ctx context.Context, reg state.SteerRegistration, nextState string, bootstrap, recovery bool) (bool, error) {
	if s.fail == "queued" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.CompleteDelivery(ctx, reg, nextState, bootstrap, recovery)
}
func (s failingSteerRuntime) ConfirmNoActiveClaim(ctx context.Context, reg state.SteerRegistration) (bool, error) {
	if s.fail == "recovery" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.ConfirmNoActiveClaim(ctx, reg)
}

func TestDynamicSteerPublishesCorruptRuntimeState(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "steer-runtime.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	var clients atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			clients.Add(1)
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, func(context.Context, string, string, string) error {
			t.Error("corrupt runtime state allowed queue")
			return nil
		}, statuses)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "degraded" && items[0].Code == "runtime_state_unavailable"
	})
	cancel()
	<-done
	if clients.Load() != 0 {
		t.Fatalf("Ticket clients created for corrupt runtime state: %d", clients.Load())
	}
}

func TestDynamicSteerReportsFailedSendingPersistenceWithoutQueue(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	go func() {
		defer close(done)
		runDynamicSteerWithPersistence(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, func(context.Context, string, string, string) error { queued.Add(1); return nil }, statuses, store)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "runtime_state_write_failed"
	})
	cancel()
	<-done
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
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	client := &writeFailureThenObservationClient{secondCall: make(chan struct{})}
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	go func() {
		defer close(done)
		runDynamicSteerWithPersistence(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil }, func(context.Context, string, string, string) error {
			t.Error("queue called without persisted sending state")
			return nil
		}, statuses, store)
	}()
	select {
	case <-client.secondCall:
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatal("second Ticket observation did not run")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "runtime_state_write_failed"
	})
	cancel()
	<-done
}

type terminalObservationSteerClient struct {
	closed atomic.Bool
}

func (c *terminalObservationSteerClient) ActiveClaims(context.Context, string, []string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, ticketclient.ErrTransport
}
func (c *terminalObservationSteerClient) ReadyFrontier(context.Context, string, []string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}
func (c *terminalObservationSteerClient) Close() error {
	c.closed.Store(true)
	return nil
}

func TestDynamicSteerReconnectsAfterTerminalTicketObservationFailure(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
	_, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	failedClient := &terminalObservationSteerClient{}
	var clients atomic.Int32
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			if clients.Add(1) == 1 {
				return failedClient, nil
			}
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
		}, func(_ context.Context, _, _, message string) error {
			if !strings.Contains(message, steerSessionBootstrapPrompt) {
				t.Errorf("initial notification omitted bootstrap: %q", message)
			}
			return nil
		}, statuses)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "ticket_observation_failed"
	})
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "idle"
	})
	if clients.Load() < 2 {
		t.Fatalf("Ticket client factory called %d time(s), want reconnect after terminal failure", clients.Load())
	}
	if !failedClient.closed.Load() {
		t.Fatal("terminal Ticket client was not closed when discarded")
	}
}

func TestDynamicSteerManagedOwnerConflictSurvivesPendingWriteFailure(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "shared-actor", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	var queued atomic.Int32
	statuses := &dynamicSteerStatus{}
	runtime := NewRuntimeState([]supervisor.RunWorker{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	store := failingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), fail: "sending"}
	go func() {
		defer close(done)
		runDynamicSteerWithPersistence(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, runtime, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, func(context.Context, string, string, string) error { queued.Add(1); return nil }, statuses, store)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none" && items[0].Code == "runtime_state_write_failed"
	})
	worker := supervisor.RunWorker{Name: "managed", Config: supervisor.RoleConfig{Actor: reg.Actor, RepositoryIdentity: reg.RepositoryID}}
	runtime.SetConfiguredWorkers([]supervisor.RunWorker{worker})
	runtime.SetEffectiveWorker(worker)
	runtime.Set(supervisor.WorkerTransition{Worker: worker.Name, State: supervisor.WorkerRunning})
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "conflict" && items[0].Code == "managed_owner" && items[0].PersistenceCode == "runtime_state_write_failed"
	})
	cancel()
	<-done
	if queued.Load() != 0 {
		t.Fatalf("queued %d wake(s) despite managed ownership conflict", queued.Load())
	}
}

func waitSteerStatus(t *testing.T, statuses *dynamicSteerStatus, check func([]daemon.SteerStatus) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if items := statuses.snapshot(); check(items) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scheduler status did not match: %#v", statuses.snapshot())
}

func TestDynamicSteerSendsSecondWakeAfterPositiveClaimEvidence(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", CodexHome: dir, ThreadID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5"}
	reg, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg)
	if err != nil {
		t.Fatal(err)
	}
	client := &sequenceSteerClient{active: []bool{false, true, false}, ready: []bool{true, true}}
	queued := make(chan string, 3)
	persistence := &countingSteerRuntime{inner: state.NewSteerRuntimeStore(dir), completed: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithPersistence(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "configured prompt"}}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil }, func(queueCtx context.Context, _ string, _ string, message string) error {
			select {
			case queued <- message:
				return nil
			case <-queueCtx.Done():
				return queueCtx.Err()
			}
		}, &dynamicSteerStatus{}, persistence)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	for i := 0; i < 2; i++ {
		select {
		case message := <-queued:
			if !strings.Contains(message, "configured prompt") || strings.Contains(message, steerSessionBootstrapPrompt) != (i == 0) {
				t.Fatalf("queued message[%d]=%q", i, message)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("scheduler did not send expected wake")
		}
	}
	completed := 0
	deadline := time.After(4 * time.Second)
	for completed < 2 {
		select {
		case <-persistence.completed:
			completed++
		case <-deadline:
			t.Fatal("second wake was not durably recorded as queued before cancellation")
		}
	}
	snapshot, err := persistence.Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != string(orc.SteerQueued) {
		t.Fatalf("delivery was not durably queued before cancellation: snapshot=%#v err=%v", snapshot, err)
	}
	cancel()
	<-done
	select {
	case extra := <-queued:
		t.Fatalf("unexpected duplicate wake %q", extra)
	default:
	}
	snapshot, err = persistence.Snapshot(context.Background())
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != string(orc.SteerQueued) {
		t.Fatalf("delivery snapshot=%#v err=%v", snapshot, err)
	}
}

func TestDynamicSteerPublishesNotificationDeliveryWithReadyTicketID(t *testing.T) {
	dir := t.TempDir()
	reg := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), reg); err != nil {
		t.Fatal(err)
	}
	statuses := &dynamicSteerStatus{}
	events := make(chan daemon.Event, 8)
	statuses.setPublisher(func(event daemon.Event) { events <- event })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "wake"}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
			},
			func(context.Context, string, string, string) error { return nil }, statuses)
	}()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "queued"
	})
	cancel()
	<-done
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
				CodexHome: dir, ThreadID: steerTestThread,
			})
			if err != nil {
				t.Fatal(err)
			}
			queued := make(chan string, 4)
			statuses := &dynamicSteerStatus{}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "role work nudge"}}, nil,
					func(state.SteerRegistration) (steerClient, error) {
						return &sequenceSteerClient{active: []bool{false}, ready: []bool{ready}}, nil
					},
					func(_ context.Context, _, thread, message string) error {
						if thread != reg.ThreadID {
							t.Errorf("bootstrap sent to thread %q, want %q", thread, reg.ThreadID)
						}
						queued <- message
						return nil
					}, statuses)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
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
			case <-time.After(2 * time.Second):
				t.Fatal("first registration did not receive its bootstrap")
			}
			wantState := "none"
			if ready {
				wantState = "queued"
			}
			waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
				return len(items) == 1 && items[0].State == wantState
			})
			snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(context.Background())
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].BootstrapPending || snapshot.Deliveries[0].State != wantState {
				t.Fatalf("bootstrap delivery snapshot=%#v err=%v", snapshot, err)
			}
			time.Sleep(steerPollInterval + 25*time.Millisecond)
			if len(queued) != 0 {
				t.Fatalf("scheduler repeated bootstrap: %q", <-queued)
			}
			cancel()
			<-done

			restartedQueue := make(chan string, 1)
			restartStatuses := &dynamicSteerStatus{}
			restartCtx, restartCancel := context.WithCancel(context.Background())
			restartDone := make(chan struct{})
			go func() {
				defer close(restartDone)
				runDynamicSteer(restartCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "role work nudge"}}, nil,
					func(state.SteerRegistration) (steerClient, error) {
						return &sequenceSteerClient{active: []bool{false}, ready: []bool{false}}, nil
					},
					func(_ context.Context, _, _, message string) error {
						restartedQueue <- message
						return nil
					}, restartStatuses)
			}()
			t.Cleanup(func() {
				restartCancel()
				<-restartDone
			})
			waitSteerStatus(t, restartStatuses, func(items []daemon.SteerStatus) bool {
				return len(items) == 1 && items[0].State == wantState
			})
			time.Sleep(steerPollInterval + 25*time.Millisecond)
			if len(restartedQueue) != 0 {
				t.Fatalf("daemon restart repeated bootstrap: %q", <-restartedQueue)
			}
			restartCancel()
			<-restartDone
		})
	}
}

func TestDynamicSteerSuppressionPreservesPendingDeliveryUntilResume(t *testing.T) {
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		CodexHome: dir, ThreadID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	controlStore := state.NewDaemonControlStore(dir)
	paused, err := controlStore.PauseDaemon(context.Background())
	if err != nil || paused.Mode != state.DaemonPaused {
		t.Fatalf("persist pause = %#v, %v", paused, err)
	}
	gate := newDispatchGate(controlStore, paused.Mode)
	wake := make(chan struct{}, 1)
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithGate(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "work"}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
			},
			func(_ context.Context, _, _, message string) error { queued <- message; return nil },
			statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), gate, wake)
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Code == "daemon_paused"
	})
	if len(queued) != 0 {
		t.Fatalf("paused daemon queued %q", <-queued)
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
	wake <- struct{}{}
	select {
	case message := <-queued:
		if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, "work") {
			t.Fatalf("resumed message = %q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("resume did not deliver pending bootstrap for %s", registration.ThreadID)
	}
}

func TestPauseWhileSteerReadyObservationIsBlockedSuppressesNotification(t *testing.T) {
	dir := t.TempDir()
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		CodexHome: dir, ThreadID: steerTestThread,
	}); err != nil {
		t.Fatal(err)
	}
	controlStore := state.NewDaemonControlStore(dir)
	gate := newDispatchGate(controlStore, state.DaemonRunning)
	client := &readyBarrierSteerClient{observed: make(chan struct{}), release: make(chan struct{})}
	queued := make(chan string, 1)
	statuses := &dynamicSteerStatus{}
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteerWithGate(ctx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "work"}}, nil,
			func(state.SteerRegistration) (steerClient, error) { return client, nil },
			func(_ context.Context, _, _, message string) error { queued <- message; return nil },
			statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), gate, wake)
	}()
	released := false
	releaseReady := func() {
		if !released {
			close(client.release)
			released = true
		}
	}
	t.Cleanup(func() { releaseReady(); cancel(); <-done })

	select {
	case <-client.observed:
	case <-time.After(time.Second):
		t.Fatal("Ticket ready frontier was not observed")
	}
	// Pause while the ready-frontier query is in flight. When its ready result
	// returns, the worker must still observe the closed dispatch gate.
	mode, applied, err := gate.Pause(context.Background())
	if err != nil || !applied || mode.Mode != state.DaemonPaused {
		t.Fatalf("pause mode=%#v applied=%t err=%v", mode, applied, err)
	}
	releaseReady()
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Code == "daemon_paused"
	})
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
	wake <- struct{}{}
	select {
	case message := <-queued:
		if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, "work") {
			t.Fatalf("resumed notification=%q", message)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not reconcile the observed ready work without rejoin")
	}
}

func TestPauseAfterReadyObservationBlocksSteerQueueReservation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		CodexHome: dir, ThreadID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewDaemonControlStore(dir)
	runtimeStore := state.NewSteerRuntimeStore(dir)
	if err := runtimeStore.Reconcile(ctx, []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	gate := newDispatchGate(store, state.DaemonRunning)
	client := &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}
	policy := steerRolePolicy{TicketQueue: "open", NudgePrompt: "work"}
	active, err := observeSteerActiveTicket(ctx, client, policy)
	if err != nil || active != "" {
		t.Fatalf("active observation=%q err=%v", active, err)
	}
	ready, err := observeSteerReadyTicket(ctx, client, policy, string(orc.SteerIdle), active)
	if err != nil || ready != "20260926-00002" {
		t.Fatalf("ready observation=%q err=%v", ready, err)
	}
	plan := planSteerNotification(policy, string(orc.SteerIdle), active, ready, false, true)
	if !plan.send {
		t.Fatal("observed ready work did not produce a send plan")
	}
	if _, _, err := gate.Pause(ctx); err != nil {
		t.Fatalf("pause after ready observation: %v", err)
	}
	var queueCalls atomic.Int32
	queue := func(context.Context, string, string, string) error { queueCalls.Add(1); return nil }
	statuses := &dynamicSteerStatus{}
	writeFailures, persisted := map[string]string{}, map[string]string{}
	allowed, err := withSteerDispatch(gate, func() error {
		current, err := currentSteerRegistration(ctx, state.NewRegistrationStore(dir), registration)
		if err != nil {
			return err
		}
		if !current {
			return errors.New("registration was replaced")
		}
		sendSteerNotification(ctx, registration, plan, queue, runtimeStore, statuses, writeFailures, persisted)
		return nil
	})
	if err != nil || allowed || queueCalls.Load() != 0 {
		t.Fatalf("dispatch after pause allowed=%t queue calls=%d err=%v", allowed, queueCalls.Load(), err)
	}
	snapshot, err := runtimeStore.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("delivery snapshot=%#v err=%v", snapshot, err)
	}
	if delivery := snapshot.Deliveries[0]; delivery.State != string(orc.SteerIdle) || !delivery.BootstrapPending {
		t.Fatalf("dispatch suppression consumed delivery: %#v", delivery)
	}
}

func TestDynamicSteerIdleBootstrapThenReadyQueuesOneRoleNudge(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	_, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		CodexHome: dir, ThreadID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued := make(chan string, 3)
	statuses := &dynamicSteerStatus{}
	steerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(steerCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "configured role nudge"}}, nil,
			func(state.SteerRegistration) (steerClient, error) {
				return &sequenceSteerClient{active: []bool{false}, ready: []bool{false, true}}, nil
			},
			func(_ context.Context, _, _, message string) error { queued <- message; return nil }, statuses)
	}()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case message := <-queued:
		if message != steerSessionBootstrapPrompt {
			t.Fatalf("first idle message=%q, want bootstrap only", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle registration did not receive bootstrap")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "none"
	})
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != "none" || snapshot.Deliveries[0].BootstrapPending {
		t.Fatalf("bootstrap-only state=%#v err=%v", snapshot.Deliveries, err)
	}
	select {
	case message := <-queued:
		if message != "configured role nudge" {
			t.Fatalf("later ready message=%q, want configured role nudge without bootstrap", message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("later ready work did not queue a role nudge")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "queued"
	})
	time.Sleep(steerPollInterval + 25*time.Millisecond)
	if len(queued) != 0 {
		t.Fatalf("scheduler queued more than one role nudge: %q", <-queued)
	}
}

func TestDynamicSteerEmitsReviewerCompletionPolicyOnlyForReviewQueue(t *testing.T) {
	tests := []struct {
		name             string
		role             string
		policy           steerRolePolicy
		wantPolicyPrompt string
	}{
		{name: "default review signoff", role: "quality", policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review nudge"}, wantPolicyPrompt: "For accepted work, approve the ticket to signoff."},
		{name: "configured signoff", role: "quality", policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review nudge", ReviewCompletion: ReviewCompletionSignoff}, wantPolicyPrompt: "For accepted work, approve the ticket to signoff."},
		{name: "configured close", role: "quality", policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review nudge", ReviewCompletion: ReviewCompletionClose}, wantPolicyPrompt: "For accepted work, approve and close the ticket."},
		{name: "coder has no reviewer policy", role: "builder", policy: steerRolePolicy{TicketQueue: "open", NudgePrompt: "code nudge"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: test.role,
				CodexHome: dir, ThreadID: steerTestThread,
			}); err != nil {
				t.Fatal(err)
			}
			queued := make(chan string, 1)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				runDynamicSteer(ctx, dir, map[string]steerRolePolicy{test.role: test.policy}, nil,
					func(state.SteerRegistration) (steerClient, error) {
						return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
					},
					func(_ context.Context, _, _, message string) error { queued <- message; return nil },
					&dynamicSteerStatus{})
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			select {
			case message := <-queued:
				if !strings.Contains(message, test.policy.NudgePrompt) {
					t.Fatalf("notification omitted role nudge: %q", message)
				}
				if strings.Index(message, steerSessionBootstrapPrompt) < 0 || strings.Index(message, steerSessionBootstrapPrompt) > strings.Index(message, test.policy.NudgePrompt) {
					t.Fatalf("new-session notification did not put bootstrap before role nudge: %q", message)
				}
				if test.wantPolicyPrompt == "" {
					if strings.Contains(message, "For accepted work") {
						t.Fatalf("non-review notification included reviewer policy: %q", message)
					}
				} else {
					if !strings.Contains(message, test.wantPolicyPrompt) {
						t.Fatalf("review notification omitted policy %q: %q", test.wantPolicyPrompt, message)
					}
					if strings.Index(message, test.policy.NudgePrompt) > strings.Index(message, test.wantPolicyPrompt) {
						t.Fatalf("review policy preceded role nudge: %q", message)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("dynamic role did not receive its ready-work notification")
			}
		})
	}
}

func TestDynamicSteerRecreatedRegistrationDoesNotInheritQueuedDelivery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registration := state.SteerRegistration{RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", CodexHome: dir, ThreadID: steerTestThread}
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
	steerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(steerCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "fresh wake"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
		}, func(_ context.Context, _, _, message string) error {
			queued <- message
			return nil
		}, statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		if message != "fresh wake" {
			t.Fatalf("queued message=%q", message)
		}
	case <-time.After(time.Second):
		t.Fatal("new registration inherited the old queued delivery instead of scheduling a fresh wake")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "queued"
	})
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
		{name: "new thread active claim", threadChanged: true, active: true, wantWake: true, wantBootstrap: true, wantRecovery: true, wantRolePrompt: "coder normal work"},
		{name: "new thread ready work", threadChanged: true, ready: true, wantWake: true, wantBootstrap: true, wantRolePrompt: "coder normal work"},
		{name: "new thread no work", threadChanged: true, wantWake: true, wantBootstrap: true},
		{name: "new thread role change active", threadChanged: true, roleChanged: true, active: true, wantWake: true, wantBootstrap: true, wantRecovery: true, wantRolePrompt: "architect normal work"},
		{name: "new thread role change recovers claim in prior queue", threadChanged: true, roleChanged: true, activeByQueue: map[string][]bool{"open": {false}, "review": {true}}, wantWake: true, wantBootstrap: true, wantRecovery: true, wantRolePrompt: "architect normal work"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			registrations := state.NewRegistrationStore(dir)
			old, _, _, err := registrations.Join(ctx, state.SteerRegistration{
				RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
				CodexHome: dir, ThreadID: steerTestThread,
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
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{
					"coder":     {TicketQueue: "open", NudgePrompt: "coder normal work"},
					"architect": {TicketQueue: "open", NudgePrompt: "architect normal work"},
				}, nil, func(state.SteerRegistration) (steerClient, error) {
					return &sequenceSteerClient{active: []bool{test.active}, activeByQueue: test.activeByQueue, ready: []bool{test.ready}}, nil
				}, func(_ context.Context, _, _, message string) error {
					queued <- message
					return nil
				}, statuses)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			initialState := "queued"
			initiallyActive := test.active
			for _, claims := range test.activeByQueue {
				initiallyActive = initiallyActive || len(claims) > 0 && claims[0]
			}
			if initiallyActive {
				initialState = "consumed"
			}
			waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
				return len(items) == 1 && items[0].Session == old.ThreadID && items[0].State == initialState
			})

			next := old
			if test.threadChanged {
				next.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
			}
			if test.roleChanged {
				next.Role = "architect"
			}
			current, _, changed, err := registrations.Join(ctx, next)
			if err != nil || changed != (test.threadChanged || test.roleChanged) {
				t.Fatalf("join changed=%t err=%v registration=%#v", changed, err, current)
			}
			if test.wantWake {
				select {
				case message := <-queued:
					if (test.wantRolePrompt != "" && !strings.Contains(message, test.wantRolePrompt)) || strings.Contains(message, steerClaimRecoveryPrompt) != test.wantRecovery || strings.Contains(message, steerSessionBootstrapPrompt) != test.wantBootstrap {
						t.Fatalf("wake message=%q want role prompt %q recovery=%t bootstrap=%t", message, test.wantRolePrompt, test.wantRecovery, test.wantBootstrap)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("join did not schedule the expected wake")
				}
				wantDeliveryState := "queued"
				if test.wantBootstrap && test.wantRolePrompt == "" {
					wantDeliveryState = "none"
				}
				waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
					return len(items) == 1 && items[0].Session == current.ThreadID && items[0].State == wantDeliveryState
				})
				if test.wantRecovery {
					for _, wantState := range []string{"sending", "queued"} {
						select {
						case event := <-deliveryEvents:
							if event.State != wantState || event.Ticket != "20260926-00001" {
								t.Fatalf("recovery delivery event=%#v, want state=%q and active Ticket ID", event, wantState)
							}
						case <-time.After(time.Second):
							t.Fatalf("missing recovery delivery event state=%q", wantState)
						}
					}
				}
			} else {
				waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
					return len(items) == 1 && items[0].Session == current.ThreadID && items[0].State == "none" && items[0].Code == test.wantStatusCode
				})
				select {
				case message := <-queued:
					t.Fatalf("unexpected wake: %q", message)
				default:
				}
			}
			time.Sleep(steerPollInterval + 25*time.Millisecond)
			if len(queued) != 0 {
				t.Fatalf("join queued more than one wake: %q", <-queued)
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
		CodexHome: dir, ThreadID: steerTestThread,
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
	newRoute.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, newRoute)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 3)
	statuses := &dynamicSteerStatus{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "normal work"}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{true}, ready: []bool{false}}, nil
		}, func(_ context.Context, _, _, message string) error {
			queued <- message
			if attempts.Add(1) == 1 {
				return &codex.ProcessError{ExitCode: 1, Stderr: "queue rejected"}
			}
			return nil
		}, statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		if !strings.Contains(message, steerSessionBootstrapPrompt) || !strings.Contains(message, steerClaimRecoveryPrompt) || !strings.Contains(message, "normal work") {
			t.Fatalf("initial handoff omitted bootstrap, recovery, or role work: %q", message)
		}
		if !(strings.Index(message, steerSessionBootstrapPrompt) < strings.Index(message, steerClaimRecoveryPrompt) && strings.Index(message, steerClaimRecoveryPrompt) < strings.Index(message, "normal work")) {
			t.Fatalf("initial handoff components were out of order: %q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement thread did not receive its recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "degraded" && items[0].Code == "queue_rejected"
	})
	afterReject, err := persistence.Snapshot(ctx)
	if err != nil || len(afterReject.Deliveries) != 1 || !afterReject.Deliveries[0].RecoveryPending || !afterReject.Deliveries[0].BootstrapPending {
		t.Fatalf("rejected handoff lost bootstrap/recovery markers: %#v err=%v", afterReject, err)
	}

	refreshed, previous, changed, err := registrations.Join(ctx, current)
	if err != nil || previous == nil || changed || refreshed.JoinSignal == current.JoinSignal {
		t.Fatalf("explicit recovery join=%#v previous=%#v changed=%t err=%v", refreshed, previous, changed, err)
	}
	select {
	case message := <-queued:
		if !strings.Contains(message, steerClaimRecoveryPrompt) {
			t.Fatalf("recovery retry message=%q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("explicit join did not retry the pending recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Session == current.ThreadID && items[0].State == "queued"
	})
	time.Sleep(steerPollInterval + 25*time.Millisecond)
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
		CodexHome: dir, ThreadID: steerTestThread,
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
	replacement.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, replacement)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{"reviewer": {
			TicketQueue: "review", NudgePrompt: "review work", ReviewCompletion: ReviewCompletionClose,
		}}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{true}}, nil
		}, func(_ context.Context, _, _, message string) error {
			queued <- message
			if attempts.Add(1) == 1 {
				return errors.New("queue outcome uncertain")
			}
			return nil
		}, statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		parts := []string{steerSessionBootstrapPrompt, steerClaimRecoveryPrompt, "review work", "approve and close"}
		last := -1
		for _, part := range parts {
			index := strings.Index(message, part)
			if index <= last {
				t.Fatalf("combined reviewer recovery message omitted or reordered %q: %q", part, message)
			}
			last = index
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement reviewer did not receive its recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "sending" && items[0].Code == "queue_uncertain"
	})
	afterUncertain, err := persistence.Snapshot(ctx)
	if err != nil || len(afterUncertain.Deliveries) != 1 || !afterUncertain.Deliveries[0].BootstrapPending || !afterUncertain.Deliveries[0].RecoveryPending {
		t.Fatalf("uncertain delivery lost pending flags: %#v err=%v", afterUncertain, err)
	}
	time.Sleep(steerPollInterval + 25*time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("uncertain delivery was retried without join; attempts=%d", attempts.Load())
	}

	refreshed, _, changed, err := registrations.Join(ctx, current)
	if err != nil || changed || refreshed.JoinSignal == current.JoinSignal {
		t.Fatalf("explicit recovery join=%#v changed=%t err=%v", refreshed, changed, err)
	}
	select {
	case <-queued:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit join did not retry uncertain recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Session == current.ThreadID && items[0].State == "queued"
	})
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
		CodexHome: dir, ThreadID: steerTestThread,
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
	newRoute.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, changed, err := registrations.Join(ctx, newRoute)
	if err != nil || !changed {
		t.Fatalf("replace thread changed=%t registration=%#v err=%v", changed, current, err)
	}

	var attempts atomic.Int32
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(runCtx, dir, map[string]steerRolePolicy{
			"coder":     {TicketQueue: "open", NudgePrompt: "coder normal work"},
			"architect": {TicketQueue: "open", NudgePrompt: "architect normal work"},
		}, nil, func(state.SteerRegistration) (steerClient, error) {
			return &sequenceSteerClient{active: []bool{true}, ready: []bool{false}}, nil
		}, func(_ context.Context, _, _, message string) error {
			queued <- message
			if attempts.Add(1) == 1 {
				return &codex.ProcessError{ExitCode: 1, Stderr: "queue rejected"}
			}
			return nil
		}, statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case message := <-queued:
		if !strings.Contains(message, steerClaimRecoveryPrompt) {
			t.Fatalf("initial handoff message=%q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement thread did not receive its recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].State == "degraded" && items[0].Code == "queue_rejected"
	})
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
	select {
	case message := <-queued:
		if !strings.Contains(message, steerClaimRecoveryPrompt) || !strings.Contains(message, "architect normal work") {
			t.Fatalf("role-change handoff message=%q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("same-thread role change lost the pending recovery handoff")
	}
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Session == reassigned.ThreadID && items[0].Role == "architect" && items[0].State == "queued"
	})
	time.Sleep(steerPollInterval + 25*time.Millisecond)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("recovery handoff attempts=%d, want rejected handoff and one role-change retry", got)
	}
	afterAccept, err := persistence.Snapshot(ctx)
	if err != nil || len(afterAccept.Deliveries) != 1 || afterAccept.Deliveries[0].RecoveryPending {
		t.Fatalf("accepted handoff retained recovery marker: %#v err=%v", afterAccept, err)
	}
}
