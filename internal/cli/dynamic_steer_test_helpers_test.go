package cli

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/testutil"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func testSteerCodexTransport(home string) state.SteerTransportRoute {
	return state.SteerTransportRoute{Kind: "codex-queue", Params: map[string]string{"home": home}}
}

type testSteerTransport struct {
	deliver func(context.Context, string, state.SteerRegistration, steertransport.Message) error
}

func (testSteerTransport) Kind() string { return "codex-queue" }
func (testSteerTransport) Prepare(context.Context, string, state.SteerRegistration) (steertransport.Endpoint, error) {
	return steertransport.Endpoint{Kind: "codex-queue"}, nil
}
func (testSteerTransport) Verify(context.Context, string, state.SteerRegistration) (steertransport.Endpoint, error) {
	return steertransport.Endpoint{Kind: "codex-queue"}, nil
}
func (transport testSteerTransport) Deliver(ctx context.Context, root string, registration state.SteerRegistration, message steertransport.Message) error {
	return transport.deliver(ctx, root, registration, message)
}
func (testSteerTransport) Retire(context.Context, string, state.SteerRegistration) error { return nil }

func testSteerRouter(t *testing.T, deliver func(context.Context, string, state.SteerRegistration, steertransport.Message) error) *steertransport.Router {
	t.Helper()
	router, err := steertransport.NewRouter(testSteerTransport{deliver: deliver})
	if err != nil {
		t.Fatal(err)
	}
	return router
}

func newTestDynamicSteerRunner(t *testing.T, ctx context.Context, stateDir string, roles map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver, gate *dispatchGate, dirty *steerDirtySet) *dynamicSteerRunner {
	t.Helper()
	runner := newDynamicSteerRunner(stateDir, newSteerPolicyStore(roles), runtime, clients, router, statuses, persistence, observer, gate, dirty)
	if runner == nil {
		t.Fatal("dynamic steer runner construction returned nil")
	}
	t.Cleanup(runner.close)
	return runner
}

func takeQueuedSteerMessage(t *testing.T, queued <-chan string) string {
	t.Helper()
	select {
	case message := <-queued:
		return message
	default:
		t.Fatal("synchronous runner pass did not queue a steer message")
		return ""
	}
}

func waitSteerBarrier(t *testing.T, barrier *testutil.Barrier, description string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := barrier.WaitReached(ctx); err != nil {
		t.Fatalf("waiting for %s: %v", description, err)
	}
}

func waitSteerPass(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

type sequenceSteerClient struct {
	mu             sync.Mutex
	active         []bool
	activeByQueue  map[string][]bool
	activeQueueIdx map[string]int
	ai, ri         int
	ready          []bool
	readyQueues    []string
	readyFilters   []ticketclient.QueueFilters
}

type loopAwareSteerClient struct {
	*sequenceSteerClient
	ticket ticketclient.Ticket
}

func (c *loopAwareSteerClient) Show(context.Context, string) (ticketclient.Ticket, error) {
	return c.ticket, nil
}

func (c *loopAwareSteerClient) ShowReadiness(context.Context, string) (ticketclient.TicketDetail, error) {
	return ticketclient.TicketDetail{ID: c.ticket.ID, State: c.ticket.State, Assignee: c.ticket.Assignee, Readiness: &ticketclient.TicketReadiness{Ready: c.ticket.Assignee == ""}}, nil
}

func (c *loopAwareSteerClient) ReadyFrontier(ctx context.Context, queue string, filters ticketclient.QueueFilters, limit int) (ticketclient.ListResult, error) {
	result, err := c.sequenceSteerClient.ReadyFrontier(ctx, queue, filters, limit)
	if err == nil && len(result.Items) > 0 {
		result.Items[0].ID = c.ticket.ID
	}
	return result, err
}

type countingSteerClient struct {
	activeCalls atomic.Int32
	readyCalls  atomic.Int32
	blockNext   atomic.Bool
	activeGate  *testutil.Barrier
}

type dirtyDuringSteerPassClient struct {
	activeCalls atomic.Int32
	markDirty   func()
	armed       atomic.Bool
	once        atomic.Bool
}

func (client *dirtyDuringSteerPassClient) ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error) {
	client.activeCalls.Add(1)
	if client.armed.Load() && client.once.CompareAndSwap(false, true) {
		client.markDirty()
	}
	return ticketclient.ListResult{}, nil
}

func (*dirtyDuringSteerPassClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}

func (*dirtyDuringSteerPassClient) Close() error { return nil }

func (c *countingSteerClient) ActiveClaims(ctx context.Context, _ string, _ int) (ticketclient.ListResult, error) {
	c.activeCalls.Add(1)
	if c.blockNext.CompareAndSwap(true, false) {
		if err := c.activeGate.Arrive(ctx); err != nil {
			return ticketclient.ListResult{}, err
		}
	}
	return ticketclient.ListResult{}, nil
}

func (c *countingSteerClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	c.readyCalls.Add(1)
	return ticketclient.ListResult{}, nil
}

func (c *countingSteerClient) Close() error { return nil }

type readyBarrierSteerClient struct {
	barrier *testutil.Barrier
}

type policyReloadBarrierSteerClient struct {
	mu      sync.Mutex
	barrier *testutil.Barrier
	filters []ticketclient.QueueFilters
	first   bool
}

func (c *policyReloadBarrierSteerClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}

func (c *policyReloadBarrierSteerClient) ReadyFrontier(ctx context.Context, queue string, filters ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	c.mu.Lock()
	c.filters = append(c.filters, filters)
	first := !c.first
	c.first = true
	c.mu.Unlock()
	if first {
		if err := c.barrier.Arrive(ctx); err != nil {
			return ticketclient.ListResult{}, err
		}
	}
	return testTicketList("20260926-00002", queue), nil
}

func (c *policyReloadBarrierSteerClient) Show(context.Context, string) (ticketclient.Ticket, error) {
	return ticketclient.Ticket{ID: "20260926-00002", State: "review"}, nil
}

func (c *policyReloadBarrierSteerClient) ShowReadiness(context.Context, string) (ticketclient.TicketDetail, error) {
	return ticketclient.TicketDetail{ID: "20260926-00002", State: "review", Readiness: &ticketclient.TicketReadiness{Ready: true}}, nil
}

func (c *policyReloadBarrierSteerClient) filterSnapshot() []ticketclient.QueueFilters {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ticketclient.QueueFilters(nil), c.filters...)
}

func (c *policyReloadBarrierSteerClient) Close() error { return nil }

func (c *readyBarrierSteerClient) ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}

func (c *readyBarrierSteerClient) ReadyFrontier(ctx context.Context, queue string, _ ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	if err := c.barrier.Arrive(ctx); err != nil {
		return ticketclient.ListResult{}, err
	}
	return testTicketList("20260926-00002", queue), nil
}

func (c *readyBarrierSteerClient) Close() error { return nil }

type writeFailureThenObservationClient struct {
	calls      atomic.Int32
	secondCall chan struct{}
}

func (c *writeFailureThenObservationClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
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
func (c *writeFailureThenObservationClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	return testTicketList("20260926-00002", "open"), nil
}
func (c *writeFailureThenObservationClient) Close() error { return nil }

func (c *sequenceSteerClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
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
func (c *sequenceSteerClient) ReadyFrontier(_ context.Context, queue string, filters ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readyQueues = append(c.readyQueues, queue)
	c.readyFilters = append(c.readyFilters, filters)
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
func (s *countingSteerRuntime) ReserveSignoffTarget(ctx context.Context, reg state.SteerRegistration, ticketID, targetQueue string) (bool, error) {
	return s.inner.ReserveSignoffTarget(ctx, reg, ticketID, targetQueue)
}
func (s *countingSteerRuntime) CompleteSignoffDelivery(ctx context.Context, reg state.SteerRegistration, ticketID string) (bool, error) {
	return s.inner.CompleteSignoffDelivery(ctx, reg, ticketID)
}
func (s *countingSteerRuntime) CompleteSignoffDeliveryWithIntents(ctx context.Context, reg state.SteerRegistration, ticketID string, bootstrap, recovery bool) (bool, error) {
	return s.inner.CompleteSignoffDeliveryWithIntents(ctx, reg, ticketID, bootstrap, recovery)
}
func (s *countingSteerRuntime) ClearSignoffTarget(ctx context.Context, reg state.SteerRegistration, ticketID string) (bool, error) {
	return s.inner.ClearSignoffTarget(ctx, reg, ticketID)
}
func (s *countingSteerRuntime) RejectSignoffDelivery(ctx context.Context, reg state.SteerRegistration, ticketID string) (bool, error) {
	return s.inner.RejectSignoffDelivery(ctx, reg, ticketID)
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
func (s failingSteerRuntime) ReserveSignoffTarget(ctx context.Context, reg state.SteerRegistration, ticketID, targetQueue string) (bool, error) {
	if s.fail == "signoff_reserve" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.ReserveSignoffTarget(ctx, reg, ticketID, targetQueue)
}
func (s failingSteerRuntime) CompleteSignoffDelivery(ctx context.Context, reg state.SteerRegistration, ticketID string) (bool, error) {
	if s.fail == "signoff_complete" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.CompleteSignoffDelivery(ctx, reg, ticketID)
}
func (s failingSteerRuntime) CompleteSignoffDeliveryWithIntents(ctx context.Context, reg state.SteerRegistration, ticketID string, bootstrap, recovery bool) (bool, error) {
	if s.fail == "signoff_complete" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.CompleteSignoffDeliveryWithIntents(ctx, reg, ticketID, bootstrap, recovery)
}
func (s failingSteerRuntime) ClearSignoffTarget(ctx context.Context, reg state.SteerRegistration, ticketID string) (bool, error) {
	if s.fail == "signoff_clear" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.ClearSignoffTarget(ctx, reg, ticketID)
}
func (s failingSteerRuntime) RejectSignoffDelivery(ctx context.Context, reg state.SteerRegistration, ticketID string) (bool, error) {
	if s.fail == "degraded" || s.fail == "signoff_reject" {
		return false, errors.New("injected persistence error")
	}
	return s.inner.RejectSignoffDelivery(ctx, reg, ticketID)
}

type terminalObservationSteerClient struct {
	closed atomic.Bool
}

func (c *terminalObservationSteerClient) ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, ticketclient.ErrTransport
}
func (c *terminalObservationSteerClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}
func (c *terminalObservationSteerClient) Close() error {
	c.closed.Store(true)
	return nil
}

type successfulObservationSteerClient struct {
	activeCalls atomic.Int32
	readyCalls  atomic.Int32
}

func (c *successfulObservationSteerClient) ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error) {
	c.activeCalls.Add(1)
	return ticketclient.ListResult{}, nil
}

func (c *successfulObservationSteerClient) ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error) {
	c.readyCalls.Add(1)
	return ticketclient.ListResult{}, nil
}

func (c *successfulObservationSteerClient) Close() error { return nil }

func steerStatusForActor(items []daemon.SteerStatus, actor string) daemon.SteerStatus {
	for _, item := range items {
		if item.Actor == actor {
			return item
		}
	}
	return daemon.SteerStatus{}
}

func waitFirstSteerStatus(t *testing.T, statuses *dynamicSteerStatus, check func([]daemon.SteerStatus) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := statuses.waitFirst(ctx); err != nil {
		t.Fatalf("waiting for initial scheduler status: %v", err)
	}
	if items := statuses.snapshot(); !check(items) {
		t.Fatalf("initial scheduler status did not match: %#v", items)
	}
}
