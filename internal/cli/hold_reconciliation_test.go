package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// A stalled Ticket boundary waits for the actual operation context to expire.
// Virtual time makes the deadline the synchronization event, without sleeping
// or injecting a timeout response unrelated to the context supplied by Orc.
type stalledHoldClient struct {
	*holdSteerClient
	stall     string
	contexts  map[string]context.Context
	remaining map[string]time.Duration
}

func (c *stalledHoldClient) observeContext(operation string, ctx context.Context) {
	c.contexts[operation] = ctx
	if deadline, ok := ctx.Deadline(); ok {
		c.remaining[operation] = time.Until(deadline)
	}
}
func (c *stalledHoldClient) OwnedHold(ctx context.Context) (*ticketclient.Ticket, error) {
	c.observeContext("owned hold observation", ctx)
	if c.stall == "owned hold observation" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.holdSteerClient.OwnedHold(ctx)
}
func (c *stalledHoldClient) AcquireHold(ctx context.Context, id string, filters ticketclient.QueueFilters) (*ticketclient.Ticket, error) {
	c.observeContext("acquire hold", ctx)
	item, err := c.holdSteerClient.AcquireHold(ctx, id, filters)
	if c.stall == "acquire hold" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return item, err
}

func TestSteeredHoldOwnershipOperationsBounded(t *testing.T) {
	for _, operation := range []string{"owned hold observation", "acquire hold"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent := context.Background()
				dir := t.TempDir()
				joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
				client := &stalledHoldClient{holdSteerClient: newHoldSteerClient(), stall: operation, contexts: map[string]context.Context{}, remaining: map[string]time.Duration{}}
				runtime := state.NewSteerRuntimeStore(dir)
				gate := newDispatchGate(state.NewDaemonControlStore(dir), state.DaemonRunning)
				runner := newTestDynamicSteerRunner(t, parent, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("review")}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil }, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
					t.Fatal("timed-out ownership dispatched a wake")
					return nil
				}), &dynamicSteerStatus{}, runtime, nil, gate, nil)
				runner.bootstrap(parent)
				if parent.Err() != nil {
					t.Fatal("operation canceled the daemon context")
				}
				for name, ctx := range client.contexts {
					if client.remaining[name] != steerOperationTimeout {
						t.Fatalf("%s deadline remaining=%v", name, client.remaining[name])
					}
					if ctx.Err() == nil {
						t.Fatalf("%s operation context was not cleaned up", name)
					}
				}
				if !errors.Is(client.contexts[operation].Err(), context.DeadlineExceeded) {
					t.Fatalf("stalled operation context: %v", client.contexts[operation].Err())
				}
				item := runner.statuses.snapshot()[0]
				if item.Failure == nil || item.Failure.Operation != operation || item.Failure.Classification != "ticket_observation_timeout" {
					t.Fatalf("timeout status: %+v", item)
				}
				snapshot, err := runtime.Snapshot(parent)
				if err != nil {
					t.Fatal(err)
				}
				d := snapshot.Deliveries[0]
				if operation == "acquire hold" {
					if d.TargetTicket != steerHoldID || d.TargetQueue != "hold" || d.State != "sending" || client.tickets[steerHoldID].Assignee != "actor" {
						t.Fatalf("timeout lost ownership or uncertainty: %+v", d)
					}
					assertHoldReconciliationVisible(t, item, "acquire hold")
					runner.dirty.MarkAll()
					runner.repositoryChanged(parent)
					if client.acquisitions != 1 || client.tickets[steerHoldID].Assignee != "actor" {
						t.Fatal("timed-out mutation retried or released")
					}
				} else if d.TargetTicket != "" || client.acquisitions != 0 {
					t.Fatal("owned-hold read timeout advanced to mutation")
				}
				// These calls complete after the stalled request, proving its dispatch and
				// policy reservations are gone while the daemon context is still live.
				policy := holdSteerPolicy("review")
				policy.HoldQueue = false
				runner.policies.Replace(map[string]steerRolePolicy{"worker": policy})
				if _, _, err := gate.Pause(parent); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func assertHoldReconciliationVisible(t *testing.T, item daemon.SteerStatus, operation string) {
	t.Helper()
	f := item.Failure
	if f == nil || f.Operation != operation || f.Ticket != steerHoldID || f.Actor != "actor" || f.Detail == "" || !strings.Contains(f.Remediation, "Operator must inspect Ticket ownership and delivery evidence") {
		t.Fatalf("missing reconciliation context: %+v", f)
	}
	var out bytes.Buffer
	renderConsoleDetailStatus(&out, daemon.Status{Steer: []daemon.SteerStatus{item}})
	for _, want := range []string{steerHoldID, f.Detail, "actor=actor", f.Remediation} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("console missing %q: %s", want, out.String())
		}
	}
}

func TestSteeredHoldReconciliationAdviceIsSafeAndVisible(t *testing.T) {
	for _, operation := range []string{"owned hold reconciliation", "acquire hold"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			client := newHoldSteerClient()
			if operation == "owned hold reconciliation" {
				held := client.tickets[steerHoldID]
				held.Assignee = "actor"
				client.tickets[steerHoldID] = held
			} else {
				client.acquireHook = func() error {
					held := client.tickets[steerHoldID]
					held.Assignee = "actor"
					client.tickets[steerHoldID] = held
					return errors.New("PRIVATE raw Ticket stderr and credentials")
				}
			}
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("review")}, nil, func(state.SteerRegistration) (steerClient, error) { return client, nil }, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				t.Fatal("reconciliation requires no delivery")
				return nil
			}), &dynamicSteerStatus{}, nil, nil, nil, nil)
			runner.bootstrap(ctx)
			item := runner.statuses.snapshot()[0]
			assertHoldReconciliationVisible(t, item, operation)
			if operation == "owned hold reconciliation" && client.acquisitions != 0 {
				t.Fatal("stale assignment was acquired again")
			}
			if client.tickets[steerHoldID].Assignee != "actor" {
				t.Fatal("assignment changed")
			}
			data, err := json.Marshal(item)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "PRIVATE") || strings.Contains(consoleSteerFailureDetail(item.Failure), "PRIVATE") {
				t.Fatalf("raw cause exposed: %s", data)
			}
		})
	}
}
