package cli

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/testutil"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const steerHoldID = "20261025-12345"
const steerPrimaryID = "20261025-12346"

type holdSteerClient struct {
	*signoffBacklogTestClient
	holdFilters  []ticketclient.QueueFilters
	frontierHook func(context.Context) error
	acquireHook  func() error
	acquisitions int
	showHook     func()
}

func (c *holdSteerClient) HoldFrontier(ctx context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	c.holdFilters = append(c.holdFilters, filters)
	if c.frontierHook != nil {
		if err := c.frontierHook(ctx); err != nil {
			return ticketclient.ListResult{}, err
		}
	}
	if limit != 1 || offset != 0 {
		return ticketclient.ListResult{}, ticketclient.ErrProtocol
	}
	ticket := c.tickets[steerHoldID]
	if !orc.EligibleExactSteerEntry(ticket, "hold", filters) {
		return ticketclient.ListResult{}, nil
	}
	return ticketclient.ListResult{Items: []ticketclient.Ticket{ticket}}, nil
}

func (c *holdSteerClient) Show(ctx context.Context, id string) (ticketclient.Ticket, error) {
	if c.showHook != nil {
		c.showHook()
	}
	return c.signoffBacklogTestClient.Show(ctx, id)
}

func newHoldSteerClient() *holdSteerClient {
	return &holdSteerClient{signoffBacklogTestClient: &signoffBacklogTestClient{
		sequenceSteerClient: &sequenceSteerClient{},
		tickets: map[string]ticketclient.Ticket{
			steerHoldID:    {ID: steerHoldID, State: "hold", Tags: []string{"api", "skip-review"}},
			steerPrimaryID: {ID: steerPrimaryID, State: "signoff", Tags: []string{"api"}},
		}, readyID: steerPrimaryID,
	}}
}

func holdSteerPolicy(queue string) steerRolePolicy {
	return steerRolePolicy{TicketQueue: queue, HoldQueue: true,
		QueueFilters:      ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"excluded", "skip-review"}},
		TicketWithoutTags: []string{"excluded"}, ReviewSkipTags: []string{"skip-review"},
		NudgePrompt: "CONFLICTING PRIMARY: ticket next review --claim; approve and close", ReviewCompletion: ReviewCompletionClose}
}

func TestDynamicHoldExactDeliveryAndPrimaryAfterOutcome(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, outcome := range []string{"open", "closed", "user", "another"} {
			t.Run(queue+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				reg := joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
				c := newHoldSteerClient()
				c.ready = []bool{true}
				policy := holdSteerPolicy(queue)
				var messages []string
				runtime := state.NewSteerRuntimeStore(dir)
				router := testSteerRouter(t, func(_ context.Context, _ string, got state.SteerRegistration, m steertransport.Message) error {
					if got.SessionID != reg.SessionID || !reflect.DeepEqual(got.Transport, reg.Transport) {
						t.Fatalf("changed harness route: %#v", got)
					}
					messages = append(messages, m.Text)
					return nil
				})
				runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": policy}, nil,
					func(state.SteerRegistration) (steerClient, error) { return c, nil }, router, &dynamicSteerStatus{}, runtime, nil, nil, nil)
				runner.bootstrap(ctx)
				want := steerSessionBootstrapPrompt + "\n\n[ticket-orc] " + orc.HoldTaskPrompt(steerHoldID, queue)
				if len(messages) != 1 || messages[0] != want {
					t.Fatalf("hold task boundary: %q", messages)
				}
				for _, unwanted := range []string{"Resolved selector JSON", "CONFLICTING PRIMARY", "For accepted work", "Resolve exactly signoff ticket", "TICKET_ACTOR", "TICKET_REPOSITORY"} {
					if strings.Contains(messages[0], unwanted) {
						t.Errorf("hold prompt contains %q", unwanted)
					}
				}
				if !reflect.DeepEqual(c.holdFilters[0], policy.holdQueueFilters()) {
					t.Fatalf("hold filters=%#v", c.holdFilters)
				}
				assertPending := func() {
					t.Helper()
					snapshot, err := runtime.Snapshot(ctx)
					statuses := runner.statuses.snapshot()
					if err != nil || snapshot.Version != 5 || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != steerHoldID || snapshot.Deliveries[0].TargetQueue != "hold" || snapshot.Deliveries[0].State != "queued" {
						t.Fatalf("pending exact hold: %#v %v", snapshot, err)
					}
					if len(statuses) != 1 || statuses[0].Code != "awaiting_hold_resolution" {
						t.Fatalf("pending status=%#v", statuses)
					}
				}
				assertPending()
				// Neither changed tags/Handoff nor an unrelated actor claim is evidence
				// that this exact owned hold finished. Restart and policy reload
				// retain the same reservation, even when hold input is disabled.
				held := c.tickets[steerHoldID]
				held.Tags = nil
				c.tickets[steerHoldID] = held
				c.activeByQueue = map[string][]bool{"open": {true}}
				policy.HoldQueue = false
				runner.policies.Replace(map[string]steerRolePolicy{"worker": policy})
				runner.dirty.MarkAll()
				runner.repositoryChanged(ctx)
				assertPending()
				runner.close()
				runner = newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": policy}, nil,
					func(state.SteerRegistration) (steerClient, error) { return c, nil }, router, &dynamicSteerStatus{}, runtime, nil, nil, nil)
				runner.bootstrap(ctx)
				runner.registrationTick(ctx, time.Now().Add(24*time.Hour))
				assertPending()
				if len(messages) != 1 {
					t.Fatalf("pending target renudged %d times", len(messages))
				}
				c.activeByQueue = nil
				policy.HoldQueue = true
				runner.policies.Replace(map[string]steerRolePolicy{"worker": policy})
				if outcome == "user" || outcome == "another" {
					held.Assignee = outcome
				} else {
					held.State = outcome
				}
				c.tickets[steerHoldID] = held
				runner.dirty.MarkAll()
				runner.repositoryChanged(ctx)
				status := runner.statuses.snapshot()[0]
				if status.State != "none" {
					t.Fatalf("outcome did not retire hold: %#v", status)
				}
				if outcome == "user" && status.Code != "human_handoff" {
					t.Fatalf("human handoff status=%#v", status)
				}
				if outcome == "another" && status.Code != "assigned_elsewhere" {
					t.Fatalf("other owner status=%#v", status)
				}
				runner.repositoryChanged(ctx) // retirement explicitly dirtied the repository
				if len(messages) != 2 {
					t.Fatalf("primary work did not resume: %q", messages)
				}
				if queue == "review" && !strings.Contains(messages[1], "Resolved selector JSON") {
					t.Fatalf("review primary=%q", messages[1])
				}
				if queue == "signoff" && !strings.Contains(messages[1], orc.SignoffTurnPrompt(steerPrimaryID)) {
					t.Fatalf("signoff primary=%q", messages[1])
				}
			})
		}
	}
}

func TestDynamicHoldArrivalRespectsOutstandingPrimaryAndClaims(t *testing.T) {
	for _, initial := range []string{"idle", "queued", "uncertain", "active"} {
		t.Run(initial, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := newHoldSteerClient()
			held := c.tickets[steerHoldID]
			held.State = "open"
			c.tickets[steerHoldID] = held
			c.ready = []bool{initial == "queued" || initial == "uncertain"}
			if initial == "active" {
				c.activeByQueue = map[string][]bool{"review": {true}}
			}
			var messages []string
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("review")}, nil,
				func(state.SteerRegistration) (steerClient, error) { return c, nil }, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
					messages = append(messages, m.Text)
					if initial == "uncertain" && len(messages) == 1 {
						return errors.New("unknown send result")
					}
					return nil
				}), &dynamicSteerStatus{}, nil, nil, nil, nil)
			runner.bootstrap(ctx)
			held.State = "hold"
			c.tickets[steerHoldID] = held
			runner.dirty.MarkAll()
			runner.repositoryChanged(ctx)
			for i := 0; i < 2; i++ {
				runner.registrationTick(ctx, time.Now().Add(48*time.Hour))
				runner.dirty.MarkAll()
				runner.repositoryChanged(ctx)
			}
			if initial != "idle" {
				if len(messages) != 1 {
					t.Fatalf("outstanding primary or claim allowed another wake: %q", messages)
				}
				if initial != "active" {
					c.activeByQueue = map[string][]bool{"review": {true}}
					runner.dirty.MarkAll()
					runner.repositoryChanged(ctx) // positive claim consumption
				}
				c.activeByQueue = nil
				c.ready = []bool{true}
				runner.dirty.MarkAll()
				runner.repositoryChanged(ctx) // claim completion retires primary
				if initial != "active" && len(messages) != 1 {
					t.Fatalf("retirement pass sent new work: %q", messages)
				}
				runner.repositoryChanged(ctx)
			}
			if len(messages) != 2 || !strings.Contains(messages[1], orc.HoldTurnPrompt(steerHoldID)) {
				t.Fatalf("later hold was not selected before primary: %q", messages)
			}
		})
	}
}

func TestDynamicHoldFiltersAndDisabledInput(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, reason := range []string{"disabled", "required", "excluded", "human", "assigned"} {
			t.Run(queue+"/"+reason, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
				c := newHoldSteerClient()
				p := holdSteerPolicy(queue)
				held := c.tickets[steerHoldID]
				switch reason {
				case "disabled":
					p.HoldQueue = false
				case "required":
					held.Tags = nil
				case "excluded":
					held.Tags = append(held.Tags, "excluded")
				case "human":
					held.Assignee = "user"
				case "assigned":
					held.Assignee = "another"
				}
				c.tickets[steerHoldID] = held
				var messages []string
				runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": p}, nil,
					func(state.SteerRegistration) (steerClient, error) { return c, nil }, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
						messages = append(messages, m.Text)
						return nil
					}), &dynamicSteerStatus{}, nil, nil, nil, nil)
				runner.bootstrap(ctx)
				if len(messages) != 1 || strings.Contains(messages[0], "Assess only held ticket") {
					t.Fatalf("ineligible hold sent: %q", messages)
				}
				if reason == "disabled" && len(c.holdFilters) != 0 {
					t.Fatal("disabled hold input was observed")
				}
			})
		}
	}
}

func TestDynamicHoldDispatchRechecksOwnershipAndClaim(t *testing.T) {
	for _, change := range []string{"open", "user", "other", "tags", "claim"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := newHoldSteerClient()
			shows := 0
			c.showHook = func() {
				shows++
				if shows != 2 {
					return
				}
				held := c.tickets[steerHoldID]
				switch change {
				case "open":
					held.State = "open"
				case "user", "other":
					held.Assignee = change
				case "tags":
					held.Tags = nil
				case "claim":
					c.activeByQueue = map[string][]bool{"review": {true}}
				}
				c.tickets[steerHoldID] = held
			}
			runtime := state.NewSteerRuntimeStore(dir)
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("review")}, nil,
				func(state.SteerRegistration) (steerClient, error) { return c, nil }, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
					if m.Text != steerSessionBootstrapPrompt {
						t.Fatal("stale hold sent")
					}
					return nil
				}), &dynamicSteerStatus{}, runtime, nil, nil, nil)
			runner.bootstrap(ctx)
			snapshot, err := runtime.Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
				t.Fatalf("stale target reserved: %#v %v", snapshot, err)
			}
		})
	}
}

func TestDynamicHoldSelectionDoesNotSurvivePolicyOrRegistrationReplacement(t *testing.T) {
	for _, change := range []string{"reload", "registration", "pause"} {
		t.Run(change, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			dir := t.TempDir()
			reg := joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := newHoldSteerClient()
			barrier := testutil.NewBarrier()
			c.frontierHook = barrier.Arrive
			runtime := state.NewSteerRuntimeStore(dir)
			policies := newSteerPolicyStore(map[string]steerRolePolicy{"worker": holdSteerPolicy("review")})
			gate := newDispatchGate(state.NewDaemonControlStore(dir), state.DaemonRunning)
			messages := 0
			runner := newDynamicSteerRunner(dir, policies, nil, func(state.SteerRegistration) (steerClient, error) { return c, nil }, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				messages++
				return nil
			}), &dynamicSteerStatus{}, runtime, nil, gate, nil)
			done := make(chan struct{})
			t.Cleanup(func() { cancel(); barrier.Release(); waitSteerPass(t, done, "hold bootstrap cleanup"); runner.close() })
			go func() { defer close(done); runner.bootstrap(ctx) }()
			waitSteerBarrier(t, barrier, "hold frontier")
			switch change {
			case "reload":
				p := holdSteerPolicy("review")
				p.HoldQueue = false
				policies.Replace(map[string]steerRolePolicy{"worker": p})
			case "registration":
				if _, _, _, err := state.NewRegistrationStore(dir).Join(ctx, reg); err != nil {
					t.Fatal(err)
				}
			case "pause":
				if _, _, err := gate.Pause(ctx); err != nil {
					t.Fatal(err)
				}
			}
			barrier.Release()
			waitSteerPass(t, done, "suppressed hold selection")
			if messages != 0 {
				t.Fatalf("stale hold dispatched %d times", messages)
			}
			snapshot, err := runtime.Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != "" {
				t.Fatalf("suppressed target reserved: %#v %v", snapshot, err)
			}
		})
	}
}

func TestDynamicHoldUncertainOrRejectedDeliveryIsNeverRetried(t *testing.T) {
	for _, send := range []string{"accepted", "uncertain", "rejected", "completion write fails"} {
		t.Run(send, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := newHoldSteerClient()
			runtime := state.NewSteerRuntimeStore(dir)
			persistence := steerRuntimePersistence(runtime)
			if send == "completion write fails" {
				persistence = failingSteerRuntime{inner: runtime, fail: "signoff_complete"}
			}
			messages := 0
			router := testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
				messages++
				if send == "uncertain" {
					return errors.New("outcome uncertain")
				}
				if send == "rejected" {
					return steertransport.Rejected(errors.New("rejected"))
				}
				return nil
			})
			newRunner := func() *dynamicSteerRunner {
				return newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("review")}, nil, func(state.SteerRegistration) (steerClient, error) { return c, nil }, router, &dynamicSteerStatus{}, persistence, nil, nil, nil)
			}
			runner := newRunner()
			runner.bootstrap(ctx)
			runner.dirty.MarkAll()
			runner.repositoryChanged(ctx)
			runner.registrationTick(ctx, time.Now().Add(7*24*time.Hour))
			runner.close()
			runner = newRunner()
			runner.bootstrap(ctx)
			if messages != 1 {
				t.Fatalf("one-shot delivery sent %d times", messages)
			}
			snapshot, err := runtime.Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 {
				t.Fatalf("snapshot=%#v %v", snapshot, err)
			}
			wantState, wantTarget := "queued", steerHoldID
			if send == "uncertain" || send == "completion write fails" {
				wantState = "sending"
			}
			if send == "rejected" {
				wantState = "degraded"
			}
			if d := snapshot.Deliveries[0]; d.State != wantState || d.TargetTicket != wantTarget {
				t.Fatalf("delivery=%#v", d)
			}
		})
	}
}

func TestDynamicHoldDirtyTriggersObserveJoinReloadAndArrival(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, trigger := range []string{"join", "reload", "arrival"} {
			t.Run(queue+"/"+trigger, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				c := newHoldSteerClient()
				c.readyID = ""
				p := holdSteerPolicy(queue)
				if trigger != "join" {
					joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
				}
				if trigger == "reload" {
					p.HoldQueue = false
				}
				held := c.tickets[steerHoldID]
				if trigger == "arrival" {
					held.State = "open"
					c.tickets[steerHoldID] = held
				}
				var messages []string
				runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": p}, nil, func(state.SteerRegistration) (steerClient, error) { return c, nil }, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
					messages = append(messages, m.Text)
					return nil
				}), &dynamicSteerStatus{}, nil, nil, nil, nil)
				runner.bootstrap(ctx)
				switch trigger {
				case "join":
					joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
					runner.registrationTick(ctx, time.Now())
				case "reload":
					p.HoldQueue = true
					runner.policies.Replace(map[string]steerRolePolicy{"worker": p})
					runner.dirty.MarkAll()
					runner.repositoryChanged(ctx)
				case "arrival":
					held.State = "hold"
					c.tickets[steerHoldID] = held
					runner.dirty.MarkRepository(joinTestRepositoryID)
					runner.repositoryChanged(ctx)
				}
				if len(messages) == 0 || !strings.Contains(messages[len(messages)-1], orc.HoldTurnPrompt(steerHoldID)) {
					t.Fatalf("%s did not dispatch hold: %q", trigger, messages)
				}
			})
		}
	}
}

func TestDynamicExactTargetOriginSurvivesPolicyDisableAndRestart(t *testing.T) {
	for _, origin := range []string{"signoff", "hold"} {
		for _, send := range []string{"accepted", "uncertain"} {
			t.Run(origin+"/"+send, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
				client := newHoldSteerClient()
				// Use the same ID, tags and final state for both task origins. Neither
				// current Ticket state nor current role policy can distinguish them.
				client.tickets[steerHoldID] = ticketclient.Ticket{ID: steerHoldID, State: origin, Tags: []string{"api"}}
				client.readyID = steerHoldID
				policy := holdSteerPolicy("signoff")
				policy.HoldQueue = origin == "hold"
				runtime := state.NewSteerRuntimeStore(dir)
				sends := 0
				router := testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
					// An uncertain signoff wake may leave bootstrap pending after
					// its authoritative outcome. Count exact work separately.
					if !strings.Contains(message.Text, steerHoldID) {
						if message.Text != steerSessionBootstrapPrompt {
							t.Fatalf("unexpected follow-up message: %q", message.Text)
						}
						return nil
					}
					reserved, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
					if err != nil || reserved.Version != 5 || len(reserved.Deliveries) != 1 || reserved.Deliveries[0].TargetTicket != steerHoldID || reserved.Deliveries[0].TargetQueue != origin || reserved.Deliveries[0].State != "sending" {
						t.Fatalf("task origin not durable before transport: %#v err=%v", reserved, err)
					}
					sends++
					if send == "uncertain" {
						return errors.New("unknown queue outcome")
					}
					return nil
				})
				newRunner := func() *dynamicSteerRunner {
					return newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": policy}, nil,
						func(state.SteerRegistration) (steerClient, error) { return client, nil }, router, &dynamicSteerStatus{}, runtime, nil, nil, nil)
				}
				runner := newRunner()
				runner.bootstrap(ctx)
				snapshot, err := runtime.Snapshot(ctx)
				wantState := "queued"
				if send == "uncertain" {
					wantState = "sending"
				}
				if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != steerHoldID || snapshot.Deliveries[0].State != wantState || snapshot.Deliveries[0].TargetQueue != origin || sends != 1 {
					t.Fatalf("initial exact delivery=%#v sends=%d err=%v", snapshot, sends, err)
				}
				policy.HoldQueue = false
				runner.policies.Replace(map[string]steerRolePolicy{"worker": policy})
				held := client.tickets[steerHoldID]
				held.State = "hold"
				client.tickets[steerHoldID] = held
				runner.close()
				runner = newRunner()
				runner.bootstrap(ctx)
				snapshot, err = runtime.Snapshot(ctx)
				if err != nil || len(snapshot.Deliveries) != 1 {
					t.Fatalf("restarted delivery=%#v err=%v", snapshot, err)
				}
				delivery := snapshot.Deliveries[0]
				if origin == "signoff" {
					if delivery.TargetTicket != "" || delivery.TargetQueue != "" || delivery.State != "none" {
						t.Fatalf("ordinary signoff departure did not retire: %#v", delivery)
					}
				} else if delivery.TargetTicket != steerHoldID || delivery.TargetQueue != "hold" || delivery.State != wantState {
					t.Fatalf("disabled input lost outstanding hold: %#v", delivery)
				}
				runner.registrationTick(ctx, time.Now().Add(24*time.Hour))
				if sends != 1 {
					t.Fatalf("reconciliation resent exact target %d times", sends)
				}
			})
		}
	}
}

func TestDynamicHoldWithoutPrimaryBacklog(t *testing.T) {
	for _, primary := range []string{"review", "signoff"} {
		t.Run(primary, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			client := newHoldSteerClient()
			delete(client.tickets, steerPrimaryID)
			client.ready = []bool{false}
			policy := holdSteerPolicy(primary)
			var messages []string
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": policy}, nil,
				func(state.SteerRegistration) (steerClient, error) { return client, nil }, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
					messages = append(messages, message.Text)
					return errors.New("delivery acceptance unknown")
				}), &dynamicSteerStatus{}, nil, nil, nil, nil)
			runner.bootstrap(ctx)
			status := runner.statuses.snapshot()[0]
			if len(messages) != 1 || !strings.Contains(messages[0], orc.HoldTaskPrompt(steerHoldID, primary)) || !status.EffectiveHoldQueue || status.EffectiveTicketQueue != primary || status.TargetQueue != "hold" || status.Code != "queue_uncertain" {
				t.Fatalf("idle hold dispatch: messages=%q status=%+v", messages, status)
			}
			// Complete synchronous observation passes after unrelated activity;
			// neither the activity nor policy disable acknowledges this delivery.
			policy.HoldQueue = false
			runner.policies.Replace(map[string]steerRolePolicy{"worker": policy})
			for i := 0; i < 2; i++ {
				runner.dirty.MarkAll()
				runner.repositoryChanged(ctx)
			}
			if len(messages) != 1 || runner.statuses.snapshot()[0].TargetQueue != "hold" {
				t.Fatal("uncertain hold was replaced or resent")
			}
			item := client.tickets[steerHoldID]
			if primary == "review" {
				item.Assignee = "user"
			} else {
				item.State = "open"
			}
			client.tickets[steerHoldID] = item
			runner.dirty.MarkAll()
			runner.repositoryChanged(ctx)
			want := "hold_resolved"
			if primary == "review" {
				want = "human_handoff"
			}
			if got := runner.statuses.snapshot()[0]; got.Code != want || got.TargetQueue != "hold" {
				t.Fatalf("hold outcome: %+v", got)
			}
			runner.repositoryChanged(ctx)
			// An uncertain combined bootstrap remains pending until an accepted
			// bootstrap; it is control information, not a repeated hold task.
			if got := runner.statuses.snapshot()[0]; got.HoldTicket != steerHoldID || got.HoldOutcome != want {
				t.Fatalf("latest hold outcome lost after primary observation: %+v", got)
			}
			if len(messages) != 2 || messages[1] != steerSessionBootstrapPrompt {
				t.Fatalf("empty primary queue messages=%q", messages)
			}
		})
	}
}

func (c *holdSteerClient) AcquireHold(_ context.Context, id string, filters ticketclient.QueueFilters) (*ticketclient.Ticket, error) {
	c.acquisitions++
	if c.acquireHook != nil {
		if err := c.acquireHook(); err != nil {
			return nil, err
		}
	}
	item := c.tickets[id]
	if !orc.EligibleExactSteerEntry(item, "hold", filters) {
		return nil, nil
	}
	item.Assignee = "actor"
	c.tickets[id] = item
	return &item, nil
}

func (c *holdSteerClient) OwnedHold(context.Context) (*ticketclient.Ticket, error) {
	for _, item := range c.tickets {
		if item.State == "hold" && item.Assignee == "actor" {
			return &item, nil
		}
	}
	return nil, nil
}

func TestDynamicHoldAcquisitionPartialFailures(t *testing.T) {
	for _, failure := range []string{"reservation", "ambiguous acquisition", "lost race", "stale same actor"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := newHoldSteerClient()
			runtime := state.NewSteerRuntimeStore(dir)
			persistence := steerRuntimePersistence(runtime)
			if failure == "reservation" {
				persistence = failingSteerRuntime{inner: runtime, fail: "signoff_reserve"}
			}
			if failure == "ambiguous acquisition" {
				c.acquireHook = func() error {
					item := c.tickets[steerHoldID]
					item.Assignee = "actor"
					c.tickets[steerHoldID] = item
					return errors.New("mutation outcome uncertain")
				}
			}
			if failure == "lost race" {
				c.acquireHook = func() error {
					item := c.tickets[steerHoldID]
					item.Assignee = "winner"
					c.tickets[steerHoldID] = item
					return nil
				}
			}
			if failure == "stale same actor" {
				item := c.tickets[steerHoldID]
				item.Assignee = "actor"
				c.tickets[steerHoldID] = item
			}
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("review")}, nil, func(state.SteerRegistration) (steerClient, error) { return c, nil }, testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
				if failure != "lost race" || m.Text != steerSessionBootstrapPrompt {
					t.Fatal("unowned or uncertain task delivered")
				}
				return nil
			}), &dynamicSteerStatus{}, persistence, nil, nil, nil)
			runner.bootstrap(ctx)
			snapshot, err := runtime.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			d := snapshot.Deliveries[0]
			switch failure {
			case "reservation", "stale same actor":
				if c.acquisitions != 0 || d.TargetTicket != "" {
					t.Fatalf("unexpected acquisition/target: %d %+v", c.acquisitions, d)
				}
			case "lost race":
				if c.tickets[steerHoldID].Assignee != "winner" || d.TargetTicket != "" {
					t.Fatalf("loser mutated winner or retained intent: %+v", d)
				}
			case "ambiguous acquisition":
				if d.TargetTicket != steerHoldID || d.State != "sending" {
					t.Fatalf("uncertainty lost: %+v", d)
				}
				runner.dirty.MarkAll()
				runner.repositoryChanged(ctx)
				if c.acquisitions != 1 || c.tickets[steerHoldID].Assignee != "actor" {
					t.Fatal("uncertain acquisition repeated or released")
				}
			}
		})
	}
}

// Authoritative in-memory Ticket mutations drive each synchronous runner pass.
// Unlike a sequence of ready booleans, both real backlogs stay populated.
type alternatingSteerClient struct{ *holdSteerClient }

func (c *alternatingSteerClient) HoldFrontier(_ context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	var items []ticketclient.Ticket
	for _, item := range c.tickets {
		if orc.EligibleExactSteerEntry(item, "hold", filters) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if offset >= len(items) {
		return ticketclient.ListResult{}, nil
	}
	end := min(offset+limit, len(items))
	return ticketclient.ListResult{Items: items[offset:end], More: end < len(items)}, nil
}
func (c *alternatingSteerClient) ReadyFrontier(_ context.Context, queue string, filters ticketclient.QueueFilters, _ int) (ticketclient.ListResult, error) {
	item := c.tickets[c.readyID]
	if item.State != queue || item.Assignee != "" {
		return ticketclient.ListResult{}, nil
	}
	return ticketclient.ListResult{Items: []ticketclient.Ticket{item}}, nil
}
func (c *alternatingSteerClient) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	for _, item := range c.tickets {
		if item.State == queue && item.Assignee == "actor" {
			return ticketclient.ListResult{Items: []ticketclient.Ticket{item}}, nil
		}
	}
	return ticketclient.ListResult{}, nil
}

func TestDynamicHoldAlternatesPopulatedBacklogs(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := &alternatingSteerClient{newHoldSteerClient()}
			c.tickets = make(map[string]ticketclient.Ticket)
			for i := 0; i < 5; i++ {
				for n, lifecycle := range []string{"hold", queue} {
					id := fmt.Sprintf("20261025-%02d%03d", 11+n, i)
					c.tickets[id] = ticketclient.Ticket{ID: id, State: lifecycle, Tags: []string{"api"}}
				}
			}
			c.readyID = "20261025-12000"
			var messages []string
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy(queue)}, nil,
				func(state.SteerRegistration) (steerClient, error) { return c, nil },
				testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
					messages = append(messages, m.Text)
					return nil
				}), &dynamicSteerStatus{}, nil, nil, nil, nil)
			pass := func() { runner.dirty.MarkAll(); runner.repositoryChanged(ctx) }
			runner.bootstrap(ctx)
			for turn := 0; turn < 8; turn++ {
				if len(messages) != turn+1 {
					t.Fatalf("turn %d: messages=%q", turn, messages)
				}
				id := fmt.Sprintf("20261025-%02d%03d", 11+turn%2, turn/2)
				message := messages[turn]
				if turn%2 == 0 {
					if !strings.Contains(message, orc.HoldTaskPrompt(id, queue)) {
						t.Fatalf("expected hold %s: %q", id, message)
					}
				} else if queue == "signoff" {
					if !strings.Contains(message, orc.SignoffTurnPrompt(id)) {
						t.Fatalf("expected signoff %s: %q", id, message)
					}
				} else if !strings.Contains(message, "Resolved selector JSON") {
					t.Fatalf("expected review: %q", message)
				}
				// Enqueue, including Orc's hold acquisition, proves no consumption.
				pass()
				pass()
				if len(messages) != turn+1 {
					t.Fatal("pending task allowed another dispatch")
				}
				item := c.tickets[id]
				if turn%2 == 1 && queue == "review" {
					item.Assignee = "actor"
					c.tickets[id] = item
					pass() // Ticket active claim positively consumes this primary wake.
					if len(messages) != turn+1 {
						t.Fatal("active claim allowed another dispatch")
					}
				}
				// Hold handoff and primary completion are explicit Ticket evidence.
				item.State, item.Assignee = "signoff", "user"
				if turn%2 == 0 {
					item.State = "hold"
				}
				c.tickets[id] = item
				if turn%2 == 1 {
					c.readyID = fmt.Sprintf("20261025-12%03d", turn/2+1)
				}
				pass() // Retirement, with the other input still populated.
				if len(messages) != turn+1 {
					t.Fatal("retirement bypassed dispatch boundary")
				}
				if turn < 7 {
					runner.repositoryChanged(ctx)
				}
			}
		})
	}
}

func TestDynamicLostHoldRaceDispatchesPrimary(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := newHoldSteerClient()
			c.ready = []bool{true}
			c.acquireHook = func() error {
				item := c.tickets[steerHoldID]
				item.Assignee = "winner"
				c.tickets[steerHoldID] = item
				return nil
			}
			var messages []string
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy(queue)}, nil,
				func(state.SteerRegistration) (steerClient, error) { return c, nil },
				testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
					messages = append(messages, m.Text)
					return nil
				}), &dynamicSteerStatus{}, nil, nil, nil, nil)
			runner.bootstrap(ctx)
			if c.acquisitions != 1 || len(messages) != 1 || strings.Contains(messages[0], orc.HoldTaskPrompt(steerHoldID, queue)) {
				t.Fatalf("attempts=%d messages=%q", c.acquisitions, messages)
			}
			if queue == "review" && !strings.Contains(messages[0], "Resolved selector JSON") || queue == "signoff" && !strings.Contains(messages[0], orc.SignoffTurnPrompt(steerPrimaryID)) {
				t.Fatalf("primary did not get the losing hold opportunity: %q", messages)
			}
		})
	}
}

func TestDynamicHoldEmptyPreferredInputUsesOtherImmediately(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			joinDiagnosticSteer(t, dir, "actor", joinTestRepositoryID)
			c := &alternatingSteerClient{newHoldSteerClient()}
			c.tickets[steerPrimaryID] = ticketclient.Ticket{ID: steerPrimaryID, State: "open", Tags: []string{"api"}}
			const nextHold = "20261025-12347"
			c.tickets[nextHold] = ticketclient.Ticket{ID: nextHold, State: "hold", Tags: []string{"api"}}
			var messages []string
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy(queue)}, nil,
				func(state.SteerRegistration) (steerClient, error) { return c, nil },
				testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, m steertransport.Message) error {
					messages = append(messages, m.Text)
					return nil
				}), &dynamicSteerStatus{}, nil, nil, nil, nil)
			runner.bootstrap(ctx)
			item := c.tickets[steerHoldID]
			item.Assignee = "user"
			c.tickets[steerHoldID] = item
			runner.dirty.MarkAll()
			runner.repositoryChanged(ctx) // retire first hold; prefer empty primary
			runner.repositoryChanged(ctx)
			if len(messages) != 2 || !strings.Contains(messages[1], orc.HoldTaskPrompt(nextHold, queue)) {
				t.Fatalf("empty primary blocked the second hold: %q", messages)
			}
			item = c.tickets[nextHold]
			item.Assignee = "user"
			c.tickets[nextHold] = item
			runner.dirty.MarkAll()
			runner.repositoryChanged(ctx)
			runner.repositoryChanged(ctx) // both empty; await a real repository change
			if len(messages) != 2 {
				t.Fatal("idle inputs sent another notification")
			}
			item = c.tickets[steerPrimaryID]
			item.State = queue
			c.tickets[steerPrimaryID] = item
			runner.dirty.MarkAll()
			runner.repositoryChanged(ctx)
			if len(messages) != 3 || strings.Contains(messages[2], "hold assessment") {
				t.Fatalf("idle primary arrival did not dispatch: %q", messages)
			}
		})
	}
}
