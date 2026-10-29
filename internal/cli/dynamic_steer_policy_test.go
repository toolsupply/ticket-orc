package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/testutil"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestSteerPolicyReplacementWaitsForReservedDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := newSteerPolicyStore(map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "old"}})
		snapshot := store.Snapshot()
		barrier := testutil.NewBarrier()
		dispatchDone := make(chan struct{})
		replacementDone := make(chan struct{})
		replacementStarted := false
		t.Cleanup(func() {
			barrier.Release()
			cancel()
			waitSteerPass(t, dispatchDone, "reserved policy dispatch cleanup")
			if replacementStarted {
				waitSteerPass(t, replacementDone, "policy replacement cleanup")
			}
		})
		go func() {
			defer close(dispatchDone)
			current, err := store.WithGeneration(snapshot.generation, func() error { return barrier.Arrive(ctx) })
			if !current || err != nil {
				t.Errorf("reserved old-generation dispatch current=%t err=%v", current, err)
			}
		}()
		waitSteerBarrier(t, barrier, "reserved old-generation dispatch")

		type replacement struct {
			generation uint64
			changed    bool
		}
		replaced := make(chan replacement, 1)
		started := make(chan struct{})
		replacementStarted = true
		go func() {
			defer close(replacementDone)
			close(started)
			generation, changed := store.Replace(map[string]steerRolePolicy{"coder": {TicketQueue: "review", NudgePrompt: "new"}})
			replaced <- replacement{generation: generation, changed: changed}
		}()
		<-started
		synctest.Wait()
		select {
		case result := <-replaced:
			t.Fatalf("policy generation %+v committed while old dispatch was reserved", result)
		default:
		}
		barrier.Release()
		waitSteerPass(t, dispatchDone, "reserved policy dispatch")
		result := testutil.Receive(t, 5*time.Second, "policy replacement after dispatch release", replaced)
		if !result.changed || result.generation != snapshot.generation+1 {
			t.Fatalf("replacement=%+v, want generation %d and changed", result, snapshot.generation+1)
		}
		if current, err := store.WithGeneration(snapshot.generation, func() error { return nil }); current || err != nil {
			t.Fatalf("old policy dispatch remained available after commit: current=%t err=%v", current, err)
		}
	})
}

func TestConcurrentEquivalentSteerPolicyReplacementAdvancesGenerationOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := newSteerPolicyStore(map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "old"}})
		snapshot := store.Snapshot()
		barrier := testutil.NewBarrier()
		dispatchDone := make(chan struct{})
		replacementsDone := make(chan struct{}, 2)
		replacementCount := 0
		t.Cleanup(func() {
			barrier.Release()
			cancel()
			waitSteerPass(t, dispatchDone, "reserved dispatch cleanup")
			for range replacementCount {
				waitSteerPass(t, replacementsDone, "replacement cleanup")
			}
		})
		go func() {
			defer close(dispatchDone)
			current, err := store.WithGeneration(snapshot.generation, func() error { return barrier.Arrive(ctx) })
			if !current || err != nil {
				t.Errorf("reserved dispatch current=%t err=%v", current, err)
			}
		}()
		waitSteerBarrier(t, barrier, "reserved dispatch")

		type replacement struct {
			generation uint64
			changed    bool
		}
		results := make(chan replacement, 2)
		started := make(chan struct{}, 2)
		replacementCount = 2
		for range 2 {
			go func() {
				defer func() { replacementsDone <- struct{}{} }()
				started <- struct{}{}
				generation, changed := store.Replace(map[string]steerRolePolicy{"coder": {TicketQueue: "review", NudgePrompt: "new"}})
				results <- replacement{generation: generation, changed: changed}
			}()
		}
		<-started
		<-started
		synctest.Wait()
		barrier.Release()
		waitSteerPass(t, dispatchDone, "reserved dispatch with competing replacements")
		first := testutil.Receive(t, 5*time.Second, "first equivalent-policy replacement", results)
		second := testutil.Receive(t, 5*time.Second, "second equivalent-policy replacement", results)
		if first.generation != snapshot.generation+1 || second.generation != snapshot.generation+1 || first.changed == second.changed {
			t.Fatalf("replacements=%+v %+v, want one change at generation %d", first, second, snapshot.generation+1)
		}
	})
}

func TestSteerPolicyReplacementIgnoresCanonicalEquivalentPolicies(t *testing.T) {
	initial := map[string]steerRolePolicy{
		"coder":   {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend", "urgent"}}, NudgePrompt: "work"},
		"quality": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"no-review"}}, NudgePrompt: "review", ReviewCompletion: ReviewCompletionSignoff},
	}
	store := newSteerPolicyStore(initial)
	before := store.Snapshot()
	identicalGeneration, identicalChanged := store.Replace(initial)
	if identicalChanged || identicalGeneration != before.generation {
		t.Fatalf("identical replacement changed policy: changed=%t generation=%d before=%#v", identicalChanged, identicalGeneration, before)
	}
	candidate := map[string]steerRolePolicy{
		"coder":   {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"urgent", "backend"}, WithoutTags: []string{}}, NudgePrompt: "work"},
		"quality": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"no-review"}}, NudgePrompt: "review", ReviewCompletion: ReviewCompletionSignoff},
	}
	generation, changed := store.Replace(candidate)
	after := store.Snapshot()
	if changed || generation != before.generation || after.generation != before.generation || !reflect.DeepEqual(after.roles, before.roles) {
		t.Fatalf("canonical-equivalent replacement changed policy: changed=%t generation=%d before=%#v after=%#v", changed, generation, before, after)
	}
}

func TestSteerPolicyReplacementDetectsEveryEffectivePolicyChange(t *testing.T) {
	base := func() map[string]steerRolePolicy {
		return map[string]steerRolePolicy{
			"coder":   {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend"}}, NudgePrompt: "work"},
			"quality": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"no-review"}}, NudgePrompt: "review", ReviewCompletion: ReviewCompletionSignoff},
		}
	}
	tests := []struct {
		name   string
		mutate func(map[string]steerRolePolicy)
	}{
		{name: "hold queue", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["quality"]
			policy.HoldQueue = true
			roles["quality"] = policy
		}},
		{name: "ticket queue", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["coder"]
			policy.TicketQueue = "review"
			roles["coder"] = policy
		}},
		{name: "required tag", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["coder"]
			policy.QueueFilters.Tags = []string{"frontend"}
			roles["coder"] = policy
		}},
		{name: "excluded review tag", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["quality"]
			policy.QueueFilters.WithoutTags = []string{"no-security-review"}
			roles["quality"] = policy
		}},
		{name: "nudge prompt", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["coder"]
			policy.NudgePrompt = "new work"
			roles["coder"] = policy
		}},
		{name: "review completion", mutate: func(roles map[string]steerRolePolicy) {
			policy := roles["quality"]
			policy.ReviewCompletion = ReviewCompletionClose
			roles["quality"] = policy
		}},
		{name: "role added", mutate: func(roles map[string]steerRolePolicy) {
			roles["triage"] = steerRolePolicy{TicketQueue: "open", NudgePrompt: "triage"}
		}},
		{name: "role removed", mutate: func(roles map[string]steerRolePolicy) { delete(roles, "quality") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSteerPolicyStore(base())
			before := store.Snapshot()
			candidate := base()
			test.mutate(candidate)
			generation, changed := store.Replace(candidate)
			after := store.Snapshot()
			if !changed || generation != before.generation+1 || after.generation != before.generation+1 || !reflect.DeepEqual(after.roles, candidate) {
				t.Fatalf("replacement changed=%t generation=%d snapshot=%#v candidate=%#v", changed, generation, after, candidate)
			}
		})
	}
}

func TestPlanSteerNotificationSeparatesControlAndWork(t *testing.T) {
	tests := []struct {
		name               string
		policy             steerRolePolicy
		state              string
		activeTicket       string
		readyTicket        string
		recoveryPending    bool
		bootstrapPending   bool
		wantSend           bool
		wantWork           bool
		wantCompletion     string
		wantMessageParts   []string
		forbidMessageParts []string
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
			name:   "recovery with active claim omits ready-only reviewer prose",
			policy: steerRolePolicy{TicketQueue: "review", NudgePrompt: "review work", ReviewCompletion: ReviewCompletionClose},
			state:  string(orc.SteerIdle), activeTicket: "20260926-00002", recoveryPending: true,
			wantSend: true, wantWork: false, wantCompletion: string(orc.SteerQueued),
			wantMessageParts:   []string{steerClaimRecoveryPrompt},
			forbidMessageParts: []string{"review work", "approve and close", "Resolved selector JSON"},
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
			for _, part := range test.forbidMessageParts {
				if strings.Contains(plan.message, part) {
					t.Fatalf("plan message %q unexpectedly included %q", plan.message, part)
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

func TestDynamicWorkMessagesBoundCustomPromptsToOneItem(t *testing.T) {
	const oldPrompt = "legacy prompt: process all actionable Ticket work until none remains\nKeep this spacing exactly."
	for _, test := range []struct {
		name   string
		queue  string
		prompt string
	}{
		{name: "renamed coder role with current custom prompt", queue: "open", prompt: "custom coding directions, byte for byte"},
		{name: "renamed reviewer role with persisted old default", queue: "review", prompt: oldPrompt},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := planSteerNotification(steerRolePolicy{TicketQueue: test.queue, NudgePrompt: test.prompt}, string(orc.SteerIdle), "", "20261005-12345", false, false)
			if !plan.workBearing || !strings.Contains(plan.message, steerOneItemTurnPrompt) {
				t.Fatalf("work-bearing message omitted one-item contract: %#v", plan)
			}
			// Keep these expectations independent of steerOneItemTurnPrompt so a
			// semantic reversal in the production constant fails this regression.
			for _, want := range []string{
				"Ticket's authoritative selection and claim flow",
				"at most one actionable item in this role's configured queue",
				"required lifecycle transition, then stop and return",
				"boundary overrides instructions below, including configured prompt text",
				"Do not select or claim another item, even after another notification this turn",
				"If no item is selected, return immediately without changes",
			} {
				if !strings.Contains(plan.message, want) {
					t.Errorf("composed %s work message omitted independent one-item requirement %q: %q", test.queue, want, plan.message)
				}
			}
			parts := strings.Split(plan.message, "\n\n")
			foundExactPrompt := false
			for _, part := range parts {
				if part == test.prompt {
					foundExactPrompt = true
				}
			}
			if !foundExactPrompt {
				t.Fatalf("configured prompt was not preserved as an exact message segment: %q", plan.message)
			}
			if strings.Contains(plan.message, "20261005-12345") || strings.Contains(plan.message, "TICKET_ACTOR") || strings.Contains(plan.message, "TICKET_REPOSITORY") {
				t.Fatalf("generic work message exposed a ticket assignment or identity setup: %q", plan.message)
			}
			if strings.Index(plan.message, steerOneItemTurnPrompt) > strings.Index(plan.message, test.prompt) {
				t.Fatalf("one-item rule must govern configured prompt text: %q", plan.message)
			}
		})
	}
}

func TestDynamicWorkNotificationAndSkillCarryExactResolvedSelector(t *testing.T) {
	config := FileConfig{
		ID: "7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa",
		Roles: map[string]RoleFileConfig{
			"quality": {
				TicketQueue: "review", TicketTags: []string{"urgent", "backend"},
				TicketWithoutTags: []string{"draft", "no-review"}, NudgePrompt: "review assigned partition",
			},
		},
		Review: ReviewFileConfig{SkipTags: []string{"legacy-skip", "no-review"}},
	}
	policies, err := resolveSteerRolePolicies(config, emptyEnv)
	if err != nil {
		t.Fatalf("resolveSteerRolePolicies: %v", err)
	}
	policy := policies["quality"]
	plan := planSteerNotification(policy, string(orc.SteerIdle), "", "20261006-12345", false, false)
	wantSelector := `{"queue":"review","tags":["backend","urgent"],"without_tags":["draft","legacy-skip","no-review"]}`
	if !plan.workBearing || !strings.Contains(plan.message, "Resolved selector JSON: "+wantSelector) {
		t.Fatalf("work notification omitted or changed exact resolved selector %s: %q", wantSelector, plan.message)
	}
	for _, forbidden := range []string{"20261006-12345", "TICKET_ACTOR", "TICKET_REPOSITORY", "TICKET_SCOPE", "TICKET_CONFIG", "delivery metadata"} {
		if strings.Contains(plan.message, forbidden) {
			t.Fatalf("work notification leaked candidate, identity, or delivery metadata: %q", plan.message)
		}
	}
	if strings.Index(plan.message, steerOneItemTurnPrompt) > strings.Index(plan.message, "Resolved selector JSON:") || strings.Index(plan.message, "Resolved selector JSON:") > strings.Index(plan.message, policy.NudgePrompt) || !strings.Contains(plan.message, "For accepted work, approve the ticket to signoff.") {
		t.Fatalf("selector or review suffix is out of order or changed policy: %q", plan.message)
	}

	skill, err := os.ReadFile(filepath.Join("..", "..", "skills", "ticket-orc-worker", "SKILL.md"))
	if err != nil {
		t.Fatalf("read Orc worker Skill: %v", err)
	}
	text := string(skill)
	for _, want := range []string{
		"use its `Resolved selector JSON` exactly",
		"ticket next <queue> [--tag TAG ...] [--without-tag TAG ...] --claim -j",
		"Do not evaluate the JSON as shell code",
		"Do not drop or change Orc-provided filters",
		"Never make a second selection or claim during the same dynamic turn",
		"An existing-claim recovery notification means continue that owned ticket",
		"Bootstrap and control notifications do not assign work",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("Orc worker Skill omitted selector/turn contract %q", want)
		}
	}
	if strings.Contains(text, "Do not narrow the claim based on ticket tags") {
		t.Fatal("Orc worker Skill still prohibits applying Orc-provided tag filters")
	}
}

func TestDefaultDynamicReviewerMessageKeepsReviewContract(t *testing.T) {
	for _, bootstrap := range []bool{false, true} {
		t.Run(fmt.Sprintf("bootstrap=%t", bootstrap), func(t *testing.T) {
			plan := planSteerNotification(steerRolePolicy{
				TicketQueue:      "review",
				NudgePrompt:      reviewerNudgePrompt,
				ReviewCompletion: ReviewCompletionClose,
			}, string(orc.SteerIdle), "", "20261005-35951", false, bootstrap)
			parts := strings.Split(plan.message, "\n\n")
			if bootstrap && (len(parts) != 6 || !strings.Contains(parts[0], "registered for Orc-steered Ticket work") || !strings.Contains(parts[1], "Ticket remains authoritative")) {
				t.Fatalf("bootstrap reviewer message parts=%#v", parts)
			}
			if !bootstrap && len(parts) != 4 {
				t.Fatalf("normal reviewer message parts=%#v", parts)
			}
			for _, want := range []string{
				"at most one actionable item in this role's configured queue",
				"required lifecycle transition, then stop and return",
				"boundary overrides instructions below, including configured prompt text",
				"Do not select or claim another item, even after another notification this turn",
				"If no item is selected, return immediately without changes",
				"substantive independent review",
				"specification compliance",
				"ticket objective and acceptance criteria",
				"correctness, edge cases, integration, error handling, and maintainability",
				"coder has run routine/basic tests",
				"targeted tests only to investigate specific concerns",
				"Set a goal before starting",
				"For accepted work, approve and close the ticket.",
			} {
				if !strings.Contains(plan.message, want) {
					t.Errorf("reviewer message omitted %q: %q", want, plan.message)
				}
			}
			if strings.Index(plan.message, steerOneItemTurnPrompt) > strings.Index(plan.message, reviewerNudgePrompt) || strings.Index(plan.message, reviewerNudgePrompt) > strings.Index(plan.message, "For accepted work, approve and close the ticket.") {
				t.Fatalf("reviewer message components are out of order: %q", plan.message)
			}
		})
	}
}

func TestSteerRecoveryAndBootstrapDoNotSelectFreshWork(t *testing.T) {
	recovery := planSteerNotification(steerRolePolicy{TicketQueue: "review", NudgePrompt: "review work", ReviewCompletion: ReviewCompletionClose}, string(orc.SteerIdle), "20261005-12345", "", true, false)
	for _, want := range []string{"continue that claim", "required lifecycle transition", "stop and return", "Do not select or claim new work", "no current actionable claim", "return immediately without changes"} {
		if !strings.Contains(recovery.message, want) {
			t.Errorf("recovery message %q omitted %q", recovery.message, want)
		}
	}
	if strings.Contains(recovery.message, "review work") || strings.Contains(recovery.message, "approve and close") || strings.Contains(recovery.message, steerOneItemTurnPrompt) {
		t.Fatalf("recovery must remain separate from ready-work and completion policy: %q", recovery.message)
	}

	bootstrap := planSteerNotification(steerRolePolicy{TicketQueue: "open", NudgePrompt: "open work"}, string(orc.SteerIdle), "", "", false, true)
	if bootstrap.message != steerSessionBootstrapPrompt || strings.Contains(bootstrap.message, steerOneItemTurnPrompt) || strings.Contains(bootstrap.message, "open work") {
		t.Fatalf("bootstrap-only notification initiated work: %q", bootstrap.message)
	}
}

func TestTicketOrcWorkerSkillScopesOneItemRuleToDynamicTurns(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "skills", "ticket-orc-worker", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	skill := string(content)
	for _, want := range []string{"dynamic/steered Orc open/review work turn", "at most one actionable item", "stop and return from the turn", "Do not select or claim a second item", "even if another Orc notification arrives", "managed execution", "exact-ticket instruction", "dynamic open/review work-bearing notifications", "exact owned hold notification", "concrete hold task instruction", "ticket query -j id:FULL_ID state:signoff unclaimed :: reassign --to user", "changed: false", "one exact unclaimed signoff entry per wake"} {
		if !strings.Contains(skill, want) {
			t.Errorf("worker Skill omitted %q", want)
		}
	}
	for _, want := range []string{"not a limit on analysis", "refine unowned related planning tickets", "Related planning maintenance does not count as accepting a second ticket", "final integration requires complete prerequisites"} {
		if !strings.Contains(skill, want) {
			t.Errorf("worker Skill omitted architectural planning guidance %q", want)
		}
	}
	for _, unwanted := range []string{"Continue selecting and processing until", "only then conclude the Orc-driven turn", "This inspection does not grant authority to act on another entry's lifecycle"} {
		if strings.Contains(skill, unwanted) {
			t.Errorf("worker Skill retains dynamic queue-drain instruction %q", unwanted)
		}
	}
}

// This barrier-controlled test preserves coverage for pause after a
// ready observation but before dispatch reservation.
func TestPauseAfterReadyObservationBlocksSteerQueueReservation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
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
	router := testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
		queueCalls.Add(1)
		return nil
	})
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
		sendSteerNotification(ctx, dir, registration, plan, router, runtimeStore, statuses, writeFailures, persisted)
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

// This barrier-controlled runner test preserves coverage for policy
// replacement while an older ready-frontier observation is in flight.
func TestDynamicSteerSuppressesReadyObservedUnderSupersededPolicy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	ticketLoops := state.NewForRepository(dir, joinTestRepositoryID)
	if _, _, err := ticketLoops.Participate(ctx, "20260926-00002", 6); err != nil {
		t.Fatal(err)
	}
	barrier := testutil.NewBarrier()
	client := &policyReloadBarrierSteerClient{barrier: barrier}
	policies := newSteerPolicyStore(map[string]steerRolePolicy{
		"coder": {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"old"}}, NudgePrompt: "old policy", MaxBounces: 6, ContainmentActor: "ticket-orc.instance"},
	})
	dirty := newSteerDirtySet()
	queued := make(chan string, 2)
	statuses := &dynamicSteerStatus{}
	initialStatus := registrationStartupStatuses([]state.SteerRegistration{registration}, "")
	initialStatus[0].State = string(orc.SteerIdle)
	applySteerStatusPolicy(&initialStatus[0], policies.Snapshot().roles[registration.Role])
	statuses.replace(initialStatus)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	runner := newDynamicSteerRunner(dir, policies, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			return nil
		}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	t.Cleanup(runner.close)
	go func() {
		defer close(done)
		runner.bootstrap(runCtx)
	}()
	t.Cleanup(func() {
		barrier.Release()
		cancel()
		waitSteerPass(t, done, "policy-replacement runner pass")
	})
	waitSteerBarrier(t, barrier, "old-policy ready query")
	policies.Replace(map[string]steerRolePolicy{
		"coder": {TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"new"}, WithoutTags: []string{"blocked"}}, TicketWithoutTags: []string{"blocked"}, NudgePrompt: "new policy", MaxBounces: 1, ContainmentActor: "ticket-orc.instance"},
	})
	dirty.MarkAll()
	// The old Ticket query is still blocked. Exercise the same projection used
	// by the daemon Status callback before the asynchronous loop can refresh its
	// cached registration status.
	status := runtimeDaemonStatusWithSteerPolicies(nil, statuses.snapshot(), policies)
	if len(status.Steer) != 1 || status.Steer[0].State != string(orc.SteerIdle) || status.Steer[0].EffectiveTicketQueue != "review" ||
		!reflect.DeepEqual(status.Steer[0].EffectiveTicketTags, []string{"new"}) || !reflect.DeepEqual(status.Steer[0].EffectiveTicketWithoutTags, []string{"blocked"}) || len(status.Steer[0].EffectiveReviewSkipTags) != 0 {
		t.Fatalf("status boundary did not project committed policy over blocked old observation: %#v", status.Steer)
	}
	cached := statuses.snapshot()
	if len(cached) != 1 || cached[0].EffectiveTicketQueue != "open" || !reflect.DeepEqual(cached[0].EffectiveTicketTags, []string{"old"}) {
		t.Fatalf("status projection mutated the asynchronous cache: %#v", cached)
	}
	loop, found, err := ticketLoops.TicketLoop(ctx, "20260926-00002")
	if err != nil || !found || loop.EffectiveBounceLimit != 6 {
		t.Fatalf("superseded observation tightened ticket loop=%#v found=%t err=%v", loop, found, err)
	}
	frontierKey := queueForecastFrontierKey(queueForecastOwner{RepositoryID: registration.RepositoryID, Queue: "review", filters: ticketclient.QueueFilters{Tags: []string{"new"}, WithoutTags: []string{"blocked"}}})
	owners, err := queueForecast(ctx, LoadedFileConfig{Instance: InstanceContext{InstanceDir: dir, LocalDir: dir}}, status, func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: map[string]map[string]ticketclient.ListResult{}, frontiers: map[string]ticketclient.ListResult{
			frontierKey: {Items: []ticketclient.Ticket{{ID: "20261002-30001", State: "review"}}},
		}}, nil
	})
	if err != nil || len(owners) != 1 || owners[0].Queue != "review" || owners[0].Next == nil || owners[0].Next.ID != "20261002-30001" {
		t.Fatalf("forecast after committed policy while old query blocked=%#v err=%v", owners, err)
	}
	barrier.Release()
	waitSteerPass(t, done, "superseded-policy runner pass")
	select {
	case message := <-queued:
		t.Fatalf("superseded policy dispatched a notification: %q", message)
	default:
	}
	loop, found, err = ticketLoops.TicketLoop(ctx, "20260926-00002")
	if err != nil || !found || loop.EffectiveBounceLimit != 6 {
		t.Fatalf("stale observation changed ticket loop: loop=%#v found=%t err=%v", loop, found, err)
	}
	dirty.MarkAll()
	runner.repositoryChanged(runCtx)
	message := takeQueuedSteerMessage(t, queued)
	if !strings.Contains(message, "new policy") || strings.Contains(message, "old policy") {
		t.Fatalf("stale or incomplete policy notification=%q", message)
	}
	loop, found, err = ticketLoops.TicketLoop(ctx, "20260926-00002")
	if err != nil || !found || loop.EffectiveBounceLimit != 1 {
		t.Fatalf("current policy did not participate at delivery: loop=%#v found=%t err=%v", loop, found, err)
	}
	filters := client.filterSnapshot()
	if len(filters) < 2 || !reflect.DeepEqual(filters[0], ticketclient.QueueFilters{Tags: []string{"old"}}) || !reflect.DeepEqual(filters[len(filters)-1], ticketclient.QueueFilters{Tags: []string{"new"}, WithoutTags: []string{"blocked"}}) {
		t.Fatalf("ready filters across policy reload=%#v", filters)
	}
	status = runtimeDaemonStatusWithSteerPolicies(nil, statuses.snapshot(), policies)
	if len(status.Steer) != 1 || status.Steer[0].EffectiveTicketQueue != "review" || !reflect.DeepEqual(status.Steer[0].EffectiveTicketTags, []string{"new"}) || !reflect.DeepEqual(status.Steer[0].EffectiveTicketWithoutTags, []string{"blocked"}) || len(status.Steer[0].EffectiveReviewSkipTags) != 0 {
		t.Fatalf("status boundary exposed stale selector after old query completed: %#v", status.Steer)
	}
	policies.Replace(map[string]steerRolePolicy{})
	status = runtimeDaemonStatusWithSteerPolicies(nil, statuses.snapshot(), policies)
	if len(status.Steer) != 1 || status.Steer[0].EffectiveTicketQueue != "" || len(status.Steer[0].EffectiveTicketTags) != 0 || len(status.Steer[0].EffectiveReviewSkipTags) != 0 {
		t.Fatalf("status boundary retained a selector after committed role removal: %#v", status.Steer)
	}
	var calls []string
	owners, err = queueForecast(ctx, LoadedFileConfig{Instance: InstanceContext{InstanceDir: dir, LocalDir: dir}}, status, func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID,
			active: map[string]map[string]ticketclient.ListResult{"worker": {
				"review": {Items: []ticketclient.Ticket{{ID: "20261002-30002", State: "review"}}},
			}}, calls: &calls}, nil
	})
	if err != nil || len(owners) != 1 || len(owners[0].Active) != 1 || owners[0].Active[0].ID != "20261002-30002" || strings.Contains(strings.Join(calls, " "), ":ready:") {
		t.Fatalf("forecast after committed role removal=%#v calls=%v err=%v", owners, calls, err)
	}
}

// This barrier-controlled runner test preserves coverage for config reload
// while the old policy's ready-frontier observation is in flight.
func TestDynamicSteerNoOpPolicyReloadDuringReconciliationPreservesDispatch(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "runtime")
	configPath := filepath.Join(root, "config.json")
	configData := func(tags string) string {
		return fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"roles":{"coder":{"ticket_queue":"open","ticket_tags":%s,"nudge_prompt":"coding work"}}}`, dir, tags)
	}
	writeConfigFixture(t, configPath, configData(`["backend","urgent"]`))
	loaded, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := resolveSteerRolePolicies(loaded.Config, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	policies := newSteerPolicyStore(initial)
	before := policies.Snapshot()
	dirty := newSteerDirtySet()
	manager := newWorkerManager(ctx, RunConfig{
		ConfigPath: configPath, StateDir: dir, InstanceID: loaded.Config.ID,
		steerPolicies: policies, steerDirty: dirty,
	}, os.Args[0], io.Discard, io.Discard, nil)
	defer manager.repositoryWatch.StopObservers()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	barrier := testutil.NewBarrier()
	client := &policyReloadBarrierSteerClient{barrier: barrier}
	queued := make(chan string, 1)
	statuses := &dynamicSteerStatus{}
	initialStatus := registrationStartupStatuses([]state.SteerRegistration{registration}, "")
	initialStatus[0].State = string(orc.SteerIdle)
	applySteerStatusPolicy(&initialStatus[0], before.roles[registration.Role])
	statuses.replace(initialStatus)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	runner := newDynamicSteerRunner(dir, policies, nil,
		func(state.SteerRegistration) (steerClient, error) { return client, nil },
		testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
			queued <- message.Text
			return nil
		}), statuses, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, dirty)
	t.Cleanup(runner.close)
	t.Cleanup(func() {
		barrier.Release()
		cancel()
		waitSteerPass(t, done, "no-op reload runner pass")
	})
	go func() {
		defer close(done)
		runner.bootstrap(runCtx)
	}()
	waitSteerBarrier(t, barrier, "ready query during no-op config reload")
	writeConfigFixture(t, configPath, configData(`["urgent","backend"]`))
	reload, err := manager.reload(ctx)
	if err != nil || !reload.Applied {
		t.Fatalf("canonical-equivalent reload result=%#v err=%v", reload, err)
	}
	if after := policies.Snapshot(); after.generation != before.generation || !reflect.DeepEqual(after.roles, before.roles) {
		t.Fatalf("canonical-equivalent reload changed policy: before=%#v after=%#v", before, after)
	}
	if dirtied := dirty.Drain(); !dirtied.empty() {
		t.Fatalf("canonical-equivalent reload dirtied steering repositories: %#v", dirtied)
	}
	barrier.Release()
	waitSteerPass(t, done, "no-op reload runner pass")
	message := takeQueuedSteerMessage(t, queued)
	if !strings.Contains(message, "coding work") {
		t.Fatalf("notification=%q, want existing policy nudge", message)
	}
	if filters := client.filterSnapshot(); len(filters) != 1 {
		t.Fatalf("no-op reload caused another reconciliation query: %#v", filters)
	}
	if items := statuses.snapshot(); len(items) != 1 || items[0].State != string(orc.SteerQueued) || items[0].EffectiveTicketQueue != "open" {
		t.Fatalf("completed no-op reload status=%#v, want queued work under the original policy", items)
	}
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != string(orc.SteerQueued) || snapshot.Deliveries[0].BootstrapPending {
		t.Fatalf("completed no-op reload delivery=%#v err=%v", snapshot.Deliveries, err)
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
				Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
			}); err != nil {
				t.Fatal(err)
			}
			queued := make(chan string, 1)
			ctx := context.Background()
			runner := newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{test.role: test.policy}, nil,
				func(state.SteerRegistration) (steerClient, error) {
					return &sequenceSteerClient{active: []bool{false}, ready: []bool{true}}, nil
				},
				testSteerRouter(t, func(_ context.Context, _ string, _ state.SteerRegistration, message steertransport.Message) error {
					queued <- message.Text
					return nil
				}),
				&dynamicSteerStatus{}, nil, newRegistrationObserver(state.NewRegistrationStore(dir)), nil, nil)
			runner.bootstrap(ctx)
			select {
			case message := <-queued:
				if !strings.Contains(message, test.policy.NudgePrompt) {
					t.Fatalf("notification omitted role nudge: %q", message)
				}
				if !strings.Contains(message, steerOneItemTurnPrompt) || strings.Index(message, steerOneItemTurnPrompt) > strings.Index(message, test.policy.NudgePrompt) {
					t.Fatalf("renamed role notification did not apply one-item boundary before configured prompt: %q", message)
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
			default:
				t.Fatal("bootstrap pass did not queue ready work")
			}
		})
	}
}
