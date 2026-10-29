package orc

import (
	"testing"

	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestPlanSignoffDeliveryKeepsClaimRecoverySeparate(t *testing.T) {
	tests := []struct {
		name                        string
		target                      bool
		state                       SteerDeliveryState
		active, bootstrap, recovery bool
		want                        SignoffStep
	}{
		{name: "target ignores claim consumption", target: true, state: SteerQueued, active: true, want: SignoffObserveTarget},
		{name: "existing generic claim delivery", state: SteerQueued, active: true, want: SignoffAdvanceExistingDelivery},
		{name: "owned claim recovery", active: true, bootstrap: true, recovery: true, want: SignoffRecoverClaim},
		{name: "clear stale recovery", recovery: true, want: SignoffConfirmNoClaim},
		{name: "bootstrap busy session", active: true, bootstrap: true, want: SignoffBootstrapBusySession},
		{name: "busy actor", active: true, want: SignoffActorBusy},
		{name: "new signoff target", bootstrap: true, want: SignoffSelectTarget},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := PlanSignoffDelivery(test.target, test.state, test.active, test.bootstrap, test.recovery)
			if plan.Step != test.want {
				t.Fatalf("plan step=%v, want %v", plan.Step, test.want)
			}
		})
	}
}

func TestSignoffEligibilityAndTargetOutcomes(t *testing.T) {
	filters := ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"blocked"}}
	for _, test := range []struct {
		name     string
		ticket   ticketclient.Ticket
		eligible bool
		outcome  SignoffTargetOutcome
	}{
		{name: "eligible", ticket: ticketclient.Ticket{State: "signoff", Tags: []string{"api"}}, eligible: true, outcome: SignoffTargetPending},
		{name: "missing required", ticket: ticketclient.Ticket{State: "signoff", Tags: []string{"frontend"}}, outcome: SignoffTargetPending},
		{name: "excluded", ticket: ticketclient.Ticket{State: "signoff", Tags: []string{"api", "blocked"}}, outcome: SignoffTargetPending},
		{name: "assigned", ticket: ticketclient.Ticket{State: "signoff", Assignee: "reviewer", Tags: []string{"api"}}, outcome: SignoffTargetAssignedElsewhere},
		{name: "human", ticket: ticketclient.Ticket{State: "signoff", Assignee: "user", Tags: []string{"api"}}, outcome: SignoffTargetHumanHandoff},
		{name: "resolved", ticket: ticketclient.Ticket{State: "review"}, outcome: SignoffTargetResolved},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := EligibleSignoffEntry(test.ticket, filters); got != test.eligible {
				t.Fatalf("eligible=%t, want %t", got, test.eligible)
			}
			if got := ClassifySignoffTarget(test.ticket.State, test.ticket.Assignee); got != test.outcome {
				t.Fatalf("outcome=%v, want %v", got, test.outcome)
			}
		})
	}
}

func TestExactSteerTargetOutcomesUseRecordedQueue(t *testing.T) {
	for _, test := range []struct {
		queue, state, assignee string
		want                   SignoffTargetOutcome
	}{
		{"signoff", "hold", "", SignoffTargetResolved},
		{"hold", "signoff", "", SignoffTargetResolved},
		{"hold", "hold", "", SignoffTargetPending},
		{"hold", "hold", "user", SignoffTargetHumanHandoff},
		{"hold", "hold", "other", SignoffTargetAssignedElsewhere},
		{"hold", "hold", "assessor", SignoffTargetPending},
		{"signoff", "signoff", "assessor", SignoffTargetAssignedElsewhere},
		{"hold", "closed", "", SignoffTargetResolved},
	} {
		if got := ClassifyExactSteerTarget(test.queue, test.state, test.assignee, "assessor"); got != test.want {
			t.Errorf("recorded=%s current=%s assignee=%q outcome=%v want=%v", test.queue, test.state, test.assignee, got, test.want)
		}
	}
}
