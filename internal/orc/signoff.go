package orc

import "github.com/toolsupply/ticket-orc/internal/ticketclient"

type SignoffTargetOutcome uint8

const (
	SignoffTargetPending SignoffTargetOutcome = iota
	SignoffTargetResolved
	SignoffTargetHumanHandoff
	SignoffTargetAssignedElsewhere
)

type SignoffStep uint8

const (
	SignoffObserveTarget SignoffStep = iota
	SignoffAdvanceExistingDelivery
	SignoffRecoverClaim
	SignoffConfirmNoClaim
	SignoffBootstrapBusySession
	SignoffActorBusy
	SignoffSelectTarget
)

type SignoffPlan struct {
	Step     SignoffStep
	Delivery Action
}

// PlanSignoffDelivery keeps claim-backed recovery separate from an unclaimed
// signoff target. Claims advance only a preexisting generic delivery; they
// never consume a signoff target.
func PlanSignoffDelivery(hasTarget bool, state SteerDeliveryState, active, bootstrapPending, recoveryPending bool) SignoffPlan {
	if hasTarget {
		return SignoffPlan{Step: SignoffObserveTarget}
	}
	if state != "" && state != SteerIdle {
		return SignoffPlan{Step: SignoffAdvanceExistingDelivery, Delivery: AdvanceSteerDelivery(state, Evidence{Active: active})}
	}
	if recoveryPending {
		if active {
			return SignoffPlan{Step: SignoffRecoverClaim}
		}
		return SignoffPlan{Step: SignoffConfirmNoClaim}
	}
	if active {
		if bootstrapPending {
			return SignoffPlan{Step: SignoffBootstrapBusySession}
		}
		return SignoffPlan{Step: SignoffActorBusy}
	}
	return SignoffPlan{Step: SignoffSelectTarget}
}

// EligibleSignoffEntry applies Ticket's selected signoff partition to the
// authoritative Show result before Orc reserves or dispatches an exact target.
func EligibleSignoffEntry(ticket ticketclient.Ticket, filters ticketclient.QueueFilters) bool {
	return EligibleExactSteerEntry(ticket, "signoff", filters)
}

func ClassifySignoffTarget(state, assignee string) SignoffTargetOutcome {
	return ClassifyExactSteerTarget("signoff", state, assignee, "")
}

func SignoffAwaitingCode(state SteerDeliveryState) string {
	switch state {
	case SteerQueued:
		return "awaiting_signoff_outcome"
	case SteerSending:
		return "queue_uncertain"
	case SteerDegraded:
		return "queue_rejected"
	default:
		return "delivery_outstanding"
	}
}

// EligibleExactSteerEntry rechecks an unclaimed target against the selected
// input. Review skip tags belong only to the primary review queue.
func EligibleExactSteerEntry(ticket ticketclient.Ticket, queue string, filters ticketclient.QueueFilters) bool {
	return ticket.State == queue && ticket.Assignee == "" && matchesQueueFilters(ticket, filters)
}

// ClassifyExactSteerTarget uses the task state recorded before delivery.
// Current role policy and unrelated actor claims cannot resolve a held target.
func ClassifyExactSteerTarget(targetQueue, state, assignee, expectedOwner string) SignoffTargetOutcome {
	if state != targetQueue {
		return SignoffTargetResolved
	}
	// Expected hold ownership was acquired before delivery and is not
	// evidence that the external session consumed a wake.
	if assignee == "" || targetQueue == "hold" && assignee == expectedOwner {
		return SignoffTargetPending
	}
	if assignee == "user" {
		return SignoffTargetHumanHandoff
	}
	return SignoffTargetAssignedElsewhere
}

func ExactSteerAwaitingCode(state SteerDeliveryState, queue string) string {
	if state == SteerQueued && queue == "hold" {
		return "awaiting_hold_resolution"
	}
	return SignoffAwaitingCode(state)
}
