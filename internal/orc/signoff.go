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
	if ticket.State != "signoff" || ticket.Assignee != "" {
		return false
	}
	tags := make(map[string]struct{}, len(ticket.Tags))
	for _, tag := range ticket.Tags {
		tags[tag] = struct{}{}
	}
	for _, required := range filters.Tags {
		if _, found := tags[required]; !found {
			return false
		}
	}
	for _, excluded := range filters.WithoutTags {
		if _, found := tags[excluded]; found {
			return false
		}
	}
	return true
}

func ClassifySignoffTarget(state, assignee string) SignoffTargetOutcome {
	if state != "signoff" {
		return SignoffTargetResolved
	}
	if assignee == "" {
		return SignoffTargetPending
	}
	if assignee == "user" {
		return SignoffTargetHumanHandoff
	}
	return SignoffTargetAssignedElsewhere
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
