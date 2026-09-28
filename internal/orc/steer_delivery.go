package orc

type SteerDeliveryState string

const (
	SteerIdle     SteerDeliveryState = "none"
	SteerSending  SteerDeliveryState = "sending"
	SteerQueued   SteerDeliveryState = "queued"
	SteerConsumed SteerDeliveryState = "consumed"
	SteerDegraded SteerDeliveryState = "degraded"
)

type Evidence struct{ Active, Ready bool }

type Action struct {
	Next          SteerDeliveryState
	Queue, Retire bool
	Code          string
}

// Advance changes delivery state only from positive Ticket evidence. Queue
// acceptance alone never establishes consumption.
func AdvanceSteerDelivery(state SteerDeliveryState, e Evidence) Action {
	switch state {
	case SteerIdle:
		if e.Active {
			return Action{Next: SteerIdle, Code: "busy"}
		}
		if e.Ready {
			return Action{Next: SteerSending, Queue: true, Code: "ready"}
		}
		return Action{Next: SteerIdle, Code: "idle"}
	case SteerSending:
		if e.Active {
			return Action{Next: SteerConsumed, Code: "consumed"}
		}
		return Action{Next: SteerSending, Code: "queue_uncertain"}
	case SteerQueued:
		if e.Active {
			return Action{Next: SteerConsumed, Code: "consumed"}
		}
		return Action{Next: SteerQueued, Code: "awaiting_claim"}
	case SteerConsumed:
		if e.Active {
			return Action{Next: SteerConsumed, Code: "busy"}
		}
		if e.Ready {
			return Action{Next: SteerSending, Queue: true, Retire: true, Code: "ready"}
		}
		return Action{Next: SteerIdle, Retire: true, Code: "idle"}
	case SteerDegraded:
		return Action{Next: SteerDegraded, Code: "queue_rejected"}
	default:
		return Action{Next: SteerDegraded, Code: "invalid_state"}
	}
}
