package orc

// Event is the process-local, privacy-safe event record emitted by worker
// loops. The supervisor translates it to the daemon event envelope.
type Event struct {
	Type    string   `json:"type"`
	Worker  string   `json:"worker,omitempty"`
	Role    string   `json:"role,omitempty"`
	State   string   `json:"state,omitempty"`
	Phase   string   `json:"phase,omitempty"`
	Code    string   `json:"code,omitempty"`
	Ticket  string   `json:"ticket,omitempty"`
	Applied bool     `json:"mutation_applied,omitempty"`
	Failure *Failure `json:"failure,omitempty"`
}

// Failure is a bounded child-to-supervisor diagnostic. It carries only safe
// local classification metadata; raw errors, argv, prompts, credentials,
// environment values, and session contents never cross the child boundary.
type Failure struct {
	Classification    string `json:"classification"`
	Phase             string `json:"phase"`
	ExitCode          int    `json:"exit_code,omitempty"`
	NestedExitCode    int    `json:"nested_exit_code,omitempty"`
	Signal            string `json:"signal,omitempty"`
	Contained         bool   `json:"contained,omitempty"`
	Origin            string `json:"origin,omitempty"`
	Operation         string `json:"operation,omitempty"`
	TicketCode        string `json:"ticket_code,omitempty"`
	TransportCategory string `json:"transport_category,omitempty"`
	// Ticket is emitted only after full-ID validation at the supervisor
	// boundary; worker and repository identity remain in their enclosing
	// runtime records.
	Ticket      string `json:"ticket,omitempty"`
	Remediation string `json:"remediation,omitempty"`
}

// EventStreamPrefix marks structured worker events on the child diagnostic
// stream. The supervisor consumes these lines before attributing ordinary
// worker diagnostics to the child, so event payloads never reach operators.
const EventStreamPrefix = "[ticket-orc-event] "

// WorkerReadyEventType identifies the one-time readiness signal emitted after
// a role worker has completed its startup gates and is about to enter steady
// polling. It is not a Ticket lifecycle mutation.
const WorkerReadyEventType = "worker.ready"

type EventSink func(Event)

func emitEvent(sink EventSink, event Event) {
	if sink != nil {
		sink(event)
	}
}
