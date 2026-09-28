package supervisor

import (
	"fmt"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
)

// WorkerFailure is a bounded description of a worker lifecycle failure. It
// contains domain classifications and identifiers, never process payloads.
type WorkerFailure struct {
	Classification    string
	Phase             string
	ExitCode          int
	NestedExitCode    int
	Signal            string
	Contained         bool
	Origin            string
	Operation         string
	TicketCode        string
	TransportCategory string
	Ticket            string
	Remediation       string
}

// MutationResult records the outcome of a lifecycle operation independently
// of any HTTP or CLI representation.
type MutationResult struct {
	Worker  string
	State   string
	Applied bool
}

// TerminationResult is the bounded outcome of an emergency stop attempt for
// one managed worker.
type TerminationResult struct {
	Worker  string
	Outcome string
}

// GroupResult contains the result for every worker in a group operation.
type GroupResult struct {
	Group   string
	Results []MutationResult
}

// Diagnostic is a bounded configuration finding attached to a supervisor
// reload outcome.
type Diagnostic struct {
	Severity    string
	Code        string
	Path        string
	Worker      string
	Message     string
	Remediation string
}

// ReloadResult reports the supervisor's committed reload outcome.
type ReloadResult struct {
	Revision string
	Applied  bool
	Warnings []Diagnostic
}

// DoctorWorkerResult describes one supervisor recovery attempt.
type DoctorWorkerResult struct {
	Worker  string
	Outcome string
	Action  string
	Reason  string
	Failure *WorkerFailure
}

// DoctorResult reports one explicit supervisor recovery pass.
type DoctorResult struct {
	Reloaded bool
	Workers  []DoctorWorkerResult
}

// LifecycleError describes an operation failure in supervisor terms. The
// daemon maps Code to an HTTP status when adapting this error for its API.
type LifecycleError struct {
	Code     string
	Message  string
	Conflict bool
	Applied  bool
	Cause    error
	Result   *MutationResult
	Failure  *WorkerFailure
}

func (e *LifecycleError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Code, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *LifecycleError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// RepositoryStatus is the supervisor's observation of a configured Ticket
// repository. The daemon converts it to the public status DTO.
type RepositoryStatus struct {
	ID            string
	Key           string
	Name          string
	Path          string
	State         string
	LastEventAt   time.Time
	LastRestartAt time.Time
	RestartCount  int
	Failure       string
}

// RuntimeEvent carries a bounded supervisor observation before the daemon
// assigns transport sequence numbers and serializes its public event DTO.
type RuntimeEvent struct {
	Type          string
	Worker        string
	Role          string
	Actor         string
	State         string
	Phase         string
	Code          string
	Ticket        string
	RepositoryID  string
	RepositoryKey string
	Applied       bool
	Replaced      bool
	Failure       *WorkerFailure
}

// RuntimeEventFromOrc adapts the child protocol's privacy-safe event into the
// supervisor's lifecycle event model. API sequence numbers and JSON fields
// remain the daemon's responsibility.
func RuntimeEventFromOrc(event orc.Event) RuntimeEvent {
	return RuntimeEvent{
		Type: event.Type, Worker: event.Worker, Role: event.Role, State: event.State,
		Phase: event.Phase, Code: event.Code, Ticket: event.Ticket, Applied: event.Applied,
	}
}
