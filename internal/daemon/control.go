package daemon

import (
	"context"
	"fmt"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// ControlError is the daemon's normalized control-plane error representation.
// Supervisor lifecycle errors are translated into this type at the HTTP
// boundary so transport status and JSON fields stay out of supervisor.
type ControlError struct {
	Code    string
	Status  int
	Message string
	Applied bool
	Cause   error
	Result  *MutationResult
	Daemon  *DaemonControlResult
	Failure *WorkerFailure
}

func (e *ControlError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Code, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *ControlError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// MutationResult is the bounded, reconciliable result of a worker operation.
// Applied distinguishes a known state change from a validation or execution
// failure; clients should refresh /v1/status when it is false or uncertain.
type MutationResult struct {
	Worker  string `json:"worker"`
	State   string `json:"state,omitempty"`
	Applied bool   `json:"mutation_applied"`
}

// GroupResult reports every attempted member of a group. A group operation
// can therefore make progress while still returning an error for one member.
type GroupResult struct {
	Group   string           `json:"group"`
	Results []MutationResult `json:"results"`
}

// ReloadResult identifies a successfully swapped configuration snapshot.
type ReloadResult struct {
	Revision string       `json:"revision,omitempty"`
	Applied  bool         `json:"mutation_applied"`
	Warnings []Diagnostic `json:"warnings,omitempty"`
}

// DaemonControlResult reports the durable global dispatch mode after a
// pause, resume, or abort operation.
type DaemonControlResult struct {
	Mode    string                    `json:"mode"`
	Applied bool                      `json:"mutation_applied"`
	Targets []DaemonAbortTargetResult `json:"targets,omitempty"`
}

// DaemonAbortTargetResult is a bounded outcome for one managed or steer abort
// target. It intentionally has no field for paths, prompts, process arguments,
// environment values, or session content.
type DaemonAbortTargetResult struct {
	Kind         string `json:"kind"`
	Worker       string `json:"worker,omitempty"`
	RepositoryID string `json:"repository_id,omitempty"`
	Actor        string `json:"actor,omitempty"`
	Outcome      string `json:"outcome"`
	Code         string `json:"code,omitempty"`
}

// DoctorWorkerResult is a bounded operator-facing recovery outcome. It names
// only the configured worker and a safe next action; opaque harness/session
// identities are intentionally excluded.
type DoctorWorkerResult struct {
	Worker  string         `json:"worker"`
	Outcome string         `json:"outcome"`
	Action  string         `json:"action,omitempty"`
	Reason  string         `json:"reason,omitempty"`
	Failure *WorkerFailure `json:"failure,omitempty"`
}

// WorkerFailure is bounded local process metadata. It deliberately excludes
// raw stderr, argv, prompts, credentials, environment values, and session
// contents.
type WorkerFailure struct {
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
	// Ticket is present only when the worker supplied a validated full ID.
	Ticket      string `json:"ticket,omitempty"`
	Remediation string `json:"remediation,omitempty"`
}

// DoctorResult reports one explicit fleet recovery pass.
type DoctorResult struct {
	Reloaded bool                 `json:"reloaded"`
	Workers  []DoctorWorkerResult `json:"workers"`
}

// Diagnostic is a safe configuration finding shared by reload responses and
// local configuration diagnostics. It never carries opaque config values.
type Diagnostic struct {
	Severity    string `json:"severity"`
	Code        string `json:"code"`
	Path        string `json:"path"`
	Worker      string `json:"worker,omitempty"`
	Message     string `json:"message"`
	Remediation string `json:"remediation"`
}

// Control adapts supervisor lifecycle operations to the daemon's request
// routing. The daemon validates resource names and supplies a server-owned
// bounded context before invoking callbacks. Callbacks must not use the HTTP
// request context.
type Control struct {
	Groups        func() []string
	DaemonMode    func() string
	PauseDaemon   func(context.Context) (DaemonControlResult, error)
	ResumeDaemon  func(context.Context) (DaemonControlResult, error)
	AbortDaemon   func(context.Context) (DaemonControlResult, error)
	StartWorker   func(context.Context, string) (supervisor.MutationResult, error)
	StopWorker    func(context.Context, string) (supervisor.MutationResult, error)
	PauseWorker   func(context.Context, string) (supervisor.MutationResult, error)
	ResumeWorker  func(context.Context, string) (supervisor.MutationResult, error)
	RestartWorker func(context.Context, string) (supervisor.MutationResult, error)
	StartGroup    func(context.Context, string) (supervisor.GroupResult, error)
	StopGroup     func(context.Context, string) (supervisor.GroupResult, error)
	Reload        func(context.Context) (supervisor.ReloadResult, error)
	Doctor        func(context.Context) (supervisor.DoctorResult, error)
	Shutdown      func(context.Context) error
}
