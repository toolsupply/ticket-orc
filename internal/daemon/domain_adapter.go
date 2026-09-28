package daemon

import "github.com/toolsupply/ticket-orc/internal/supervisor"

// MutationResultDTO converts a supervisor outcome to the stable HTTP response
// shape. The supervisor model deliberately carries no serialization tags.
func MutationResultDTO(result supervisor.MutationResult) MutationResult {
	return MutationResult{
		Worker: result.Worker, State: result.State, Applied: result.Applied,
	}
}

// GroupResultDTO adapts every attempted lifecycle result for the control API.
func GroupResultDTO(result supervisor.GroupResult) GroupResult {
	dto := GroupResult{Group: result.Group}
	if result.Results != nil {
		dto.Results = make([]MutationResult, 0, len(result.Results))
	}
	for _, item := range result.Results {
		dto.Results = append(dto.Results, MutationResultDTO(item))
	}
	return dto
}

// ReloadResultDTO adapts a committed supervisor reload outcome to the stable
// control API representation.
func ReloadResultDTO(result supervisor.ReloadResult) ReloadResult {
	dto := ReloadResult{Revision: result.Revision, Applied: result.Applied, Warnings: make([]Diagnostic, 0, len(result.Warnings))}
	for _, warning := range result.Warnings {
		dto.Warnings = append(dto.Warnings, Diagnostic{
			Severity: warning.Severity, Code: warning.Code, Path: warning.Path,
			Worker: warning.Worker, Message: warning.Message, Remediation: warning.Remediation,
		})
	}
	return dto
}

// DoctorResultDTO adapts recovery outcomes and bounded failures to the public
// control API representation.
func DoctorResultDTO(result supervisor.DoctorResult) DoctorResult {
	dto := DoctorResult{Reloaded: result.Reloaded}
	if result.Workers != nil {
		dto.Workers = make([]DoctorWorkerResult, 0, len(result.Workers))
	}
	for _, worker := range result.Workers {
		dto.Workers = append(dto.Workers, DoctorWorkerResult{
			Worker: worker.Worker, Outcome: worker.Outcome, Action: worker.Action,
			Reason: worker.Reason, Failure: WorkerFailureDTO(worker.Failure),
		})
	}
	return dto
}

// WorkerFailureDTO adapts bounded supervisor failure metadata to its public
// JSON representation.
func WorkerFailureDTO(failure *supervisor.WorkerFailure) *WorkerFailure {
	if failure == nil {
		return nil
	}
	return &WorkerFailure{
		Classification: failure.Classification, Phase: failure.Phase,
		ExitCode: failure.ExitCode, NestedExitCode: failure.NestedExitCode,
		Signal: failure.Signal, Contained: failure.Contained, Origin: failure.Origin,
		Operation: failure.Operation, TicketCode: failure.TicketCode,
		TransportCategory: failure.TransportCategory,
		Ticket:            failure.Ticket, Remediation: failure.Remediation,
	}
}

// RepositoryStatusDTO adapts the supervisor's repository observation for the
// daemon status API.
func RepositoryStatusDTO(status supervisor.RepositoryStatus) RepositoryStatus {
	return RepositoryStatus{
		ID: status.ID, Key: status.Key, Name: status.Name, Path: status.Path, State: status.State,
		LastEventAt: status.LastEventAt, LastRestartAt: status.LastRestartAt,
		RestartCount: status.RestartCount, Failure: status.Failure,
	}
}

// RuntimeEventDTO copies only bounded lifecycle classifications and
// identifiers into the public event envelope. It has no free-form Ticket
// message field to serialize.
func RuntimeEventDTO(event supervisor.RuntimeEvent) Event {
	return Event{
		Type: event.Type, Worker: event.Worker, Role: event.Role, State: event.State,
		Phase: event.Phase, Code: event.Code, Ticket: event.Ticket,
		RepositoryID: event.RepositoryID, RepositoryKey: event.RepositoryKey,
		Applied: event.Applied, Failure: WorkerFailureDTO(event.Failure),
	}
}
