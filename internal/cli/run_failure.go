package cli

import (
	"errors"
	"os/exec"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func classifyWorkerFailure(err error) *supervisor.WorkerFailure {
	return classifyWorkerFailureWithEnvelope(err, nil)
}

func classifyWorkerFailureError(err error) *supervisor.WorkerFailure {
	var controlErr *supervisor.LifecycleError
	if errors.As(err, &controlErr) && controlErr != nil && controlErr.Failure != nil {
		copy := *controlErr.Failure
		return &copy
	}
	return classifyWorkerFailure(err)
}

func classifyWorkerFailureWithEnvelope(err error, envelope *orc.Failure) *supervisor.WorkerFailure {
	if envelope != nil && strings.TrimSpace(envelope.Classification) != "" {
		failure := &supervisor.WorkerFailure{
			Classification:    safeFailureToken(envelope.Classification),
			Phase:             safeFailurePhase(envelope.Phase),
			ExitCode:          envelope.ExitCode,
			NestedExitCode:    envelope.NestedExitCode,
			Signal:            optionalFailureToken(envelope.Signal),
			Contained:         envelope.Contained,
			Origin:            optionalFailureToken(envelope.Origin),
			Operation:         safeFailureOperation(envelope.Operation),
			TicketCode:        optionalFailureToken(envelope.TicketCode),
			TransportCategory: optionalFailureToken(envelope.TransportCategory),
			Ticket:            optionalFailureTicket(envelope.Ticket),
			Remediation:       safeFailureRemediation(envelope.Remediation),
		}
		mergeChildExitStatus(failure, err)
		return failure
	}
	var contextErr *orc.FailureContextError
	if errors.As(err, &contextErr) && contextErr != nil {
		failure := classifyWorkerFailureWithEnvelope(contextErr.Cause, nil)
		if failure == nil {
			failure = &supervisor.WorkerFailure{Classification: "worker_error"}
		}
		metadata := contextErr.Context
		if metadata.Phase != "" {
			failure.Phase = safeFailurePhase(metadata.Phase)
		}
		if metadata.Origin != "" {
			failure.Origin = safeFailureToken(metadata.Origin)
		}
		if metadata.Operation != "" {
			failure.Operation = safeFailureOperation(metadata.Operation)
		}
		if metadata.Ticket != "" {
			failure.Ticket = optionalFailureTicket(metadata.Ticket)
		}
		failure.Contained = failure.Contained || metadata.Contained
		return failure
	}
	failure := &supervisor.WorkerFailure{Phase: "worker operation"}
	if err == nil {
		failure.Classification = "silent_exit"
		return failure
	}
	if errors.Is(err, ErrWorkerLeaseConflict) {
		failure.Classification = "lease_conflict"
		failure.Phase = "lease acquisition"
		failure.Contained = true
		return failure
	}
	var commandErr *ticketclient.CommandError
	if errors.As(err, &commandErr) {
		failure.Classification = "ticket_command_failed"
		failure.TicketCode = optionalFailureToken(commandErr.Code)
		failure.Ticket = optionalFailureTicket(commandErr.TicketID)
		failure.ExitCode = commandErr.ExitCode
		return failure
	}
	var transportErr *ticketclient.TransportError
	if errors.As(err, &transportErr) {
		failure.Classification = "ticket_transport_failed"
		failure.TransportCategory = optionalFailureToken(transportErr.Category)
		if transportErr.ExitCode >= 0 {
			failure.ExitCode = transportErr.ExitCode
		}
		failure.Signal = optionalFailureToken(transportErr.Signal)
		return failure
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		failure.ExitCode = exitErr.ExitCode()
		if failure.ExitCode < 0 {
			failure.Classification = "signal_exit"
			failure.Signal = "terminated"
		} else {
			failure.Classification = "child_exit"
		}
		return failure
	}
	if operationalFailure := classifyOperationalFailure(err); operationalFailure != nil {
		return operationalFailure
	}
	failure.Classification = "worker_error"
	return failure
}

func classifyManagedChildExit(child *runChild, err error) supervisor.ExitOutcome {
	failure := classifyWorkerFailureWithEnvelope(err, child.failureEnvelope())
	if workerLeaseConflictExit(err) {
		failure.Classification = "lease_conflict"
		failure.Phase = "lease acquisition"
		failure.Contained = true
	}
	if workerLeaseConflictExit(err) {
		return supervisor.ExitOutcome{State: WorkerConflict, Failure: failure, TransitionError: errors.New("another worker process owns this worker lease")}
	}
	if err != nil {
		return supervisor.ExitOutcome{State: WorkerFailed, Failure: failure, TransitionError: err}
	}
	return supervisor.ExitOutcome{State: WorkerStopped, Failure: failure}
}

func classifyOperationalFailure(err error) *supervisor.WorkerFailure {
	if err == nil {
		return nil
	}
	failure := &supervisor.WorkerFailure{Phase: "worker operation"}
	switch {
	case errors.Is(err, ticketclient.ErrProtocol):
		failure.Classification = "ticket_protocol_failed"
		failure.Remediation = "inspect Ticket JSON/protocol compatibility before retrying"
	case errors.Is(err, state.ErrLockTimeout):
		failure.Classification = "state_lock_failed"
		failure.Remediation = "inspect Orc state lock ownership before retrying"
	case errors.Is(err, state.ErrMalformed):
		failure.Classification = "state_protocol_failed"
		failure.Remediation = "repair malformed Orc state before retrying"
	default:
		return nil
	}
	return failure
}

func mergeChildExitStatus(failure *supervisor.WorkerFailure, err error) {
	if failure == nil || err == nil {
		return
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return
	}
	// Command/transport envelopes use -1 to mean that the role did not know
	// the process status. Once the supervisor has reaped the child, a
	// nonnegative process status is authoritative; preserve positive transport
	// statuses that describe a distinct command failure.
	if failure.NestedExitCode != 0 && exitErr.ExitCode() >= 0 {
		failure.ExitCode = exitErr.ExitCode()
	} else if exitErr.ExitCode() >= 0 && failure.ExitCode <= 0 {
		failure.ExitCode = exitErr.ExitCode()
	}
	if failure.Signal == "" && exitErr.ExitCode() < 0 {
		failure.Signal = "terminated"
	}
}

func roleFailureEnvelope(err error) *orc.Failure {
	failure := classifyWorkerFailure(err)
	if failure == nil {
		return nil
	}
	return &orc.Failure{
		Classification:    failure.Classification,
		Phase:             failure.Phase,
		ExitCode:          failure.ExitCode,
		Signal:            failure.Signal,
		Contained:         failure.Contained,
		TicketCode:        failure.TicketCode,
		TransportCategory: failure.TransportCategory,
		NestedExitCode:    failure.NestedExitCode,
		Origin:            failure.Origin,
		Operation:         failure.Operation,
		Ticket:            failure.Ticket,
		Remediation:       failure.Remediation,
	}
}

func emitRoleFailure(sink orc.EventSink, worker, role string, err error) {
	if sink == nil || err == nil {
		return
	}
	failure := roleFailureEnvelope(err)
	if failure == nil {
		return
	}
	emit := orc.Event{Type: "worker.failure", Worker: worker, Role: role, Code: failure.Classification, Phase: failure.Phase, Failure: failure}
	sink(emit)
}

func safeFailurePhase(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "worker operation"
	}
	if len(value) > 128 {
		return "worker failure"
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return "worker failure"
		}
	}
	return value
}

func safeFailureToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 64 {
		return "unknown"
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-' && r != '.' {
			return "unknown"
		}
	}
	return value
}

func safeFailureOperation(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) > 96 {
		return "unknown"
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-' && r != ' ' {
			return "unknown"
		}
	}
	return value
}

func optionalFailureToken(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return safeFailureToken(value)
}

func optionalFailureTicket(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || ticketclient.ValidateFullID(value) != nil {
		return ""
	}
	return value
}

func safeFailureRemediation(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 160 {
		return ""
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return ""
		}
	}
	return value
}
