package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Attach operation context at the read boundary, before a caller collapses
// failures into a scheduling/status code. The cause stays local.
func steerObservationError(operation string, err error) error {
	return orc.NewFailureContextError(orc.FailureContext{
		Origin: "ticket", Phase: "Ticket observation", Operation: operation,
	}, err)
}

func steerObservationFailure(err error) *daemon.WorkerFailure {
	failure := classifyWorkerFailure(err)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		failure.Classification = "ticket_observation_timeout"
	case errors.Is(err, context.Canceled):
		failure.Classification = "ticket_observation_canceled"
	case errors.Is(err, ticketclient.ErrClosed):
		failure.Classification = "ticket_client_closed"
	case errors.Is(err, ticketclient.ErrTransport) && failure.Classification == "worker_error":
		failure.Classification = "ticket_transport_failed"
	case failure.Classification == "worker_error":
		failure.Classification = "ticket_observation_failed"
	}
	return daemon.WorkerFailureDTO(failure)
}

// Explanations are derived from safe classification fields, never Error(),
// Ticket messages/stderr, or session-routing parameters.
func consoleSteerFailureDetail(failure *daemon.WorkerFailure) string {
	if failure == nil {
		return ""
	}
	cause := "cause has no safe structured details"
	switch failure.Classification {
	case "ticket_observation_timeout":
		cause = "context deadline exceeded"
	case "ticket_observation_canceled":
		cause = "observation canceled"
	case "ticket_client_closed":
		cause = "Ticket client is closed"
	case "ticket_command_failed":
		cause = "Ticket command rejected the read"
	case "ticket_transport_failed":
		cause = "Ticket process or transport is unavailable"
	case "ticket_protocol_failed":
		cause = "invalid Ticket JSON/protocol response"
	case "child_exit":
		cause = "Ticket process exited"
	case "signal_exit":
		cause = "Ticket process was terminated by a signal"
	}
	operation := consoleField(failure.Operation)
	if operation == "" {
		operation = "observation"
	}
	parts := []string{"Ticket " + operation + " failed: " + cause}
	if failure.TicketCode != "" {
		parts = append(parts, "code="+consoleField(failure.TicketCode))
	}
	if failure.TransportCategory != "" {
		parts = append(parts, "transport="+consoleField(failure.TransportCategory))
	}
	if failure.ExitCode != 0 {
		parts = append(parts, fmt.Sprintf("exit_code=%d", failure.ExitCode))
	}
	if failure.Signal != "" {
		parts = append(parts, "signal="+consoleField(failure.Signal))
	}
	return strings.Join(parts, "; ")
}
