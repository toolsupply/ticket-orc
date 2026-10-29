package cli

import (
	"context"
	"errors"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type steerHoldClient interface {
	steerTargetClient
	OwnedHold(context.Context) (*ticketclient.Ticket, error)
	AcquireHold(context.Context, string, ticketclient.QueueFilters) (*ticketclient.Ticket, error)
	HoldFrontier(context.Context, ticketclient.QueueFilters, int, int) (ticketclient.ListResult, error)
}

func exactSteerFilters(policy steerRolePolicy, queue string) ticketclient.QueueFilters {
	if queue == "hold" {
		return policy.holdQueueFilters()
	}
	return policy.QueueFilters
}

// A false result leaves primary selection to the existing queue path. This is
// called only after actor claims and outstanding deliveries have been checked.
func (runner *dynamicSteerRunner) selectAndSendHold(ctx context.Context, reg state.SteerRegistration, policy steerRolePolicy, client steerClient, generation uint64, status daemon.SteerStatus, bootstrap bool) (daemon.SteerStatus, bool) {
	reader, ok := client.(steerHoldClient)
	if !ok {
		status.State, status.Code = "degraded", "hold_observation_unavailable"
		return status, true
	}
	readCtx, cancelRead := context.WithTimeout(ctx, steerOperationTimeout)
	old, err := reader.OwnedHold(readCtx)
	err = steerObservationError("owned hold observation", err)
	cancelRead()
	if err == nil && old != nil {
		err = steerHoldReconciliationError("owned hold reconciliation", old.ID, old.Assignee, errors.New("held ticket remains assigned without current execution authority"))
		status.Ticket, status.TargetQueue = old.ID, "hold"
	}
	if err != nil {
		runner.recordSignoffObservationError(reg, err)
		status.State, status.Code = "degraded", "ticket_observation_failed"
		status.Failure = steerObservationFailure(err)
		return status, true
	}
	filters := policy.holdQueueFilters()
	// Ticket may return an empty rechecked page with More=true when a candidate
	// changed ownership during its public frontier observation.
	for offset := 0; ; offset++ {
		readCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
		frontier, err := reader.HoldFrontier(readCtx, filters, 1, offset)
		err = steerObservationError("hold frontier", err)
		cancel()
		if err != nil {
			runner.recordSignoffObservationError(reg, err)
			status.State, status.Code = string(orc.SteerIdle), "ticket_observation_failed"
			status.Failure = steerObservationFailure(err)
			return status, true
		}
		for _, candidate := range frontier.Items {
			ticket, err := showSignoffTarget(ctx, reader, candidate.ID)
			if err != nil {
				runner.recordSignoffObservationError(reg, err)
				status.State, status.Code = string(orc.SteerIdle), "ticket_observation_failed"
				status.Failure = steerObservationFailure(err)
				return status, true
			}
			if !orc.EligibleExactSteerEntry(ticket, "hold", filters) {
				continue
			}
			status.Ticket = candidate.ID
			// The shared task stands alone. In particular, a custom primary
			// nudge may contain next/approve/close and cannot wrap a hold task.
			message := "[ticket-orc] " + orc.HoldTaskPrompt(candidate.ID, policy.TicketQueue)
			if bootstrap {
				message = steerSessionBootstrapPrompt + "\n\n" + message
			}
			status = runner.sendSignoffTarget(ctx, reg, policy, reader, generation, status, candidate.ID, "hold", message, bootstrap)
			if status.Code == "target_changed" {
				// A definite lost acquisition/eligibility race leaves no delivery.
				// Give primary work a chance in this pass; never retry the candidate.
				return status, false
			}
			if status.TargetQueue == "hold" {
				status.HoldTicket, status.HoldOutcome = status.Ticket, status.Code
			}
			return status, true
		}
		if !frontier.More {
			return status, false
		}
	}
}
