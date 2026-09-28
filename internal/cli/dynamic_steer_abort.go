package cli

import (
	"context"
	"errors"

	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const steerAbortPrompt = `[ticket-orc] EMERGENCY STOP.

Stop the current Orc-directed work as soon as it is safe to do so. Do not select, claim, modify, submit, approve, close, or begin additional Ticket work.

The Orc daemon is in emergency-stop mode. Preserve the current repository and Ticket state for operator inspection. Do not attempt rollback unless explicitly instructed.`

type steerAbortResult struct {
	RepositoryID string `json:"repository_id"`
	Actor        string `json:"actor"`
	Outcome      string `json:"outcome"`
	Code         string `json:"code,omitempty"`
}

func requestConfiguredSteerAbort(
	ctx context.Context,
	config RunConfig,
	clients steerClientFactory,
	queue steerQueue,
) ([]steerAbortResult, error) {
	steerDir := config.StateDir
	return requestDynamicSteerAbort(ctx, config.StateDir, steerDir, config.SteerRoles, config.Runtime, clients, queue, nil, nil)
}

// requestDynamicSteerAbort makes one bounded, non-durable stop request for
// each current steer registration with evidence that it may be working. It
// deliberately does not consult the normal dispatch gate or change delivery
// state: aborted mode closes ordinary dispatch, while these requests are
// emergency control messages and are never retried from persisted state.
func requestDynamicSteerAbort(
	ctx context.Context,
	controlStateDir string,
	steerDir string,
	policies map[string]steerRolePolicy,
	runtime *supervisor.RuntimeState[supervisor.RunWorker],
	clients steerClientFactory,
	queue steerQueue,
	observer *registrationObserver,
	persistence steerRuntimePersistence,
) ([]steerAbortResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(policies) == 0 {
		return nil, nil
	}
	controlState, err := state.NewDaemonControlStore(controlStateDir).DaemonControl(ctx)
	if err != nil {
		return nil, err
	}
	if controlState.Mode != state.DaemonAborted {
		return nil, errors.New("steer abort requests require durable aborted mode")
	}
	if clients == nil {
		clients = func(reg state.SteerRegistration) (steerClient, error) {
			return ticketclient.NewWithWorkingDirAndTarget(reg.Actor, reg.RepositoryPath, ticketclient.Target{Repository: reg.RepositoryPath})
		}
	}
	if queue == nil {
		queue = func(ctx context.Context, home, thread, message string) error {
			return codex.New().Queue(ctx, home, thread, message)
		}
	}
	if observer == nil {
		observer = newRegistrationObserver(state.NewRegistrationStore(steerDir))
	}
	if persistence == nil {
		persistence = state.NewSteerRuntimeStore(steerDir)
	}

	observeCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
	observation := observer.Observe(observeCtx)
	cancel()
	if observation.Code != "" {
		if observation.Err != nil {
			return nil, observation.Err
		}
		return nil, errors.New("current steer registrations are unavailable")
	}
	if len(observation.Registrations) == 0 {
		return nil, nil
	}

	snapshotCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
	snapshot, err := persistence.Snapshot(snapshotCtx)
	cancel()
	if err != nil {
		return nil, err
	}
	deliveries := indexSteerDeliveries(snapshot)

	results := make([]steerAbortResult, 0)
	for _, reg := range observation.Registrations {
		policy, knownRole := policies[reg.Role]
		if !knownRole || managedOwner(runtime, reg) {
			continue
		}
		delivery, hasDelivery := deliveries[steerRegistrationKey(reg)]
		outstandingDelivery := hasDelivery && delivery.RegistrationID == reg.RegistrationID &&
			delivery.JoinSignal == reg.JoinSignal && delivery.ThreadID == reg.ThreadID &&
			(delivery.State == "sending" || delivery.State == "queued")

		client, err := clients(reg)
		activeTicket := ""
		if err == nil {
			activeTicket, err = observeSteerActiveTicket(ctx, client, policy)
			_ = client.Close()
		}
		if activeTicket == "" && !outstandingDelivery {
			continue
		}
		currentCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
		current, currentErr := currentSteerRegistration(currentCtx, observer.store, reg)
		cancel()
		if currentErr != nil {
			results = append(results, steerAbortResult{RepositoryID: reg.RepositoryID, Actor: reg.Actor, Outcome: "not_attempted", Code: "registration_unavailable"})
			continue
		}
		if !current {
			continue
		}
		results = append(results, queueSteerAbort(ctx, reg, queue))
	}
	return results, nil
}

func queueSteerAbort(ctx context.Context, reg state.SteerRegistration, queue steerQueue) steerAbortResult {
	queueCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
	defer cancel()
	result := steerAbortResult{RepositoryID: reg.RepositoryID, Actor: reg.Actor}
	if err := queue(queueCtx, reg.CodexHome, reg.ThreadID, steerAbortPrompt); err != nil {
		result.Outcome = "request_failed"
		result.Code = "transport_unavailable"
		return result
	}
	result.Outcome = "stop_requested"
	result.Code = "termination_unconfirmed"
	return result
}
