package cli

import (
	"context"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Called only by ObserveExit after consuming the exact current child, with the
// global lifecycle gate held. The gate keeps reload and launch from overlapping
// reconciliation; no manager-state mutex spans Ticket work or state locking.
func (m *workerManager) classifyManagedExit(child *runChild, err error) supervisor.ExitOutcome {
	outcome := classifyManagedChildExit(child, err)
	defer func() {
		if err != nil || child.failureEnvelope() != nil {
			m.retainWorkerFailure(child.worker, outcome.Failure)
		}
	}()
	worker := child.worker
	failure := outcome.Failure
	boundary := child.boundaryResult()
	select {
	case <-child.waited:
	default:
		boundary.Confirmed = false
	}
	confirmed := boundary.Confirmed && boundary.Err == nil
	// The client is always constructed from captured configured routing, never
	// the envelope's actor. Refuse a reload or active conflicting execution.
	desired, exists := m.lifecycle.Worker(worker.Name)
	var active []supervisor.RunWorker
	for _, other := range m.lifecycle.Children() {
		active = append(active, other.worker)
	}
	if !supervisor.PrepareFailedCoderClaim(worker, desired, exists, active, confirmed, &outcome) {
		return outcome
	}
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()
	lease, leaseErr := state.AcquireLock(ctx, workerLeasePath(worker.Config), 250*time.Millisecond)
	if leaseErr != nil {
		failure.Remediation = "worker execution lease unavailable; reconcile the exact claim manually"
		return outcome
	}
	defer lease.Release()
	info, probeErr := probeRunWorkerTicketTarget(ctx, worker)
	if probeErr != nil || repositoryIdentity(info) != worker.Config.RepositoryIdentity {
		failure.Remediation = "Ticket repository identity unconfirmed; preserve the claim and reconcile routing"
		return outcome
	}
	tickets, clientErr := ticketclient.NewWithWorkingDirAndTarget(worker.Config.Actor, effectiveWorkingDirectory(worker.Config), ticketTarget(worker.Config))
	if clientErr != nil {
		failure.Remediation = "Ticket unavailable; preserve the claim and inspect the failed attempt"
		return outcome
	}
	defer tickets.Close()
	expected := orc.FailureContext{RepositoryID: worker.Config.RepositoryIdentity, Actor: worker.Config.Actor, Worker: worker.Name, Role: worker.Config.RoleName}
	if expected.Role == "" {
		expected.Role = string(worker.Config.Role)
	}
	envelope := &orc.Failure{RepositoryID: failure.RepositoryID, Actor: failure.Actor, Worker: failure.Worker, Role: failure.Role,
		Ticket: failure.Ticket, AttemptID: failure.AttemptID, Classification: failure.Classification, Execution: failure.Execution, Origin: failure.Origin, Operation: failure.Operation}
	store := state.NewForRepository(worker.Config.StateDir, worker.Config.RepositoryIdentity)
	holder := containmentTicketClient{actor: orc.ContainmentActor(worker.Config.InstanceID), workingDir: effectiveWorkingDirectory(worker.Config), target: ticketTarget(worker.Config)}
	var reconcileErr error
	failure.Remediation, reconcileErr = orc.ReconcileFailedCoderClaim(ctx, expected, envelope, confirmed, store, tickets, holder)
	if reconcileErr != nil {
		failure.Remediation += "; reconciliation incomplete; inspect Ticket and Orc state before continuing"
	}
	return outcome
}
