package supervisor

import (
	"path/filepath"

	"github.com/toolsupply/ticket-orc/internal/orc"
)

func ManagedWorkspace(worker RunWorker) string {
	path, err := filepath.Abs(worker.Config.WorkingDir)
	if err != nil {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	return path
}

// PrepareFailedCoderClaim is called under the lifecycle gate for the exact
// current child's unexpected exit or startup failure. Deliberate stops consume
// the child without entering this path. Workspace protection does not require
// a completed exit; Ticket work requires positive boundary completion and
// unchanged, exclusive configured execution.
func PrepareFailedCoderClaim(worker, desired RunWorker, configured bool, active []RunWorker, cleanupConfirmed bool, outcome *ExitOutcome) bool {
	failure := outcome.Failure
	// Workspace safety does not depend on receiving a final worker envelope.
	// Abrupt leader termination may erase all claim/execution diagnostics while
	// descendants can still write. Only positive pre-execution evidence exempts
	// an ended child; absence of evidence never authorizes another launch.
	unexpectedEnd := outcome.State == WorkerFailed || outcome.State == WorkerStopped
	if unexpectedEnd && !cleanupConfirmed && !knownPreExecutionFailure(failure) {
		if failure == nil {
			failure = &WorkerFailure{Classification: "worker_error"}
			outcome.Failure = failure
		}
		failure.Remediation = orc.CleanupUnconfirmed
		outcome.BlockWorkspace = true
		return false
	}
	if outcome.State != WorkerFailed || worker.Config.Harness != "codex" ||
		(worker.Config.Role != RoleCoder && worker.Config.TicketQueue != "open") || failure == nil || (failure.Execution == nil && failure.AttemptID == 0) {
		return false
	}
	if !cleanupConfirmed {
		return false
	}
	if !configured || desired.Config != worker.Config {
		failure.Remediation = "worker configuration changed; reconcile the exact claim manually"
		return false
	}
	workspace := ManagedWorkspace(worker)
	for _, other := range active {
		otherWorkspace := ManagedWorkspace(other)
		if workspace == "" || otherWorkspace == "" || workspacesOverlap(workspace, otherWorkspace) ||
			(other.Config.Actor == worker.Config.Actor && (other.Config.RepositoryIdentity == "" || worker.Config.RepositoryIdentity == "" || other.Config.RepositoryIdentity == worker.Config.RepositoryIdentity)) {
			failure.Remediation = "execution exclusivity unconfirmed; reconcile the exact claim manually"
			return false
		}
	}
	return true
}

func knownPreExecutionFailure(failure *WorkerFailure) bool {
	if failure == nil {
		return false
	}
	if failure.Classification == "lease_conflict" && failure.Phase == "lease acquisition" && failure.Contained {
		return true // Dedicated pre-execution worker-lease refusal.
	}
	return failure.Classification == "harness_start_failed" && failure.Execution != nil &&
		failure.Execution.Bounded().Process.Start == "not_started"
}
