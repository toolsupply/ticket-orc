package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// Retention never alters lifecycle, claim reconciliation or workspace exclusion.
// The lifecycle gate may be held, but never a manager/runtime state mutex.
// A bounded context limits lock acquisition; file IO is one bounded replacement.
func (m *workerManager) retainWorkerFailure(worker supervisor.RunWorker, failure *supervisor.WorkerFailure) {
	if failure == nil || m.localDir == "" {
		return
	}
	record := *failure
	if record.OuterProcess == nil {
		process := harness.ObservedProcess(nil)
		record.OuterProcess = &process
	}
	if record.Detail == "" {
		record.Detail = "no underlying explanation was captured"
	}
	// Namespace comes from the captured launch, never the child's envelope.
	record.RepositoryID, record.Worker = worker.Config.RepositoryIdentity, worker.Name
	record.Role = worker.Config.RoleName
	if record.Role == "" {
		record.Role = string(worker.Config.Role)
	}
	record.Actor = worker.Config.Actor
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.SaveLatestFailure(ctx, m.localDir, record); err != nil {
		if m.stderr != nil {
			m.errorMu.Lock()
			fmt.Fprintf(m.stderr, "[ticket-orc] worker %s diagnostic_persist_failed: latest failure evidence could not be saved (%s)\n", optionalFailureToken(worker.Name), safeRetainedFailureDetail(err))
			m.errorMu.Unlock()
		}
		if m.runtimeEventSink != nil {
			m.runtimeEventSink(supervisor.RuntimeEvent{Type: "worker.diagnostic", Worker: worker.Name, Code: "diagnostic_persist_failed"})
		}
	}
}
