package cli

import "github.com/toolsupply/ticket-orc/internal/supervisor"

func NewRuntimeState(workers []supervisor.RunWorker) *supervisor.RuntimeState[supervisor.RunWorker] {
	return supervisor.NewRuntimeState(workers, func(worker supervisor.RunWorker) string { return worker.Name })
}
