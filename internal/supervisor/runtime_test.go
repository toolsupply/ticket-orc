package supervisor

import (
	"fmt"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/orc"
)

func TestRuntimeStateConcurrentUpdatesAndSnapshots(t *testing.T) {
	type worker struct{ name, revision string }
	state := NewRuntimeState([]worker{{name: "coder"}}, func(item worker) string { return item.name })
	const writers = 8
	const updates = 100
	var wait sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		writer := writer
		wait.Add(1)
		go func() {
			defer wait.Done()
			for i := 0; i < updates; i++ {
				state.Set(WorkerTransition{Worker: "coder", State: WorkerRunning, Error: fmt.Sprint(writer, ":", i)})
				state.SetEffectiveWorker(worker{name: "coder", revision: fmt.Sprint(writer, ":", i)})
				state.SetRevision(fmt.Sprint(i))
				state.SetRepositoryStatus(RepositoryStatus{Key: "tickets", State: "running"})
				state.Observe(orc.Event{Worker: "coder", Type: "ticket.lifecycle", State: "closed"})
				_ = state.Snapshot()
				_ = state.EffectiveWorkers()
				_ = state.RepositoryStatuses()
				_ = state.Observed("coder")
			}
		}()
	}
	wait.Wait()

	if got := state.Snapshot()["coder"].State; got != WorkerRunning {
		t.Fatalf("worker state = %q, want %q", got, WorkerRunning)
	}
	if got := state.Observed("coder").TicketEvent; got != "ticket lifecycle: closed" {
		t.Fatalf("ticket event = %q", got)
	}
	if got := state.RepositoryStatuses(); len(got) != 1 || got[0].Key != "tickets" {
		t.Fatalf("repository statuses = %#v", got)
	}
}
