package supervisor

import (
	"context"
	"errors"
	"fmt"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

var ErrRepositoryWorkersRunning = errors.New("stop managed workers in this repository before deleting tickets")

// TicketDeletionCleanupError preserves positive deletion evidence when Orc
// could not invalidate its own bookkeeping. It must never invite a delete retry.
type TicketDeletionCleanupError struct{ Cause error }

func (e *TicketDeletionCleanupError) Error() string {
	return "deleted ticket bookkeeping cleanup failed"
}
func (e *TicketDeletionCleanupError) Unwrap() error { return e.Cause }

// DeleteRepositoryTicket serializes deletion with managed worker lifecycle.
// A running child can write ticket state independently, so require all managed
// children in the repository to stop first (pausing dispatch is insufficient).
// The global lifecycle lock may cover Ticket work; no manager state lock does.
func DeleteRepositoryTicket[C comparable](ctx context.Context, lifecycle *LifecycleManager[RunWorker, C], client *ticketclient.Client, stateDir, repositoryID, id string) (ticketclient.DeleteResult, error) {
	if lifecycle == nil || stateDir == "" || !ticketclient.ValidRepositoryID(repositoryID) {
		return ticketclient.DeleteResult{}, fmt.Errorf("repository deletion coordinator is unavailable")
	}
	lifecycle.Lock()
	defer lifecycle.Unlock()
	for _, child := range lifecycle.Children() {
		worker := lifecycle.hooks.ChildWorker(child)
		identity := worker.Config.RepositoryIdentity
		if worker.TicketInfo != nil && ticketclient.ValidRepositoryID(worker.TicketInfo.ID) {
			identity = worker.TicketInfo.ID
		}
		// Unknown child routing cannot establish that deletion is safe.
		if !ticketclient.ValidRepositoryID(identity) || identity == repositoryID {
			return ticketclient.DeleteResult{}, ErrRepositoryWorkersRunning
		}
	}
	result, err := client.DeleteTicket(ctx, id)
	if (err == nil && result.Changed) || ticketclient.MutationApplied(err) {
		// This is deletion evidence from Ticket, never an inference from Show.
		// State removal is atomic, ticket-wide, and repository-scoped. Retained
		// harness artifacts are left alone; they can no longer be resumed by Orc.
		if _, _, cleanupErr := state.NewForRepository(stateDir, repositoryID).RemoveTicket(ctx, id); cleanupErr != nil {
			return ticketclient.DeleteResult{}, &TicketDeletionCleanupError{Cause: errors.Join(err, cleanupErr)}
		}
	}
	return result, err
}
