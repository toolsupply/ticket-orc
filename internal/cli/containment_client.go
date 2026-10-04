package cli

import (
	"context"

	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// containmentTicketClient opens a separate Orc-identity Ticket client only
// when a circuit actually needs a hold mutation.
type containmentTicketClient struct {
	actor      string
	workingDir string
	target     ticketclient.Target
}

func (c containmentTicketClient) HoldTicket(ctx context.Context, id string, options ticketclient.MutationOptions) (ticketclient.MutationResult, error) {
	client, err := ticketclient.NewWithWorkingDirAndTarget(c.actor, c.workingDir, c.target)
	if err != nil {
		return ticketclient.MutationResult{}, err
	}
	defer client.Close()
	return client.HoldTicket(ctx, id, options)
}
