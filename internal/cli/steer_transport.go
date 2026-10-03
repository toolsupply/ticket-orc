package cli

import (
	"context"
	"errors"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/steertransport/codexqueue"
	"github.com/toolsupply/ticket-orc/internal/steertransport/spool"
)

func newDefaultSteerTransportRouter() (*steertransport.Router, error) {
	return steertransport.NewRouter(codexqueue.New(), spool.New())
}

func reconcileSteerSpoolEndpoints(ctx context.Context, store *state.RegistrationStore, localRoot string) error {
	if store == nil {
		return errors.New("steer registration store is unavailable")
	}
	transport := spool.New()
	return store.WithSnapshot(ctx, func(snapshot state.SteerSnapshot) error {
		return transport.Reconcile(ctx, localRoot, snapshot.Registrations)
	})
}
