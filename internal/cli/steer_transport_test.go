package cli

import (
	"context"
	"os"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport/spool"
)

func TestDynamicSteerStartupPrunesOrphanSpoolEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	store := state.NewRegistrationStore(dir)
	registration, _, _, err := store.Join(ctx, state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: dir, Actor: "worker", Role: "coder", Harness: "future-harness",
		SessionID: "current-session", Transport: state.SteerTransportRoute{Kind: "spool"},
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	transport := spool.New()
	currentEndpoint, err := transport.Prepare(ctx, dir, registration)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stale := registration
	stale.IncarnationID = "abcdef0123456789abcdef0123456789"
	staleEndpoint, err := transport.Prepare(ctx, dir, stale)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	statuses := &dynamicSteerStatus{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSteer(ctx, dir, map[string]steerRolePolicy{"reviewer": {TicketQueue: "review"}}, nil, nil, nil, statuses)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitSteerStatus(t, statuses, func(items []daemon.SteerStatus) bool {
		return len(items) == 1 && items[0].Code == "unknown_role"
	})
	cancel()
	<-done
	if _, err := os.Stat(currentEndpoint.Root); err != nil {
		t.Fatalf("startup removed current endpoint: %v", err)
	}
	if _, err := os.Lstat(staleEndpoint.Root); !os.IsNotExist(err) {
		t.Fatalf("startup left orphan endpoint: %v", err)
	}
}
