package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestRegistrationObserverLossMalformedRecoveryAndJoinRepair(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := state.NewRegistrationStore(dir)
	observer := newRegistrationObserver(store)

	if observation := observer.Observe(ctx); observation.Code != "" || len(observation.Registrations) != 0 {
		t.Fatalf("initial missing registration state=%#v", observation)
	}

	registration := dynamicTestRegistration(t, "coder", "/ticket/project")
	current, _, _, err := store.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	validFile := filepath.Join(dir, "steer.json")
	validBytes, err := os.ReadFile(validFile)
	if err != nil {
		t.Fatal(err)
	}
	if observation := observer.Observe(ctx); observation.Code != "" || len(observation.Registrations) != 1 || observation.Registrations[0] != current {
		t.Fatalf("valid registration observation=%#v", observation)
	}
	manager := &workerManager{
		repositories:      supervisor.RepositoryRegistry{},
		repositoriesByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicReposByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicStatusKeys: map[string]bool{},
		runtime:           NewRuntimeState(nil),
	}
	watchCtx, stopWatcher := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		runDynamicRepositoryDiscovery(watchCtx, dir, manager, func(_ context.Context, registration state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
			return ticketclient.RepositoryInfo{ID: registration.RepositoryID, Path: registration.RepositoryPath}, nil
		})
	}()
	t.Cleanup(func() {
		stopWatcher()
		<-watchDone
	})
	waitForDynamicRepositoryStatus(t, manager, "healthy", "")

	runtime := state.NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []state.SteerRegistration{current}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtime.CompleteDelivery(ctx, current, "queued", true, true); err != nil || !updated {
		t.Fatalf("record queued delivery updated=%t err=%v", updated, err)
	}

	events := []daemon.Event{}
	statuses := &dynamicSteerStatus{publish: func(event daemon.Event) { events = append(events, event) }}
	statuses.replace([]daemon.SteerStatus{{RepositoryID: current.RepositoryID, RepositoryName: current.RepositoryName, Role: current.Role, Actor: current.Actor, Session: current.ThreadID, State: "queued"}})
	if err := os.Remove(validFile); err != nil {
		t.Fatal(err)
	}
	lost := observer.Observe(ctx)
	if lost.Code != "registration_state_lost" || len(lost.Registrations) != 1 || lost.Registrations[0] != current {
		t.Fatalf("lost registration observation=%#v", lost)
	}
	statuses.replace(registrationFailureStatuses(lost))
	waitForDynamicRepositoryStatus(t, manager, "degraded", "registration_state_lost")
	items := statuses.snapshot()
	if len(items) != 1 || items[0].State != "degraded" || items[0].Code != "registration_state_lost" || items[0].RepositoryID != current.RepositoryID || items[0].Actor != current.Actor {
		t.Fatalf("loss status=%#v", items)
	}
	for _, event := range events {
		if event.State == "left" || event.Code == "registration_removed" {
			t.Fatalf("registration loss emitted ordinary leave event: %#v", event)
		}
	}

	if err := os.WriteFile(validFile, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	malformed := observer.Observe(ctx)
	if malformed.Code != "registration_state_malformed" || malformed.Err == nil || len(malformed.Registrations) != 1 || malformed.Registrations[0] != current {
		t.Fatalf("malformed registration observation=%#v", malformed)
	}
	if fresh := newRegistrationObserver(store).Observe(ctx); fresh.Code != "registration_state_malformed" || fresh.Err == nil || len(fresh.Registrations) != 0 {
		t.Fatalf("malformed startup observation=%#v", fresh)
	}
	statuses.replace(registrationFailureStatuses(malformed))
	waitForDynamicRepositoryStatus(t, manager, "degraded", "registration_state_malformed")
	for _, event := range events {
		if event.State == "left" || event.Code == "registration_removed" {
			t.Fatalf("malformed state emitted ordinary leave event: %#v", event)
		}
	}

	if err := os.WriteFile(validFile, validBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	recovered := observer.Observe(ctx)
	if recovered.Code != "" || len(recovered.Registrations) != 1 || recovered.Registrations[0] != current {
		t.Fatalf("recovered registration observation=%#v", recovered)
	}
	waitForDynamicRepositoryStatus(t, manager, "healthy", "")
	if err := runtime.Reconcile(ctx, recovered.Registrations); err != nil {
		t.Fatal(err)
	}
	delivery, err := runtime.Snapshot(ctx)
	if err != nil || len(delivery.Deliveries) != 1 || delivery.Deliveries[0].State != "queued" || delivery.Deliveries[0].Code != "" {
		t.Fatalf("recovered delivery=%#v err=%v", delivery, err)
	}

	if err := os.Remove(validFile); err != nil {
		t.Fatal(err)
	}
	if observation := observer.Observe(ctx); observation.Code != "registration_state_lost" {
		t.Fatalf("second loss observation=%#v", observation)
	}
	waitForDynamicRepositoryStatus(t, manager, "degraded", "registration_state_lost")
	current, previous, changed, err := store.Join(ctx, registration)
	if err != nil || !changed || previous != nil || current.RegistrationID == registration.RegistrationID {
		t.Fatalf("join repair registration=%#v changed=%t err=%v", current, changed, err)
	}
	repaired := observer.Observe(ctx)
	if repaired.Code != "" || len(repaired.Registrations) != 1 || repaired.Registrations[0] != current {
		t.Fatalf("repaired registration observation=%#v", repaired)
	}
	waitForDynamicRepositoryStatus(t, manager, "healthy", "")
	if err := runtime.Reconcile(ctx, repaired.Registrations); err != nil {
		t.Fatal(err)
	}
	delivery, err = runtime.Snapshot(ctx)
	if err != nil || len(delivery.Deliveries) != 1 || delivery.Deliveries[0].RegistrationID != current.RegistrationID || delivery.Deliveries[0].State != "none" {
		t.Fatalf("join repair inherited old-incarnation delivery=%#v err=%v", delivery, err)
	}
}

func waitForDynamicRepositoryStatus(t *testing.T, manager *workerManager, stateName, failure string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		statuses := manager.runtime.RepositoryStatuses()
		if len(statuses) == 1 && statuses[0].ID == dynamicTestRepositoryID && statuses[0].State == stateName && statuses[0].Failure == failure {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("repository status did not reach state=%q failure=%q: %#v", stateName, failure, manager.runtime.RepositoryStatuses())
}
