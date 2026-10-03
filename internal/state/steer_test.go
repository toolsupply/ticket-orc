package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const testSteerRepositoryID = "d659917f-5939-4e93-bfde-6346a0f2bc50"

func testSteerRegistration(t *testing.T) SteerRegistration {
	t.Helper()
	root := t.TempDir()
	repositoryPath := filepath.Join(root, "repository")
	codexHome := filepath.Join(root, "codex")
	if err := os.Mkdir(repositoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	return SteerRegistration{
		RepositoryID: testSteerRepositoryID, RepositoryPath: repositoryPath,
		RepositoryName: "repo", Actor: "reviewer", Role: "reviewer",
		Harness: "codex", SessionID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5",
		Transport: testCodexQueueTransport(codexHome),
	}
}

func testCodexQueueTransport(home string) SteerTransportRoute {
	return SteerTransportRoute{Kind: "codex-queue", Params: map[string]string{"home": home}}
}

func TestRegistrationStoreJoinIncarnationAndDisplayName(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	registration := testSteerRegistration(t)
	first, previous, changed, err := store.Join(ctx, registration)
	if err != nil || previous != nil || !changed || !validSteerRegistrationID(first.RegistrationID) || !validSteerRegistrationID(first.IncarnationID) {
		t.Fatalf("initial join = %#v previous=%#v changed=%v err=%v", first, previous, changed, err)
	}

	rejoin := registration
	current, previous, changed, err := store.Join(ctx, rejoin)
	if err != nil || previous == nil || changed || current.RegistrationID != first.RegistrationID || current.IncarnationID == first.IncarnationID {
		t.Fatalf("same join = %#v previous=%#v changed=%v err=%v", current, previous, changed, err)
	}

	rename := registration
	rename.RepositoryName = "renamed"
	current, _, changed, err = store.Join(ctx, rename)
	if err != nil || changed || current.RepositoryName != "renamed" || current.RegistrationID != first.RegistrationID || current.IncarnationID == first.IncarnationID {
		t.Fatalf("display rename = %#v changed=%v err=%v", current, changed, err)
	}

	changedRole := registration
	changedRole.Role = "architect"
	current, previous, changed, err = store.Join(ctx, changedRole)
	if err != nil || !changed || previous == nil || previous.Role != "reviewer" || current.RegistrationID == first.RegistrationID {
		t.Fatalf("role change = %#v previous=%#v changed=%v err=%v", current, previous, changed, err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil || len(snapshot.Registrations) != 1 {
		t.Fatalf("snapshot = %#v err=%v; want one owner for key", snapshot, err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(store.dir, steerFileName))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("steer file mode=%v err=%v, want 0600", info, err)
		}
	}
}

func TestRegistrationStoreWithSnapshotKeepsJoinLockedDuringCallback(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewRegistrationStore(dir)
	registration, _, _, err := store.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WithSnapshot(ctx, func(snapshot SteerSnapshot) error {
		if len(snapshot.Registrations) != 1 || snapshot.Registrations[0].IncarnationID != registration.IncarnationID {
			t.Fatalf("locked snapshot=%#v", snapshot)
		}
		if _, err := TryAcquireLock(ctx, dir); !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("snapshot callback did not hold registration lock: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrationStorePreparationFailurePreservesPreviousIncarnation(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	previous, _, _, err := store.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	next := testSteerRegistration(t)
	next.Harness = "pi"
	next.Transport = SteerTransportRoute{Kind: "spool"}
	prepareErr := errors.New("endpoint unavailable")
	called := false
	if _, _, _, err := store.JoinWithPreparation(ctx, next, func(candidate SteerRegistration) error {
		called = true
		if candidate.RegistrationID == previous.RegistrationID || candidate.IncarnationID == "" {
			t.Fatalf("candidate was not assigned the new route identity: %#v", candidate)
		}
		return prepareErr
	}); !errors.Is(err, prepareErr) {
		t.Fatalf("preparation error = %v, want %v", err, prepareErr)
	}
	if !called {
		t.Fatal("endpoint preparation was not called")
	}
	current, ok, err := store.Find(ctx, previous.RepositoryID, previous.Actor)
	if err != nil || !ok || current.RegistrationID != previous.RegistrationID || current.IncarnationID != previous.IncarnationID || current.Transport.Kind != previous.Transport.Kind {
		t.Fatalf("previous registration changed after failed preparation: %#v ok=%v err=%v", current, ok, err)
	}
}

func TestRegistrationStoreRoutingChangesCreateIncarnations(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	current, _, _, err := store.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*SteerRegistration){
		func(value *SteerRegistration) { value.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6" },
		func(value *SteerRegistration) { value.Harness = "pi" },
		func(value *SteerRegistration) {
			value.Transport = testCodexQueueTransport(filepath.Join(filepath.Dir(value.Transport.Params["home"]), "other-codex"))
		},
		func(value *SteerRegistration) { value.Transport = SteerTransportRoute{Kind: "spool"} },
		func(value *SteerRegistration) {
			value.RepositoryPath = filepath.Join(filepath.Dir(value.RepositoryPath), "moved-repository")
		},
	}
	for _, mutate := range mutations {
		next := current
		mutate(&next)
		updated, _, changed, err := store.Join(ctx, next)
		if err != nil || !changed || updated.RegistrationID == current.RegistrationID || updated.IncarnationID == current.IncarnationID {
			t.Fatalf("routing change = %#v changed=%v err=%v", updated, changed, err)
		}
		current = updated
	}
}

func TestSteerTransportParametersCompareSemantically(t *testing.T) {
	left := SteerTransportRoute{Kind: "example", Params: map[string]string{"alpha": "one", "beta": "two"}}
	right := SteerTransportRoute{Kind: "example", Params: map[string]string{"beta": "two", "alpha": "one"}}
	if !sameSteerTransport(left, right) {
		t.Fatal("transport parameters with equal key/value pairs compared unequal")
	}
	if sameSteerTransport(left, SteerTransportRoute{Kind: "example", Params: map[string]string{"alpha": "one", "beta": "changed"}}) {
		t.Fatal("different transport parameter value compared equal")
	}
}

func TestRegistrationStoreValidatesGenericRouteAndBoundedTransport(t *testing.T) {
	tests := []struct {
		name   string
		change func(*SteerRegistration)
	}{
		{"empty harness", func(reg *SteerRegistration) { reg.Harness = "" }},
		{"empty session", func(reg *SteerRegistration) { reg.SessionID = "" }},
		{"unsupported transport", func(reg *SteerRegistration) { reg.Transport.Kind = "unknown" }},
		{"missing codex home", func(reg *SteerRegistration) { reg.Transport.Params = nil }},
		{"unknown codex parameter", func(reg *SteerRegistration) { reg.Transport.Params["extra"] = "value" }},
		{"noncanonical codex home", func(reg *SteerRegistration) {
			reg.Transport.Params["home"] += string(filepath.Separator) + ".."
		}},
		{"spool parameter", func(reg *SteerRegistration) {
			reg.Transport = SteerTransportRoute{Kind: "spool", Params: map[string]string{"extra": "value"}}
		}},
		{"too many parameters", func(reg *SteerRegistration) {
			reg.Transport = SteerTransportRoute{Kind: "unknown", Params: map[string]string{
				"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6", "g": "7", "h": "8", "i": "9",
			}}
		}},
		{"oversized parameter", func(reg *SteerRegistration) {
			reg.Transport = SteerTransportRoute{Kind: "unknown", Params: map[string]string{"value": strings.Repeat("x", maxSteerTransportValueBytes+1)}}
		}},
		{"control parameter", func(reg *SteerRegistration) { reg.Transport.Params["home"] = "/home/codex\x00bad" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewRegistrationStore(t.TempDir())
			registration := testSteerRegistration(t)
			test.change(&registration)
			if _, _, _, err := store.Join(context.Background(), registration); err == nil {
				t.Fatal("invalid steer route was accepted")
			}
		})
	}
	valid := testSteerRegistration(t)
	valid.Harness = "pi"
	if _, _, _, err := NewRegistrationStore(t.TempDir()).Join(context.Background(), valid); err != nil {
		t.Fatalf("opaque valid harness label rejected: %v", err)
	}
}

func TestRegistrationStoreLeaveRequiresCurrentThread(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	registration := testSteerRegistration(t)
	current, _, _, err := store.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	stale := current
	stale.SessionID = "old-thread"
	if removed, err := store.Leave(ctx, stale); err != nil || removed {
		t.Fatalf("stale leave removed=%v err=%v", removed, err)
	}
	if removed, err := store.Leave(ctx, current); err != nil || !removed {
		t.Fatalf("current leave removed=%v err=%v", removed, err)
	}
	if removed, err := store.Leave(ctx, current); err != nil || removed {
		t.Fatalf("repeated leave removed=%v err=%v", removed, err)
	}
}

func TestSteerRuntimeIsSeparateAndRegistrationIncarnationScoped(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := NewRegistrationStore(dir)
	reg, _, _, err := registrations.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
		t.Fatal(err)
	}
	if ok, err := runtime.Update(ctx, reg, "sending", "queue_uncertain"); err != nil || !ok {
		t.Fatalf("update current generation ok=%v err=%v", ok, err)
	}
	changed := reg
	changed.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	newReg, _, _, err := registrations.Join(ctx, changed)
	if err != nil {
		t.Fatal(err)
	}
	if newReg.RegistrationID == reg.RegistrationID {
		t.Fatal("thread replacement reused its registration incarnation ID")
	}
	if ok, err := runtime.Update(ctx, reg, "queued", ""); err != nil || ok {
		t.Fatalf("stale update ok=%v err=%v", ok, err)
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{newReg}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != newReg.RegistrationID || snapshot.Deliveries[0].State != "none" {
		t.Fatalf("runtime snapshot=%#v err=%v", snapshot, err)
	}
	if !snapshot.Deliveries[0].RecoveryPending || snapshot.Deliveries[0].SessionID != newReg.SessionID {
		t.Fatalf("thread replacement did not preserve recovery context: %#v", snapshot.Deliveries[0])
	}
	if _, err := os.Stat(filepath.Join(dir, steerRuntimeFileName)); err != nil {
		t.Fatal(err)
	}
}

func TestSteerRuntimeBootstrapIntentTriggersAndPersists(t *testing.T) {
	tests := []struct {
		name       string
		change     func(SteerRegistration) SteerRegistration
		rejoin     bool
		loseRecord bool
		completed  bool
		want       bool
	}{
		{name: "first registration", want: true},
		{name: "exact same-thread rejoin", rejoin: true, completed: true, want: false},
		{name: "same-thread role change", change: func(reg SteerRegistration) SteerRegistration { reg.Role = "architect"; return reg }, completed: true, want: false},
		{name: "replacement thread", change: func(reg SteerRegistration) SteerRegistration {
			reg.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
			return reg
		}, completed: true, want: true},
		{name: "lost delivery record", loseRecord: true, completed: true, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			registrations := NewRegistrationStore(dir)
			reg, _, _, err := registrations.Join(ctx, testSteerRegistration(t))
			if err != nil {
				t.Fatal(err)
			}
			runtime := NewSteerRuntimeStore(dir)
			if err := runtime.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
				t.Fatal(err)
			}
			if test.completed {
				if updated, err := runtime.CompleteDelivery(ctx, reg, "queued", true, true); err != nil || !updated {
					t.Fatalf("mark bootstrap complete updated=%t err=%v", updated, err)
				}
			}
			if test.loseRecord {
				if err := runtime.Reconcile(ctx, nil); err != nil {
					t.Fatal(err)
				}
			}
			if test.change != nil {
				changed := test.change(reg)
				reg, _, _, err = registrations.Join(ctx, changed)
				if err != nil {
					t.Fatal(err)
				}
			} else if test.rejoin {
				reg, _, _, err = registrations.Join(ctx, reg)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := runtime.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
				t.Fatal(err)
			}
			snapshot, err := runtime.Snapshot(ctx)
			if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].BootstrapPending != test.want {
				t.Fatalf("persisted bootstrap intent=%#v err=%v, want pending=%t", snapshot.Deliveries, err, test.want)
			}
			if test.name == "replacement thread" && !snapshot.Deliveries[0].RecoveryPending {
				t.Fatal("replacement thread did not keep claim recovery distinct from bootstrap")
			}
			if test.name == "same-thread role change" && snapshot.Deliveries[0].RecoveryPending {
				t.Fatal("same-thread role change invented claim recovery")
			}
		})
	}
}

func TestSteerRuntimeCompletionClearsOnlyIncludedIntents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registrations := NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtime.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("complete initial delivery updated=%t err=%v", updated, err)
	}
	replacement := old
	replacement.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, _, err := registrations.Join(ctx, replacement)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{current}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtime.CompleteDelivery(ctx, current, "none", true, false); err != nil || !updated {
		t.Fatalf("complete bootstrap-only delivery updated=%t err=%v", updated, err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("runtime snapshot=%#v err=%v", snapshot, err)
	}
	if snapshot.Deliveries[0].State != "none" || snapshot.Deliveries[0].BootstrapPending || !snapshot.Deliveries[0].RecoveryPending {
		t.Fatalf("completion did not preserve the unrepresented recovery intent: %#v", snapshot.Deliveries[0])
	}
}

func TestSteerRuntimeNoActiveClaimClearsOnlyRecoveryIntent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	registrations := NewRegistrationStore(dir)
	old, _, _, err := registrations.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtime.CompleteDelivery(ctx, old, "queued", true, true); err != nil || !updated {
		t.Fatalf("complete old delivery updated=%t err=%v", updated, err)
	}
	replacement := old
	replacement.SessionID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
	current, _, _, err := registrations.Join(ctx, replacement)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{current}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtime.ConfirmNoActiveClaim(ctx, current); err != nil || !updated {
		t.Fatalf("confirm no active claim updated=%t err=%v", updated, err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 {
		t.Fatalf("runtime snapshot=%#v err=%v", snapshot, err)
	}
	delivery := snapshot.Deliveries[0]
	if delivery.State != "none" || !delivery.BootstrapPending || delivery.RecoveryPending {
		t.Fatalf("no-active confirmation changed unrelated delivery state: %#v", delivery)
	}
}

func TestSteerRuntimeLostDeliveryReconstructsBootstrapOnlyForMissingOwner(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	first := testSteerRegistration(t)
	second := first
	second.Actor = "second-actor"
	registrations := NewRegistrationStore(dir)
	first, _, _, err := registrations.Join(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	second, _, _, err = registrations.Join(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []SteerRegistration{first, second}); err != nil {
		t.Fatal(err)
	}
	for _, reg := range []SteerRegistration{first, second} {
		if updated, err := runtime.CompleteDelivery(ctx, reg, "queued", true, true); err != nil || !updated {
			t.Fatalf("mark %s bootstrap complete updated=%t err=%v", reg.Actor, updated, err)
		}
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{first}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{first, second}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 2 {
		t.Fatalf("reconstructed deliveries=%#v err=%v", snapshot.Deliveries, err)
	}
	for _, delivery := range snapshot.Deliveries {
		wantPending := delivery.Actor == second.Actor
		if delivery.BootstrapPending != wantPending {
			t.Errorf("actor %q bootstrap pending=%t, want %t", delivery.Actor, delivery.BootstrapPending, wantPending)
		}
	}
}

func TestRegistrationLeaveAndRejoinCreatesNewIncarnation(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	registration := testSteerRegistration(t)
	first, _, _, err := store.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	current, _, changed, err := store.Join(ctx, registration)
	if err != nil || changed || current.RegistrationID != first.RegistrationID {
		t.Fatalf("exact rejoin=%#v changed=%t err=%v", current, changed, err)
	}
	if removed, err := store.Leave(ctx, first); err != nil || removed {
		t.Fatalf("stale incarnation leave removed=%t err=%v", removed, err)
	}
	if removed, err := store.Leave(ctx, current); err != nil || !removed {
		t.Fatalf("leave removed=%t err=%v", removed, err)
	}
	second, previous, changed, err := store.Join(ctx, registration)
	if err != nil || previous != nil || !changed || second.RegistrationID == first.RegistrationID {
		t.Fatalf("rejoin after leave=%#v previous=%#v changed=%t err=%v", second, previous, changed, err)
	}
}

func TestRegistrationReplacementRetiresStaleDelivery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := NewRegistrationStore(dir)
	registration, _, _, err := registrations.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}

	runtime := NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtime.CompleteDelivery(ctx, registration, "queued", true, true); err != nil || !updated {
		t.Fatalf("persist old delivery updated=%t err=%v", updated, err)
	}

	replacement := registration
	replacement.Role = "architect"
	current, previous, changed, err := registrations.Join(ctx, replacement)
	if err != nil || !changed || previous == nil || previous.RegistrationID != registration.RegistrationID || current.RegistrationID == registration.RegistrationID {
		t.Fatalf("replacement=%#v previous=%#v changed=%t err=%v", current, previous, changed, err)
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{current}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != current.RegistrationID || snapshot.Deliveries[0].State != "none" || snapshot.Deliveries[0].RecoveryPending {
		t.Fatalf("replacement inherited stale delivery=%#v err=%v", snapshot, err)
	}
}

func TestSteerRuntimeExactJoinClearsStaleDelivery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registrations := NewRegistrationStore(dir)
	registration := testSteerRegistration(t)
	first, _, _, err := registrations.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []SteerRegistration{first}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtime.Update(ctx, first, "degraded", "queue_rejected"); err != nil || !updated {
		t.Fatalf("persist degraded delivery updated=%t err=%v", updated, err)
	}

	refreshed, previous, changed, err := registrations.Join(ctx, registration)
	if err != nil || previous == nil || changed || refreshed.RegistrationID != first.RegistrationID || refreshed.IncarnationID == first.IncarnationID {
		t.Fatalf("same-thread join refresh=%#v previous=%#v changed=%t err=%v", refreshed, previous, changed, err)
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{refreshed}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != first.RegistrationID || snapshot.Deliveries[0].IncarnationID != refreshed.IncarnationID || snapshot.Deliveries[0].State != "none" || snapshot.Deliveries[0].RecoveryPending {
		t.Fatalf("exact join did not refresh delivery: %#v err=%v", snapshot, err)
	}
}

func TestRegistrationStoreLeaveRequiresFullCurrentRoute(t *testing.T) {
	for name, change := range map[string]func(*SteerRegistration){
		"transport params": func(value *SteerRegistration) {
			value.Transport = testCodexQueueTransport(filepath.Join(filepath.Dir(value.Transport.Params["home"]), "replacement-codex"))
		},
		"repository path": func(value *SteerRegistration) {
			value.RepositoryPath = filepath.Join(filepath.Dir(value.RepositoryPath), "replacement-repository")
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := NewRegistrationStore(t.TempDir())
			ctx := context.Background()
			oldRoute := testSteerRegistration(t)
			oldRoute, _, _, err := store.Join(ctx, oldRoute)
			if err != nil {
				t.Fatal(err)
			}
			newRoute := oldRoute
			change(&newRoute)
			newRoute, _, changed, err := store.Join(ctx, newRoute)
			if err != nil || !changed {
				t.Fatalf("replace route changed=%v err=%v", changed, err)
			}
			if removed, err := store.Leave(ctx, oldRoute); err != nil || removed {
				t.Fatalf("stale leave removed replacement=%v err=%v", removed, err)
			}
			if removed, err := store.Leave(ctx, newRoute); err != nil || !removed {
				t.Fatalf("current leave removed=%v err=%v", removed, err)
			}
		})
	}
}

func TestRegistrationStoreLeaveRejectsStaleSameRouteIncarnation(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	registration := testSteerRegistration(t)
	first, _, _, err := store.Join(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	current, previous, changed, err := store.Join(ctx, registration)
	if err != nil || changed || previous == nil || current.RegistrationID != first.RegistrationID || current.IncarnationID == first.IncarnationID {
		t.Fatalf("same-route rejoin=%#v previous=%#v changed=%v err=%v", current, previous, changed, err)
	}
	if removed, err := store.Leave(ctx, first); err != nil || removed {
		t.Fatalf("stale incarnation leave removed=%v err=%v", removed, err)
	}
	if removed, err := store.Leave(ctx, current); err != nil || !removed {
		t.Fatalf("current incarnation leave removed=%v err=%v", removed, err)
	}
}

func duplicateOwnerFixture(t *testing.T) string {
	t.Helper()
	first := testSteerRegistration(t)
	first.RegistrationID = "11111111111111111111111111111111"
	first.IncarnationID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	second := first
	second.RegistrationID = "22222222222222222222222222222222"
	second.IncarnationID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	second.Role = "architect"
	data, err := json.Marshal(struct {
		Version       int                 `json:"version"`
		Registrations []SteerRegistration `json:"registrations"`
	}{Version: steerRegistrationFileVersion, Registrations: []SteerRegistration{first, second}})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestRegistrationStoreRejectsMalformedAndDuplicateOwners(t *testing.T) {
	for name, data := range map[string]string{
		"unsupported legacy version":     `{"version":1,"registrations":[]}`,
		"unsupported prerelease version": `{"version":3,"registrations":[]}`,
		"unsupported future version":     `{"version":6,"registrations":[]}`,
		"missing registrations":          `{"version":5}`,
		"duplicate owner":                duplicateOwnerFixture(t),
		"obsolete generation field":      `{"version":5,"registrations":[{"generation":1}]}`,
		"unknown field":                  `{"version":5,"registrations":[],"extra":true}`,
		"unknown previous field":         `{"version":4,"registrations":[],"extra":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, steerFileName), []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewRegistrationStore(dir).Snapshot(context.Background()); err == nil {
				t.Fatal("expected malformed steer state error")
			}
		})
	}
}

func writeSteerV4Fixture(t *testing.T, dir string, registration steerRegistrationV4) []byte {
	t.Helper()
	data, err := json.Marshal(steerSnapshotV4{Version: 4, Registrations: []steerRegistrationV4{registration}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, steerFileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRegistrationStoreMigratesV4WithoutChangingIdentityOrEagerlyWriting(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	current := testSteerRegistration(t)
	old := steerRegistrationV4{
		RepositoryID: current.RepositoryID, RegistrationID: "11111111111111111111111111111111",
		JoinSignal: "22222222222222222222222222222222", RepositoryPath: current.RepositoryPath,
		RepositoryName: current.RepositoryName, Actor: current.Actor, Role: current.Role,
		CodexHome: current.Transport.Params["home"], ThreadID: current.SessionID,
	}
	oldBytes := writeSteerV4Fixture(t, dir, old)
	store := NewRegistrationStore(dir)
	snapshot, err := store.Snapshot(ctx)
	if err != nil || snapshot.Version != steerRegistrationFileVersion || len(snapshot.Registrations) != 1 {
		t.Fatalf("migrated snapshot=%#v err=%v", snapshot, err)
	}
	migrated := snapshot.Registrations[0]
	if migrated.RepositoryID != old.RepositoryID || migrated.RegistrationID != old.RegistrationID ||
		migrated.IncarnationID != old.JoinSignal || migrated.RepositoryPath != old.RepositoryPath ||
		migrated.RepositoryName != old.RepositoryName || migrated.Actor != old.Actor || migrated.Role != old.Role ||
		migrated.Harness != "codex" || migrated.SessionID != old.ThreadID ||
		migrated.Transport.Kind != "codex-queue" || migrated.Transport.Params["home"] != old.CodexHome {
		t.Fatalf("migrated registration=%#v, previous=%#v", migrated, old)
	}
	filePath := filepath.Join(dir, steerFileName)
	if got, err := os.ReadFile(filePath); err != nil || string(got) != string(oldBytes) {
		t.Fatalf("read-only migration rewrote old file: err=%v", err)
	}

	rejoined, previous, changed, err := store.Join(ctx, migrated)
	if err != nil || changed || previous == nil || rejoined.RegistrationID != old.RegistrationID || rejoined.IncarnationID == old.JoinSignal {
		t.Fatalf("post-migration rejoin=%#v previous=%#v changed=%t err=%v", rejoined, previous, changed, err)
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted["version"] != float64(steerRegistrationFileVersion) ||
		strings.Contains(string(data), "join_signal") || strings.Contains(string(data), "codex_home") || strings.Contains(string(data), "thread_id") {
		t.Fatalf("successful mutation did not write only the current schema: %s", data)
	}
}

func TestRegistrationStoreRejectsMalformedPreviousSchema(t *testing.T) {
	tests := []struct {
		name string
		edit func(*steerRegistrationV4)
	}{
		{"relative Codex home", func(reg *steerRegistrationV4) { reg.CodexHome = "relative" }},
		{"malformed registration ID", func(reg *steerRegistrationV4) { reg.RegistrationID = "bad" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			current := testSteerRegistration(t)
			old := steerRegistrationV4{
				RepositoryID: current.RepositoryID, RegistrationID: "11111111111111111111111111111111",
				JoinSignal: "22222222222222222222222222222222", RepositoryPath: current.RepositoryPath,
				RepositoryName: current.RepositoryName, Actor: current.Actor, Role: current.Role,
				CodexHome: current.Transport.Params["home"], ThreadID: current.SessionID,
			}
			test.edit(&old)
			writeSteerV4Fixture(t, dir, old)
			if _, err := NewRegistrationStore(dir).Snapshot(context.Background()); !errors.Is(err, ErrMalformed) {
				t.Fatalf("malformed previous schema error=%v", err)
			}
		})
	}
	old := steerRegistrationV4{
		RepositoryID: testSteerRepositoryID, RegistrationID: "11111111111111111111111111111111",
		JoinSignal: "22222222222222222222222222222222", RepositoryPath: "/repo",
		Actor: "reviewer", Role: "reviewer", CodexHome: "/codex", ThreadID: "session",
	}
	dir := t.TempDir()
	data, err := json.Marshal(steerSnapshotV4{Version: 4, Registrations: []steerRegistrationV4{old}})
	if err != nil {
		t.Fatal(err)
	}
	var withUnknown map[string]any
	if err := json.Unmarshal(data, &withUnknown); err != nil {
		t.Fatal(err)
	}
	entries := withUnknown["registrations"].([]any)
	entries[0].(map[string]any)["unknown"] = "value"
	data, err = json.Marshal(withUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, steerFileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistrationStore(dir).Snapshot(context.Background()); !errors.Is(err, ErrMalformed) {
		t.Fatalf("unknown previous field error=%v", err)
	}
}

func TestSteerRuntimeMigratesV3PreservingPendingDeliveryAndWritesOnReconcile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	registration := testSteerRegistration(t)
	registration.RegistrationID = "11111111111111111111111111111111"
	registration.IncarnationID = "22222222222222222222222222222222"
	old := steerRuntimeSnapshotV3{Version: 3, Deliveries: []steerDeliveryV3{{
		RepositoryID: registration.RepositoryID, Actor: registration.Actor, RegistrationID: registration.RegistrationID,
		JoinSignal: registration.IncarnationID, ThreadID: registration.SessionID, State: "queued", Code: "queue_uncertain",
		BootstrapPending: true, RecoveryPending: true, UpdatedAt: time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC),
	}}}
	oldBytes, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, steerRuntimeFileName)
	if err := os.WriteFile(path, oldBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewSteerRuntimeStore(dir)
	snapshot, err := store.Snapshot(ctx)
	if err != nil || snapshot.Version != steerRuntimeFileVersion || len(snapshot.Deliveries) != 1 {
		t.Fatalf("migrated runtime=%#v err=%v", snapshot, err)
	}
	delivery := snapshot.Deliveries[0]
	if delivery.RepositoryID != registration.RepositoryID || delivery.Actor != registration.Actor ||
		delivery.RegistrationID != registration.RegistrationID || delivery.IncarnationID != registration.IncarnationID ||
		delivery.SessionID != registration.SessionID || delivery.State != "queued" || delivery.Code != "queue_uncertain" ||
		!delivery.BootstrapPending || !delivery.RecoveryPending || !delivery.UpdatedAt.Equal(old.Deliveries[0].UpdatedAt) {
		t.Fatalf("migrated delivery=%#v", delivery)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(oldBytes) {
		t.Fatalf("read-only runtime migration rewrote old file: err=%v", err)
	}
	if err := store.Reconcile(ctx, []SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted["version"] != float64(steerRuntimeFileVersion) ||
		strings.Contains(string(data), "join_signal") || strings.Contains(string(data), "thread_id") {
		t.Fatalf("successful reconciliation did not write only the current runtime schema: %s", data)
	}
}

func TestSteerFilesHaveIndependentSchemaVersions(t *testing.T) {
	if steerRegistrationFileVersion == steerRuntimeFileVersion {
		t.Fatal("steer registration and runtime files share one schema version")
	}
	dir := t.TempDir()
	ctx := context.Background()
	registration, _, _, err := NewRegistrationStore(dir).Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	registrations, err := NewRegistrationStore(dir).Snapshot(ctx)
	if err != nil || registrations.Version != steerRegistrationFileVersion {
		t.Fatalf("registration version=%d err=%v, want %d", registrations.Version, err, steerRegistrationFileVersion)
	}
	deliveries, err := runtime.Snapshot(ctx)
	if err != nil || deliveries.Version != steerRuntimeFileVersion {
		t.Fatalf("runtime version=%d err=%v, want %d", deliveries.Version, err, steerRuntimeFileVersion)
	}
}

func TestSteerRuntimeRejectsUnsupportedVersion(t *testing.T) {
	for _, version := range []int{1, 2, 5} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, steerRuntimeFileName)
			data := fmt.Sprintf(`{"version":%d,"deliveries":[]}`, version)
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := NewSteerRuntimeStore(dir).Snapshot(context.Background())
			if !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), fmt.Sprintf("unsupported steer runtime version %d", version)) {
				t.Fatalf("runtime error=%v, want precise unsupported version", err)
			}
		})
	}
}

func TestSteerRuntimeRejectsUnknownPreviousSchemaFields(t *testing.T) {
	dir := t.TempDir()
	registration := testSteerRegistration(t)
	previous := steerRuntimeSnapshotV3{Version: 3, Deliveries: []steerDeliveryV3{{
		RepositoryID: registration.RepositoryID, Actor: registration.Actor,
		RegistrationID: "11111111111111111111111111111111", JoinSignal: "22222222222222222222222222222222",
		ThreadID: registration.SessionID, State: "queued", UpdatedAt: time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC),
	}}}
	data, err := json.Marshal(previous)
	if err != nil {
		t.Fatal(err)
	}
	var withUnknown map[string]any
	if err := json.Unmarshal(data, &withUnknown); err != nil {
		t.Fatal(err)
	}
	entries := withUnknown["deliveries"].([]any)
	entries[0].(map[string]any)["generation"] = 1
	data, err = json.Marshal(withUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, steerRuntimeFileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSteerRuntimeStore(dir).Snapshot(context.Background()); !errors.Is(err, ErrMalformed) {
		t.Fatalf("unknown previous runtime field error=%v", err)
	}
}

func TestRegistrationStoreConcurrentJoinKeepsOneOwner(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	registration := testSteerRegistration(t)
	var wg sync.WaitGroup
	errors := make(chan error, 12)
	for i := 0; i < cap(errors); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, _, err := store.Join(context.Background(), registration); err != nil {
				errors <- err
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	snapshot, err := store.Snapshot(context.Background())
	if err != nil || len(snapshot.Registrations) != 1 || !validSteerRegistrationID(snapshot.Registrations[0].RegistrationID) {
		t.Fatalf("concurrent registrations = %#v err=%v", snapshot, err)
	}
}

func TestRegistrationStoreKeysOwnersByRepositoryAndActor(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	first := testSteerRegistration(t)
	if _, _, _, err := store.Join(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	otherRepository := first
	otherRepository.RepositoryID = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	if _, _, _, err := store.Join(context.Background(), otherRepository); err != nil {
		t.Fatal(err)
	}
	otherActor := first
	otherActor.Actor = "coder"
	otherActor.Role = "coder"
	if _, _, _, err := store.Join(context.Background(), otherActor); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background())
	if err != nil || len(snapshot.Registrations) != 3 {
		t.Fatalf("registrations = %#v err=%v; expected actor reuse across repositories and actors within repository", snapshot.Registrations, err)
	}
	for _, registration := range snapshot.Registrations {
		if registration.RepositoryID == first.RepositoryID && registration.Actor == otherActor.Actor && registration.Role != "coder" {
			t.Fatalf("second actor role = %q, want coder: %#v", registration.Role, registration)
		}
	}
}
