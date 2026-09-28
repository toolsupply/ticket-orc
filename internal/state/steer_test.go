package state

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
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
		CodexHome: codexHome, ThreadID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5",
	}
}

func TestRegistrationStoreJoinIncarnationAndDisplayName(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	registration := testSteerRegistration(t)
	first, previous, changed, err := store.Join(ctx, registration)
	if err != nil || previous != nil || !changed || !validSteerRegistrationID(first.RegistrationID) || !validSteerRegistrationID(first.JoinSignal) {
		t.Fatalf("initial join = %#v previous=%#v changed=%v err=%v", first, previous, changed, err)
	}

	rejoin := registration
	current, previous, changed, err := store.Join(ctx, rejoin)
	if err != nil || previous == nil || changed || current.RegistrationID != first.RegistrationID || current.JoinSignal == first.JoinSignal {
		t.Fatalf("same join = %#v previous=%#v changed=%v err=%v", current, previous, changed, err)
	}

	rename := registration
	rename.RepositoryName = "renamed"
	current, _, changed, err = store.Join(ctx, rename)
	if err != nil || changed || current.RepositoryName != "renamed" || current.RegistrationID != first.RegistrationID || current.JoinSignal == first.JoinSignal {
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

func TestRegistrationStoreRoutingChangesCreateIncarnations(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	current, _, _, err := store.Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*SteerRegistration){
		func(value *SteerRegistration) { value.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6" },
		func(value *SteerRegistration) {
			value.CodexHome = filepath.Join(filepath.Dir(value.CodexHome), "other-codex")
		},
		func(value *SteerRegistration) {
			value.RepositoryPath = filepath.Join(filepath.Dir(value.RepositoryPath), "moved-repository")
		},
	}
	for _, mutate := range mutations {
		next := current
		mutate(&next)
		updated, _, changed, err := store.Join(ctx, next)
		if err != nil || !changed || updated.RegistrationID == current.RegistrationID {
			t.Fatalf("routing change = %#v changed=%v err=%v", updated, changed, err)
		}
		current = updated
	}
}

func TestRegistrationStoreLeaveRequiresCurrentThread(t *testing.T) {
	store := NewRegistrationStore(t.TempDir())
	ctx := context.Background()
	registration := testSteerRegistration(t)
	if _, _, _, err := store.Join(ctx, registration); err != nil {
		t.Fatal(err)
	}
	if removed, err := store.Leave(ctx, registration.RepositoryID, registration.Actor, registration.RepositoryPath, registration.CodexHome, "old-thread"); err != nil || removed {
		t.Fatalf("stale leave removed=%v err=%v", removed, err)
	}
	if removed, err := store.Leave(ctx, registration.RepositoryID, registration.Actor, registration.RepositoryPath, registration.CodexHome, registration.ThreadID); err != nil || !removed {
		t.Fatalf("current leave removed=%v err=%v", removed, err)
	}
	if removed, err := store.Leave(ctx, registration.RepositoryID, registration.Actor, registration.RepositoryPath, registration.CodexHome, registration.ThreadID); err != nil || removed {
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
	changed.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
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
	if !snapshot.Deliveries[0].RecoveryPending || snapshot.Deliveries[0].ThreadID != newReg.ThreadID {
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
			reg.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
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
	replacement.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
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
	replacement.ThreadID = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a6"
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
	if current, _, changed, err := store.Join(ctx, registration); err != nil || changed || current.RegistrationID != first.RegistrationID {
		t.Fatalf("exact rejoin=%#v changed=%t err=%v", current, changed, err)
	}
	if removed, err := store.Leave(ctx, first.RepositoryID, first.Actor, first.RepositoryPath, first.CodexHome, first.ThreadID); err != nil || !removed {
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
	if err != nil || previous == nil || changed || refreshed.RegistrationID != first.RegistrationID || refreshed.JoinSignal == first.JoinSignal {
		t.Fatalf("same-thread join refresh=%#v previous=%#v changed=%t err=%v", refreshed, previous, changed, err)
	}
	if err := runtime.Reconcile(ctx, []SteerRegistration{refreshed}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].RegistrationID != first.RegistrationID || snapshot.Deliveries[0].JoinSignal != refreshed.JoinSignal || snapshot.Deliveries[0].State != "none" || snapshot.Deliveries[0].RecoveryPending {
		t.Fatalf("exact join did not refresh delivery: %#v err=%v", snapshot, err)
	}
}

func TestRegistrationStoreLeaveRequiresFullCurrentRoute(t *testing.T) {
	for name, change := range map[string]func(*SteerRegistration){
		"Codex home": func(value *SteerRegistration) {
			value.CodexHome = filepath.Join(filepath.Dir(value.CodexHome), "replacement-codex")
		},
		"repository path": func(value *SteerRegistration) {
			value.RepositoryPath = filepath.Join(filepath.Dir(value.RepositoryPath), "replacement-repository")
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := NewRegistrationStore(t.TempDir())
			ctx := context.Background()
			oldRoute := testSteerRegistration(t)
			if _, _, _, err := store.Join(ctx, oldRoute); err != nil {
				t.Fatal(err)
			}
			newRoute := oldRoute
			change(&newRoute)
			if _, _, changed, err := store.Join(ctx, newRoute); err != nil || !changed {
				t.Fatalf("replace route changed=%v err=%v", changed, err)
			}
			if removed, err := store.Leave(ctx, oldRoute.RepositoryID, oldRoute.Actor, oldRoute.RepositoryPath, oldRoute.CodexHome, oldRoute.ThreadID); err != nil || removed {
				t.Fatalf("stale leave removed replacement=%v err=%v", removed, err)
			}
			if removed, err := store.Leave(ctx, newRoute.RepositoryID, newRoute.Actor, newRoute.RepositoryPath, newRoute.CodexHome, newRoute.ThreadID); err != nil || !removed {
				t.Fatalf("current leave removed=%v err=%v", removed, err)
			}
		})
	}
}

func duplicateOwnerFixture(t *testing.T) string {
	t.Helper()
	first := testSteerRegistration(t)
	first.RegistrationID = "11111111111111111111111111111111"
	first.JoinSignal = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	second := first
	second.RegistrationID = "22222222222222222222222222222222"
	second.JoinSignal = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
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
		"unsupported future version":     `{"version":5,"registrations":[]}`,
		"missing registrations":          `{"version":4}`,
		"duplicate owner":                duplicateOwnerFixture(t),
		"obsolete generation field":      `{"version":4,"registrations":[{"generation":1}]}`,
		"unknown field":                  `{"version":4,"registrations":[],"extra":true}`,
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
	dir := t.TempDir()
	path := filepath.Join(dir, steerRuntimeFileName)
	legacy := `{"version":1,"deliveries":[{"repository_id":"d659917f-5939-4e93-bfde-6346a0f2bc50","actor":"reviewer","generation":1,"state":"queued","updated_at":"2026-09-26T08:00:00Z"}]}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewSteerRuntimeStore(dir).Snapshot(context.Background())
	if !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), "unsupported steer runtime version 1") {
		t.Fatalf("legacy runtime error=%v, want precise unsupported version", err)
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
