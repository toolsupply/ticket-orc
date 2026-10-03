package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const dynamicTestRepositoryID = "d659917f-5939-4e93-bfde-6346a0f2bc50"

func dynamicTestRegistration(t *testing.T, actor, path string) state.SteerRegistration {
	t.Helper()
	root := t.TempDir()
	repositoryPath := filepath.Join(root, filepath.FromSlash(strings.Trim(path, "/\\")))
	codexHome := filepath.Join(root, "codex")
	if err := os.MkdirAll(repositoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	return state.SteerRegistration{
		RepositoryID: dynamicTestRepositoryID, RepositoryPath: repositoryPath, RepositoryName: "Ticket project",
		RegistrationID: "11111111111111111111111111111111", IncarnationID: "22222222222222222222222222222222",
		Actor: actor, Role: "coder", Harness: "codex", Transport: testSteerCodexTransport(codexHome), SessionID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5",
	}
}

func TestDynamicRepositoryTargetIsTicketValidatedAndMergedByID(t *testing.T) {
	registration := dynamicTestRegistration(t, "coder", "/ticket/project")
	dir := t.TempDir()
	if _, _, _, err := state.NewRegistrationStore(dir).Join(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	var probes int
	manager := &workerManager{
		repositories:      supervisor.RepositoryRegistry{},
		repositoriesByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicReposByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicStatusKeys: map[string]bool{},
		runtime:           NewRuntimeState(nil),
	}
	if err := refreshDynamicRepositories(context.Background(), dir, manager, func(_ context.Context, got state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		probes++
		if got.Actor != registration.Actor || got.RepositoryPath != registration.RepositoryPath {
			t.Fatalf("probe registration=%#v", got)
		}
		name := "Ticket project"
		return ticketclient.RepositoryInfo{ID: dynamicTestRepositoryID, Path: registration.RepositoryPath, Name: &name}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if probes != 1 || len(manager.runtime.RepositoryStatuses()) != 1 || manager.runtime.RepositoryStatuses()[0].ID != dynamicTestRepositoryID || manager.runtime.RepositoryStatuses()[0].State != "healthy" {
		t.Fatalf("probes=%d statuses=%#v", probes, manager.runtime.RepositoryStatuses())
	}
	got, err := manager.repositoryTarget(dynamicTestRepositoryID)
	if err != nil || got.Target.Repository != registration.RepositoryPath || got.ID != dynamicTestRepositoryID {
		t.Fatalf("dynamic gateway target=%#v err=%v", got, err)
	}
	if gotStatuses := manager.runtime.RepositoryStatuses(); len(gotStatuses) != 1 || gotStatuses[0].ID != dynamicTestRepositoryID {
		t.Fatalf("dynamic repository status=%#v", gotStatuses)
	}
	manager.replaceDynamicRepositories(nil, nil)
	if _, err := manager.repositoryTarget(dynamicTestRepositoryID); err == nil {
		t.Fatal("removed registration remained available to the repository gateway")
	}
	if gotStatuses := manager.runtime.RepositoryStatuses(); len(gotStatuses) != 0 {
		t.Fatalf("removed dynamic repository status remained: %#v", gotStatuses)
	}
}

func TestDynamicRepositoryTargetRejectsUnverifiedAndConflictingPaths(t *testing.T) {
	registration := dynamicTestRegistration(t, "coder", "/ticket/project")
	wrong, statuses := resolveDynamicRepositories(context.Background(), []state.SteerRegistration{registration}, nil, func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		return ticketclient.RepositoryInfo{ID: joinOtherRepositoryID}, nil
	})
	if len(wrong) != 0 || len(statuses) != 1 || statuses[0].State != "degraded" {
		t.Fatalf("unverified target=%#v statuses=%#v", wrong, statuses)
	}
	conflicting := registration
	conflicting.Actor = "reviewer"
	conflicting.RepositoryPath = t.TempDir()
	probes := 0
	resolved, statuses := resolveDynamicRepositories(context.Background(), []state.SteerRegistration{registration, conflicting}, nil, func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		probes++
		return ticketclient.RepositoryInfo{ID: dynamicTestRepositoryID}, nil
	})
	if len(resolved) != 0 || probes != 0 || len(statuses) != 1 || statuses[0].State != "degraded" {
		t.Fatalf("conflicting target=%#v probes=%d statuses=%#v", resolved, probes, statuses)
	}
}

func TestDynamicRepositoryValidationFailureRemainsUnavailable(t *testing.T) {
	registration := dynamicTestRegistration(t, "coder", "/ticket/project")
	resolved, statuses := resolveDynamicRepositories(context.Background(), []state.SteerRegistration{registration}, nil, func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		return ticketclient.RepositoryInfo{}, errors.New("Ticket unavailable")
	})
	if len(resolved) != 0 || len(statuses) != 1 || statuses[0].State != "degraded" || statuses[0].Path != "" {
		t.Fatalf("failed validation exposed target: %#v statuses=%#v", resolved, statuses)
	}
}

func TestDynamicRepositoryDiscoveryRejectsChangedIdentityAtSamePath(t *testing.T) {
	registration := dynamicTestRegistration(t, "coder", "/ticket/project")
	probeCalls := 0
	probe := func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		probeCalls++
		if probeCalls == 1 {
			return ticketclient.RepositoryInfo{ID: dynamicTestRepositoryID, Path: registration.RepositoryPath}, nil
		}
		return ticketclient.RepositoryInfo{ID: joinOtherRepositoryID, Path: registration.RepositoryPath}, nil
	}
	first, firstStatuses := resolveDynamicRepositories(context.Background(), []state.SteerRegistration{registration}, nil, probe)
	if len(first) != 1 || len(firstStatuses) != 1 || firstStatuses[0].State != "healthy" {
		t.Fatalf("initial validation=%#v statuses=%#v", first, firstStatuses)
	}
	second, secondStatuses := resolveDynamicRepositories(context.Background(), []state.SteerRegistration{registration}, nil, probe)
	if len(second) != 0 || len(secondStatuses) != 1 || secondStatuses[0].State != "degraded" || probeCalls != 2 {
		t.Fatalf("revalidation exposed stale target=%#v statuses=%#v probes=%d", second, secondStatuses, probeCalls)
	}
}

func TestDynamicRepositoryIdentityCacheReusesAndSeparatesIdentities(t *testing.T) {
	first := dynamicTestRegistration(t, "first", "/ticket/first")
	second := dynamicTestRegistration(t, "second", "/ticket/second")
	second.RepositoryID = joinOtherRepositoryID
	registrations := []state.SteerRegistration{first, second}
	cache := newDynamicRepositoryIdentityCache()
	now := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	probes := 0
	probe := func(_ context.Context, registration state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		probes++
		return ticketclient.RepositoryInfo{ID: registration.RepositoryID, Path: registration.RepositoryPath}, nil
	}
	resolved, statuses := resolveDynamicRepositoriesWithCache(context.Background(), registrations, nil, probe, cache, now)
	if len(resolved) != 2 || len(statuses) != 2 || probes != 2 {
		t.Fatalf("initial identities resolved=%#v statuses=%#v probes=%d", resolved, statuses, probes)
	}
	resolved, statuses = resolveDynamicRepositoriesWithCache(context.Background(), registrations, nil, probe, cache, now.Add(time.Second))
	if len(resolved) != 2 || len(statuses) != 2 || probes != 2 {
		t.Fatalf("unchanged identities were not cached: resolved=%#v statuses=%#v probes=%d", resolved, statuses, probes)
	}
}

func TestDynamicRepositoryIdentityCacheInvalidatesPathAndConflicts(t *testing.T) {
	registration := dynamicTestRegistration(t, "coder", "/ticket/project")
	cache := newDynamicRepositoryIdentityCache()
	now := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	probes := 0
	probe := func(_ context.Context, current state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		probes++
		return ticketclient.RepositoryInfo{ID: current.RepositoryID, Path: current.RepositoryPath}, nil
	}
	first, _ := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{registration}, nil, probe, cache, now)
	if len(first) != 1 || probes != 1 {
		t.Fatalf("initial path verification resolved=%#v probes=%d", first, probes)
	}
	changedPath := registration
	changedPath.RepositoryPath = t.TempDir()
	failedProbe := func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		probes++
		return ticketclient.RepositoryInfo{}, errors.New("unavailable")
	}
	changed, statuses := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{changedPath}, nil, failedProbe, cache, now.Add(time.Second))
	if len(changed) != 0 || len(statuses) != 1 || probes != 2 {
		t.Fatalf("changed path reused old trust: resolved=%#v statuses=%#v probes=%d", changed, statuses, probes)
	}
	conflicting := changedPath
	conflicting.Actor = "reviewer"
	conflicting.RepositoryPath = t.TempDir()
	conflicted, statuses := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{changedPath, conflicting}, nil, probe, cache, now.Add(2*time.Second))
	if len(conflicted) != 0 || len(statuses) != 1 || probes != 2 {
		t.Fatalf("conflicting routes should invalidate without probing: resolved=%#v statuses=%#v probes=%d", conflicted, statuses, probes)
	}
	repaired, _ := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{changedPath}, nil, probe, cache, now.Add(3*time.Second))
	if len(repaired) != 1 || probes != 3 {
		t.Fatalf("route repair reused trust invalidated by conflict: resolved=%#v probes=%d", repaired, probes)
	}
}

func TestDynamicRepositoryIdentityCacheRetriesFailuresAndRevalidates(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		name := "probe failure"
		if mismatch {
			name = "identity mismatch"
		}
		t.Run(name, func(t *testing.T) {
			registration := dynamicTestRegistration(t, "coder", "/ticket/project")
			cache := newDynamicRepositoryIdentityCache()
			now := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
			probes := 0
			probe := func(_ context.Context, current state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
				probes++
				if probes == 2 {
					if mismatch {
						return ticketclient.RepositoryInfo{ID: joinOtherRepositoryID}, nil
					}
					return ticketclient.RepositoryInfo{}, errors.New("temporary Ticket outage")
				}
				return ticketclient.RepositoryInfo{ID: current.RepositoryID, Path: current.RepositoryPath}, nil
			}
			initial, _ := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{registration}, nil, probe, cache, now)
			if len(initial) != 1 || probes != 1 {
				t.Fatalf("initial verification=%#v probes=%d", initial, probes)
			}
			failed, statuses := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{registration}, nil, probe, cache, now.Add(dynamicRepositoryReverifyInterval))
			if len(failed) != 0 || len(statuses) != 1 || statuses[0].State != "degraded" || probes != 2 {
				t.Fatalf("failed revalidation retained target=%#v statuses=%#v probes=%d", failed, statuses, probes)
			}
			_, _ = resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{registration}, nil, probe, cache, now.Add(dynamicRepositoryReverifyInterval+time.Second))
			if probes != 2 {
				t.Fatalf("failed probe retried before retry interval: probes=%d", probes)
			}
			recovered, _ := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{registration}, nil, probe, cache, now.Add(dynamicRepositoryReverifyInterval+dynamicRepositoryRetryInterval))
			if len(recovered) != 1 || probes != 3 {
				t.Fatalf("failed verification did not retry at bounded cadence: resolved=%#v probes=%d", recovered, probes)
			}
		})
	}
}

func TestDynamicRepositoryPathChangeWithdrawsOldTargetBeforeProbe(t *testing.T) {
	registration := dynamicTestRegistration(t, "coder", "/ticket/old")
	cache := newDynamicRepositoryIdentityCache()
	now := time.Now()
	initial, _ := resolveDynamicRepositoriesWithCache(context.Background(), []state.SteerRegistration{registration}, nil, func(_ context.Context, current state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		return ticketclient.RepositoryInfo{ID: current.RepositoryID, Path: current.RepositoryPath}, nil
	}, cache, now)
	manager := &workerManager{
		repositories: supervisor.RepositoryRegistry{}, repositoriesByID: map[string]supervisor.ConfiguredRepository{},
		dynamicReposByID: map[string]supervisor.ConfiguredRepository{}, dynamicStatusKeys: map[string]bool{}, runtime: NewRuntimeState(nil),
	}
	manager.replaceDynamicRepositories(initial, []supervisor.RepositoryStatus{{ID: registration.RepositoryID, Key: dynamicRepositoryKey(registration.RepositoryID), State: "healthy"}})
	changed := registration
	changed.RepositoryPath = t.TempDir()
	probeStarted := make(chan struct{})
	probeRelease := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		applyDynamicRepositoryObservationWithCache(context.Background(), manager, func(_ context.Context, current state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
			close(probeStarted)
			<-probeRelease
			return ticketclient.RepositoryInfo{ID: current.RepositoryID, Path: current.RepositoryPath}, nil
		}, registrationObservation{Registrations: []state.SteerRegistration{changed}}, cache, now.Add(time.Second))
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		close(probeRelease)
		t.Fatal("new path was not probed")
	}
	if _, err := manager.repositoryTarget(registration.RepositoryID); err == nil {
		close(probeRelease)
		t.Fatal("old path remained active while the new path was unverified")
	}
	close(probeRelease)
	<-done
	resolved, err := manager.repositoryTarget(registration.RepositoryID)
	if err != nil || resolved.Target.Repository != changed.RepositoryPath {
		t.Fatalf("verified replacement target=%#v err=%v", resolved, err)
	}
	conflicting := changed
	conflicting.Actor = "reviewer"
	conflicting.RepositoryPath = t.TempDir()
	applyDynamicRepositoryObservationWithCache(context.Background(), manager, func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
		t.Fatal("conflicting registrations must not be probed")
		return ticketclient.RepositoryInfo{}, nil
	}, registrationObservation{Registrations: []state.SteerRegistration{changed, conflicting}}, cache, now.Add(2*time.Second))
	if _, err := manager.repositoryTarget(registration.RepositoryID); err == nil {
		t.Fatal("conflicting registration path kept a dynamic repository active")
	}
	statuses := manager.runtime.RepositoryStatuses()
	if len(statuses) != 1 || statuses[0].Failure != "dynamic registrations disagree about the Ticket repository path" {
		t.Fatalf("conflicting route status=%#v", statuses)
	}
}

func TestDynamicRepositoryCannotReplaceConfiguredTarget(t *testing.T) {
	configuredPath := t.TempDir()
	info := ticketclient.RepositoryInfo{ID: dynamicTestRepositoryID, Path: configuredPath}
	configured := supervisor.ConfiguredRepository{
		Key: "configured", ID: dynamicTestRepositoryID,
		Target: supervisor.TicketTarget{Mode: supervisor.TicketTargetRepository, Repository: configuredPath}, Info: &info,
	}
	manager := &workerManager{
		repositories:      supervisor.RepositoryRegistry{"configured": configured},
		repositoriesByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicReposByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicStatusKeys: map[string]bool{},
		runtime:           NewRuntimeState(nil),
	}
	dynamic := configured
	dynamic.Key = dynamicRepositoryKey(dynamicTestRepositoryID)
	dynamic.Target.Repository = t.TempDir()
	manager.replaceDynamicRepositories(map[string]supervisor.ConfiguredRepository{dynamicTestRepositoryID: dynamic}, []supervisor.RepositoryStatus{{ID: dynamicTestRepositoryID, Key: dynamic.Key, State: "healthy"}})
	got, err := manager.repositoryTarget(dynamicTestRepositoryID)
	if err != nil || got.Target.Repository != configuredPath || got.Key != "configured" {
		t.Fatalf("configured gateway target=%#v err=%v", got, err)
	}
	if statuses := manager.runtime.RepositoryStatuses(); len(statuses) != 0 {
		t.Fatalf("dynamic status duplicated configured repository: %#v", statuses)
	}
}

func TestDynamicRepositoryDiscoveryTracksJoinAndLeaveWithoutManagedWorkers(t *testing.T) {
	dir := t.TempDir()
	manager := &workerManager{
		repositories:      supervisor.RepositoryRegistry{},
		repositoriesByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicReposByID:  map[string]supervisor.ConfiguredRepository{},
		dynamicStatusKeys: map[string]bool{},
		runtime:           NewRuntimeState(nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicRepositoryDiscovery(ctx, dir, manager, func(_ context.Context, registration state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
			return ticketclient.RepositoryInfo{ID: registration.RepositoryID, Path: registration.RepositoryPath}, nil
		})
	}()
	store := state.NewRegistrationStore(dir)
	registration := dynamicTestRegistration(t, "coder", "/ticket/project")
	registration, _, _, err := store.Join(context.Background(), registration)
	if err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	waitForDynamicRepositoryTarget(t, manager, dynamicTestRepositoryID, true)
	if removed, err := store.Leave(context.Background(), registration); err != nil || !removed {
		cancel()
		<-done
		t.Fatalf("remove registration removed=%t err=%v", removed, err)
	}
	waitForDynamicRepositoryTarget(t, manager, dynamicTestRepositoryID, false)
	cancel()
	<-done
}

type trackedDynamicWatchProcess struct {
	done     chan error
	stopped  chan struct{}
	once     sync.Once
	stopGate <-chan struct{}
}

func newTrackedDynamicWatchProcess() *trackedDynamicWatchProcess {
	return &trackedDynamicWatchProcess{done: make(chan error, 1), stopped: make(chan struct{})}
}

func (p *trackedDynamicWatchProcess) Wait() error { return <-p.done }
func (p *trackedDynamicWatchProcess) Stop() error {
	p.once.Do(func() {
		close(p.stopped)
		if p.stopGate != nil {
			<-p.stopGate
		}
		p.done <- context.Canceled
	})
	return nil
}

func TestDynamicRepositoryObserversTrackRegistrationsWithoutRestartingConfiguredObservers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configured := testRepositoryRegistry()
	runtime := NewRuntimeState(nil)
	var mu sync.Mutex
	starts := make(map[string][]*trackedDynamicWatchProcess)
	notifiers := make(map[string]func(supervisor.RepositoryWatchEvent))
	watch := newRepositoryWatchManagerWithStarter(ctx, runtime, configured, nil, func(_ context.Context, repository supervisor.ConfiguredRepository, notify func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		process := newTrackedDynamicWatchProcess()
		mu.Lock()
		starts[repository.Key] = append(starts[repository.Key], process)
		notifiers[repository.Key] = notify
		mu.Unlock()
		return process, nil
	})
	events := make(chan supervisor.RuntimeEvent, 1)
	watch.SetEventSink(func(event supervisor.RuntimeEvent) { events <- event })
	manager := &workerManager{
		ctx:               ctx,
		repositories:      configured,
		repositoriesByID:  repositoryRegistryByID(configured),
		dynamicReposByID:  make(map[string]supervisor.ConfiguredRepository),
		dynamicStatusKeys: make(map[string]bool),
		repositoryWatch:   watch,
		runtime:           runtime,
	}
	watch.StartObservers()
	waitForObserverStarts(t, &mu, starts, map[string]int{"alpha": 1, "beta": 1})

	dynamicInfo := ticketclient.RepositoryInfo{ID: dynamicTestRepositoryID, Path: t.TempDir()}
	dynamic := supervisor.ConfiguredRepository{
		Key: dynamicRepositoryKey(dynamicTestRepositoryID), ID: dynamicTestRepositoryID,
		Target: supervisor.TicketTarget{Mode: supervisor.TicketTargetRepository, Repository: dynamicInfo.Path}, Info: &dynamicInfo,
	}
	active := map[string]supervisor.ConfiguredRepository{dynamicTestRepositoryID: dynamic}
	manager.replaceDynamicRepositories(active, []supervisor.RepositoryStatus{{ID: dynamicTestRepositoryID, Key: dynamic.Key, Path: dynamicInfo.Path, State: "healthy"}})
	waitForObserverStarts(t, &mu, starts, map[string]int{"alpha": 1, "beta": 1, dynamic.Key: 1})

	mu.Lock()
	notify := notifiers[dynamic.Key]
	mu.Unlock()
	if notify == nil {
		t.Fatal("dynamic observer did not retain its Ticket event callback")
	}
	notify(supervisor.RepositoryWatchEvent{Ticket: "20260926-00001", Event: "claimed", State: "open"})
	select {
	case event := <-events:
		if event.RepositoryID != dynamicTestRepositoryID || event.RepositoryKey != dynamic.Key || event.Ticket != "20260926-00001" {
			t.Fatalf("dynamic Ticket event=%#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("dynamic Ticket event was not published")
	}

	manager.replaceDynamicRepositories(active, []supervisor.RepositoryStatus{{ID: dynamicTestRepositoryID, Key: dynamic.Key, Path: dynamicInfo.Path, State: "healthy"}})
	waitForObserverStarts(t, &mu, starts, map[string]int{"alpha": 1, "beta": 1, dynamic.Key: 1})
	manager.replaceDynamicRepositories(nil, nil)
	mu.Lock()
	dynamicProcess := starts[dynamic.Key][0]
	configuredProcesses := []*trackedDynamicWatchProcess{starts["alpha"][0], starts["beta"][0]}
	mu.Unlock()
	select {
	case <-dynamicProcess.stopped:
	case <-time.After(time.Second):
		t.Fatal("removing the final registration did not stop its dynamic observer")
	}
	for _, process := range configuredProcesses {
		select {
		case <-process.stopped:
			t.Fatal("dynamic registration removal stopped a configured observer")
		default:
		}
	}
	watch.StopObservers()
}

func TestDynamicRefreshCannotRestoreRemovedConfiguredObserverAfterReload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldConfiguredPath := t.TempDir()
	newConfiguredPath := t.TempDir()
	oldDynamicPath := t.TempDir()
	oldConfigured := supervisor.ConfiguredRepository{Key: "configured-old", ID: "repo-old", Target: supervisor.TicketTarget{Mode: supervisor.TicketTargetRepository, Repository: oldConfiguredPath}}
	newConfigured := supervisor.ConfiguredRepository{Key: "configured-new", ID: "repo-new", Target: supervisor.TicketTarget{Mode: supervisor.TicketTargetRepository, Repository: newConfiguredPath}}
	configured := supervisor.RepositoryRegistry{oldConfigured.Key: oldConfigured}
	runtime := NewRuntimeState(nil)
	stopOldDynamic := make(chan struct{})
	var releaseOnce sync.Once
	releaseStop := func() { releaseOnce.Do(func() { close(stopOldDynamic) }) }
	defer releaseStop()
	var mu sync.Mutex
	starts := make(map[string][]*trackedDynamicWatchProcess)
	watch := newRepositoryWatchManagerWithStarter(ctx, runtime, configured, nil, func(_ context.Context, repository supervisor.ConfiguredRepository, _ func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
		process := newTrackedDynamicWatchProcess()
		if repository.Key == dynamicRepositoryKey(dynamicTestRepositoryID) && repository.Target.Repository == oldDynamicPath {
			process.stopGate = stopOldDynamic
		}
		mu.Lock()
		starts[repository.Key] = append(starts[repository.Key], process)
		mu.Unlock()
		return process, nil
	})
	manager := &workerManager{
		ctx: ctx, repositories: configured, repositoriesByID: repositoryRegistryByID(configured),
		dynamicReposByID: make(map[string]supervisor.ConfiguredRepository), dynamicStatusKeys: make(map[string]bool),
		repositoryWatch: watch, runtime: runtime,
	}
	watch.StartObservers()
	waitForObserverStarts(t, &mu, starts, map[string]int{oldConfigured.Key: 1})
	oldDynamic := supervisor.ConfiguredRepository{
		Key: dynamicRepositoryKey(dynamicTestRepositoryID), ID: dynamicTestRepositoryID,
		Target: supervisor.TicketTarget{Mode: supervisor.TicketTargetRepository, Repository: oldDynamicPath},
		Info:   &ticketclient.RepositoryInfo{ID: dynamicTestRepositoryID, Path: oldDynamicPath},
	}
	manager.replaceDynamicRepositories(map[string]supervisor.ConfiguredRepository{dynamicTestRepositoryID: oldDynamic}, []supervisor.RepositoryStatus{{ID: dynamicTestRepositoryID, Key: oldDynamic.Key, State: "healthy"}})
	waitForObserverStarts(t, &mu, starts, map[string]int{oldConfigured.Key: 1, oldDynamic.Key: 1})
	mu.Lock()
	oldDynamicProcess := starts[oldDynamic.Key][0]
	mu.Unlock()
	newDynamic := oldDynamic
	newDynamicPath := t.TempDir()
	newDynamic.Target.Repository = newDynamicPath
	newDynamic.Info = &ticketclient.RepositoryInfo{ID: dynamicTestRepositoryID, Path: newDynamicPath}
	dynamicRefreshDone := make(chan struct{})
	go func() {
		defer close(dynamicRefreshDone)
		manager.replaceDynamicRepositories(map[string]supervisor.ConfiguredRepository{dynamicTestRepositoryID: newDynamic}, []supervisor.RepositoryStatus{{ID: dynamicTestRepositoryID, Key: newDynamic.Key, State: "healthy"}})
	}()
	select {
	case <-oldDynamicProcess.stopped:
	case <-time.After(time.Second):
		t.Fatal("dynamic refresh did not enter observer replacement")
	}
	select {
	case <-dynamicRefreshDone:
		t.Fatal("dynamic refresh was expected to remain in the old observer's stop")
	default:
	}

	// This is the configured reload's candidate replacement and commit. The
	// dynamic refresh is still finishing an older observer replacement.
	watchRepositories, watchRevision := manager.repositoryWatchInventory(supervisor.RepositoryRegistry{newConfigured.Key: newConfigured})
	if err := watch.ReplaceAtRevision(ctx, watchRevision, watchRepositories); err != nil {
		t.Fatal(err)
	}
	manager.configMu.Lock()
	manager.repositories = supervisor.RepositoryRegistry{newConfigured.Key: newConfigured}
	manager.repositoriesByID = repositoryRegistryByID(manager.repositories)
	manager.repositoryWatchRevision++
	manager.configMu.Unlock()
	if err := manager.syncRepositoryWatchInventory(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if got := len(starts[oldConfigured.Key]); got != 1 {
		mu.Unlock()
		t.Fatalf("removed configured observer starts after reload commit = %d, want 1", got)
	}
	mu.Unlock()
	for _, status := range runtime.RepositoryStatuses() {
		if status.Key == oldConfigured.Key {
			t.Fatalf("removed configured observer status after reload commit: %#v", runtime.RepositoryStatuses())
		}
	}
	releaseStop()
	select {
	case <-dynamicRefreshDone:
	case <-time.After(time.Second):
		t.Fatal("dynamic refresh did not reconcile after reload commit")
	}
	waitForObserverStarts(t, &mu, starts, map[string]int{newConfigured.Key: 1, oldDynamic.Key: 2})
	mu.Lock()
	if got := len(starts[oldConfigured.Key]); got != 1 {
		mu.Unlock()
		t.Fatalf("stale discovery replacement restarted removed configured observer: starts=%d", got)
	}
	mu.Unlock()
	for _, status := range runtime.RepositoryStatuses() {
		if status.Key == oldConfigured.Key {
			t.Fatalf("removed configured observer was restored: %#v", runtime.RepositoryStatuses())
		}
	}
	statuses := runtime.RepositoryStatuses()
	if len(statuses) != 2 || statuses[0].Key != newConfigured.Key || statuses[1].Key != oldDynamic.Key || statuses[1].Path != newDynamicPath {
		t.Fatalf("post-reload observer inventory=%#v", statuses)
	}
	watch.StopObservers()
}

func waitForObserverStarts(t *testing.T, mu *sync.Mutex, starts map[string][]*trackedDynamicWatchProcess, want map[string]int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ready := true
		for key, count := range want {
			if len(starts[key]) < count {
				ready = false
				break
			}
		}
		mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("observer starts=%v want at least=%v", countsOfObserverStarts(starts), want)
}

func countsOfObserverStarts(starts map[string][]*trackedDynamicWatchProcess) map[string]int {
	counts := make(map[string]int, len(starts))
	for key, processes := range starts {
		counts[key] = len(processes)
	}
	return counts
}

func waitForDynamicRepositoryTarget(t *testing.T, manager *workerManager, repositoryID string, wantPresent bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, err := manager.repositoryTarget(repositoryID)
		if (err == nil) == wantPresent {
			return
		}
		time.Sleep(time.Millisecond)
	}
	_, err := manager.repositoryTarget(repositoryID)
	t.Fatalf("dynamic target present=%t err=%v want_present=%t", err == nil, err, wantPresent)
}
