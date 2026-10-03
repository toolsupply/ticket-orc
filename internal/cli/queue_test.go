package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type forecastTicketReader struct {
	actor        string
	repositoryID string
	active       map[string]map[string]ticketclient.ListResult
	ready        map[string]ticketclient.ListResult
	frontiers    map[string]ticketclient.ListResult
	activeErr    error
	readyErr     error
	calls        *[]string
	filters      *[]ticketclient.QueueFilters
	readyLimits  *[]int
}

func (r *forecastTicketReader) ActiveClaims(_ context.Context, queue string, _ int) (ticketclient.ListResult, error) {
	if r.calls != nil {
		*r.calls = append(*r.calls, r.actor+":active:"+queue)
	}
	if r.activeErr != nil {
		return ticketclient.ListResult{}, r.activeErr
	}
	return r.active[r.actor][queue], nil
}

func (r *forecastTicketReader) ReadyFrontier(_ context.Context, queue string, filters ticketclient.QueueFilters, limit int) (ticketclient.ListResult, error) {
	if r.calls != nil {
		*r.calls = append(*r.calls, r.actor+":ready:"+queue)
	}
	if r.filters != nil {
		*r.filters = append(*r.filters, filters)
	}
	if r.readyLimits != nil {
		*r.readyLimits = append(*r.readyLimits, limit)
	}
	if r.readyErr != nil {
		return ticketclient.ListResult{}, r.readyErr
	}
	if r.frontiers != nil {
		key := queueForecastFrontierKey(queueForecastOwner{RepositoryID: r.repositoryID, Queue: queue, filters: filters})
		return r.frontiers[key], nil
	}
	return r.ready[queue], nil
}

func TestManagedQueueForecastUsesEffectiveSelectors(t *testing.T) {
	dir := t.TempDir()
	repositoryPath := t.TempDir()
	effective := supervisor.RunWorker{Name: "reviewer", Config: supervisor.RoleConfig{
		Role: RoleCoder, RoleName: "backend", TicketQueue: "open", Actor: "reviewer",
		RepositoryIdentity: localRepoID, Repository: repositoryPath,
		TicketTags: "effective", ReviewSkipTags: "effective-exclusion",
	}, TicketInfo: &ticketclient.RepositoryInfo{ID: localRepoID, Path: repositoryPath}}
	runtime := NewRuntimeState([]supervisor.RunWorker{effective})
	desired := effective
	desired.Config.Role = RoleReviewer
	desired.Config.RoleName = "quality"
	desired.Config.TicketQueue = "review"
	desired.Config.TicketTags = "desired"
	desired.Config.ReviewSkipTags = "desired-exclusion"
	runtime.SetConfiguredWorkers([]supervisor.RunWorker{desired})
	status := runtimeDaemonStatus(runtime, nil)
	loaded := LoadedFileConfig{
		Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
		Config: FileConfig{
			// The effective role was removed by reload. Forecasting a running
			// worker must not require its old desired role definition to remain.
			Roles: map[string]RoleFileConfig{"quality": {TicketQueue: "review", TicketTags: []string{"desired"}}},
		},
	}
	active := map[string]map[string]ticketclient.ListResult{}
	ready := map[string]ticketclient.ListResult{"open": {}, "review": {}}
	var calls []string
	var filters []ticketclient.QueueFilters
	open := func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, active: active, ready: ready, calls: &calls, filters: &filters}, nil
	}
	forecast, err := queueForecast(context.Background(), loaded, status, open)
	if err != nil {
		t.Fatal(err)
	}
	wantEffective := ticketclient.QueueFilters{Tags: []string{"effective"}}
	if len(forecast) != 1 || forecast[0].Role != "backend" || forecast[0].Queue != "open" || len(filters) != 1 || !reflect.DeepEqual(filters[0], wantEffective) || !reflect.DeepEqual(calls, []string{"reviewer:active:open", "reviewer:active:review", "reviewer:ready:open"}) {
		t.Fatalf("effective forecast=%#v filters=%#v calls=%v", forecast, filters, calls)
	}

	// Restart adopts the desired role and all its selectors. The same producer
	// path should now report that new effective policy.
	runtime.SetEffectiveWorker(desired)
	status = runtimeDaemonStatus(runtime, nil)
	calls, filters = nil, nil
	forecast, err = queueForecast(context.Background(), loaded, status, open)
	if err != nil {
		t.Fatal(err)
	}
	wantDesired := ticketclient.QueueFilters{Tags: []string{"desired"}, WithoutTags: []string{"desired-exclusion"}}
	if len(forecast) != 1 || forecast[0].Role != "quality" || forecast[0].Queue != "review" || len(filters) != 1 || !reflect.DeepEqual(filters[0], wantDesired) || !reflect.DeepEqual(calls, []string{"reviewer:active:open", "reviewer:active:review", "reviewer:ready:review"}) {
		t.Fatalf("restarted forecast=%#v filters=%#v calls=%v", forecast, filters, calls)
	}
}

func TestSteerQueueForecastUsesCommittedPolicyProjection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	repositoryPath := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: localRepoID, RepositoryPath: repositoryPath, Actor: "steered", Role: "quality",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	policies := newSteerPolicyStore(map[string]steerRolePolicy{
		"quality": {TicketQueue: "open", QueueFilters: ticketclient.QueueFilters{Tags: []string{"backend"}}},
	})

	makeStatus := func(code string) daemon.Status {
		steer := daemon.SteerStatus{
			RepositoryID: registration.RepositoryID, RepositoryName: registration.RepositoryName,
			Role: registration.Role, Actor: registration.Actor, Harness: registration.Harness,
			Session: registration.SessionID, State: "idle", Code: code,
		}
		return runtimeDaemonStatusWithSteerPolicies(nil, []daemon.SteerStatus{steer}, policies)
	}
	desired := LoadedFileConfig{
		Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
		Config: FileConfig{
			Roles:  map[string]RoleFileConfig{"quality": {TicketQueue: "open", TicketTags: []string{"frontend"}}},
			Review: ReviewFileConfig{SkipTags: []string{"desired-exclusion"}},
		},
	}
	backend := queueForecastFrontierKey(queueForecastOwner{RepositoryID: localRepoID, Queue: "open", filters: ticketclient.QueueFilters{Tags: []string{"backend"}}})
	frontend := queueForecastFrontierKey(queueForecastOwner{RepositoryID: localRepoID, Queue: "open", filters: ticketclient.QueueFilters{Tags: []string{"frontend"}}})
	review := queueForecastFrontierKey(queueForecastOwner{RepositoryID: localRepoID, Queue: "review", filters: ticketclient.QueueFilters{Tags: []string{"urgent"}, WithoutTags: []string{"manual"}}})
	frontiers := map[string]ticketclient.ListResult{
		backend:  {Items: []ticketclient.Ticket{{ID: "20261001-20001", State: "open"}}},
		frontend: {Items: []ticketclient.Ticket{{ID: "20261001-20002", State: "open"}}},
		review:   {Items: []ticketclient.Ticket{{ID: "20261001-20003", State: "review"}}},
	}
	forecast := func(t *testing.T, loaded LoadedFileConfig, status daemon.Status) ([]queueForecastOwner, []ticketclient.QueueFilters, []string) {
		t.Helper()
		var filters []ticketclient.QueueFilters
		var calls []string
		owners, err := queueForecast(ctx, loaded, status, func(identity currentTicketIdentity) (localTicketReader, error) {
			return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID,
				active: map[string]map[string]ticketclient.ListResult{}, frontiers: frontiers, filters: &filters, calls: &calls}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return owners, filters, calls
	}

	t.Run("forecast follows committed status over changed desired config", func(t *testing.T) {
		// The file now desires frontend, but the supplied daemon status still
		// carries the active backend policy.
		owners, filters, _ := forecast(t, desired, makeStatus(""))
		if len(owners) != 1 || owners[0].Queue != "open" || owners[0].Next == nil || owners[0].Next.ID != "20261001-20001" || !reflect.DeepEqual(filters, []ticketclient.QueueFilters{{Tags: []string{"backend"}}}) {
			t.Fatalf("forecast followed desired config instead of committed policy: owners=%#v filters=%#v", owners, filters)
		}
	})

	t.Run("removed desired role retains the committed selector", func(t *testing.T) {
		removed := desired
		removed.Config.Roles = nil
		owners, filters, _ := forecast(t, removed, makeStatus(""))
		if len(owners) != 1 || owners[0].Next == nil || owners[0].Next.ID != "20261001-20001" || !reflect.DeepEqual(filters, []ticketclient.QueueFilters{{Tags: []string{"backend"}}}) {
			t.Fatalf("forecast failed for still-committed role removed from disk: owners=%#v filters=%#v", owners, filters)
		}
	})

	t.Run("forecast uses review exclusions from the status projection", func(t *testing.T) {
		committed := steerRolePolicy{TicketQueue: "review", QueueFilters: ticketclient.QueueFilters{Tags: []string{"urgent"}, WithoutTags: []string{"manual"}}}
		policies.Replace(map[string]steerRolePolicy{"quality": committed})
		// Disk state can move again after the policy commit; the forecast must
		// describe the committed policy that dynamic dispatch now queries.
		loaded := desired
		loaded.Config.Roles = map[string]RoleFileConfig{"quality": {TicketQueue: "open", TicketTags: []string{"frontend"}}}
		loaded.Config.Review = ReviewFileConfig{SkipTags: []string{"desired-exclusion"}}
		owners, filters, _ := forecast(t, loaded, makeStatus(""))
		want := []ticketclient.QueueFilters{{Tags: []string{"urgent"}, WithoutTags: []string{"manual"}}}
		if len(owners) != 1 || owners[0].Queue != "review" || owners[0].Next == nil || owners[0].Next.ID != "20261001-20003" || !reflect.DeepEqual(filters, want) {
			t.Fatalf("forecast did not use committed review policy: owners=%#v filters=%#v", owners, filters)
		}
	})

	t.Run("committed role removal keeps active claims visible without querying readiness", func(t *testing.T) {
		policies.Replace(map[string]steerRolePolicy{})
		status := makeStatus("unknown_role")
		status.Steer[0].State = "degraded"
		var readyCalls []string
		owners, err := queueForecast(ctx, desired, status, func(identity currentTicketIdentity) (localTicketReader, error) {
			return &forecastTicketReader{actor: identity.Actor,
				active: map[string]map[string]ticketclient.ListResult{"steered": {
					"open": {Items: []ticketclient.Ticket{{ID: "20261001-20004", State: "open"}}},
				}},
				frontiers: frontiers, calls: &readyCalls}, nil
		})
		if err != nil {
			t.Fatalf("forecast after role removal: %v", err)
		}
		if len(owners) != 1 || owners[0].Reason != "role policy unavailable" || len(owners[0].Active) != 1 || owners[0].Active[0].ID != "20261001-20004" || strings.Contains(strings.Join(readyCalls, " "), ":ready:") {
			t.Fatalf("role removal lost active ownership or queried an unsupported frontier: owners=%#v calls=%v", owners, readyCalls)
		}
	})
}

func TestSteerQueueForecastTracksCommittedPolicyReloads(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stateDir := filepath.Join(root, "local")
	configPath := filepath.Join(root, "config.json")
	configData := func(tags, exclusions string, includeQuality bool) string {
		roles := `"coder":{"ticket_queue":"open","nudge_prompt":"coding"}`
		if includeQuality {
			roles += fmt.Sprintf(`,"quality":{"ticket_queue":"review","nudge_prompt":"review","ticket_tags":[%q]}`, tags)
		} else {
			roles += `,"reviewer":{"ticket_queue":"review","nudge_prompt":"review"}`
		}
		return fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"default_role":"coder","roles":{%s},"review":{"skip_tags":[%q]}}`, stateDir, roles, exclusions)
	}
	writeConfigFixture(t, configPath, configData("backend", "old-exclusion", true))
	loaded, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	rolePolicies, err := resolveSteerRolePolicies(loaded.Config, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	policies := newSteerPolicyStore(rolePolicies)
	manager := newWorkerManager(ctx, RunConfig{
		ConfigPath: configPath, StateDir: stateDir, InstanceID: loaded.Config.ID, steerPolicies: policies,
		steerDirty: newSteerDirtySet(),
	}, os.Args[0], io.Discard, io.Discard, nil)
	defer manager.repositoryWatch.StopObservers()

	repositoryPath := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(stateDir).Join(ctx, state.SteerRegistration{
		RepositoryID: localRepoID, RepositoryPath: repositoryPath, Actor: "quality-worker", Role: "quality",
		Harness: "codex", Transport: testSteerCodexTransport(stateDir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	frontiers := make(map[string]ticketclient.ListResult)
	addFrontier := func(queue string, filters ticketclient.QueueFilters, id string) {
		key := queueForecastFrontierKey(queueForecastOwner{RepositoryID: localRepoID, Queue: queue, filters: filters})
		frontiers[key] = ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: id, State: queue}}}
	}
	addFrontier("review", ticketclient.QueueFilters{Tags: []string{"backend"}, WithoutTags: []string{"old-exclusion"}}, "20261001-20101")
	addFrontier("review", ticketclient.QueueFilters{Tags: []string{"frontend"}, WithoutTags: []string{"new-exclusion"}}, "20261001-20102")
	addFrontier("review", ticketclient.QueueFilters{Tags: []string{"committed"}, WithoutTags: []string{"committed-exclusion"}}, "20261001-20103")
	active := map[string]map[string]ticketclient.ListResult{}
	cachedStatuses := &dynamicSteerStatus{}
	initialStatus := registrationStartupStatuses([]state.SteerRegistration{registration}, "")
	initialStatus[0].State = string(orc.SteerIdle)
	applySteerStatusPolicy(&initialStatus[0], policies.Snapshot().roles[registration.Role])
	cachedStatuses.replace(initialStatus)
	makeStatus := func() daemon.Status {
		return runtimeDaemonStatusWithSteerPolicies(nil, cachedStatuses.snapshot(), policies)
	}
	forecast := func(t *testing.T, candidate LoadedFileConfig) ([]queueForecastOwner, []ticketclient.QueueFilters, []string) {
		t.Helper()
		var filters []ticketclient.QueueFilters
		var calls []string
		owners, err := queueForecast(ctx, candidate, makeStatus(), func(identity currentTicketIdentity) (localTicketReader, error) {
			return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: active,
				frontiers: frontiers, filters: &filters, calls: &calls}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return owners, filters, calls
	}
	assertSelector := func(t *testing.T, candidate LoadedFileConfig, want ticketclient.QueueFilters, ticketID string) {
		t.Helper()
		owners, filters, _ := forecast(t, candidate)
		if len(owners) != 1 || owners[0].Queue != "review" || owners[0].Next == nil || owners[0].Next.ID != ticketID || !reflect.DeepEqual(filters, []ticketclient.QueueFilters{want}) {
			t.Fatalf("forecast=%#v filters=%#v, want selector=%#v ticket=%s", owners, filters, want, ticketID)
		}
	}

	assertSelector(t, loaded, ticketclient.QueueFilters{Tags: []string{"backend"}, WithoutTags: []string{"old-exclusion"}}, "20261001-20101")

	writeConfigFixture(t, configPath, configData("frontend", "new-exclusion", true))
	pending, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	// A valid file edit is not active policy until reload commits.
	assertSelector(t, pending, ticketclient.QueueFilters{Tags: []string{"backend"}, WithoutTags: []string{"old-exclusion"}}, "20261001-20101")
	if result, err := manager.reload(&errorAfterErrChecks{Context: ctx, failAt: 1}); err == nil || result.Applied {
		t.Fatalf("pre-commit reload result=%#v err=%v, want rejected reload", result, err)
	}
	assertSelector(t, pending, ticketclient.QueueFilters{Tags: []string{"backend"}, WithoutTags: []string{"old-exclusion"}}, "20261001-20101")

	if result, err := manager.reload(ctx); err != nil || !result.Applied {
		t.Fatalf("committed reload result=%#v err=%v", result, err)
	}
	pending, err = LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	assertSelector(t, pending, ticketclient.QueueFilters{Tags: []string{"frontend"}, WithoutTags: []string{"new-exclusion"}}, "20261001-20102")

	writeConfigFixture(t, configPath, configData("committed", "committed-exclusion", true))
	postCommit, err := manager.reload(&errorAfterErrChecks{Context: ctx, failAt: 4})
	if err == nil || !postCommit.Applied {
		t.Fatalf("post-commit reload result=%#v err=%v, want applied state with reconciliation error", postCommit, err)
	}
	writeConfigFixture(t, configPath, configData("frontend", "new-exclusion", true))
	changedAfterCommit, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	assertSelector(t, changedAfterCommit, ticketclient.QueueFilters{Tags: []string{"committed"}, WithoutTags: []string{"committed-exclusion"}}, "20261001-20103")

	removed := changedAfterCommit
	removed.Config.Roles = nil
	assertSelector(t, removed, ticketclient.QueueFilters{Tags: []string{"committed"}, WithoutTags: []string{"committed-exclusion"}}, "20261001-20103")
	writeConfigFixture(t, configPath, configData("", "safe", false))
	if _, err := LoadFileConfig(root, configPath, true); err != nil {
		t.Fatalf("load role-removal config: %v", err)
	}
	if result, err := manager.reload(ctx); err != nil || !result.Applied {
		t.Fatalf("role-removal reload result=%#v err=%v", result, err)
	}
	removed, err = LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}

	// Once a committed policy no longer contains the registered role, readiness
	// cannot be queried. Actor-wide active ownership still remains visible.
	active[registration.Actor] = map[string]ticketclient.ListResult{
		"open": {Items: []ticketclient.Ticket{{ID: "20261001-20104", State: "open"}}},
	}
	owners, filters, calls := forecast(t, removed)
	if len(owners) != 1 || owners[0].Reason != "role policy unavailable" || len(owners[0].Active) != 1 || owners[0].Active[0].ID != "20261001-20104" || len(filters) != 0 || strings.Contains(strings.Join(calls, " "), ":ready:") {
		t.Fatalf("role removal lost active ownership or queried readiness: owners=%#v filters=%#v calls=%v", owners, filters, calls)
	}
}

func (r *forecastTicketReader) Close() error { return nil }

func TestQueueForecastGloballyDeduplicatesOverlappingSelectorFrontiers(t *testing.T) {
	dir := t.TempDir()
	repositoryPath := t.TempDir()
	workers := []daemon.WorkerStatus{
		{Name: "owner-a", EffectiveRoleName: "backend-urgent", EffectiveTicketQueue: "open", EffectiveTicketTags: []string{"backend", "urgent"}},
		{Name: "owner-b", EffectiveRoleName: "backend", EffectiveTicketQueue: "open", EffectiveTicketTags: []string{"backend"}},
		{Name: "owner-c", EffectiveRoleName: "urgent", EffectiveTicketQueue: "open", EffectiveTicketTags: []string{"urgent"}},
	}
	for index := range workers {
		workers[index].TicketActor = workers[index].Name
		workers[index].RepositoryID = localRepoID
		workers[index].RepositoryPath = repositoryPath
		workers[index].State = "running"
	}
	frontier := func(tags []string) string {
		return queueForecastFrontierKey(queueForecastOwner{RepositoryID: localRepoID, Queue: "open", filters: ticketclient.QueueFilters{Tags: tags}})
	}
	frontiers := map[string]ticketclient.ListResult{
		frontier([]string{"backend", "urgent"}): {
			Items: []ticketclient.Ticket{{ID: "20261001-10001", State: "open"}},
		},
		frontier([]string{"backend"}): {
			Items: []ticketclient.Ticket{{ID: "20261001-10001", State: "open"}, {ID: "20261001-10002", State: "open"}}, More: true,
		},
		frontier([]string{"urgent"}): {
			Items: []ticketclient.Ticket{{ID: "20261001-10001", State: "open"}, {ID: "20261001-10003", State: "open"}},
		},
	}
	var limits []int
	var queriedFilters []ticketclient.QueueFilters
	status := daemon.Status{Workers: workers}
	owners, err := queueForecast(context.Background(), LoadedFileConfig{
		Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
	}, status, func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: map[string]map[string]ticketclient.ListResult{}, frontiers: frontiers, filters: &queriedFilters, readyLimits: &limits}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{"20261001-10001", "20261001-10002", "20261001-10003"}
	seen := make(map[string]bool)
	for index, owner := range owners {
		if owner.Owner != workers[index].Name || owner.Next == nil || owner.Next.ID != wantIDs[index] {
			t.Fatalf("owner[%d] forecast=%#v, want owner=%q ticket=%q", index, owner, workers[index].Name, wantIDs[index])
		}
		if seen[owner.Next.ID] {
			t.Fatalf("ticket %s was assigned more than once: %#v", owner.Next.ID, owners)
		}
		seen[owner.Next.ID] = true
	}
	if !owners[1].MoreReady || owners[2].MoreReady {
		t.Fatalf("MoreReady did not preserve authoritative bounded frontier state: %#v", owners)
	}
	if !reflect.DeepEqual(limits, []int{localTicketLimit, localTicketLimit, localTicketLimit}) {
		t.Fatalf("ready frontier limits=%v, want bounded limit %d for each distinct selector", limits, localTicketLimit)
	}
	wantFilters := []ticketclient.QueueFilters{
		{Tags: []string{"backend", "urgent"}}, {Tags: []string{"backend"}}, {Tags: []string{"urgent"}},
	}
	if !reflect.DeepEqual(queriedFilters, wantFilters) {
		t.Fatalf("selector-specific frontier queries=%#v, want %#v", queriedFilters, wantFilters)
	}
}

func TestQueueForecastDeduplicationIsScopedByRepositoryAndQueue(t *testing.T) {
	dir := t.TempDir()
	repositoryPath := t.TempDir()
	workers := []daemon.WorkerStatus{
		{Name: "same-repo-open", EffectiveRoleName: "open", EffectiveTicketQueue: "open", TicketActor: "open-a", RepositoryID: localRepoID, RepositoryPath: repositoryPath, State: "running"},
		{Name: "other-repo-open", EffectiveRoleName: "open", EffectiveTicketQueue: "open", TicketActor: "open-b", RepositoryID: joinOtherRepositoryID, RepositoryPath: repositoryPath, State: "running"},
		{Name: "same-repo-review", EffectiveRoleName: "review", EffectiveTicketQueue: "review", TicketActor: "review-a", RepositoryID: localRepoID, RepositoryPath: repositoryPath, State: "running"},
	}
	frontiers := make(map[string]ticketclient.ListResult)
	for _, worker := range workers {
		owner := queueForecastOwner{RepositoryID: worker.RepositoryID, Queue: worker.EffectiveTicketQueue}
		frontiers[queueForecastFrontierKey(owner)] = ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20261001-10001", State: worker.EffectiveTicketQueue}}}
	}
	owners, err := queueForecast(context.Background(), LoadedFileConfig{
		Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
	}, daemon.Status{Workers: workers}, func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: map[string]map[string]ticketclient.ListResult{}, frontiers: frontiers}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(owners) != 3 {
		t.Fatalf("forecast owners=%#v", owners)
	}
	for _, owner := range owners {
		if owner.Next == nil || owner.Next.ID != "20261001-10001" {
			t.Fatalf("deduplication crossed repository/queue scope: %#v", owners)
		}
	}
}

func TestQueueForecastCacheKeyUsesCanonicalCompleteFilters(t *testing.T) {
	base := queueForecastOwner{RepositoryID: localRepoID, Queue: "review", filters: ticketclient.QueueFilters{
		Tags: []string{"urgent", "backend"}, WithoutTags: []string{"blocked", "obsolete"},
	}}
	reordered := base
	reordered.filters = ticketclient.QueueFilters{Tags: []string{"backend", "urgent"}, WithoutTags: []string{"obsolete", "blocked"}}
	empty := base
	empty.filters = ticketclient.QueueFilters{Tags: []string{}, WithoutTags: nil}
	if queueForecastFrontierKey(base) != queueForecastFrontierKey(reordered) {
		t.Fatal("reordered selectors did not reuse the canonical frontier key")
	}
	if queueForecastFrontierKey(empty) == queueForecastFrontierKey(base) {
		t.Fatal("different required/excluded selectors shared a frontier key")
	}
	emptyNil := empty
	emptyNil.filters = ticketclient.QueueFilters{}
	if queueForecastFrontierKey(empty) != queueForecastFrontierKey(emptyNil) {
		t.Fatal("nil and empty selector slices did not reuse the same frontier key")
	}
	withoutDifferentRequired := base
	withoutDifferentRequired.filters = ticketclient.QueueFilters{Tags: []string{"backend"}, WithoutTags: []string{"blocked", "obsolete"}}
	if queueForecastFrontierKey(withoutDifferentRequired) == queueForecastFrontierKey(base) {
		t.Fatal("different required tag sets shared a frontier key")
	}
	withoutDifferentExclusion := base
	withoutDifferentExclusion.filters = ticketclient.QueueFilters{Tags: []string{"backend", "urgent"}, WithoutTags: []string{"blocked"}}
	if queueForecastFrontierKey(withoutDifferentExclusion) == queueForecastFrontierKey(base) {
		t.Fatal("different exclusion sets shared a frontier key")
	}
	otherScope := base
	otherScope.RepositoryID = joinOtherRepositoryID
	if queueForecastFrontierKey(otherScope) == queueForecastFrontierKey(base) {
		t.Fatal("different repositories shared a frontier key")
	}
	otherScope = base
	otherScope.Queue = "open"
	if queueForecastFrontierKey(otherScope) == queueForecastFrontierKey(base) {
		t.Fatal("different queues shared a frontier key")
	}
}

func TestQueueForecastReusesCanonicalEquivalentFrontiers(t *testing.T) {
	for _, test := range []struct {
		name       string
		firstTags  []string
		secondTags []string
	}{
		{name: "reordered selectors", firstTags: []string{"urgent", "backend"}, secondTags: []string{"backend", "urgent"}},
		{name: "nil and empty selectors", firstTags: nil, secondTags: []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := t.TempDir()
			workers := []daemon.WorkerStatus{
				{Name: "first", EffectiveRoleName: "first", EffectiveTicketQueue: "open", EffectiveTicketTags: test.firstTags, TicketActor: "first", RepositoryID: localRepoID, RepositoryPath: path, State: "running"},
				{Name: "second", EffectiveRoleName: "second", EffectiveTicketQueue: "open", EffectiveTicketTags: test.secondTags, TicketActor: "second", RepositoryID: localRepoID, RepositoryPath: path, State: "running"},
			}
			filters := ticketclient.QueueFilters{Tags: append([]string(nil), test.firstTags...)}
			key := queueForecastFrontierKey(queueForecastOwner{RepositoryID: localRepoID, Queue: "open", filters: filters})
			frontiers := map[string]ticketclient.ListResult{key: {Items: []ticketclient.Ticket{
				{ID: "20261001-10001", State: "open"}, {ID: "20261001-10002", State: "open"},
			}}}
			var queriedFilters []ticketclient.QueueFilters
			owners, err := queueForecast(context.Background(), LoadedFileConfig{
				Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
			}, daemon.Status{Workers: workers}, func(identity currentTicketIdentity) (localTicketReader, error) {
				return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: map[string]map[string]ticketclient.ListResult{}, frontiers: frontiers, filters: &queriedFilters}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(queriedFilters) != 1 || len(owners) != 2 || owners[0].Next == nil || owners[1].Next == nil || owners[0].Next.ID != "20261001-10001" || owners[1].Next.ID != "20261001-10002" {
				t.Fatalf("equivalent frontier was not reused with stable offsets: owners=%#v queries=%#v", owners, queriedFilters)
			}
		})
	}
}

func TestQueueForecastReturnsActiveAndReadyQueryFailures(t *testing.T) {
	for _, test := range []struct {
		name      string
		activeErr error
		readyErr  error
		want      string
	}{
		{name: "active ownership", activeErr: errors.New("active unavailable"), want: "read active claims"},
		{name: "ready frontier", readyErr: errors.New("frontier unavailable"), want: "read open ready frontier"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := t.TempDir()
			status := daemon.Status{Workers: []daemon.WorkerStatus{{
				Name: "worker", EffectiveRoleName: "backend", EffectiveTicketQueue: "open", TicketActor: "worker",
				RepositoryID: localRepoID, RepositoryPath: path, State: "running",
			}}}
			_, err := queueForecast(context.Background(), LoadedFileConfig{
				Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
			}, status, func(identity currentTicketIdentity) (localTicketReader, error) {
				return &forecastTicketReader{actor: identity.Actor, repositoryID: identity.RepositoryID, active: map[string]map[string]ticketclient.ListResult{}, activeErr: test.activeErr, readyErr: test.readyErr}, nil
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("queue forecast error=%v, want it to contain %q", err, test.want)
			}
		})
	}
}

func TestQueueForecastSharesOrderedFrontierAndLeavesStateUnchanged(t *testing.T) {
	dir := t.TempDir()
	repositoryPath := t.TempDir()
	ctx := context.Background()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: localRepoID, RepositoryPath: repositoryPath, RepositoryName: "project",
		Actor: "reviewer", Role: "reviewer", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	conflictedRegistration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: localRepoID, RepositoryPath: repositoryPath, RepositoryName: "project",
		Actor: "coder", Role: "reviewer", Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a7",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeStore := state.NewSteerRuntimeStore(dir)
	if err := runtimeStore.Reconcile(ctx, []state.SteerRegistration{registration, conflictedRegistration}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtimeStore.CompleteDelivery(ctx, registration, "queued", true, true); err != nil || !updated {
		t.Fatalf("seed delivery updated=%t err=%v", updated, err)
	}
	registrationBefore, err := state.NewRegistrationStore(dir).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deliveryBefore, err := runtimeStore.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	openFrontier := ticketclient.ListResult{Items: []ticketclient.Ticket{
		{ID: "20260926-10001", Title: "First open item", State: "open", Priority: 1},
		{ID: "20260926-10002", Title: "Second open item", State: "open", Priority: 2},
	}}
	reviewFrontier := ticketclient.ListResult{Items: []ticketclient.Ticket{
		{ID: "20260926-10003", Title: "Review item", State: "review", Priority: 2},
	}}
	active := map[string]map[string]ticketclient.ListResult{
		"coder": {"open": {Items: []ticketclient.Ticket{{ID: "20260926-10000", Title: "Fix workers output formatting", State: "open", Assignee: "coder", Priority: 2}}}},
	}
	ready := map[string]ticketclient.ListResult{"open": openFrontier, "review": reviewFrontier}
	ticketStateBefore := struct {
		Active map[string]map[string]ticketclient.ListResult
		Ready  map[string]ticketclient.ListResult
	}{active, ready}
	var ticketCalls []string
	open := func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, active: active, ready: ready, calls: &ticketCalls}, nil
	}
	loaded := LoadedFileConfig{
		Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
		Config: FileConfig{
			DefaultRole: "coder",
			Roles: map[string]RoleFileConfig{
				"coder": {TicketQueue: "open"}, "architect": {TicketQueue: "open"}, "reviewer": {TicketQueue: "review"},
			},
			Workers: map[string]WorkerFileConfig{
				"coder-owner": {Role: "coder"}, "architect-owner": {Role: "architect"},
			},
		},
	}
	status := daemon.Status{
		Workers: []daemon.WorkerStatus{
			{Name: "coder-owner", Role: "coder", TicketActor: "coder", State: "running", RepositoryID: localRepoID, RepositoryName: "project", RepositoryPath: repositoryPath},
			{Name: "architect-owner", Role: "architect", TicketActor: "architect", State: "running", RepositoryID: localRepoID, RepositoryName: "project", RepositoryPath: repositoryPath},
		},
		Steer: []daemon.SteerStatus{
			{RepositoryID: localRepoID, RepositoryName: "project", Role: "reviewer", Actor: "reviewer", Harness: registration.Harness, Session: steerTestThread, EffectiveTicketQueue: "review", State: "queued"},
			{RepositoryID: localRepoID, RepositoryName: "project", Role: "reviewer", Actor: "coder", Harness: conflictedRegistration.Harness, Session: conflictedRegistration.SessionID, EffectiveTicketQueue: "review", State: "conflict", ManagedOwner: "coder-owner"},
		},
	}

	first, err := queueForecast(ctx, loaded, status, open)
	if err != nil {
		t.Fatal(err)
	}
	second, err := queueForecast(ctx, loaded, status, open)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated forecast changed: first=%#v second=%#v", first, second)
	}
	if len(first) != 4 {
		t.Fatalf("forecast owner count=%d, want two managed workers and two steer sessions: %#v", len(first), first)
	}
	if first[0].Owner != "coder-owner" || first[0].Next == nil || first[0].Next.ID != "20260926-10001" || first[1].Owner != "architect-owner" || first[1].Next == nil || first[1].Next.ID != "20260926-10002" {
		t.Fatalf("same-queue forecast did not preserve managed dispatch order and Ticket frontier: %#v", first[:2])
	}
	var output bytes.Buffer
	if err := renderQueueForecast(&output, first); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.HasPrefix(text, "\n") || !strings.HasSuffix(text, "\n\n") {
		t.Fatalf("queue output must start and end with a blank line: %q", text)
	}
	for _, want := range []string{"Repository: project (d659917f…) " + repositoryPath} {
		if !strings.Contains(text, want) {
			t.Errorf("queue output missing %q: %s", want, text)
		}
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) != 7 || !reflect.DeepEqual(strings.Fields(lines[2]), []string{"Actor", "Ticket", "Title", "State"}) {
		t.Fatalf("queue table header or row count is wrong: %q", lines)
	}
	wantRows := [][]string{
		{"coder", "20260926-10000", "Fix", "workers", "outp...", "claimed"},
		{"coder", "20260926-10001", "First", "open", "item", "queued"},
		{"architect", "20260926-10002", "Second", "open", "item", "queued"},
		{"reviewer", "20260926-10003", "Review", "item", "queued"},
	}
	for index, want := range wantRows {
		if got := strings.Fields(lines[index+3]); !reflect.DeepEqual(got, want) {
			t.Errorf("queue row[%d]=%v, want %v", index, got, want)
		}
	}
	for _, forbidden := range []string{"coder-owner", "managed", "project/reviewer", "already notified", "  active", "  next"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("queue output included owner diagnostics %q: %s", forbidden, text)
		}
	}
	positions := []int{
		strings.Index(text, "20260926-10000"), strings.Index(text, "20260926-10001"),
		strings.Index(text, "20260926-10002"), strings.Index(text, "20260926-10003"),
	}
	for index := 1; index < len(positions); index++ {
		if positions[index] <= positions[index-1] {
			t.Fatalf("queue tickets are not in forecast order: positions=%v output=%s", positions, text)
		}
	}
	for _, ticketID := range []string{"20260926-10000", "20260926-10001", "20260926-10002", "20260926-10003"} {
		if strings.Count(text, ticketID) != 1 {
			t.Fatalf("Ticket %s was assigned more than once: %s", ticketID, text)
		}
	}
	unconfiguredWorker := status
	unconfiguredWorker.Workers = append(append([]daemon.WorkerStatus(nil), status.Workers...), daemon.WorkerStatus{
		Name: "unconfigured-owner", TicketActor: "coder", State: "running",
		RepositoryID: localRepoID, RepositoryName: "project", RepositoryPath: repositoryPath,
	})
	if _, err := queueForecast(ctx, loaded, unconfiguredWorker, open); err == nil || !strings.Contains(err.Error(), `managed worker "unconfigured-owner" has unsupported role or queue ""/""`) {
		t.Fatalf("unconfigured managed worker inherited default_role: error=%v", err)
	}
	pausedStatus := status
	pausedStatus.Mode = "paused"
	var pausedOutput bytes.Buffer
	if err := writeQueueForecast(ctx, &pausedOutput, false, loaded, pausedStatus, open); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(pausedOutput.String(), "daemon mode=paused dispatch=inhibited") || !strings.Contains(pausedOutput.String(), "20260926-10001") {
		t.Fatalf("paused queue did not report inhibition and keep forecasting ready Ticket work: %q", pausedOutput.String())
	}

	registrationAfter, err := state.NewRegistrationStore(dir).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deliveryAfter, err := runtimeStore.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(registrationBefore, registrationAfter) || !reflect.DeepEqual(deliveryBefore, deliveryAfter) {
		t.Fatalf("queue forecast mutated Orc state: registrations before/after=%#v/%#v deliveries=%#v/%#v", registrationBefore, registrationAfter, deliveryBefore, deliveryAfter)
	}
	ticketStateAfter := struct {
		Active map[string]map[string]ticketclient.ListResult
		Ready  map[string]ticketclient.ListResult
	}{active, ready}
	if !reflect.DeepEqual(ticketStateBefore, ticketStateAfter) {
		t.Fatalf("queue forecast mutated Ticket fixture: before=%#v after=%#v", ticketStateBefore, ticketStateAfter)
	}
	if len(ticketCalls) == 0 {
		t.Fatal("forecast did not query Ticket")
	}
	for _, call := range ticketCalls {
		if strings.Contains(call, "claim") || strings.Contains(call, "release") || strings.Contains(call, "next") || strings.Contains(call, "wait") {
			t.Fatalf("forecast issued a mutating Ticket operation: %q", call)
		}
	}
}

func TestQueueForecastIgnoresStatusFromDifferentHarness(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	repositoryPath := t.TempDir()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: localRepoID, RepositoryPath: repositoryPath, Actor: "reviewer", Role: "reviewer",
		Harness: "codex", Transport: testSteerCodexTransport(dir), SessionID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	loaded := LoadedFileConfig{
		Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
		Config:   FileConfig{Roles: map[string]RoleFileConfig{"reviewer": {TicketQueue: "review"}}},
	}
	status := daemon.Status{Steer: []daemon.SteerStatus{{
		RepositoryID: registration.RepositoryID, Actor: registration.Actor, Harness: "replacement-harness",
		Session: registration.SessionID, State: "conflict", ManagedOwner: "stale-owner",
	}}}
	owners, err := queueForecast(ctx, loaded, status, func(currentTicketIdentity) (localTicketReader, error) {
		t.Fatal("Ticket reader opened for a status row belonging to another harness")
		return nil, nil
	})
	if err != nil || len(owners) != 0 {
		t.Fatalf("queue forecast accepted stale harness status: owners=%#v err=%v", owners, err)
	}
}

func TestQueueForecastEmptyOutputHasBlankBoundaries(t *testing.T) {
	var output bytes.Buffer
	if err := renderQueueForecast(&output, nil); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.HasPrefix(got, "\n") || !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("empty queue output must start and end with a blank line: %q", got)
	}
}

func TestQueueOutputReportsDispatchInhibitionAndKeepsForecast(t *testing.T) {
	dir := t.TempDir()
	loaded := LoadedFileConfig{Instance: InstanceContext{InstanceDir: dir, LocalDir: dir}}
	status := daemon.Status{Mode: "paused"}
	open := func(currentTicketIdentity) (localTicketReader, error) { return &forecastTicketReader{}, nil }
	var compact bytes.Buffer
	if err := writeQueueForecast(context.Background(), &compact, false, loaded, status, open); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compact.String(), "daemon mode=paused dispatch=inhibited") || !strings.Contains(compact.String(), "No configured or registered execution owners") {
		t.Fatalf("compact queue output did not show inhibited mode and forecast: %q", compact.String())
	}
	var encoded bytes.Buffer
	if err := writeQueueForecast(context.Background(), &encoded, true, loaded, status, open); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Mode              string               `json:"daemon_mode"`
		DispatchInhibited bool                 `json:"dispatch_inhibited"`
		Owners            []queueForecastOwner `json:"owners"`
	}
	if err := json.Unmarshal(encoded.Bytes(), &result); err != nil {
		t.Fatalf("decode queue JSON %q: %v", encoded.String(), err)
	}
	if result.Mode != "paused" || !result.DispatchInhibited || result.Owners == nil {
		t.Fatalf("queue JSON omitted mode or owners: %#v", result)
	}
}

func TestQueueCommandAndConsoleHelp(t *testing.T) {
	var output, stderr bytes.Buffer
	if code := run([]string{"help", "queue"}, &output, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 || !strings.Contains(output.String(), "read-only forecast") {
		t.Fatalf("queue help code=%d out=%q err=%q", code, output.String(), stderr.String())
	}
	output.Reset()
	if code := run([]string{"queue", "--help"}, &output, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 || !strings.Contains(output.String(), "ticket-orc queue") {
		t.Fatalf("queue command help code=%d out=%q err=%q", code, output.String(), stderr.String())
	}
	command, err := parseConsoleCommand("queue")
	if err != nil || command.name != "queue" {
		t.Fatalf("console queue command=%#v err=%v", command, err)
	}
	if _, err := parseConsoleCommand("queue detail"); err == nil {
		t.Fatal("queue accepted unsupported arguments")
	}
	var consoleHelp bytes.Buffer
	writeConsoleTopicHelp(&consoleHelp, "queue")
	if !strings.Contains(consoleHelp.String(), "scheduling forecast") {
		t.Fatalf("console queue help=%q", consoleHelp.String())
	}
}
