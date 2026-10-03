package cli

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	dynamicRepositoryPollInterval     = time.Second
	dynamicRepositoryRetryInterval    = 5 * time.Second
	dynamicRepositoryReverifyInterval = 30 * time.Second
)

type dynamicRepositoryProbe func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error)

type dynamicRepositoryIdentity struct {
	repositoryID string
	path         string
}

type dynamicRepositoryVerification struct {
	info        ticketclient.RepositoryInfo
	verifiedAt  time.Time
	lastAttempt time.Time
	failure     string
}

type dynamicRepositoryIdentityCache struct {
	entries map[dynamicRepositoryIdentity]dynamicRepositoryVerification
}

func newDynamicRepositoryIdentityCache() *dynamicRepositoryIdentityCache {
	return &dynamicRepositoryIdentityCache{entries: make(map[dynamicRepositoryIdentity]dynamicRepositoryVerification)}
}

func runDynamicRepositoryDiscovery(ctx context.Context, stateDir string, manager *workerManager, probe dynamicRepositoryProbe) {
	runDynamicRepositoryDiscoveryWithObserver(ctx, manager, probe, newRegistrationObserver(state.NewRegistrationStore(stateDir)))
}

func runDynamicRepositoryDiscoveryWithObserver(ctx context.Context, manager *workerManager, probe dynamicRepositoryProbe, observer *registrationObserver) {
	if ctx == nil || manager == nil {
		return
	}
	if probe == nil {
		probe = probeDynamicRepository
	}
	cache := newDynamicRepositoryIdentityCache()
	refresh := func() {
		readCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
		observation := observer.Observe(readCtx)
		cancel()
		applyDynamicRepositoryObservationWithCache(ctx, manager, probe, observation, cache, time.Now())
	}
	refresh()
	ticker := time.NewTicker(dynamicRepositoryPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func applyDynamicRepositoryObservation(ctx context.Context, manager *workerManager, probe dynamicRepositoryProbe, observation registrationObservation) {
	applyDynamicRepositoryObservationWithCache(ctx, manager, probe, observation, newDynamicRepositoryIdentityCache(), time.Now())
}

func applyDynamicRepositoryObservationWithCache(ctx context.Context, manager *workerManager, probe dynamicRepositoryProbe, observation registrationObservation, cache *dynamicRepositoryIdentityCache, now time.Time) {
	if manager == nil {
		return
	}
	if probe == nil {
		probe = probeDynamicRepository
	}
	configuredIDs := manager.configuredRepositoryIDs()
	withdrawUnverifiedDynamicRepositories(manager, observation.Registrations, configuredIDs)
	resolved, statuses := resolveDynamicRepositoriesWithCache(ctx, observation.Registrations, configuredIDs, probe, cache, now)
	if observation.Code != "" {
		for i := range statuses {
			statuses[i].State = "degraded"
			statuses[i].Failure = observation.Code
		}
	}
	manager.replaceDynamicRepositories(resolved, statuses)
}

func resolveDynamicRepositories(ctx context.Context, registrations []state.SteerRegistration, configuredIDs map[string]bool, probe dynamicRepositoryProbe) (map[string]supervisor.ConfiguredRepository, []supervisor.RepositoryStatus) {
	return resolveDynamicRepositoriesWithCache(ctx, registrations, configuredIDs, probe, newDynamicRepositoryIdentityCache(), time.Now())
}

func resolveDynamicRepositoriesWithCache(ctx context.Context, registrations []state.SteerRegistration, configuredIDs map[string]bool, probe dynamicRepositoryProbe, cache *dynamicRepositoryIdentityCache, now time.Time) (map[string]supervisor.ConfiguredRepository, []supervisor.RepositoryStatus) {
	if cache == nil {
		cache = newDynamicRepositoryIdentityCache()
	}
	if cache.entries == nil {
		cache.entries = make(map[dynamicRepositoryIdentity]dynamicRepositoryVerification)
	}
	resolved := make(map[string]supervisor.ConfiguredRepository)
	statuses := make([]supervisor.RepositoryStatus, 0, len(registrations))
	byID := make(map[string][]state.SteerRegistration)
	for _, registration := range registrations {
		if !configuredIDs[registration.RepositoryID] {
			byID[registration.RepositoryID] = append(byID[registration.RepositoryID], registration)
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for key := range cache.entries {
		registrations, found := byID[key.repositoryID]
		if !found || len(registrations) == 0 || registrationsDisagreeAboutPath(registrations) || registrations[0].RepositoryPath != key.path {
			delete(cache.entries, key)
		}
	}
	for _, id := range ids {
		items := byID[id]
		repository := items[0]
		status := supervisor.RepositoryStatus{ID: id, Key: dynamicRepositoryKey(id), Name: repository.RepositoryName, State: "degraded"}
		path := repository.RepositoryPath
		if registrationsDisagreeAboutPath(items) {
			status.Failure = "dynamic registrations disagree about the Ticket repository path"
			statuses = append(statuses, status)
			continue
		}
		if path == "" {
			continue
		}
		info, failure := verifyDynamicRepositoryIdentity(ctx, repository, id, probe, cache, now)
		if failure != "" {
			status.Failure = failure
			statuses = append(statuses, status)
			continue
		}
		if info.Name != nil && *info.Name != "" {
			status.Name = *info.Name
		}
		status.Path = info.Path
		status.State = "healthy"
		statuses = append(statuses, status)
		infoCopy := info
		resolved[id] = supervisor.ConfiguredRepository{
			Key: status.Key, ID: id,
			Target: supervisor.TicketTarget{Mode: supervisor.TicketTargetRepository, Repository: repository.RepositoryPath},
			Info:   &infoCopy,
		}
	}
	return resolved, statuses
}

func registrationsDisagreeAboutPath(registrations []state.SteerRegistration) bool {
	if len(registrations) < 2 {
		return false
	}
	path := registrations[0].RepositoryPath
	for _, registration := range registrations[1:] {
		if registration.RepositoryPath != path {
			return true
		}
	}
	return false
}

func withdrawUnverifiedDynamicRepositories(manager *workerManager, registrations []state.SteerRegistration, configuredIDs map[string]bool) {
	if manager == nil {
		return
	}
	type route struct {
		path       string
		conflicted bool
		items      []state.SteerRegistration
	}
	routes := make(map[string]route)
	for _, registration := range registrations {
		id := registration.RepositoryID
		if configuredIDs[id] {
			continue
		}
		current, found := routes[id]
		if !found {
			current.path = registration.RepositoryPath
		}
		if current.path != registration.RepositoryPath {
			current.conflicted = true
		}
		current.items = append(current.items, registration)
		routes[id] = current
	}

	manager.configMu.Lock()
	active := make(map[string]supervisor.ConfiguredRepository, len(manager.dynamicReposByID))
	for id, repository := range manager.dynamicReposByID {
		active[id] = repository
	}
	oldStatusKeys := make(map[string]bool, len(manager.dynamicStatusKeys))
	for key := range manager.dynamicStatusKeys {
		oldStatusKeys[key] = true
	}
	manager.configMu.Unlock()
	remaining := make(map[string]supervisor.ConfiguredRepository, len(active))
	affected := make(map[string]bool)
	for id, repository := range active {
		current, found := routes[id]
		if found && !current.conflicted && current.path == repository.Target.Repository {
			remaining[id] = repository
			continue
		}
		affected[id] = true
	}
	if len(affected) == 0 {
		return
	}
	statuses := make([]supervisor.RepositoryStatus, 0, len(affected))
	if manager.runtime != nil {
		for _, status := range manager.runtime.RepositoryStatuses() {
			if oldStatusKeys[status.Key] && !affected[status.ID] {
				statuses = append(statuses, status)
			}
		}
	}
	for id := range affected {
		current, found := routes[id]
		if !found || len(current.items) == 0 {
			continue
		}
		registration := current.items[0]
		failure := "Ticket repository identity is being verified"
		if current.conflicted {
			failure = "dynamic registrations disagree about the Ticket repository path"
		}
		statuses = append(statuses, supervisor.RepositoryStatus{
			ID: id, Key: dynamicRepositoryKey(id), Name: registration.RepositoryName,
			State: "degraded", Failure: failure,
		})
	}
	manager.replaceDynamicRepositories(remaining, statuses)
}

func verifyDynamicRepositoryIdentity(ctx context.Context, registration state.SteerRegistration, repositoryID string, probe dynamicRepositoryProbe, cache *dynamicRepositoryIdentityCache, now time.Time) (ticketclient.RepositoryInfo, string) {
	key := dynamicRepositoryIdentity{repositoryID: repositoryID, path: registration.RepositoryPath}
	entry, found := cache.entries[key]
	if found && entry.failure == "" && !entry.verifiedAt.IsZero() && now.Sub(entry.verifiedAt) < dynamicRepositoryReverifyInterval {
		return entry.info, ""
	}
	if found && !entry.lastAttempt.IsZero() && now.Sub(entry.lastAttempt) < dynamicRepositoryRetryInterval {
		return ticketclient.RepositoryInfo{}, entry.failure
	}
	entry.lastAttempt = now
	if probe == nil {
		entry.info = ticketclient.RepositoryInfo{}
		entry.verifiedAt = time.Time{}
		entry.failure = "Ticket repository identity could not be verified"
		cache.entries[key] = entry
		return ticketclient.RepositoryInfo{}, entry.failure
	}
	probeCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
	info, err := probe(probeCtx, registration)
	cancel()
	if err != nil {
		entry.info = ticketclient.RepositoryInfo{}
		entry.verifiedAt = time.Time{}
		entry.failure = "Ticket repository identity could not be verified"
		cache.entries[key] = entry
		return ticketclient.RepositoryInfo{}, entry.failure
	}
	if repositoryIdentity(info) != repositoryID {
		entry.info = ticketclient.RepositoryInfo{}
		entry.verifiedAt = time.Time{}
		entry.failure = "Ticket repository ID did not match the registration"
		cache.entries[key] = entry
		return ticketclient.RepositoryInfo{}, entry.failure
	}
	entry.info = info
	entry.verifiedAt = now
	entry.failure = ""
	cache.entries[key] = entry
	return info, ""
}

func probeDynamicRepository(ctx context.Context, registration state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
	if !ticketclient.ValidRepositoryID(registration.RepositoryID) {
		return ticketclient.RepositoryInfo{}, fmt.Errorf("invalid Ticket repository ID")
	}
	return ticketclient.ProbeInfo(ctx, registration.Actor, registration.RepositoryPath, ticketclient.Target{Repository: registration.RepositoryPath})
}

func dynamicRepositoryKey(repositoryID string) string { return "dynamic:" + repositoryID }

func (m *workerManager) configuredRepositoryIDs() map[string]bool {
	ids := make(map[string]bool)
	if m == nil {
		return ids
	}
	m.configMu.Lock()
	defer m.configMu.Unlock()
	for id := range repositoryRegistryByID(m.repositories) {
		ids[id] = true
	}
	return ids
}

func (m *workerManager) replaceDynamicRepositories(repositories map[string]supervisor.ConfiguredRepository, statuses []supervisor.RepositoryStatus) {
	if m == nil {
		return
	}
	m.configMu.Lock()
	oldStatusKeys := m.dynamicStatusKeys
	oldRepositories := m.dynamicReposByID
	configured := repositoryRegistryByID(m.repositories)
	activeRepositories := make(map[string]supervisor.ConfiguredRepository, len(repositories))
	for id, repository := range repositories {
		if _, exists := configured[id]; !exists {
			activeRepositories[id] = repository
		}
	}
	newStatusKeys := make(map[string]bool, len(statuses))
	for _, status := range statuses {
		if _, exists := configured[status.ID]; !exists {
			newStatusKeys[status.Key] = true
		}
	}
	m.dynamicReposByID = activeRepositories
	m.repositoriesByID = mergeRepositoryTargets(m.repositories, m.dynamicReposByID)
	if !sameDynamicRepositoryTargets(oldRepositories, activeRepositories) {
		m.repositoryWatchRevision++
	}
	if m.runtime != nil {
		for key := range oldStatusKeys {
			m.runtime.RemoveRepositoryStatus(key)
		}
		for _, status := range statuses {
			if !newStatusKeys[status.Key] {
				continue
			}
			m.runtime.SetRepositoryStatus(status)
		}
	}
	m.dynamicStatusKeys = newStatusKeys
	m.configMu.Unlock()
	if m.repositoryWatch != nil {
		ctx := m.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		_ = m.syncRepositoryWatchInventory(ctx)
	}
}

func sameDynamicRepositoryTargets(left, right map[string]supervisor.ConfiguredRepository) bool {
	if len(left) != len(right) {
		return false
	}
	for id, repository := range left {
		candidate, ok := right[id]
		if !ok || repository.ID != candidate.ID || repository.Target != candidate.Target {
			return false
		}
	}
	return true
}

func refreshDynamicRepositories(ctx context.Context, stateDir string, manager *workerManager, probe dynamicRepositoryProbe) error {
	if ctx == nil || manager == nil {
		return nil
	}
	if probe == nil {
		probe = probeDynamicRepository
	}
	observer := newRegistrationObserver(state.NewRegistrationStore(stateDir))
	observation := observer.Observe(ctx)
	applyDynamicRepositoryObservation(ctx, manager, probe, observation)
	return observation.Err
}

func mergeRepositoryTargets(configured supervisor.RepositoryRegistry, dynamic map[string]supervisor.ConfiguredRepository) map[string]supervisor.ConfiguredRepository {
	byID := repositoryRegistryByID(configured)
	for id, repository := range dynamic {
		if _, configured := byID[id]; !configured {
			byID[id] = repository
		}
	}
	return byID
}

func mergeRepositoryWatchInventory(configured supervisor.RepositoryRegistry, dynamic map[string]supervisor.ConfiguredRepository) supervisor.RepositoryRegistry {
	watch := cloneRepositoryRegistry(configured)
	configuredIDs := repositoryRegistryByID(configured)
	for id, repository := range dynamic {
		if _, exists := configuredIDs[id]; exists {
			continue
		}
		if _, exists := watch[repository.Key]; exists {
			continue
		}
		watch[repository.Key] = repository
	}
	return watch
}

func (m *workerManager) repositoryWatchInventory(configured supervisor.RepositoryRegistry) (supervisor.RepositoryRegistry, uint64) {
	if m == nil {
		return cloneRepositoryRegistry(configured), 0
	}
	m.configMu.Lock()
	dynamic := make(map[string]supervisor.ConfiguredRepository, len(m.dynamicReposByID))
	for id, repository := range m.dynamicReposByID {
		dynamic[id] = repository
	}
	m.repositoryWatchPublish++
	revision := m.repositoryWatchPublish
	m.configMu.Unlock()
	return mergeRepositoryWatchInventory(configured, dynamic), revision
}

func (m *workerManager) syncRepositoryWatchInventory(ctx context.Context) error {
	if m == nil || m.repositoryWatch == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		m.configMu.Lock()
		configured := cloneRepositoryRegistry(m.repositories)
		dynamic := make(map[string]supervisor.ConfiguredRepository, len(m.dynamicReposByID))
		for id, repository := range m.dynamicReposByID {
			dynamic[id] = repository
		}
		revision := m.repositoryWatchRevision
		m.repositoryWatchPublish++
		publishRevision := m.repositoryWatchPublish
		m.configMu.Unlock()

		if err := m.repositoryWatch.ReplaceAtRevision(ctx, publishRevision, mergeRepositoryWatchInventory(configured, dynamic)); err != nil {
			return err
		}
		m.configMu.Lock()
		unchanged := revision == m.repositoryWatchRevision
		m.configMu.Unlock()
		if unchanged {
			return nil
		}
	}
}
