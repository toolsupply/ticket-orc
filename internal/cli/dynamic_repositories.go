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

const dynamicRepositoryPollInterval = time.Second

type dynamicRepositoryProbe func(context.Context, state.SteerRegistration) (ticketclient.RepositoryInfo, error)

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
	refresh := func() {
		readCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
		observation := observer.Observe(readCtx)
		cancel()
		applyDynamicRepositoryObservation(ctx, manager, probe, observation)
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
	if manager == nil {
		return
	}
	if probe == nil {
		probe = probeDynamicRepository
	}
	resolved, statuses := resolveDynamicRepositories(ctx, observation.Registrations, manager.configuredRepositoryIDs(), probe)
	if observation.Code != "" {
		for i := range statuses {
			statuses[i].State = "degraded"
			statuses[i].Failure = observation.Code
		}
	}
	manager.replaceDynamicRepositories(resolved, statuses)
}

func resolveDynamicRepositories(ctx context.Context, registrations []state.SteerRegistration, configuredIDs map[string]bool, probe dynamicRepositoryProbe) (map[string]supervisor.ConfiguredRepository, []supervisor.RepositoryStatus) {
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
	for _, id := range ids {
		items := byID[id]
		repository := items[0]
		status := supervisor.RepositoryStatus{ID: id, Key: dynamicRepositoryKey(id), Name: repository.RepositoryName, State: "degraded"}
		path := repository.RepositoryPath
		for _, item := range items[1:] {
			if item.RepositoryPath != path {
				status.Failure = "dynamic registrations disagree about the Ticket repository path"
				statuses = append(statuses, status)
				path = ""
				break
			}
		}
		if path == "" {
			continue
		}
		// A registration path is routing data, not proof of repository identity.
		// Probe on every refresh so replacing or retargeting the path withdraws
		// the previous ID before it can keep reaching a different repository.
		if probe == nil {
			status.Failure = "Ticket repository identity could not be verified"
			statuses = append(statuses, status)
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
		info, err := probe(probeCtx, repository)
		cancel()
		if err != nil {
			status.Failure = "Ticket repository identity could not be verified"
			statuses = append(statuses, status)
			continue
		}
		if repositoryIdentity(info) != id {
			status.Failure = "Ticket repository ID did not match the registration"
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
