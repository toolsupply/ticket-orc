package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type workerManager struct {
	ctx               context.Context
	configPath        string
	localDir          string
	instanceID        string
	executable        string
	lifecycle         *supervisor.LifecycleManager[supervisor.RunWorker, *runChild]
	configMu          sync.Mutex
	starter           runChildStarter
	queueHarness      func(supervisor.RunWorker) (harness.Harness, error)
	ticketProbe       func(context.Context, supervisor.RunWorker) (ticketclient.RepositoryInfo, error)
	repositoryProbe   repositoryProbe
	repositories      supervisor.RepositoryRegistry
	repositoriesByID  map[string]supervisor.ConfiguredRepository
	dynamicReposByID  map[string]supervisor.ConfiguredRepository
	dynamicStatusKeys map[string]bool
	// repositoryWatchRevision invalidates observer inventories that were
	// replaced while a configured reload or dynamic registration was committing.
	repositoryWatchRevision uint64
	repositoryWatchPublish  uint64
	repositoryWatch         *supervisor.RepositoryWatchManager
	preflighted             map[string]bool
	stdout                  io.Writer
	stderr                  io.Writer
	childStdout             io.Writer
	childStderr             io.Writer
	outputMu                *sync.Mutex
	errorMu                 *sync.Mutex
	transition              func(string, supervisor.WorkerState, error)
	eventSink               orc.EventSink
	runtimeEventSink        func(supervisor.RuntimeEvent)
	runtime                 *supervisor.RuntimeState[supervisor.RunWorker]
	dispatchGate            *dispatchGate
	steerPolicies           *steerPolicyStore
	steerDirty              *steerDirtySet
	results                 chan struct {
		child *runChild
		err   error
	}
}

func newWorkerManager(ctx context.Context, config RunConfig, executable string, stdout, stderr io.Writer, transition func(string, supervisor.WorkerState, error)) *workerManager {
	workers := make(map[string]supervisor.RunWorker, len(config.Workers))
	for _, worker := range config.Workers {
		workers[worker.Name] = worker
	}
	starter := startRunChild
	if config.startChild != nil {
		starter = config.startChild
	}
	childStdout, childStderr := stdout, stderr
	steerPolicies := config.steerPolicies
	if config.Interactive {
		// Interactive consoles own the terminal. Raw child streams can contain
		// prompts, diagnostics, and opaque transport targets that bypass the
		// console renderer and corrupt an operator's command line. Structured
		// worker events continue through eventSink and are shown only by watch.
		childStdout, childStderr = io.Discard, io.Discard
	}
	manager := &workerManager{
		ctx: ctx, configPath: config.ConfigPath, localDir: config.StateDir, instanceID: config.InstanceID, executable: executable,
		starter: starter, stdout: stdout, stderr: stderr, childStdout: childStdout, childStderr: childStderr,
		outputMu: &sync.Mutex{}, errorMu: &sync.Mutex{}, transition: transition,
		ticketProbe:       config.ticketProbe,
		repositoryProbe:   config.repositoryProbe,
		repositories:      cloneRepositoryRegistry(config.Repositories),
		repositoriesByID:  repositoryRegistryByID(config.Repositories),
		dynamicReposByID:  make(map[string]supervisor.ConfiguredRepository),
		dynamicStatusKeys: make(map[string]bool),
		repositoryWatch:   newRepositoryWatchManager(ctx, config.Runtime, config.Repositories, config.repositoryProbe),
		preflighted:       make(map[string]bool, len(config.Workers)),
		runtime:           config.Runtime,
		dispatchGate:      config.dispatchGate,
		steerPolicies:     steerPolicies,
		steerDirty:        config.steerDirty,
		results: make(chan struct {
			child *runChild
			err   error
		}, len(config.Workers)+8),
	}
	manager.lifecycle = supervisor.NewLifecycleManager(config.Workers, supervisor.LifecycleHooks[supervisor.RunWorker, *runChild]{
		Name:        func(worker supervisor.RunWorker) string { return worker.Name },
		ChildWorker: func(child *runChild) supervisor.RunWorker { return child.worker },
		Prepare:     manager.preflightWorker,
		Conflict: func(name string, candidate supervisor.RunWorker, children map[string]*runChild) error {
			return manager.activeWorkerConflict(name, candidate, children)
		},
		StartChild: func(_ context.Context, worker supervisor.RunWorker) (*runChild, error) {
			worker.EventSink = manager.eventSink
			return manager.starter(manager.ctx, manager.configPath, manager.executable, worker, manager.childStdout, manager.childStderr, manager.outputMu, manager.errorMu)
		},
		AttachStartupAttempt: func(child *runChild, attempt *supervisor.StartupAttempt) { child.startupAttempt = attempt },
		Ready:                func(child *runChild) <-chan struct{} { return child.ready },
		ReadyObserved:        func(child *runChild) bool { return child.readyObserved() },
		StartupAttemptOf:     func(child *runChild) *supervisor.StartupAttempt { return child.startupAttempt },
		StartupAttemptError:  manager.startupAttemptError,
		StartupExited:        manager.reconcileStartupExit,
		StartupTimedOut:      manager.failStartupReadiness,
		ClassifyExit:         classifyManagedChildExit,
		MarkStartupPending:   manager.markStartupResultPending,
		CompleteStartup:      manager.completeStartupResult,
		ChildStarted: func(child *runChild) {
			if manager.runtime != nil {
				manager.runtime.SetEffectiveWorker(child.worker)
			}
			go func() {
				manager.results <- struct {
					child *runChild
					err   error
				}{child: child, err: <-child.done}
			}()
		},
		ProcessAvailable: func(child *runChild) bool { return child != nil && child.cmd != nil && child.cmd.Process != nil },
		RequestStop:      func(child *runChild) error { return child.requestStop() },
		ForceStop:        func(child *runChild) error { return child.forceStop() },
		Waited:           func(child *runChild) <-chan struct{} { return child.waited },
		SetEffective: func(worker supervisor.RunWorker) {
			if manager.runtime != nil {
				manager.runtime.SetEffectiveWorker(worker)
			}
		},
		Transition:  transition,
		StopTimeout: runChildStopTimeout,
	})
	return manager
}

func probeRunWorkerTicketTarget(ctx context.Context, worker supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
	return ticketclient.ProbeInfo(ctx, worker.Config.Actor, effectiveWorkingDirectory(worker.Config), ticketTarget(worker.Config))
}

func equalTicketTargets(left, right supervisor.RoleConfig) bool {
	return ticketTarget(left) == ticketTarget(right)
}

func (m *workerManager) preflightWorker(ctx context.Context, worker supervisor.RunWorker) (supervisor.RunWorker, error) {
	if m.ticketProbe == nil {
		return worker, nil
	}
	m.configMu.Lock()
	if m.preflighted[worker.Name] {
		delete(m.preflighted, worker.Name)
		m.configMu.Unlock()
		return worker, nil
	}
	m.configMu.Unlock()
	info, err := m.ticketProbe(ctx, worker)
	if err != nil {
		return supervisor.RunWorker{}, err
	}
	identity := repositoryIdentity(info)
	if identity == "" {
		return supervisor.RunWorker{}, fmt.Errorf("Ticket info did not return a stable repository ID")
	}
	worker.TicketInfo = &info
	worker.Config.RepositoryIdentity = identity
	m.configMu.Lock()
	m.configMu.Unlock()
	return worker, nil
}

func (m *workerManager) activeWorkerConflict(name string, candidate supervisor.RunWorker, children map[string]*runChild) error {
	for otherName, child := range children {
		if otherName == name || child == nil {
			continue
		}
		other := child.worker
		if candidate.Config.Actor != "" && candidate.Config.Actor == other.Config.Actor && ticketRepositoriesConflict(workerRepositoryIdentity(candidate), workerRepositoryIdentity(other)) {
			return &supervisor.LifecycleError{Code: "worker_identity_conflict", Message: "worker shares a Ticket actor with a running worker"}
		}
	}
	return nil
}

func (m *workerManager) reload(ctx context.Context) (supervisor.ReloadResult, error) {
	if strings.TrimSpace(m.configPath) == "" {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "config path is unavailable"}
	}
	loaded, err := LoadFileConfig(filepath.Dir(m.configPath), m.configPath, true)
	if err != nil {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate config is invalid"}
	}
	if strings.TrimSpace(m.localDir) == "" || loaded.Instance.LocalDir != m.localDir {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{
			Code:    "local_root_changed",
			Message: "candidate config selects a different local runtime root; restart the daemon with the selected config",
		}
	}
	candidateSteerPolicies, err := resolveSteerRolePolicies(loaded.Config, os.LookupEnv)
	if err != nil {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate steering policy is invalid", Cause: err}
	}
	if m.instanceID != "" && loaded.Config.ID != m.instanceID {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{
			Code:    "instance_id_changed",
			Message: "candidate config selects a different instance ID; restart the daemon with the selected config",
		}
	}
	if err := ctx.Err(); err != nil {
		return supervisor.ReloadResult{}, err
	}
	candidateRepositories, err := resolveConfiguredRepositories(loaded.Config)
	if err != nil {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate repository configuration is invalid", Cause: err}
	}
	probeRepository := m.repositoryProbe
	if probeRepository == nil {
		probeRepository = probeConfiguredRepository
	}
	for _, key := range repositoryKeys(candidateRepositories) {
		repository := candidateRepositories[key]
		info, probeErr := probeRepository(ctx, repository)
		if probeErr != nil {
			return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "ticket_target_unavailable", Message: "candidate Ticket repository preflight failed", Cause: probeErr}
		}
		repository.Info = &info
		repository.ID = repositoryIdentity(info)
		candidateRepositories[key] = repository
	}
	if err := validateResolvedRepositories(candidateRepositories); err != nil {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate repository configuration is invalid", Cause: err}
	}
	type resolvedWorker struct {
		name   string
		worker supervisor.RunWorker
	}
	currentWorkers := m.lifecycle.Workers()
	allResolved, resolutionDiagnostics := resolveAllWorkers(loaded, os.LookupEnv)
	if resolutionDiagnostics.HasErrors() {
		return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate worker configuration is invalid", Cause: resolutionDiagnostics}
	}
	resolved := make([]resolvedWorker, 0, len(loaded.Config.Workers))
	newNames := make([]string, 0, len(loaded.Config.Workers))
	for name := range loaded.Config.Workers {
		newNames = append(newNames, name)
	}
	sort.Strings(newNames)
	for _, name := range newNames {
		resolved = append(resolved, resolvedWorker{name: name, worker: allResolved[name]})
	}
	for _, item := range resolved {
		if err := preflightRoleConfig(item.worker.Config); err != nil {
			return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate worker runtime preflight failed", Cause: err}
		}
	}
	candidateWorkers := make(map[string]supervisor.RunWorker, len(resolved))
	for _, item := range resolved {
		candidateWorkers[item.name] = item.worker
	}
	// Probe only new or changed targets. A successful result is retained on the
	// candidate worker as derived state; failures leave the active map untouched.
	if m.ticketProbe != nil {
		for i := range resolved {
			current, exists := currentWorkers[resolved[i].name]
			if exists && equalTicketTargets(current.Config, resolved[i].worker.Config) {
				resolved[i].worker.TicketInfo = current.TicketInfo
				resolved[i].worker.Config.RepositoryIdentity = current.Config.RepositoryIdentity
				candidateWorkers[resolved[i].name] = resolved[i].worker
				continue
			}
			if key := resolved[i].worker.Config.RepositoryKey; key != "" {
				repository, ok := candidateRepositories[key]
				if !ok || repository.Info == nil {
					return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate worker references an unresolved repository"}
				}
				resolved[i].worker.TicketInfo = repository.Info
				resolved[i].worker.Config.RepositoryIdentity = repository.ID
				candidateWorkers[resolved[i].name] = resolved[i].worker
				continue
			}
			info, probeErr := m.ticketProbe(ctx, resolved[i].worker)
			if probeErr != nil {
				return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "ticket_target_unavailable", Message: "candidate Ticket target preflight failed", Cause: probeErr}
			}
			identity := repositoryIdentity(info)
			if identity == "" {
				return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "ticket_target_unavailable", Message: "candidate Ticket target did not return a stable repository ID"}
			}
			resolved[i].worker.TicketInfo = &info
			resolved[i].worker.Config.RepositoryIdentity = identity
			candidateWorkers[resolved[i].name] = resolved[i].worker
		}
	}
	for i := range resolved {
		if resolved[i].worker.Config.RepositoryKey == "" {
			continue
		}
		repository, ok := candidateRepositories[resolved[i].worker.Config.RepositoryKey]
		if !ok || repository.Info == nil {
			return supervisor.ReloadResult{}, &supervisor.LifecycleError{Code: "invalid_config", Message: "candidate worker references an unresolved repository"}
		}
		resolved[i].worker.TicketInfo = repository.Info
		resolved[i].worker.Config.RepositoryIdentity = repository.ID
		candidateWorkers[resolved[i].name] = resolved[i].worker
	}
	diagnostics := AnalyzeConfiguredWorkers(candidateWorkers)
	var warnings []supervisor.Diagnostic
	sortDiagnostics(diagnostics)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == DiagnosticWarning {
			warnings = append(warnings, supervisor.Diagnostic{Severity: string(diagnostic.Severity), Code: diagnostic.Code, Path: diagnostic.Path, Worker: diagnostic.Worker, Message: diagnostic.Message, Remediation: diagnostic.Remediation})
		}
	}
	updatedWorkers := make(map[string]supervisor.RunWorker, len(resolved))
	for _, item := range resolved {
		// Keep the desired configuration current even while a child continues
		// using its captured effective configuration. Restart resolves from this
		// application-owned lifecycle state, while the child remains untouched.
		updatedWorkers[item.name] = item.worker
	}
	revision := ""
	transition, err := m.lifecycle.ApplyReload(ctx, updatedWorkers, supervisor.ReloadHooks[supervisor.RunWorker]{
		ReplaceWatchers: func(ctx context.Context) error {
			if m.repositoryWatch == nil {
				return nil
			}
			watchRepositories, watchRevision := m.repositoryWatchInventory(candidateRepositories)
			if err := m.repositoryWatch.ReplaceAtRevision(ctx, watchRevision, watchRepositories); err != nil {
				return &supervisor.LifecycleError{Code: "repository_observer_failed", Message: "candidate repository observers could not be updated", Cause: err}
			}
			return nil
		},
		CompareWorker: func(effective, desired supervisor.RunWorker) bool {
			return !equalLaunchSnapshots(effective, desired)
		},
		Commit: func(workers []supervisor.RunWorker, running map[string]bool) string {
			revision = fmt.Sprintf("%d", time.Now().UnixNano())
			m.configMu.Lock()
			m.repositories = cloneRepositoryRegistry(candidateRepositories)
			m.repositoriesByID = mergeRepositoryTargets(candidateRepositories, m.dynamicReposByID)
			m.repositoryWatchRevision++
			m.configMu.Unlock()
			if m.runtime != nil {
				m.runtime.SetConfiguredWorkers(workers)
				for _, worker := range workers {
					if !running[worker.Name] {
						m.runtime.SetEffectiveWorker(worker)
					}
				}
				m.runtime.SetRevision(revision)
			}
			if m.steerPolicies != nil {
				_, policyChanged := m.steerPolicies.Replace(candidateSteerPolicies)
				if policyChanged && m.steerDirty != nil {
					m.steerDirty.MarkAll()
				}
			}
			return revision
		},
	})
	if err != nil {
		return supervisor.ReloadResult{}, err
	}
	if m.repositoryWatch != nil {
		if err := m.syncRepositoryWatchInventory(ctx); err != nil {
			return supervisor.ReloadResult{Revision: transition.Revision, Applied: true}, &supervisor.LifecycleError{Code: "repository_observer_failed", Message: "configuration was applied but repository observers could not be reconciled", Cause: err}
		}
	}
	for _, changed := range transition.Changes {
		warnings = append(warnings, supervisor.Diagnostic{Severity: string(DiagnosticWarning), Code: "reload.restart_required", Path: "workers." + changed.Worker, Worker: changed.Worker, Message: "running worker keeps its effective configuration until restarted", Remediation: "restart this worker to apply the desired configuration"})
	}
	for _, warning := range warnings {
		if m.stderr != nil {
			_, _ = fmt.Fprintf(m.stderr, "[ticket-orc] config warning: %s\n", runtimeConfigWarningMessage(warning.Code, warning.Worker, warning.Message, warning.Remediation))
		}
	}
	return supervisor.ReloadResult{Revision: transition.Revision, Applied: true, Warnings: warnings}, nil
}

func (m *workerManager) snapshotWorkers() []supervisor.RunWorker {
	workers := m.lifecycle.Workers()
	result := make([]supervisor.RunWorker, 0, len(workers))
	for _, worker := range workers {
		result = append(result, worker)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}
