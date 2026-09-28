package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func executeRun(config RunConfig, stdout, stderr io.Writer) error {
	endpointKey, err := ensureEndpointCapability(config.ConfigPath, config.StateDir, config.EndpointKey)
	if err != nil {
		return fmt.Errorf("load local endpoint capability: %w", err)
	}
	config.EndpointKey = endpointKey
	ctx, stop := supervisorSignalContext(context.Background(), config.Interactive)
	defer stop()
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve ticket-orc executable: %w", err)
	}
	if config.Runtime == nil {
		config.Runtime = NewRuntimeState(config.Workers)
	}
	if config.ticketProbe == nil {
		config.ticketProbe = probeRunWorkerTicketTarget
	}
	if config.repositoryProbe == nil {
		config.repositoryProbe = probeConfiguredRepository
	}
	control := &daemon.Control{}
	config.control = control
	config.startDaemon = func(_ context.Context, runtime *supervisor.RuntimeState[supervisor.RunWorker], workers []supervisor.RunWorker) (*daemon.Server, error) {
		return daemon.NewServer(daemon.Config{
			StateDir:      config.StateDir,
			EndpointKey:   config.EndpointKey,
			ListenAddress: config.ListenAddress,
			Port:          config.Port,
			Version:       Version,
			Status: func() daemon.Status {
				var sessions []daemon.SteerStatus
				if config.steerStatus != nil {
					sessions = config.steerStatus.snapshot()
				}
				return runtimeDaemonStatus(runtime, sessions)
			},
			Control: control,
		})
	}
	return runSupervisor(ctx, config, executable, stdout, stderr)
}

func addWorkerRepositoryStatus(status *daemon.WorkerStatus, worker supervisor.RunWorker) {
	if status == nil {
		return
	}
	status.RepositoryKey = worker.Config.RepositoryKey
	if ticketclient.ValidRepositoryID(worker.Config.RepositoryIdentity) {
		status.RepositoryID = worker.Config.RepositoryIdentity
	}
	if worker.TicketInfo == nil {
		return
	}
	if ticketclient.ValidRepositoryID(worker.TicketInfo.ID) {
		status.RepositoryID = worker.TicketInfo.ID
	}
	status.RepositoryPath = worker.TicketInfo.Path
	if worker.TicketInfo.Name != nil {
		status.RepositoryName = *worker.TicketInfo.Name
	}
	if worker.TicketInfo.Scope != nil {
		status.TicketScope = *worker.TicketInfo.Scope
	}
}

func workerStatusReason(transition supervisor.WorkerTransition) string {
	if transition.Failure != nil {
		if transition.Failure.Phase == "worker readiness" || transition.Failure.Classification == "startup_failure" || transition.Failure.Classification == "startup_timeout" {
			return "worker exited during startup verification"
		}
		reason := transition.Failure.Classification
		if transition.Failure.Phase != "" {
			reason += " during " + transition.Failure.Phase
		}
		if transition.Failure.TicketCode != "" {
			reason += " (Ticket " + transition.Failure.TicketCode + ")"
		}
		if transition.Failure.TransportCategory != "" {
			reason += " [transport " + transition.Failure.TransportCategory + "]"
		}
		if transition.Failure.Origin != "" {
			reason += " [origin " + transition.Failure.Origin
			if transition.Failure.Operation != "" {
				reason += "/" + transition.Failure.Operation
			}
			reason += "]"
		}
		if transition.Failure.Ticket != "" {
			reason += " (ticket " + transition.Failure.Ticket + ")"
		}
		if transition.Failure.Remediation != "" {
			reason += "; " + transition.Failure.Remediation
		}
		return reason
	}
	if transition.Error == "" {
		return ""
	}
	if transition.State == WorkerFailed {
		return "worker failure requires operator recovery; run doctor"
	}
	return "worker transition failed; run doctor"
}

func runSupervisor(ctx context.Context, config RunConfig, executable string, stdout, stderr io.Writer) error {
	if ctx == nil {
		return errors.New("run context must not be nil")
	}
	if strings.TrimSpace(executable) == "" {
		return errors.New("run executable must not be empty")
	}
	// Claim this state root before reading state, probing Ticket, or starting
	// observers. A second supervisor then fails without touching its daemon's
	// endpoint or beginning worker startup.
	lock, err := state.TryAcquireLock(ctx, filepath.Join(config.StateDir, "run"))
	if err != nil {
		if errors.Is(err, state.ErrLockTimeout) {
			_, _ = fmt.Fprintf(stderr, "error: this Orc instance is already running\n       instance: %s\n", config.StateDir)
			return markRunFailureRendered(fmt.Errorf("this Orc instance is already running (instance: %s): %w", config.StateDir, err))
		}
		return fmt.Errorf("acquire daemon lock: %w", err)
	}
	defer lock.Release()

	controlStore := state.NewDaemonControlStore(config.StateDir)
	controlState, err := controlStore.DaemonControl(ctx)
	if err != nil {
		return fmt.Errorf("load daemon control state: %w", err)
	}
	config.dispatchGate = newDispatchGate(controlStore, controlState.Mode)
	config.steerWake = make(chan struct{}, 1)
	dispatchEnabled := controlState.Mode == state.DaemonRunning
	if len(config.Workers) > 1 {
		for _, worker := range config.Workers {
			if worker.Config.Output == OutputJSON {
				return fmt.Errorf("run cannot multiplex json output for multiple workers; use compact or quiet output")
			}
		}
	}
	if config.Runtime == nil {
		config.Runtime = NewRuntimeState(config.Workers)
	}
	if config.repositoryProbe == nil {
		config.repositoryProbe = probeConfiguredRepository
	}
	renderRuntimeConfigWarnings(stderr, config.Diagnostics)
	emitEvent, setEventPublisher := newSupervisorEventPublisher(config)
	transition := newSupervisorWorkerTransition(config, emitEvent)
	preflightedWorkers, preflightFailures, err := preflightSupervisorWorkers(ctx, &config, stderr)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manager := newWorkerManager(ctx, config, executable, stdout, stderr, transition)
	steerDir := config.StateDir
	repositoryObserver, steerObserver, initialSteerStatuses := seedRunSteerObservers(ctx, config, manager, steerDir)
	for name := range preflightedWorkers {
		manager.preflighted[name] = true
	}
	recordFailure := wireSupervisorRuntimeEvents(config, manager, emitEvent)
	shutdownRequested := make(chan struct{})
	var shutdownOnce sync.Once
	wireSupervisorControlCallbacks(&config, manager, emitEvent, shutdownRequested, &shutdownOnce)
	httpDaemon, listenerAddress, err := startSupervisorDaemon(ctx, config, manager, emitEvent, setEventPublisher, stderr)
	if err != nil {
		return err
	}
	if httpDaemon != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), runChildStopTimeout)
			defer cancel()
			_ = httpDaemon.Shutdown(shutdownCtx)
		}()
	}
	if manager.repositoryWatch != nil {
		manager.repositoryWatch.StartObservers()
		defer manager.repositoryWatch.StopObservers()
	}

	if dispatchEnabled {
		if err := startInitialManagedWorkers(ctx, config, manager, preflightFailures, transition, stderr); err != nil {
			return err
		}
		if err := runInitialDoctor(ctx, &config, manager, stderr); err != nil {
			return err
		}
	}
	stopSteer := startSupervisorSteer(ctx, &config, manager, steerDir, repositoryObserver, steerObserver)
	if stopSteer != nil {
		defer stopSteer()
	}
	setSupervisorTitleIfEligible(config.Interactive, config.Workers)
	// Interactive mode presents its own startup header and worker snapshot in
	// the console. Keep the daemon-oriented readiness summary for ordinary
	// foreground runs, but do not insert it between the run -i diagnostics and
	// the console view.
	if !config.Interactive {
		sessions := initialSteerStatuses
		if config.steerStatus != nil && len(config.steerStatus.snapshot()) != 0 {
			sessions = config.steerStatus.snapshot()
		}
		status := runtimeDaemonStatus(config.Runtime, sessions)
		renderServiceReady(stderr, config.ConfigPath, listenerAddress, status)
	}
	if config.Interactive {
		renderRunBanner(stdout, config.ConfigPath, listenerAddress)
		// Start the console only after every configured worker has reached its
		// initial state and the first steer and repository observations complete.
		go func() {
			var steer *dynamicSteerStatus
			if len(config.SteerRoles) != 0 {
				steer = config.steerStatus
			}
			if err := waitInitialConsoleStatus(ctx, config.Runtime, steer); err != nil {
				return
			}
			initialSelection := ""
			if len(config.Workers) == 1 && config.Workers[0].Name != "" && foregroundTerminalEligible() {
				initialSelection = config.Workers[0].Name
			}
			if consoleErr := runInteractiveSupervisorConsoleWithSelection(ctx, config.StateDir, os.Stdin, stdout, stderr, initialSelection, func() {
				shutdownOnce.Do(func() { close(shutdownRequested) })
			}); consoleErr != nil && !errors.Is(consoleErr, context.Canceled) {
				_, _ = fmt.Fprintf(stderr, "[ticket-orc] console unavailable: %s\n", safeConsoleError(consoleErr))
			}
		}()
	}
	return coordinateSupervisorRun(ctx, config, manager, shutdownRequested, recordFailure, stderr)
}

// waitInitialConsoleStatus waits for the first steer observation and for
// repository observers to leave their startup state. A failed first
// observation is still a completed observation and remains visible in status.
func waitInitialConsoleStatus(ctx context.Context, runtime *supervisor.RuntimeState[supervisor.RunWorker], steer *dynamicSteerStatus) error {
	if err := steer.waitFirst(ctx); err != nil {
		return err
	}
	if runtime == nil {
		return nil
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		for _, repository := range runtime.RepositoryStatuses() {
			if repository.State == "starting" {
				ready = false
				break
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func addWorkerRepositoryEventContext(event *supervisor.RuntimeEvent, workers []supervisor.RunWorker) {
	if event == nil || event.Worker == "" {
		return
	}
	for _, worker := range workers {
		if worker.Name != event.Worker {
			continue
		}
		if !ticketclient.ValidRepositoryID(event.RepositoryID) {
			event.RepositoryID = ""
			if ticketclient.ValidRepositoryID(worker.Config.RepositoryIdentity) {
				event.RepositoryID = worker.Config.RepositoryIdentity
			}
			if worker.TicketInfo != nil && ticketclient.ValidRepositoryID(worker.TicketInfo.ID) {
				event.RepositoryID = worker.TicketInfo.ID
			}
		}
		if event.RepositoryKey == "" {
			event.RepositoryKey = worker.Config.RepositoryKey
		}
		return
	}
}

func workerTransitionFailure(err error) *supervisor.WorkerFailure {
	if err == nil {
		return nil
	}
	failure := classifyWorkerFailureError(err)
	var lifecycleErr *supervisor.LifecycleError
	if errors.As(err, &lifecycleErr) && (lifecycleErr.Code == "worker_startup_failed" || lifecycleErr.Code == "worker_startup_timeout") {
		if failure == nil {
			failure = &supervisor.WorkerFailure{Classification: "startup_failure"}
		}
		failure.Phase = "worker readiness"
	}
	return failure
}

func workerLeaseConflictExit(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == workerLeaseConflictExitCode
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (m *workerManager) workerRole(name string) string {
	if worker, ok := m.lifecycle.Worker(name); ok {
		return string(worker.Config.Role)
	}
	return ""
}

func mutationEventCode(result supervisor.MutationResult, err error) string {
	if err != nil {
		var controlErr *supervisor.LifecycleError
		if errors.As(err, &controlErr) && controlErr != nil && controlErr.Code != "" {
			return controlErr.Code
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return "mutation_timeout"
		}
		if errors.Is(err, context.Canceled) {
			return "daemon_stopping"
		}
		return "control_error"
	}
	if result.Applied {
		return "mutation_applied"
	}
	return "mutation_unchanged"
}

func newSupervisorEventPublisher(config RunConfig) (func(supervisor.RuntimeEvent), func(func(daemon.Event))) {
	var eventMu sync.RWMutex
	var eventPublisher func(daemon.Event)
	emitEvent := func(event supervisor.RuntimeEvent) {
		addWorkerRepositoryEventContext(&event, config.Runtime.EffectiveWorkers())
		eventMu.RLock()
		publisher := eventPublisher
		eventMu.RUnlock()
		if publisher != nil {
			publisher(daemon.RuntimeEventDTO(event))
		}
	}
	setPublisher := func(publisher func(daemon.Event)) {
		eventMu.Lock()
		eventPublisher = publisher
		eventMu.Unlock()
	}
	return emitEvent, setPublisher
}

func newSupervisorWorkerTransition(config RunConfig, emitEvent func(supervisor.RuntimeEvent)) func(string, supervisor.WorkerState, error) {
	return func(worker string, state supervisor.WorkerState, err error) {
		event := supervisor.WorkerTransition{Worker: worker, State: state, Failure: workerTransitionFailure(err)}
		if err != nil {
			event.Error = err.Error()
		}
		config.Runtime.Set(event)
		if config.Hooks.WorkerState != nil {
			config.Hooks.WorkerState(event)
		}
		published := supervisor.RuntimeEvent{Type: "worker.state", Worker: worker, State: string(state)}
		if err != nil {
			published.Code = "worker_failed"
		}
		emitEvent(published)
	}
}

func seedRunSteerObservers(ctx context.Context, config RunConfig, manager *workerManager, steerDir string) (*registrationObserver, *registrationObserver, []daemon.SteerStatus) {
	if len(config.SteerRoles) == 0 {
		return nil, nil, nil
	}
	seedObserver := newRegistrationObserver(state.NewRegistrationStore(steerDir))
	discoveryCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
	seed := seedObserver.Observe(discoveryCtx)
	cancel()
	applyDynamicRepositoryObservation(ctx, manager, nil, seed)
	initialStatuses := registrationStartupStatuses(seed.Registrations, seed.Code)
	repositoryObserver := newRegistrationObserver(state.NewRegistrationStore(steerDir))
	repositoryObserver.Seed(seed)
	steerObserver := newRegistrationObserver(state.NewRegistrationStore(steerDir))
	steerObserver.Seed(seed)
	return repositoryObserver, steerObserver, initialStatuses
}

func wireSupervisorRuntimeEvents(config RunConfig, manager *workerManager, emitEvent func(supervisor.RuntimeEvent)) func(string, supervisor.WorkerState, *supervisor.WorkerFailure) {
	manager.eventSink = func(event orc.Event) {
		config.Runtime.Observe(event)
		emitEvent(supervisor.RuntimeEventFromOrc(event))
	}
	manager.runtimeEventSink = emitEvent
	if manager.repositoryWatch != nil {
		manager.repositoryWatch.SetEventSink(emitEvent)
	}
	return func(worker string, state supervisor.WorkerState, failure *supervisor.WorkerFailure) {
		if failure == nil {
			return
		}
		config.Runtime.SetFailure(worker, failure)
		emitEvent(supervisor.RuntimeEvent{Type: "worker.failure", Worker: worker, State: string(state), Code: failure.Classification, Phase: failure.Phase, Failure: failure})
	}
}

func wireSupervisorControlCallbacks(config *RunConfig, manager *workerManager, emitEvent func(supervisor.RuntimeEvent), shutdownRequested chan struct{}, shutdownOnce *sync.Once) {
	if config.control == nil {
		return
	}
	config.control.Groups = manager.groups
	config.control.DaemonMode = func() string { return string(config.dispatchGate.Mode()) }
	config.control.PauseDaemon = func(ctx context.Context) (daemon.DaemonControlResult, error) {
		mode, applied, err := config.dispatchGate.Pause(ctx)
		if err != nil {
			return daemon.DaemonControlResult{}, err
		}
		if applied {
			emitEvent(supervisor.RuntimeEvent{Type: "daemon.paused", State: string(mode.Mode), Applied: true})
		}
		return daemon.DaemonControlResult{Mode: string(mode.Mode), Applied: applied}, nil
	}
	config.control.ResumeDaemon = func(ctx context.Context) (daemon.DaemonControlResult, error) {
		mode, applied, err := config.dispatchGate.Resume(ctx)
		if err != nil {
			return daemon.DaemonControlResult{}, err
		}
		if config.steerWake != nil {
			select {
			case config.steerWake <- struct{}{}:
			default:
			}
		}
		reconcileErr := manager.reconcileDispatch(ctx)
		emitEvent(supervisor.RuntimeEvent{Type: "daemon.resumed", State: string(mode.Mode), Applied: applied})
		if reconcileErr != nil {
			result := daemon.DaemonControlResult{Mode: string(mode.Mode), Applied: applied}
			return result, &daemon.ControlError{
				Code: "daemon_reconciliation_failed", Status: 503,
				Message: "daemon resumed but worker reconciliation failed",
				Applied: applied, Cause: reconcileErr, Daemon: &result,
			}
		}
		return daemon.DaemonControlResult{Mode: string(mode.Mode), Applied: applied}, nil
	}
	config.control.AbortDaemon = func(ctx context.Context) (daemon.DaemonControlResult, error) {
		applied, err := manager.beginAbort(ctx)
		if err != nil {
			return daemon.DaemonControlResult{}, err
		}
		result := daemon.DaemonControlResult{Mode: string(state.DaemonAborted), Applied: applied}
		result.Targets = collectDaemonAbortTargets(ctx,
			func(ctx context.Context) ([]steerAbortResult, error) {
				return requestConfiguredSteerAbort(ctx, *config, nil, nil)
			}, manager.terminateActive)
		emitEvent(supervisor.RuntimeEvent{Type: "daemon.aborted", State: result.Mode, Applied: result.Applied})
		return result, nil
	}
	publishControlIntent := func(kind, name string) {
		role := manager.workerRole(name)
		emitEvent(supervisor.RuntimeEvent{Type: "worker.control", Worker: name, Role: role, Phase: kind, Code: "intent"})
	}
	publishMutation := func(kind, name string, result supervisor.MutationResult, err error) {
		role := manager.workerRole(name)
		emitEvent(supervisor.RuntimeEvent{Type: "worker.control", Worker: name, Role: role, Phase: kind, State: result.State, Code: mutationEventCode(result, err), Applied: result.Applied})
	}
	config.control.StartWorker = func(ctx context.Context, name string) (supervisor.MutationResult, error) {
		publishControlIntent("start", name)
		result, err := manager.start(ctx, name)
		publishMutation("start", name, result, err)
		return result, err
	}
	config.control.StopWorker = func(ctx context.Context, name string) (supervisor.MutationResult, error) {
		publishControlIntent("stop", name)
		result, err := manager.stop(ctx, name)
		publishMutation("stop", name, result, err)
		return result, err
	}
	config.control.PauseWorker = func(ctx context.Context, name string) (supervisor.MutationResult, error) {
		publishControlIntent("pause", name)
		result, err := manager.pause(ctx, name)
		publishMutation("pause", name, result, err)
		return result, err
	}
	config.control.ResumeWorker = func(ctx context.Context, name string) (supervisor.MutationResult, error) {
		publishControlIntent("resume", name)
		result, err := manager.resume(ctx, name)
		publishMutation("resume", name, result, err)
		return result, err
	}
	config.control.RestartWorker = func(ctx context.Context, name string) (supervisor.MutationResult, error) {
		publishControlIntent("restart", name)
		result, err := manager.restart(ctx, name)
		publishMutation("restart", name, result, err)
		return result, err
	}
	config.control.StartGroup = func(ctx context.Context, name string) (supervisor.GroupResult, error) {
		return manager.group(ctx, name, true)
	}
	config.control.StopGroup = func(ctx context.Context, name string) (supervisor.GroupResult, error) {
		return manager.group(ctx, name, false)
	}
	config.control.Reload = func(ctx context.Context) (supervisor.ReloadResult, error) {
		result, err := manager.reload(ctx)
		if err != nil {
			emitEvent(supervisor.RuntimeEvent{Type: "config.reload_failed", Code: mutationEventCode(supervisor.MutationResult{}, err)})
		} else {
			emitEvent(supervisor.RuntimeEvent{Type: "config.reloaded", Code: "config_applied", Applied: result.Applied})
		}
		return result, err
	}
	config.control.Doctor = func(ctx context.Context) (supervisor.DoctorResult, error) {
		result, err := manager.doctor(ctx)
		if err != nil {
			emitEvent(supervisor.RuntimeEvent{Type: "doctor.failed", Code: mutationEventCode(supervisor.MutationResult{}, err)})
			return result, err
		}
		emitEvent(supervisor.RuntimeEvent{Type: "doctor.completed", Code: "doctor_applied", Applied: true})
		return result, nil
	}
	config.control.Shutdown = func(ctx context.Context) error {
		emitEvent(supervisor.RuntimeEvent{Type: "daemon.stopping"})
		manager.stopAll(ctx)
		shutdownOnce.Do(func() { close(shutdownRequested) })
		return nil
	}
}

func startSupervisorDaemon(ctx context.Context, config RunConfig, manager *workerManager, emitEvent func(supervisor.RuntimeEvent), setEventPublisher func(func(daemon.Event)), stderr io.Writer) (*daemon.Server, string, error) {
	listenerAddress := "unavailable"
	if config.startDaemon == nil {
		return nil, listenerAddress, nil
	}
	httpDaemon, err := config.startDaemon(ctx, config.Runtime, config.Workers)
	if err != nil {
		return nil, listenerAddress, fmt.Errorf("create daemon: %w", err)
	}
	if err := httpDaemon.SetRepositoryGateway(manager.repositoryGateway()); err != nil {
		return nil, listenerAddress, fmt.Errorf("configure daemon repository gateway: %w", err)
	}
	if err := httpDaemon.Start(ctx); err != nil {
		var listenErr *net.OpError
		startupErr := fmt.Errorf("daemon startup failed: %w", err)
		if errors.As(err, &listenErr) && listenErr.Op == "listen" && listenErr.Addr != nil {
			fmt.Fprintf(stderr, "[ticket-orc] daemon listen failed address=%s: %v\n", listenErr.Addr, listenErr.Err)
			return nil, listenerAddress, markRunFailureRendered(startupErr)
		}
		return nil, listenerAddress, startupErr
	}
	endpoint := httpDaemon.Endpoint()
	listenerAddress = endpoint.URL
	setEventPublisher(httpDaemon.PublishEvent)
	if config.steerStatus != nil {
		config.steerStatus.setPublisher(httpDaemon.PublishEvent)
	}
	emitEvent(supervisor.RuntimeEvent{Type: "daemon.started"})
	return httpDaemon, listenerAddress, nil
}

// collectDaemonAbortTargets attempts steer stops before the bounded managed
// process waits, so a slow managed child cannot consume the entire API deadline
// before externally owned sessions receive their one abort attempt.
func collectDaemonAbortTargets(
	ctx context.Context,
	requestSteer func(context.Context) ([]steerAbortResult, error),
	terminateManaged func(context.Context) []supervisor.TerminationResult,
) []daemon.DaemonAbortTargetResult {
	results := make([]daemon.DaemonAbortTargetResult, 0)
	steer, steerErr := requestSteer(ctx)
	if steerErr != nil {
		results = append(results, daemon.DaemonAbortTargetResult{
			Kind: "steer", Outcome: "not_attempted", Code: "target_observation_unavailable",
		})
	} else {
		for _, target := range steer {
			results = append(results, daemon.DaemonAbortTargetResult{
				Kind: "steer", RepositoryID: target.RepositoryID, Actor: target.Actor,
				Outcome: target.Outcome, Code: target.Code,
			})
		}
	}
	for _, target := range terminateManaged(ctx) {
		results = append(results, daemon.DaemonAbortTargetResult{
			Kind: "managed", Worker: target.Worker, Outcome: target.Outcome,
		})
	}
	return results
}

func startInitialManagedWorkers(ctx context.Context, config RunConfig, manager *workerManager, preflightFailures map[string]error, transition func(string, supervisor.WorkerState, error), stderr io.Writer) error {
	if config.Doctor {
		return nil
	}
	for _, worker := range config.Workers {
		if preflightErr := preflightFailures[worker.Name]; preflightErr != nil {
			transition(worker.Name, WorkerFailed, preflightErr)
			continue
		}
		result, startErr := manager.start(ctx, worker.Name)
		if isDispatchSuppressed(startErr) {
			continue
		}
		if startErr != nil && result.State == string(WorkerPaused) {
			transition(worker.Name, WorkerPaused, nil)
			continue
		}
		if startErr != nil {
			manager.stopAll(context.Background())
			for _, running := range config.Workers {
				if running.Name != worker.Name {
					transition(running.Name, WorkerStopped, nil)
				}
			}
			return fmt.Errorf("worker %q failed to start: %w", worker.Name, startErr)
		}
		if verifyErr := manager.verifyWorkerStartup(ctx, worker.Name); verifyErr != nil {
			renderServiceFailure(stderr, worker.Name, "readiness")
			if failure := classifyWorkerFailureError(verifyErr); failure != nil {
				renderWorkerFailure(stderr, worker.Name, WorkerFailed, failure)
			}
			// The failed worker has already been removed and reconciled by
			// verifyWorkerStartup. Keep starting independent workers so one
			// startup failure does not hide the rest of the fleet.
			continue
		}
	}
	return nil
}

func runInitialDoctor(ctx context.Context, config *RunConfig, manager *workerManager, stderr io.Writer) error {
	if !config.Doctor {
		return nil
	}
	if config.dispatchGate != nil && config.dispatchGate.Mode() != state.DaemonRunning {
		return nil
	}
	doctorResult, doctorErr := manager.doctor(ctx)
	renderDoctorResult(stderr, doctorResult)
	if doctorErr != nil {
		manager.stopAll(context.Background())
		if errors.Is(doctorErr, context.Canceled) && ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("doctor recovery failed: %w", doctorErr)
	}
	if ctx.Err() != nil {
		manager.stopAll(context.Background())
		return nil
	}
	if !doctorResultComplete(doctorResult) {
		manager.stopAll(context.Background())
		return fmt.Errorf("doctor recovery incomplete")
	}
	// Standalone doctor is the explicit foreground force-start route. Keep
	// recovered workers under this supervisor until cancellation or shutdown.
	config.Doctor = false
	return nil
}

func startSupervisorSteer(ctx context.Context, config *RunConfig, manager *workerManager, steerDir string, repositoryObserver, steerObserver *registrationObserver) context.CancelFunc {
	if len(config.SteerRoles) == 0 {
		return nil
	}
	if config.steerStatus == nil {
		config.steerStatus = &dynamicSteerStatus{}
	}
	steerCtx, stopSteer := context.WithCancel(ctx)
	go runDynamicRepositoryDiscoveryWithObserver(steerCtx, manager, nil, repositoryObserver)
	go runDynamicSteerWithGate(steerCtx, steerDir, config.SteerRoles, config.Runtime, nil, nil, config.steerStatus, nil, steerObserver, config.dispatchGate, config.steerWake)
	return stopSteer
}

func coordinateSupervisorRun(ctx context.Context, config RunConfig, manager *workerManager, shutdownRequested <-chan struct{}, recordFailure func(string, supervisor.WorkerState, *supervisor.WorkerFailure), stderr io.Writer) error {
	for {
		select {
		case result := <-manager.results:
			observation := manager.lifecycle.ObserveExit(result.child.worker.Name, result.child, result.err, ctx.Err() != nil)
			if !observation.Current {
				continue
			}
			outcome := observation.Outcome
			name := result.child.worker.Name
			if !outcome.Shutdown {
				recordFailure(name, outcome.State, outcome.Failure)
				renderWorkerFailure(stderr, name, outcome.State, outcome.Failure)
			}
		case <-ctx.Done():
			manager.stopAll(context.Background())
			_, _ = fmt.Fprintf(stderr, "[ticket-orc] stopped workers=%d\n", len(config.Workers))
			return nil
		case <-shutdownRequested:
			manager.stopAll(context.Background())
			_, _ = fmt.Fprintf(stderr, "[ticket-orc] stopped workers=%d\n", len(config.Workers))
			return nil
		}
	}
}

func preflightSupervisorWorkers(ctx context.Context, config *RunConfig, stderr io.Writer) (map[string]bool, map[string]error, error) {
	preflightedWorkers := make(map[string]bool, len(config.Workers))
	preflightFailures := make(map[string]error)
	preflightRepositories := make(map[string]error, len(config.Repositories))
	for _, key := range repositoryKeys(config.Repositories) {
		repository := config.Repositories[key]
		if repository.Info != nil && repository.ID != "" {
			continue
		}
		info, probeErr := config.repositoryProbe(ctx, repository)
		if probeErr != nil {
			preflightRepositories[key] = probeErr
			if config.Doctor {
				continue
			}
			return nil, nil, fmt.Errorf("repository %q preflight failed: %w", key, probeErr)
		}
		repository.Info = &info
		repository.ID = repositoryIdentity(info)
		if repository.ID == "" {
			repositoryErr := fmt.Errorf("repository %q preflight returned no stable Ticket repository ID", key)
			preflightRepositories[key] = repositoryErr
			if config.Doctor {
				continue
			}
			return nil, nil, repositoryErr
		}
		config.Repositories[key] = repository
	}
	if len(preflightRepositories) == 0 {
		if err := validateResolvedRepositories(config.Repositories); err != nil {
			if config.Doctor {
				for _, key := range repositoryKeys(config.Repositories) {
					preflightRepositories[key] = err
				}
			} else {
				return nil, nil, fmt.Errorf("repository preflight failed: %w", err)
			}
		}
	}
	for i := range config.Workers {
		worker := config.Workers[i]
		if worker.Config.RepositoryKey != "" {
			repository, ok := config.Repositories[worker.Config.RepositoryKey]
			if !ok || repository.Info == nil || preflightRepositories[worker.Config.RepositoryKey] != nil {
				repositoryErr := preflightRepositories[worker.Config.RepositoryKey]
				if repositoryErr == nil {
					repositoryErr = fmt.Errorf("repository %q was not resolved", worker.Config.RepositoryKey)
				}
				preflightFailures[worker.Name] = repositoryErr
				if !config.Doctor {
					return nil, nil, fmt.Errorf("worker %q repository preflight failed: %w", worker.Name, repositoryErr)
				}
				continue
			}
			worker.TicketInfo = repository.Info
			worker.Config.RepositoryIdentity = repository.ID
			config.Workers[i] = worker
			preflightedWorkers[worker.Name] = true
		}
		if config.ticketProbe != nil && worker.Config.RepositoryKey == "" {
			info, probeErr := config.ticketProbe(ctx, worker)
			if probeErr != nil {
				renderTicketPreflightFailure(stderr, worker.Name, probeErr)
				preflightFailures[worker.Name] = probeErr
				continue
			}
			identity := repositoryIdentity(info)
			if identity == "" {
				renderTicketPreflightFailure(stderr, worker.Name, fmt.Errorf("Ticket info did not return a stable repository ID"))
				preflightFailures[worker.Name] = fmt.Errorf("Ticket info did not return a stable repository ID")
				continue
			}
			config.Workers[i].TicketInfo = &info
			config.Workers[i].Config.RepositoryIdentity = identity
			preflightedWorkers[worker.Name] = true
		}
		agent, harnessErr := newHarness(worker.Config)
		if harnessErr != nil {
			if config.Doctor {
				preflightFailures[worker.Name] = harnessErr
				continue
			}
			return nil, nil, fmt.Errorf("worker %q preflight failed: %w", worker.Name, harnessErr)
		}
		if preflightErr := preflightRoleHarness(worker.Config, agent); preflightErr != nil {
			if config.Doctor {
				preflightFailures[worker.Name] = preflightErr
				continue
			}
			return nil, nil, fmt.Errorf("worker %q preflight failed: %w", worker.Name, preflightErr)
		}
	}
	if config.Runtime != nil {
		config.Runtime.SetConfiguredWorkers(config.Workers)
		for _, worker := range config.Workers {
			config.Runtime.SetEffectiveWorker(worker)
		}
	}
	if config.ticketProbe != nil {
		resolvedWorkers := make(map[string]supervisor.RunWorker, len(config.Workers))
		for _, worker := range config.Workers {
			if preflightFailures[worker.Name] == nil {
				resolvedWorkers[worker.Name] = worker
			}
		}
		selected := make(map[string]bool, len(resolvedWorkers))
		for name := range resolvedWorkers {
			selected[name] = true
		}
		postDiagnostics := AnalyzeRunWorkers(resolvedWorkers, selected)
		if postDiagnostics.HasErrors() {
			renderConfigDiagnostics(stderr, postDiagnostics)
			return nil, nil, fmt.Errorf("worker target diagnostics failed")
		}
		renderRuntimeConfigWarnings(stderr, postDiagnostics)
	}
	return preflightedWorkers, preflightFailures, nil
}
