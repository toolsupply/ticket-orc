package cli

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// RunConfig describes one foreground supervisor invocation.
type RunConfig struct {
	ConfigPath      string
	StateDir        string
	ListenAddress   string
	Port            int
	EndpointKey     string
	Interactive     bool
	Doctor          bool
	Repositories    supervisor.RepositoryRegistry
	Workers         []supervisor.RunWorker
	Diagnostics     []ConfigDiagnostic
	SteerRoles      map[string]steerRolePolicy
	steerStatus     *dynamicSteerStatus
	dispatchGate    *dispatchGate
	steerWake       chan struct{}
	Runtime         *supervisor.RuntimeState[supervisor.RunWorker]
	Hooks           supervisor.SupervisorHooks
	startChild      runChildStarter
	startDaemon     runDaemonStarter
	ticketProbe     func(context.Context, supervisor.RunWorker) (ticketclient.RepositoryInfo, error)
	repositoryProbe repositoryProbe
	control         *daemon.Control
}

type runDaemonStarter func(context.Context, *supervisor.RuntimeState[supervisor.RunWorker], []supervisor.RunWorker) (*daemon.Server, error)

func parseRunConfig(args []string, lookupEnv envLookup) (RunConfig, bool, error) {
	configValue := ""
	listenValue := ""
	listenSet := false
	portValue := 0
	portSet := false
	workers := make([]string, 0)
	groups := make([]string, 0)
	all := false
	interactive := false
	help := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-h" || arg == "--help" {
			if help {
				return RunConfig{}, false, fmt.Errorf("duplicate flag --help")
			}
			help = true
			continue
		}
		if arg == "-a" || arg == "--all" {
			if all {
				return RunConfig{}, false, fmt.Errorf("duplicate flag --all")
			}
			all = true
			continue
		}
		if arg == "-i" || arg == "--interactive" {
			if interactive {
				return RunConfig{}, false, fmt.Errorf("duplicate flag --interactive")
			}
			interactive = true
			continue
		}
		if arg == "--listen" || strings.HasPrefix(arg, "--listen=") {
			if listenSet {
				return RunConfig{}, false, fmt.Errorf("duplicate flag --listen")
			}
			value := strings.TrimPrefix(arg, "--listen=")
			if arg == "--listen" {
				i++
				if i >= len(args) {
					return RunConfig{}, false, fmt.Errorf("flag --listen requires a value")
				}
				value = args[i]
			}
			if strings.TrimSpace(value) == "" {
				return RunConfig{}, false, fmt.Errorf("--listen must not be empty")
			}
			listenValue, listenSet = value, true
			continue
		}
		if arg == "--port" || strings.HasPrefix(arg, "--port=") {
			if portSet {
				return RunConfig{}, false, fmt.Errorf("duplicate flag --port")
			}
			value := strings.TrimPrefix(arg, "--port=")
			if arg == "--port" {
				i++
				if i >= len(args) {
					return RunConfig{}, false, fmt.Errorf("flag --port requires a value")
				}
				value = args[i]
			}
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return RunConfig{}, false, fmt.Errorf("--port must be an integer")
			}
			portValue, portSet = parsed, true
			continue
		}
		if arg == "-c" || strings.HasPrefix(arg, "-c=") {
			if configValue != "" {
				return RunConfig{}, false, fmt.Errorf("duplicate flag --config")
			}
			value := strings.TrimPrefix(arg, "-c=")
			if arg == "-c" {
				i++
				if i >= len(args) {
					return RunConfig{}, false, fmt.Errorf("flag -c requires a value")
				}
				value = args[i]
			}
			if strings.TrimSpace(value) == "" {
				return RunConfig{}, false, fmt.Errorf("-c must not be empty")
			}
			configValue = value
			continue
		}
		if !strings.HasPrefix(arg, "--") {
			return RunConfig{}, false, fmt.Errorf("unexpected argument: %s", arg)
		}
		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if name != "config" && name != "worker" && name != "group" {
			return RunConfig{}, false, fmt.Errorf("unknown flag --%s", name)
		}
		if !hasValue {
			i++
			if i >= len(args) {
				return RunConfig{}, false, fmt.Errorf("flag --%s requires a value", name)
			}
			value = args[i]
		}
		if strings.TrimSpace(value) == "" {
			return RunConfig{}, false, fmt.Errorf("--%s must not be empty", name)
		}
		switch name {
		case "config":
			if configValue != "" {
				return RunConfig{}, false, fmt.Errorf("duplicate flag --config")
			}
			configValue = value
		case "worker":
			workers = append(workers, value)
		case "group":
			groups = append(groups, value)
		}
	}
	if help {
		return RunConfig{}, true, nil
	}
	values := make(map[string]string)
	if configValue != "" {
		values["config"] = configValue
	}
	loaded, err := loadInvocationConfig(values, lookupEnv)
	if err != nil {
		return RunConfig{}, false, err
	}
	listenAddress := loaded.Config.Supervisor.ListenAddress
	listenPort := daemon.DefaultListenPort
	if loaded.Config.Supervisor.Port != nil {
		listenPort = *loaded.Config.Supervisor.Port
	}
	if value, ok := lookupEnv("TICKET_ORC_LISTEN"); ok {
		listenAddress = value
	}
	if value, ok := lookupEnv("TICKET_ORC_PORT"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return RunConfig{}, false, fmt.Errorf("TICKET_ORC_PORT must be an integer")
		}
		listenPort = parsed
	}
	if listenSet {
		listenAddress = listenValue
	}
	if portSet {
		listenPort = portValue
	}
	listenAddress, err = daemon.NormalizeListenAddress(listenAddress)
	if err != nil {
		return RunConfig{}, false, fmt.Errorf("--listen: %w", err)
	}
	if err := daemon.ValidateListenPort(listenPort); err != nil {
		return RunConfig{}, false, fmt.Errorf("--port: %w", err)
	}
	repositories, err := resolveConfiguredRepositories(loaded.Config)
	if err != nil {
		return RunConfig{}, false, err
	}
	allWorkers, resolutionDiagnostics := resolveAllWorkers(loaded, lookupEnv)
	if resolutionDiagnostics.HasErrors() {
		return RunConfig{}, false, resolutionDiagnostics
	}
	selected, err := selectRunWorkers(loaded.Config, workers, groups, all)
	if err != nil {
		return RunConfig{}, false, err
	}

	resolved := make([]supervisor.RunWorker, 0, len(selected))
	for _, name := range selected {
		resolved = append(resolved, allWorkers[name])
	}
	selectedSet := make(map[string]bool, len(selected))
	for _, name := range selected {
		selectedSet[name] = true
	}
	diagnostics := append(ConfigDiagnostics(nil), resolutionDiagnostics...)
	diagnostics = append(diagnostics, AnalyzeRunWorkers(allWorkers, selectedSet)...)
	if diagnostics.HasErrors() {
		return RunConfig{}, false, diagnostics
	}
	stateDir := loaded.Instance.LocalDir
	steerRoles := make(map[string]steerRolePolicy, len(loaded.Config.Roles))
	for name, role := range loaded.Config.Roles {
		reviewCompletion := ""
		if role.TicketQueue == "review" {
			reviewCompletion = firstNonEmpty(role.ReviewCompletion, loaded.Config.Defaults.ReviewCompletion, ReviewCompletionSignoff)
			if value, ok := lookupEnv("TICKET_ORC_REVIEW_COMPLETION"); ok {
				if !oneOf(value, ReviewCompletionSignoff, ReviewCompletionClose) {
					return RunConfig{}, false, configValidation("review_completion.invalid", "review_completion", "review completion is invalid", "set TICKET_ORC_REVIEW_COMPLETION to signoff or close")
				}
				reviewCompletion = value
			}
		}
		steerRoles[name] = steerRolePolicy{TicketQueue: role.TicketQueue, NudgePrompt: role.NudgePrompt, ReviewCompletion: reviewCompletion}
	}
	return RunConfig{ConfigPath: loaded.Instance.ConfigPath, StateDir: stateDir, ListenAddress: listenAddress, Port: listenPort, EndpointKey: loaded.Config.Supervisor.EndpointKey, Interactive: interactive, Repositories: repositories, Workers: resolved, Diagnostics: diagnostics, SteerRoles: steerRoles, steerStatus: &dynamicSteerStatus{}}, false, nil
}

func resolveAllWorkers(loaded LoadedFileConfig, lookupEnv envLookup) (map[string]supervisor.RunWorker, ConfigDiagnostics) {
	names := make([]string, 0, len(loaded.Config.Workers))
	for name := range loaded.Config.Workers {
		names = append(names, name)
	}
	sort.Strings(names)
	workers := make(map[string]supervisor.RunWorker, len(names))
	var diagnostics ConfigDiagnostics
	for _, name := range names {
		worker := loaded.Config.Workers[name]
		policy := workerPolicy(loaded.Config, worker)
		roleConfig, _, err := resolveRoleConfig(workflowRole(policy), map[string]string{"worker": name}, loaded, lookupEnv)
		if err != nil {
			diagnostics = append(diagnostics, workerResolutionDiagnostic(name, err))
			continue
		}
		workers[name] = supervisor.RunWorker{Name: name, Groups: effectiveWorkerGroups(loaded.Config, name), RequiredSkills: effectiveWorkerRequiredSkills(loaded.Config, name), Config: roleConfig}
	}
	return workers, diagnostics
}

func workerResolutionDiagnostic(name string, err error) ConfigDiagnostic {
	message, code, path, remediation := "worker has invalid resolved settings", "worker.invalid", "workers."+name, "fix the worker's effective settings"
	var typed *ConfigValidationError
	if errors.As(err, &typed) {
		path := "workers." + name + "." + typed.Path
		return ConfigDiagnostic{Severity: DiagnosticError, Code: typed.Code, Path: path, Worker: name, Message: typed.Message, Remediation: typed.Remediation}
	}
	return ConfigDiagnostic{Severity: DiagnosticError, Code: code, Path: path, Worker: name, Message: message, Remediation: remediation}
}

func selectRunWorkers(config FileConfig, workerNames, groupNames []string, all bool) ([]string, error) {
	selected := make(map[string]struct{})
	ordered := make([]string, 0)
	addWorker := func(name string) error {
		if _, ok := config.Workers[name]; !ok {
			return fmt.Errorf("worker %q is not configured", name)
		}
		if _, ok := selected[name]; ok {
			return nil
		}
		selected[name] = struct{}{}
		ordered = append(ordered, name)
		return nil
	}
	addGroup := func(group string) error {
		matches := make([]string, 0)
		for name := range config.Workers {
			for _, candidate := range effectiveWorkerGroups(config, name) {
				if candidate == group {
					matches = append(matches, name)
					break
				}
			}
		}
		if len(matches) == 0 {
			return fmt.Errorf("group %q has no configured workers", group)
		}
		sort.Strings(matches)
		for _, name := range matches {
			if err := addWorker(name); err != nil {
				return err
			}
		}
		return nil
	}

	if all {
		allNames := make([]string, 0, len(config.Workers))
		for name := range config.Workers {
			allNames = append(allNames, name)
		}
		sort.Strings(allNames)
		for _, name := range allNames {
			if err := addWorker(name); err != nil {
				return nil, err
			}
		}
	}
	for _, name := range workerNames {
		if err := addWorker(name); err != nil {
			return nil, err
		}
	}
	for _, group := range groupNames {
		if err := addGroup(group); err != nil {
			return nil, err
		}
	}
	if len(workerNames) == 0 && len(groupNames) == 0 && !all {
		for _, group := range config.Supervisor.StartupGroups {
			if err := addGroup(group); err != nil {
				return nil, fmt.Errorf("startup group: %w", err)
			}
		}
	}
	if (all || len(workerNames) > 0 || len(groupNames) > 0) && len(ordered) == 0 {
		return nil, errors.New("run selection produced no workers")
	}
	return ordered, nil
}
