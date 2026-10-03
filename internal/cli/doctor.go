package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

type doctorConfig struct {
	configPath string
	resetLocal bool
}

// doctorResultComplete reports whether every worker in a recovery pass started.
func doctorResultComplete(result supervisor.DoctorResult) bool {
	for _, worker := range result.Workers {
		if worker.Outcome != "recovered" {
			return false
		}
	}
	return true
}

func parseDoctorConfig(args []string, lookupEnv envLookup) (doctorConfig, bool, error) {
	config := doctorConfig{}
	help := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			help = true
		case arg == "--reset-local":
			if config.resetLocal {
				return doctorConfig{}, false, fmt.Errorf("duplicate flag --reset-local")
			}
			config.resetLocal = true
		case arg == "-c" || arg == "--config" || strings.HasPrefix(arg, "--config=") || strings.HasPrefix(arg, "-c="):
			value := strings.TrimPrefix(strings.TrimPrefix(arg, "--config="), "-c=")
			if arg == "-c" || arg == "--config" {
				i++
				if i >= len(args) {
					return doctorConfig{}, false, fmt.Errorf("flag --config requires a value")
				}
				value = args[i]
			}
			if strings.TrimSpace(value) == "" || config.configPath != "" {
				return doctorConfig{}, false, fmt.Errorf("--config must be provided once with a non-empty value")
			}
			config.configPath = value
		default:
			return doctorConfig{}, false, fmt.Errorf("unknown option or argument %s", arg)
		}
	}
	if help {
		return doctorConfig{}, true, nil
	}
	_ = lookupEnv
	return config, false, nil
}

func executeDoctor(config doctorConfig, stdout, stderr io.Writer, lookupEnv envLookup) error {
	if config.resetLocal {
		return executeDoctorResetLocal(config, stdout, stderr, lookupEnv)
	}
	loaded, err := doctorLoadedConfig(config.configPath, lookupEnv)
	if err != nil {
		return err
	}
	if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: loaded.Instance.ConfigPath, Explicit: true, Output: OutputCompact}, stdout, stderr, lookupEnv); err != nil {
		return err
	}
	runtimeReady, err := inspectDoctorRuntime(loaded)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Configuration is valid: %s\n", terminaltext.Sanitize(loaded.Instance.ConfigPath, false))
	fmt.Fprintf(stdout, "Instance ID: %s\n", loaded.Config.ID)
	if !runtimeReady {
		fmt.Fprintln(stdout, "Local runtime: not initialized")
	} else {
		fmt.Fprintf(stdout, "Local runtime: %s\n", terminaltext.Sanitize(loaded.Instance.LocalDir, false))
	}
	return nil
}

func inspectDoctorRuntime(loaded LoadedFileConfig) (bool, error) {
	localRoot := loaded.Instance.LocalDir
	info, err := os.Lstat(localRoot)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect local runtime root %s: %w", localRoot, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return false, fmt.Errorf("local runtime root is not a real directory: %s", localRoot)
	}
	if loaded.Instance.LocalDirConfigured {
		marker, err := readAndValidateRuntimeMarker(localRoot)
		if err != nil {
			return false, fmt.Errorf("cannot prove ownership of configured runtime root %s: %w", localRoot, err)
		}
		if marker.InstanceID != loaded.Config.ID {
			return false, fmt.Errorf("configured runtime root %s belongs to instance %s, not config ID %s", localRoot, marker.InstanceID, loaded.Config.ID)
		}
		return true, nil
	}
	markerPath := filepath.Join(localRoot, runtimeMarkerFileName)
	marker, err := readAndValidateRuntimeMarker(localRoot)
	if err == nil {
		if marker.InstanceID != loaded.Config.ID {
			return false, fmt.Errorf("local runtime root %s belongs to instance %s, not config ID %s", localRoot, marker.InstanceID, loaded.Config.ID)
		}
		return true, nil
	}
	if _, statErr := os.Lstat(markerPath); statErr == nil {
		return false, fmt.Errorf("validate runtime marker %s: %w", markerPath, err)
	} else if !os.IsNotExist(statErr) {
		return false, fmt.Errorf("inspect runtime marker %s: %w", markerPath, statErr)
	}
	entries, readErr := os.ReadDir(localRoot)
	if readErr != nil {
		return false, fmt.Errorf("inspect local runtime root %s: %w", localRoot, readErr)
	}
	if isLegacyRuntimeLayout(localRoot, entries) {
		return false, fmt.Errorf("unsupported legacy ticket-orc runtime layout detected in %s\n\nThe configuration is valid, but its local runtime uses an older, unsupported layout.\n\nTo preserve config.json and reset only local runtime state, run:\n\n    ticket-orc doctor --reset-local\n\nTo replace both configuration and runtime with generated defaults, run:\n\n    ticket-orc init --force", localRoot)
	}
	if len(entries) == 0 {
		return false, nil
	}
	return false, fmt.Errorf("local runtime root has no valid Orc ownership marker: %s", localRoot)
}

func doctorLoadedConfig(configPath string, lookupEnv envLookup) (LoadedFileConfig, error) {
	values := make(map[string]string, 1)
	if configPath != "" {
		values["config"] = configPath
	}
	return loadInvocationConfig(values, lookupEnv)
}

func executeDoctorResetLocal(config doctorConfig, stdout, stderr io.Writer, lookupEnv envLookup) error {
	loaded, err := doctorLoadedConfig(config.configPath, lookupEnv)
	if err != nil {
		return err
	}
	localRoot := loaded.Instance.LocalDir
	if err := validateDoctorResetRoot(loaded.Instance.InstanceDir, localRoot, loaded.Instance.LocalDirConfigured, loaded.Config.ID); err != nil {
		return err
	}
	guard, err := acquireRuntimeGuard(context.Background(), localRoot)
	if err != nil {
		return fmt.Errorf("acquire Orc instance runtime guard: %w", err)
	}
	defer guard.Release()

	// init --force and daemon startup share this guard. Reload identity after
	// acquiring it so reset cannot target a runtime selected by stale config.
	current, err := LoadFileConfig(loaded.Instance.InstanceDir, loaded.Instance.ConfigPath, true)
	if err != nil {
		return fmt.Errorf("reload Orc config before local runtime reset: %w", err)
	}
	if current.Config.ID != loaded.Config.ID || current.Instance.LocalDir != localRoot || current.Instance.LocalDirConfigured != loaded.Instance.LocalDirConfigured {
		return fmt.Errorf("Orc config changed before local runtime reset; reload the config and retry")
	}

	cleanupRoots, daemonRoots, registrationRoot, exists, err := doctorRuntimeResetPaths(current)
	if err != nil {
		return err
	}
	activeRoot, err := activeRuntimeDaemonRoot(daemonRoots)
	if err != nil {
		return err
	}
	if activeRoot != "" {
		return fmt.Errorf("cannot reset local runtime while this Orc instance is running (runtime: %s)", activeRoot)
	}
	registrationCount := 0
	registrationWarning := ""
	if registrationRoot != "" {
		snapshot, _, snapshotErr := state.NewRegistrationStore(registrationRoot).SnapshotWithPresence(context.Background())
		if snapshotErr != nil {
			registrationWarning = fmt.Sprintf("could not read the authoritative steer registration store at %s; the number of steered sessions requiring rejoin is unknown: %s", terminaltext.Sanitize(registrationRoot, false), terminaltext.Sanitize(snapshotErr.Error(), true))
		} else {
			registrationCount = len(snapshot.Registrations)
		}
	}

	if err := removeRuntimeRoots(cleanupRoots, daemonRoots, func(root string) error {
		return fmt.Errorf("cannot reset local runtime while this Orc instance is running (runtime: %s)", root)
	}); err != nil {
		return err
	}
	if registrationWarning != "" {
		fmt.Fprintf(stderr, "warning: %s\n", registrationWarning)
	}
	if !exists {
		fmt.Fprintln(stdout, "Local Orc runtime is already reset.")
	} else {
		fmt.Fprintln(stdout, "Reset local Orc runtime.")
	}
	fmt.Fprintf(stdout, "Config preserved: %s\n", terminaltext.Sanitize(current.Instance.ConfigPath, false))
	fmt.Fprintf(stdout, "Instance ID: %s\n", current.Config.ID)
	if registrationWarning == "" && registrationCount > 0 {
		noun := "sessions"
		verb := "were"
		if registrationCount == 1 {
			noun, verb = "session", "was"
		}
		fmt.Fprintf(stdout, "\n%d steered %s %s registered in the removed runtime.\nThose sessions must run `ticket-orc join` again.\n", registrationCount, noun, verb)
	}
	return nil
}

func renderDoctorResult(out io.Writer, result supervisor.DoctorResult) {
	fmt.Fprintf(out, "doctor reloaded=%t workers=%d\n", result.Reloaded, len(result.Workers))
	for _, worker := range result.Workers {
		fmt.Fprintf(out, "doctor worker=%s outcome=%s", worker.Worker, worker.Outcome)
		if worker.Action != "" {
			fmt.Fprintf(out, " action=%s", worker.Action)
		}
		if worker.Reason != "" {
			fmt.Fprintf(out, " reason=%s", worker.Reason)
		}
		if worker.Failure != nil {
			fmt.Fprintf(out, " failure=%s", consoleWorkerFailureDetail(daemon.WorkerFailureDTO(worker.Failure)))
		}
		fmt.Fprintln(out)
	}
}
