package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

type ConfigCheckConfig struct {
	ConfigPath string
	Explicit   bool
	Output     supervisor.OutputMode
}

func parseConfigCheckConfig(args []string, lookupEnv envLookup) (ConfigCheckConfig, bool, error) {
	values := make(map[string]string)
	help := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-h" || arg == "--help" {
			help = true
			continue
		}
		if arg == "-c" || strings.HasPrefix(arg, "-c=") {
			if _, duplicate := values["config"]; duplicate {
				return ConfigCheckConfig{}, false, fmt.Errorf("duplicate flag --config")
			}
			value := strings.TrimPrefix(arg, "-c=")
			if arg == "-c" {
				i++
				if i >= len(args) {
					return ConfigCheckConfig{}, false, fmt.Errorf("flag -c requires a value")
				}
				value = args[i]
			}
			if strings.TrimSpace(value) == "" {
				return ConfigCheckConfig{}, false, fmt.Errorf("-c must not be empty")
			}
			values["config"] = value
			continue
		}
		if !strings.HasPrefix(arg, "--") {
			return ConfigCheckConfig{}, false, fmt.Errorf("unexpected argument: %s", arg)
		}
		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if name != "config" && name != "output" {
			return ConfigCheckConfig{}, false, fmt.Errorf("unknown flag --%s", name)
		}
		if _, duplicate := values[name]; duplicate {
			return ConfigCheckConfig{}, false, fmt.Errorf("duplicate flag --%s", name)
		}
		if !hasValue {
			i++
			if i >= len(args) {
				return ConfigCheckConfig{}, false, fmt.Errorf("flag --%s requires a value", name)
			}
			value = args[i]
		}
		if strings.TrimSpace(value) == "" {
			return ConfigCheckConfig{}, false, fmt.Errorf("--%s must not be empty", name)
		}
		values[name] = value
	}
	if help {
		return ConfigCheckConfig{}, true, nil
	}
	path, explicit, err := invocationConfigPath(values, lookupEnv)
	if err != nil {
		return ConfigCheckConfig{}, false, err
	}
	output := OutputCompact
	if value := values["output"]; value != "" {
		output = supervisor.OutputMode(value)
	}
	if output != OutputCompact && output != OutputJSON && output != OutputQuiet {
		return ConfigCheckConfig{}, false, fmt.Errorf("output must be compact, quiet, or json")
	}
	return ConfigCheckConfig{ConfigPath: path, Explicit: explicit, Output: output}, false, nil
}

func executeConfigCheck(config ConfigCheckConfig, stdout, stderr io.Writer, lookupEnv envLookup) error {
	loaded, err := LoadFileConfig(filepath.Dir(config.ConfigPath), config.ConfigPath, config.Explicit)
	if err != nil {
		if config.Output != OutputQuiet {
			_, _ = fmt.Fprintf(stderr, "config error: %s\n", safeConfigLoadError(err))
		}
		return err
	}
	workers, diagnostics := resolveAllWorkers(loaded, lookupEnv)
	names := make([]string, 0, len(workers))
	for name := range workers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		worker := workers[name]
		if err := preflightRoleConfig(worker.Config); err != nil {
			diagnostics = append(diagnostics, workerResolutionDiagnostic(name, err))
		}
	}
	// A config check validates every worker while keeping collisions between
	// dormant workers non-blocking.
	diagnostics = append(diagnostics, AnalyzeConfiguredWorkers(workers)...)
	sortDiagnostics(diagnostics)
	if config.Output == OutputJSON {
		if err := json.NewEncoder(stdout).Encode(diagnostics); err != nil {
			return err
		}
	} else if config.Output != OutputQuiet {
		renderConfigDiagnostics(stdout, diagnostics)
	}
	if diagnostics.HasErrors() {
		return diagnostics
	}
	return nil
}

func safeConfigLoadError(err error) string {
	if err == nil {
		return "configuration could not be loaded"
	}
	message := strings.Join(strings.Fields(err.Error()), " ")
	if len(message) > 240 {
		message = message[:240]
	}
	return message
}
