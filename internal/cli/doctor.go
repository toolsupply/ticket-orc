package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

type doctorConfig struct {
	configPath string
}

// doctorResultComplete is the standalone command's exit policy. A recovery
// pass is successful only when every configured worker reports recovery.
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
	args := []string{"--all"}
	if config.configPath != "" {
		args = append(args, "--config", config.configPath)
	}
	runConfig, _, err := parseRunConfig(args, lookupEnv)
	if err != nil {
		return err
	}
	runConfig.Doctor = true
	return executeRun(runConfig, stdout, stderr)
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
