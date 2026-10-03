package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

type daemonCommandOptions struct {
	localDir           string
	instanceID         string
	localDirConfigured bool
	output             supervisor.OutputMode
	endpoint           string
	environmentURL     string
	configPath         string
}

func writeDaemonHelp(out io.Writer, command string) {
	const common = "  --endpoint URL    Override the complete daemon capability URL\n  --config PATH     Orc config file (for endpoint fallback)\n  -h, --help        Show this help\n"
	switch command {
	case "status":
		fmt.Fprint(out, "Usage: ticket-orc status [--endpoint URL] [--config PATH] [--output compact|json]\n\nShow daemon status.\n\n"+common+"  --output MODE     compact or json\n")
	case "worker":
		fmt.Fprint(out, "Usage: ticket-orc worker start|stop|restart|pause|resume NAME\n\nControl one daemon worker.\n\n"+common)
	case "group":
		fmt.Fprint(out, "Usage: ticket-orc group start|stop NAME\n\nControl a daemon worker group.\n\n"+common)
	case "reload":
		fmt.Fprint(out, "Usage: ticket-orc reload\n\nReload daemon configuration.\n\n"+common)
	case "pause":
		fmt.Fprint(out, "Usage: ticket-orc pause\n\nPause daemon-wide work dispatch. Running work may finish. This is distinct from worker pause.\n\n"+common)
	case "resume":
		fmt.Fprint(out, "Usage: ticket-orc resume\n\nResume daemon-wide work dispatch and reconcile current state. Worker-level pauses remain in effect.\n\n"+common)
	case "abort":
		fmt.Fprint(out, "Usage: ticket-orc abort\n\nAbort daemon-wide work dispatch and stop managed workers. Orc sends best-effort abort requests to relevant steer sessions; remote termination is not guaranteed. Use resume to recover.\n\n"+common+"  --output MODE     compact or json\n")
	case "shutdown":
		fmt.Fprint(out, "Usage: ticket-orc shutdown\n\nStop the daemon.\n\n"+common)
	}
	fmt.Fprintln(out)
}

func parseDaemonCommandOptions(args []string, lookupEnv envLookup, allowOutput bool) (daemonCommandOptions, []string, bool, error) {
	options := daemonCommandOptions{output: OutputCompact}
	options.environmentURL, _ = lookupEnv("TICKET_ORC_ENDPOINT")
	if value, ok := lookupEnv("TICKET_ORC_OUTPUT"); ok && value != "" {
		options.output = supervisor.OutputMode(value)
	}
	positionals := make([]string, 0, len(args))
	help := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			help = true
		case arg == "--endpoint" || strings.HasPrefix(arg, "--endpoint="):
			value := strings.TrimPrefix(arg, "--endpoint=")
			if arg == "--endpoint" {
				i++
				if i >= len(args) {
					return daemonCommandOptions{}, nil, false, fmt.Errorf("flag --endpoint requires a value")
				}
				value = args[i]
			}
			if strings.TrimSpace(value) == "" {
				return daemonCommandOptions{}, nil, false, fmt.Errorf("--endpoint must not be empty")
			}
			if options.endpoint != "" {
				return daemonCommandOptions{}, nil, false, fmt.Errorf("duplicate flag --endpoint")
			}
			options.endpoint = value
		case arg == "--config" || strings.HasPrefix(arg, "--config=") || arg == "-c" || strings.HasPrefix(arg, "-c="):
			value := ""
			if strings.HasPrefix(arg, "--config=") {
				value = strings.TrimPrefix(arg, "--config=")
			} else if strings.HasPrefix(arg, "-c=") {
				value = strings.TrimPrefix(arg, "-c=")
			} else {
				i++
				if i >= len(args) {
					return daemonCommandOptions{}, nil, false, fmt.Errorf("flag --config requires a value")
				}
				value = args[i]
			}
			if strings.TrimSpace(value) == "" {
				return daemonCommandOptions{}, nil, false, fmt.Errorf("--config must not be empty")
			}
			if options.configPath != "" {
				return daemonCommandOptions{}, nil, false, fmt.Errorf("duplicate flag --config")
			}
			options.configPath = value
		case arg == "--output" || strings.HasPrefix(arg, "--output="):
			if !allowOutput {
				return daemonCommandOptions{}, nil, false, fmt.Errorf("--output is only valid for status")
			}
			value := strings.TrimPrefix(arg, "--output=")
			if arg == "--output" {
				i++
				if i >= len(args) {
					return daemonCommandOptions{}, nil, false, fmt.Errorf("flag --output requires a value")
				}
				value = args[i]
			}
			if value != string(OutputCompact) && value != string(OutputJSON) {
				return daemonCommandOptions{}, nil, false, fmt.Errorf("--output must be compact or json")
			}
			options.output = supervisor.OutputMode(value)
		default:
			if strings.HasPrefix(arg, "-") {
				return daemonCommandOptions{}, nil, false, fmt.Errorf("unknown option %s", arg)
			}
			positionals = append(positionals, arg)
		}
	}
	if help {
		return options, nil, true, nil
	}
	if options.configPath != "" && options.endpoint == "" {
		// An explicitly selected instance owns daemon discovery. The ambient
		// endpoint is only a fallback when no command-line instance is selected.
		options.environmentURL = ""
	}
	// An endpoint override is a complete daemon destination and bypasses local
	// instance discovery. An explicitly supplied config still loads so its
	// errors remain visible and its state directory can be used where needed.
	if options.configPath != "" || (options.endpoint == "" && strings.TrimSpace(options.environmentURL) == "") {
		values := map[string]string{}
		if options.configPath != "" {
			values["config"] = options.configPath
		}
		loaded, configErr := loadInvocationConfig(values, lookupEnv)
		if configErr != nil {
			return daemonCommandOptions{}, nil, false, configErr
		}
		options.localDir = loaded.Instance.LocalDir
		options.instanceID = loaded.Config.ID
		options.localDirConfigured = loaded.Instance.LocalDirConfigured
	}
	return options, positionals, help, nil
}

func executeDaemonCommand(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	if len(args) == 0 {
		return usageError(stderr, "daemon command is required")
	}
	command := args[0]
	options, positional, help, err := parseDaemonCommandOptions(args[1:], lookupEnv, command == "status" || command == "abort")
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	if help {
		writeDaemonHelp(stdout, command)
		return 0
	}
	if command == "worker" || command == "group" {
		if command == "worker" && (len(positional) != 2 || (positional[0] != "start" && positional[0] != "stop" && positional[0] != "restart" && positional[0] != "pause" && positional[0] != "resume")) {
			return usageError(stderr, "usage: ticket-orc worker start|stop|restart|pause|resume NAME")
		}
		if command == "group" && (len(positional) != 2 || (positional[0] != "start" && positional[0] != "stop")) {
			return usageError(stderr, "usage: ticket-orc group start|stop NAME")
		}
	} else if len(positional) != 0 {
		return usageError(stderr, "%s accepts no positional arguments", command)
	}
	switch command {
	case "status", "reload", "pause", "resume", "abort", "shutdown", "worker", "group":
	default:
		return usageError(stderr, "unknown daemon command: %s", command)
	}
	if options.localDir != "" {
		if err := ensureRuntimeOwnershipIfKnown(options.localDir, options.instanceID, options.localDirConfigured); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
	}
	if command == "worker" {
		return executeDaemonWorker(options, positional[0], positional[1], stdout, stderr)
	}
	if command == "group" {
		return executeDaemonGroup(options, positional[0], positional[1], stdout, stderr)
	}
	switch command {
	case "status":
		return executeDaemonStatus(options, stdout, stderr)
	case "reload":
		return executeDaemonReload(options, stdout, stderr)
	case "pause":
		return executeDaemonControlMode(options, "pause", stdout, stderr)
	case "resume":
		return executeDaemonControlMode(options, "resume", stdout, stderr)
	case "abort":
		return executeDaemonControlMode(options, "abort", stdout, stderr)
	case "shutdown":
		return executeDaemonShutdown(options, stdout, stderr)
	default:
		return usageError(stderr, "unknown daemon command: %s", command)
	}
}

func executeDaemonControlMode(options daemonCommandOptions, operation string, stdout, stderr io.Writer) int {
	client, ctx, cancel, err := daemonClient(options)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	defer cancel()
	var result daemon.DaemonControlResult
	switch operation {
	case "pause":
		result, err = client.Pause(ctx)
	case "resume":
		result, err = client.Resume(ctx)
	case "abort":
		result, err = client.Abort(ctx)
	}
	if err != nil {
		if operation == "abort" && (result.Mode != "" || len(result.Targets) > 0) {
			if options.output == OutputJSON {
				writeDaemonJSON(result, stdout, stderr)
			} else {
				renderDaemonAbortCompact(stdout, result)
			}
		}
		return daemonCommandError(stderr, err)
	}
	if options.output == OutputJSON {
		return writeDaemonJSON(result, stdout, stderr)
	}
	if operation == "abort" {
		renderDaemonAbortCompact(stdout, result)
	} else {
		fmt.Fprintf(stdout, "daemon mode=%s mutation_applied=%t\n", result.Mode, result.Applied)
	}
	return 0
}

func renderDaemonAbortCompact(stdout io.Writer, result daemon.DaemonControlResult) {
	fmt.Fprintf(stdout, "daemon mode=%s mutation_applied=%t\n", result.Mode, result.Applied)
	for _, target := range result.Targets {
		fmt.Fprintf(stdout, "abort target kind=%s", target.Kind)
		if target.Worker != "" {
			fmt.Fprintf(stdout, " worker=%s", target.Worker)
		}
		if target.RepositoryID != "" {
			fmt.Fprintf(stdout, " repository_id=%s", target.RepositoryID)
		}
		if target.Actor != "" {
			fmt.Fprintf(stdout, " actor=%s", target.Actor)
		}
		fmt.Fprintf(stdout, " outcome=%s", target.Outcome)
		if target.Code != "" {
			fmt.Fprintf(stdout, " code=%s", target.Code)
		}
		fmt.Fprintln(stdout)
	}
}

func daemonClient(options daemonCommandOptions) (*daemonclient.Client, context.Context, context.CancelFunc, error) {
	client, err := daemonclient.NewWithEndpoint(options.localDir, options.endpoint, options.environmentURL)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	return client, ctx, cancel, nil
}

func executeDaemonStatus(options daemonCommandOptions, stdout, stderr io.Writer) int {
	client, ctx, cancel, err := daemonClient(options)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	defer cancel()
	status, err := client.Status(ctx)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	if options.output == OutputJSON {
		return writeDaemonJSON(status, stdout, stderr)
	}
	renderDaemonCompactStatus(stdout, status)
	return 0
}

func renderDaemonCompactStatus(stdout io.Writer, status daemon.Status) {
	fmt.Fprintln(stdout)
	fmt.Fprintf(stdout, "daemon mode=%s version=%s protocol=%d pid=%d url=%s started=%s\n", status.Mode, status.Version, status.Protocol, status.PID, status.URL, status.StartedAt.Local().Format(time.RFC3339))
	for _, worker := range status.Workers {
		fmt.Fprintf(stdout, "worker %s state=%s role=%s harness=%s", worker.Name, worker.State, worker.Role, worker.Harness)
		if worker.TicketActor != "" {
			fmt.Fprintf(stdout, " actor=%s", worker.TicketActor)
		}
		if worker.RepositoryName != "" {
			fmt.Fprintf(stdout, " repository_name=%s", worker.RepositoryName)
		} else if worker.RepositoryPath != "" {
			fmt.Fprintf(stdout, " repository_path=%s", worker.RepositoryPath)
		}
		if worker.TicketScope != "" {
			fmt.Fprintf(stdout, " ticket_scope=%s", worker.TicketScope)
		}
		if len(worker.Groups) > 0 {
			fmt.Fprintf(stdout, " groups=%s", strings.Join(worker.Groups, ","))
		}
		fmt.Fprintln(stdout)
	}
	for _, repository := range status.Repositories {
		fmt.Fprintf(stdout, "repository %s state=%s", repository.Key, repository.State)
		if repository.Name != "" {
			fmt.Fprintf(stdout, " name=%s", repository.Name)
		}
		if repository.Path != "" {
			fmt.Fprintf(stdout, " path=%s", repository.Path)
		}
		if repository.RestartCount != 0 {
			fmt.Fprintf(stdout, " restarts=%d", repository.RestartCount)
		}
		if repository.Failure != "" {
			fmt.Fprintf(stdout, " failure=%s", repository.Failure)
		}
		fmt.Fprintln(stdout)
	}
	fmt.Fprintln(stdout)
}

func executeDaemonWorker(options daemonCommandOptions, operation, name string, stdout, stderr io.Writer) int {
	client, ctx, cancel, err := daemonClient(options)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	defer cancel()
	result, err := client.Worker(ctx, name, operation)
	if err != nil {
		if result.Worker != "" || result.State != "" {
			if options.output == OutputJSON {
				_ = writeDaemonJSON(result, stdout, stderr)
			} else {
				_ = writeDaemonResult(result, OutputCompact, stdout, stderr)
			}
		}
		return daemonCommandError(stderr, err)
	}
	return writeDaemonResult(result, options.output, stdout, stderr)
}

func executeDaemonGroup(options daemonCommandOptions, operation, name string, stdout, stderr io.Writer) int {
	client, ctx, cancel, err := daemonClient(options)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	defer cancel()
	result, err := client.Group(ctx, name, operation)
	if err != nil {
		if result.Group != "" || len(result.Results) > 0 {
			if options.output == OutputJSON {
				_ = writeDaemonJSON(result, stdout, stderr)
			} else {
				fmt.Fprintf(stdout, "group %s mutation_applied=%t\n", result.Group, groupApplied(result))
				for _, item := range result.Results {
					fmt.Fprintf(stdout, "worker %s state=%s mutation_applied=%t\n", item.Worker, item.State, item.Applied)
				}
			}
		}
		return daemonCommandError(stderr, err)
	}
	if options.output == OutputJSON {
		return writeDaemonJSON(result, stdout, stderr)
	}
	fmt.Fprintf(stdout, "group %s mutation_applied=%t\n", result.Group, groupApplied(result))
	for _, item := range result.Results {
		fmt.Fprintf(stdout, "worker %s state=%s mutation_applied=%t\n", item.Worker, item.State, item.Applied)
	}
	return 0
}

func executeDaemonReload(options daemonCommandOptions, stdout, stderr io.Writer) int {
	client, ctx, cancel, err := daemonClient(options)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	defer cancel()
	result, err := client.Reload(ctx)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	if options.output == OutputJSON {
		return writeDaemonJSON(result, stdout, stderr)
	}
	fmt.Fprintf(stdout, "reload revision=%s mutation_applied=%t\n", result.Revision, result.Applied)
	return 0
}

func executeDaemonShutdown(options daemonCommandOptions, stdout, stderr io.Writer) int {
	client, ctx, cancel, err := daemonClient(options)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		return daemonCommandError(stderr, err)
	}
	fmt.Fprintln(stdout, "shutdown mutation_applied=true")
	return 0
}

func writeDaemonResult(result daemon.MutationResult, output supervisor.OutputMode, stdout, stderr io.Writer) int {
	if output == OutputJSON {
		return writeDaemonJSON(result, stdout, stderr)
	}
	fmt.Fprintf(stdout, "worker=%s state=%s mutation_applied=%t", result.Worker, result.State, result.Applied)
	fmt.Fprintln(stdout)
	return 0
}

func groupApplied(result daemon.GroupResult) bool {
	if len(result.Results) == 0 {
		return false
	}
	for _, item := range result.Results {
		if !item.Applied {
			return false
		}
	}
	return true
}

func writeDaemonJSON(value any, stdout, stderr io.Writer) int {
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		fmt.Fprintf(stderr, "error: unable to render daemon response\n")
		return 1
	}
	return 0
}

func daemonCommandError(stderr io.Writer, err error) int {
	var clientErr *daemonclient.Error
	if errors.As(err, &clientErr) {
		label := string(clientErr.Kind)
		message := fmt.Sprintf("error: daemon %s failure: %s", label, clientErr.Error())
		if clientErr.Kind == daemonclient.ErrorApplication && daemonErrorNeedsStatus(clientErr) {
			message += "; inspect 'ticket-orc status' before retrying"
		}
		fmt.Fprintln(stderr, message)
		return 1
	}
	fmt.Fprintf(stderr, "error: daemon request failed\n")
	return 1
}

func daemonErrorNeedsStatus(err *daemonclient.Error) bool {
	if err == nil || err.Kind != daemonclient.ErrorApplication {
		return false
	}
	return err.Result != nil && !err.Applied
}
