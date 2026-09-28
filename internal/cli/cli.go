// Package cli implements process invocation and user-facing output.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

// Version is the build version reported by the version command. Release and
// Makefile builds replace the development value at link time.
var Version = "dev"

// Commit is the optional build commit, settable at link time.
var Commit = ""

type roleExecutor func(supervisor.RoleConfig) error
type stateExecutor func(StateConfig, io.Writer) error
type currentStateExecutor func(StateConfig, io.Writer, envLookup) error
type gcExecutor func(GCConfig, io.Writer, io.Writer) error
type runExecutor func(RunConfig, io.Writer, io.Writer) error
type configCheckExecutor func(ConfigCheckConfig, io.Writer, io.Writer, envLookup) error
type doctorExecutor func(doctorConfig, io.Writer, io.Writer, envLookup) error

type renderedRunFailure struct {
	cause error
}

func (e *renderedRunFailure) Error() string { return e.cause.Error() }
func (e *renderedRunFailure) Unwrap() error { return e.cause }

func markRunFailureRendered(err error) error {
	if err == nil {
		return nil
	}
	return &renderedRunFailure{cause: err}
}

func runFailureWasRendered(err error) bool {
	var rendered *renderedRunFailure
	return errors.As(err, &rendered)
}

type commandExecutors struct {
	role         roleExecutor
	state        stateExecutor
	currentState currentStateExecutor
	gc           gcExecutor
	run          runExecutor
	configCheck  configCheckExecutor
	doctor       doctorExecutor
}

// Run executes one CLI invocation and returns its process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	return run(args, stdout, stderr, os.LookupEnv, executeRole)
}

func run(args []string, stdout, stderr io.Writer, lookupEnv envLookup, execute roleExecutor) int {
	return runWithExecutors(args, stdout, stderr, lookupEnv, commandExecutors{
		role:  execute,
		state: executeState,
		currentState: func(config StateConfig, out io.Writer, lookup envLookup) error {
			return executeSessionState(config, out, lookup, runTicketJSON)
		},
		gc:          executeGC,
		run:         executeRun,
		configCheck: executeConfigCheck,
		doctor:      executeDoctor,
	})
}

func runWithExecutors(args []string, stdout, stderr io.Writer, lookupEnv envLookup, execute commandExecutors) int {
	if len(args) == 0 {
		writeHelp(stdout)
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		return runHelp(args, stdout, stderr)
	case "version", "-v", "--version":
		if len(args) != 1 {
			return usageError(stderr, "%s accepts no arguments", args[0])
		}
		writeVersion(stdout)
		return 0
	case "init":
		return executeInit(args[1:], stdout, stderr, lookupEnv)
	case "join":
		return executeJoin(args[1:], stdout, stderr, lookupEnv)
	case "leave":
		return executeLeave(args[1:], stdout, stderr, lookupEnv)
	case "whoami":
		return executeWhoami(args[1:], stdout, stderr, lookupEnv)
	case "next":
		return executeNext(args[1:], stdout, stderr, lookupEnv)
	case "queue":
		return executeQueue(args[1:], stdout, stderr, lookupEnv)
	case string(RoleCoder), string(RoleReviewer):
		config, help, err := parseRoleConfig(supervisor.Role(args[0]), args[1:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeRoleHelp(stdout, supervisor.Role(args[0]))
			return 0
		}
		if err := execute.role(config); err != nil {
			renderRoleFailure(stderr, err)
			if errors.Is(err, ErrWorkerLeaseConflict) {
				return workerLeaseConflictExitCode
			}
			return 1
		}
		return 0
	case "state":
		config, help, err := parseStateConfig(args[1:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeStateHelp(stdout)
			return 0
		}
		if execute.currentState != nil {
			if err := execute.currentState(config, stdout, lookupEnv); err != nil {
				fmt.Fprintf(stderr, "error: %v\n", err)
				return 1
			}
			return 0
		}
		if err := execute.state(config, stdout); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		return 0
	case "gc":
		config, help, err := parseGCConfig(args[1:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeGCHelp(stdout)
			return 0
		}
		if err := execute.gc(config, stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		return 0
	case "run":
		config, help, err := parseRunConfig(args[1:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeRunHelp(stdout)
			return 0
		}
		if execute.run == nil {
			return usageError(stderr, "run executor is unavailable")
		}
		if err := execute.run(config, stdout, stderr); err != nil {
			if !runFailureWasRendered(err) {
				fmt.Fprintf(stderr, "error: %s\n", terminaltext.Sanitize(err.Error(), false))
			}
			return 1
		}
		return 0
	case "doctor":
		config, help, err := parseDoctorConfig(args[1:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeDoctorHelp(stdout)
			return 0
		}
		if execute.doctor == nil {
			return usageError(stderr, "doctor executor is unavailable")
		}
		if err := execute.doctor(config, stdout, stderr, lookupEnv); err != nil {
			fmt.Fprintf(stderr, "error: doctor recovery failed: %s\n", terminaltext.Sanitize(err.Error(), true))
			return 1
		}
		return 0
	case "report":
		config, help, err := parseReviewReportConfig(args[1:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeReviewReportHelp(stdout)
			return 0
		}
		if err := executeReviewReport(context.Background(), config, stdout); err != nil {
			fmt.Fprintf(stderr, "error: review report failed: %s\n", terminaltext.Sanitize(err.Error(), true))
			return 1
		}
		return 0
	case "endpoint":
		config, help, err := parseStateConfig(args[1:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeEndpointHelp(stdout)
			return 0
		}
		endpoint, err := daemon.ReadEndpoint(config.StateDir)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		fmt.Fprintln(stderr, "warning: this capability URL reveals the endpoint key; share it only with trusted local processes")
		fmt.Fprintln(stdout, endpoint.CapabilityURL())
		return 0
	case "attach":
		return executeAttach(args[1:], stdout, stderr, lookupEnv)
	case "config":
		if len(args) < 2 || args[1] != "check" {
			return usageError(stderr, "config requires the check subcommand")
		}
		config, help, err := parseConfigCheckConfig(args[2:], lookupEnv)
		if err != nil {
			return usageError(stderr, "%v", err)
		}
		if help {
			writeConfigCheckHelp(stdout)
			return 0
		}
		if execute.configCheck == nil {
			return usageError(stderr, "config check executor is unavailable")
		}
		if err := execute.configCheck(config, stdout, stderr, lookupEnv); err != nil {
			return 1
		}
		return 0
	case "status", "worker", "group", "pause", "resume", "abort", "reload", "shutdown":
		return executeDaemonCommand(args, stdout, stderr, lookupEnv)
	default:
		return usageError(stderr, "unknown command: %s", args[0])
	}
}

func renderRoleFailure(out io.Writer, err error) {
	failure := roleFailureEnvelope(err)
	if failure == nil {
		fmt.Fprintln(out, "error: worker role failed")
		return
	}
	if failure.Classification == "worker_error" {
		fmt.Fprintf(out, "error: worker failure classification=%s phase=%s", safeFailureToken(failure.Classification), safeFailurePhase(failure.Phase))
		renderRoleFailureMetadataAfterPrefix(out, failure)
		if detail := safeRoleFailureDetail(err); detail != "" {
			fmt.Fprintf(out, " detail=%s", detail)
		}
		fmt.Fprintln(out)
		return
	}
	if failure.Classification == "ticket_command_failed" {
		var waitErr *orc.WorkWaitError
		if errors.As(err, &waitErr) {
			label := "review"
			if waitErr.Role == "coder" {
				label = "implementation"
			}
			fmt.Fprintf(out, "error: wait for %s work failed", label)
			renderRoleFailureMetadata(out, failure)
			return
		}
	}
	if failure.Classification == "child_exit" && failure.ExitCode > 0 {
		fmt.Fprintf(out, "error: Codex process exited with status %d", failure.ExitCode)
		renderRoleFailureMetadata(out, failure)
		return
	}
	fmt.Fprintf(out, "error: worker failure classification=%s phase=%s", safeFailureToken(failure.Classification), safeFailurePhase(failure.Phase))
	renderRoleFailureMetadataAfterPrefix(out, failure)
	fmt.Fprintln(out)
}

func safeRoleFailureDetail(err error) string {
	if err == nil {
		return ""
	}
	detail := terminaltext.Sanitize(err.Error(), true)
	if detail == "" || containsUnsafeRoleFailureDetail(detail) {
		return "worker failure detail redacted"
	}
	return detail
}

func containsUnsafeRoleFailureDetail(detail string) bool {
	lower := strings.ToLower(detail)
	for _, marker := range []string{"bearer-token", "access-token", "authorization", "password=", "prompt=", "argv="} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func renderRoleFailureMetadata(out io.Writer, failure *orc.Failure) {
	renderRoleFailureMetadataAfterPrefix(out, failure)
	fmt.Fprintln(out)
}

func renderRoleFailureMetadataAfterPrefix(out io.Writer, failure *orc.Failure) {
	if failure.ExitCode != 0 {
		fmt.Fprintf(out, " exit_code=%d", failure.ExitCode)
	}
	if failure.Signal != "" {
		fmt.Fprintf(out, " signal=%s", safeFailureToken(failure.Signal))
	}
	if failure.TicketCode != "" {
		fmt.Fprintf(out, " ticket_code=%s", safeFailureToken(failure.TicketCode))
	}
	if failure.TransportCategory != "" {
		fmt.Fprintf(out, " transport_category=%s", safeFailureToken(failure.TransportCategory))
	}
	if failure.Origin != "" {
		fmt.Fprintf(out, " origin=%s", safeFailureToken(failure.Origin))
	}
	if failure.Operation != "" {
		fmt.Fprintf(out, " operation=%s", safeFailureOperation(failure.Operation))
	}
	if failure.NestedExitCode != 0 {
		fmt.Fprintf(out, " nested_exit_code=%d", failure.NestedExitCode)
	}
	if failure.Ticket != "" {
		fmt.Fprintf(out, " ticket=%s", optionalFailureTicket(failure.Ticket))
	}
	if failure.Remediation != "" {
		fmt.Fprintf(out, " remediation=%s", safeFailureRemediation(failure.Remediation))
	}
	if failure.Contained {
		fmt.Fprint(out, " contained=true")
	}
}

func runHelp(args []string, stdout, stderr io.Writer) int {
	if args[0] != "help" {
		if len(args) != 1 {
			return usageError(stderr, "%s accepts no arguments", args[0])
		}
		writeHelp(stdout)
		return 0
	}

	if writeHelpTopic(args[1:], stdout) {
		return 0
	}
	if len(args) == 2 && args[1] == "config" {
		return usageError(stderr, "use 'ticket-orc help config check'")
	}
	if len(args) > 3 {
		return usageError(stderr, "help accepts at most two command words")
	}
	return usageError(stderr, "unknown help topic: %s", strings.Join(args[1:], " "))
}

func unavailableRoleExecutor(config supervisor.RoleConfig) error {
	return fmt.Errorf("%s worker is not implemented yet", config.Role)
}

func writeVersion(w io.Writer) {
	if Commit == "" {
		fmt.Fprintf(w, "ticket-orc %s\n", Version)
		return
	}
	fmt.Fprintf(w, "ticket-orc %s (%s)\n", Version, Commit)
}

func usageError(w io.Writer, format string, args ...any) int {
	fmt.Fprintf(w, "error: "+format+"\n", args...)
	return 2
}
