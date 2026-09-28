package pi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"unicode"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

const defaultExecutable = "pi"

type processRequest struct {
	executable string
	args       []string
	dir        string
	env        []string
	stdout     io.Writer
}

type processResult struct {
	exitCode int
	stderr   string
}

type processRunner func(context.Context, processRequest) (processResult, error)
type environFunc func() []string

type Adapter struct {
	executable string
	environ    environFunc
	run        processRunner
}

var _ harness.Harness = (*Adapter)(nil)
var _ harness.PreflightAdapter = (*Adapter)(nil)

func New() *Adapter {
	adapter, _ := newAdapter(defaultExecutable, os.Environ, runProcess)
	return adapter
}

func NewWithExecutable(executable string) (*Adapter, error) {
	return newAdapter(executable, os.Environ, runProcess)
}

func (a *Adapter) Capabilities() harness.Capabilities {
	return harness.Capabilities{Resume: true, Fresh: true}
}

func (a *Adapter) Validate(config harness.PreflightConfig) error {
	if strings.ContainsRune(config.Model, '\x00') || strings.ContainsRune(config.Reasoning, '\x00') || strings.ContainsRune(config.PiProvider, '\x00') {
		return fmt.Errorf("Pi model, reasoning, and provider must not contain NUL")
	}
	if config.Reasoning != "" && !oneOf(config.Reasoning, "off", "minimal", "low", "medium", "high", "xhigh", "max") {
		return fmt.Errorf("Pi thinking value %q is not supported", config.Reasoning)
	}
	if config.OutputMode != "compact" && config.OutputMode != "quiet" && config.OutputMode != "json" {
		return fmt.Errorf("Pi output mode %q is not supported", config.OutputMode)
	}
	return nil
}

func oneOf(value string, allowed ...string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
}

func newAdapter(executable string, environ environFunc, runner processRunner) (*Adapter, error) {
	if strings.TrimSpace(executable) == "" {
		return nil, fmt.Errorf("Pi executable must not be empty")
	}
	if environ == nil || runner == nil {
		return nil, fmt.Errorf("Pi process dependencies must not be nil")
	}
	return &Adapter{executable: executable, environ: environ, run: runner}, nil
}

func (a *Adapter) Run(ctx context.Context, request harness.RunRequest) (harness.RunResult, error) {
	return a.execute(ctx, "", request)
}

func (a *Adapter) Resume(ctx context.Context, sessionID string, request harness.RunRequest) (harness.RunResult, error) {
	if err := validateSessionID(sessionID); err != nil {
		return harness.RunResult{}, err
	}
	return a.execute(ctx, sessionID, request)
}

func (a *Adapter) Cleanup(ctx context.Context, sessionID string, policy harness.CleanupPolicy) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateSessionID(sessionID); err != nil {
		return err
	}
	if policy == harness.CleanupKeep {
		return nil
	}
	return fmt.Errorf("Pi harness does not support session cleanup policy %q", policy)
}

func (a *Adapter) execute(ctx context.Context, sessionID string, request harness.RunRequest) (harness.RunResult, error) {
	if err := validateContext(ctx); err != nil {
		return harness.RunResult{}, err
	}
	if err := validateRequest(request); err != nil {
		return harness.RunResult{}, err
	}
	output, err := OpenExecutionOutput(request.StateDir, request.TicketID, request.Role, OutputMode(request.OutputMode), request.Operator, "pi", request.WorkerName)
	if err != nil {
		return harness.RunResult{}, err
	}
	args := []string{"--mode", "json"}
	if request.Model != "" {
		args = append(args, "--model", request.Model)
	}
	if request.Reasoning != "" {
		args = append(args, "--thinking", request.Reasoning)
	}
	if request.PiProvider != "" {
		args = append(args, "--provider", request.PiProvider)
	}
	if request.RequireSession {
		if sessionID != "" {
			args = append(args, "--session", sessionID)
		}
	} else {
		args = append(args, "--no-session")
	}
	args = append(args, request.Prompt)
	parser := newStreamParser(output, sessionID)
	process, processErr := a.run(ctx, processRequest{executable: a.executable, args: args, dir: request.WorkingDir, env: withActor(a.environ(), request.Actor), stdout: parser})
	finishErr := parser.Finish()
	closeErr := output.Close()
	streamErr := errors.Join(finishErr, closeErr)
	result := harness.RunResult{SessionID: parser.session, SessionOutcome: func() harness.SessionOutcome {
		if parser.sessionInvalidated {
			return harness.SessionInvalidated
		}
		return harness.SessionReusable
	}(), ExitCode: process.exitCode, LogPath: output.Path()}
	if contextErr := ctx.Err(); contextErr != nil {
		return result, errors.Join(contextErr, streamErr)
	}
	if streamErr != nil {
		return result, streamErr
	}
	result.StreamEndedNormally = true
	if processErr != nil || process.exitCode != 0 {
		if process.exitCode < 0 && processErr != nil {
			var exitErr *exec.ExitError
			if !errors.As(processErr, &exitErr) {
				return result, fmt.Errorf("run Pi: %w", processErr)
			}
		}
		return result, &ProcessError{ExitCode: process.exitCode, Stderr: process.stderr, cause: processErr}
	}
	if request.RequireSession && result.SessionID == "" {
		return result, fmt.Errorf("%w: successful run did not report a session ID", ErrProtocol)
	}
	return result, nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("Pi context must not be nil")
	}
	return ctx.Err()
}

func validateRequest(request harness.RunRequest) error {
	for name, value := range map[string]string{"working directory": request.WorkingDir, "state directory": request.StateDir, "role": request.Role, "actor": request.Actor, "ticket ID": request.TicketID, "prompt": request.Prompt} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Pi %s must not be empty", name)
		}
		if strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("Pi %s must not contain NUL", name)
		}
	}
	if strings.TrimSpace(request.OutputMode) == "" {
		return fmt.Errorf("Pi output mode must not be empty")
	}
	if strings.IndexFunc(request.Actor, unicode.IsSpace) >= 0 {
		return fmt.Errorf("Pi actor must not contain whitespace")
	}
	if request.Reasoning != "" && !oneOf(request.Reasoning, "off", "minimal", "low", "medium", "high", "xhigh", "max") {
		return fmt.Errorf("Pi thinking value %q is not supported", request.Reasoning)
	}
	if strings.ContainsRune(request.Model, '\x00') || strings.ContainsRune(request.PiProvider, '\x00') {
		return fmt.Errorf("Pi model and provider must not contain NUL")
	}
	return nil
}

func validateSessionID(sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("Pi session ID must not be empty")
	}
	if len(sessionID) > 512 || strings.IndexFunc(sessionID, unicode.IsSpace) >= 0 || strings.ContainsRune(sessionID, '\x00') {
		return fmt.Errorf("Pi session ID is invalid")
	}
	return nil
}

func runProcess(ctx context.Context, request processRequest) (processResult, error) {
	result, err := harness.RunProcess(ctx, harness.ProcessRequest{
		Executable: request.executable,
		Args:       request.args,
		Dir:        request.dir,
		Env:        request.env,
		Stdout:     request.stdout,
	})
	return processResult{exitCode: result.ExitCode, stderr: result.Stderr}, err
}

func withActor(base []string, actor string) []string {
	env := make([]string, 0, len(base)+1)
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok && sameEnvKey(key, "TICKET_ACTOR") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "TICKET_ACTOR="+actor)
}

func sameEnvKey(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}
