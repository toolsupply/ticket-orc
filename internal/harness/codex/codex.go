// Package codex implements the Codex CLI harness adapter.
package codex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"unicode"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

const defaultExecutable = "codex"

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

// Adapter runs Codex CLI sessions without owning ticket lifecycle policy.
type Adapter struct {
	executable string
	environ    environFunc
	run        processRunner
}

var _ harness.Harness = (*Adapter)(nil)
var _ harness.PreflightAdapter = (*Adapter)(nil)

// New constructs an adapter that locates codex through PATH.
func New() *Adapter {
	adapter, _ := newAdapter(defaultExecutable, os.Environ, runProcess)
	return adapter
}

// NewWithExecutable constructs an adapter using a specific executable. It is
// intended for executable-level tests and controlled embedding.
func NewWithExecutable(executable string) (*Adapter, error) {
	return newAdapter(executable, os.Environ, runProcess)
}

// Capabilities reports the static Codex session and cleanup operations Orc
// currently relies on.
func (a *Adapter) Capabilities() harness.Capabilities {
	return harness.Capabilities{Resume: true, Fresh: true, CleanupDelete: true, CleanupArchive: true}
}

// Validate checks generic invocation settings that Codex can reject without a
// ticket claim. Model names remain opaque; reasoning is validated here because
// other adapters have different vocabularies.
func (a *Adapter) Validate(config harness.PreflightConfig) error {
	if strings.ContainsRune(config.Model, '\x00') {
		return fmt.Errorf("Codex model must not contain NUL")
	}
	if strings.ContainsRune(config.Reasoning, '\x00') {
		return fmt.Errorf("Codex reasoning must not contain NUL")
	}
	if config.Reasoning != "" && !oneOf(config.Reasoning, "low", "medium", "high", "xhigh", "max", "ultra") {
		return fmt.Errorf("Codex reasoning value %q is not supported", config.Reasoning)
	}
	if config.OutputMode != "compact" && config.OutputMode != "quiet" && config.OutputMode != "json" {
		return fmt.Errorf("Codex output mode %q is not supported", config.OutputMode)
	}
	if config.CodexSandbox != "" && !oneOf(config.CodexSandbox, "read-only", "workspace-write", "danger-full-access") {
		return fmt.Errorf("Codex sandbox mode %q is not supported", config.CodexSandbox)
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
		return nil, fmt.Errorf("Codex executable must not be empty")
	}
	if environ == nil || runner == nil {
		return nil, fmt.Errorf("Codex process dependencies must not be nil")
	}
	return &Adapter{executable: executable, environ: environ, run: runner}, nil
}

// Run starts a new Codex session.
func (a *Adapter) Run(ctx context.Context, request harness.RunRequest) (harness.RunResult, error) {
	return a.execute(ctx, "", request)
}

// Resume continues an existing Codex session and rejects a different thread
// identity reported by the stream.
func (a *Adapter) Resume(ctx context.Context, sessionID string, request harness.RunRequest) (harness.RunResult, error) {
	if err := validateSessionID(sessionID); err != nil {
		return harness.RunResult{}, err
	}
	return a.execute(ctx, sessionID, request)
}

// CurrentTarget returns the running Codex thread exposed by Codex's supported
// CODEX_THREAD_ID environment contract. It does not inspect session files or
// start a Codex process.
func (a *Adapter) CurrentTarget() (string, error) {
	if a == nil || a.environ == nil {
		return "", fmt.Errorf("Codex environment is unavailable")
	}
	for _, entry := range a.environ() {
		name, value, ok := strings.Cut(entry, "=")
		if ok && name == "CODEX_THREAD_ID" {
			if err := validateSessionID(value); err != nil {
				return "", fmt.Errorf("CODEX_THREAD_ID is invalid")
			}
			return value, nil
		}
	}
	return "", fmt.Errorf("CODEX_THREAD_ID is unavailable; pass --target SESSION_ID")
}

// ValidateSessionTarget checks the opaque thread identifier accepted by the
// Codex queue transport without contacting the target.
func ValidateSessionTarget(target string) error { return validateSessionID(target) }

// Queue delivers an opaque message to the exact external Codex session using
// its recorded Codex home. Its subprocess is independent of the caller cwd and
// inherits no Ticket routing identity.
func (a *Adapter) Queue(ctx context.Context, home, thread, message string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(home) == "" || !filepath.IsAbs(home) || filepath.Clean(home) != home {
		return fmt.Errorf("Codex queue home must be an absolute canonical path")
	}
	if err := validateQueueTarget(thread); err != nil {
		return err
	}
	if message == "" || strings.ContainsRune(message, '\x00') {
		return fmt.Errorf("Codex queue message must not be empty or contain NUL")
	}
	process, err := a.run(ctx, processRequest{
		executable: a.executable,
		args:       []string{"queue", "--thread", thread, "--message", message},
		dir:        home,
		env:        withCodexHome(withoutTicketRouting(a.environ()), home),
		stdout:     io.Discard,
	})
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if process.exitCode < 0 {
		if err == nil {
			err = errors.New("Codex queue process failed before exit")
		}
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return fmt.Errorf("run Codex queue: %w", err)
		}
		return &ProcessError{ExitCode: process.exitCode, Stderr: process.stderr, cause: err}
	}
	if process.exitCode != 0 {
		return &ProcessError{ExitCode: process.exitCode, Stderr: process.stderr, cause: err}
	}
	if err != nil {
		return fmt.Errorf("run Codex queue: %w", err)
	}
	return nil
}

func (a *Adapter) execute(ctx context.Context, sessionID string, request harness.RunRequest) (harness.RunResult, error) {
	if err := validateContext(ctx); err != nil {
		return harness.RunResult{}, err
	}
	if err := validateRequest(request); err != nil {
		return harness.RunResult{}, err
	}
	output, err := OpenExecutionOutput(
		request.StateDir,
		request.TicketID,
		request.Role,
		OutputMode(request.OutputMode),
		request.Operator,
		"codex", request.WorkerName,
	)
	if err != nil {
		return harness.RunResult{}, err
	}
	args := []string{"exec", "--json"}
	if request.Model != "" {
		args = append(args, "-m", request.Model)
	}
	if request.Reasoning != "" {
		args = append(args, "-c", fmt.Sprintf("model_reasoning_effort=%q", request.Reasoning))
	}
	if request.CodexSandbox != "" {
		args = append(args, "-s", request.CodexSandbox)
	}
	if sessionID != "" {
		args = append(args, "resume", sessionID)
	}
	args = append(args, request.Prompt)

	parser := newStreamParser(output, sessionID)
	process, processErr := a.run(ctx, processRequest{
		executable: a.executable,
		args:       args,
		dir:        request.WorkingDir,
		env:        withActor(a.environ(), request.Actor),
		stdout:     parser,
	})
	finishErr := parser.Finish()
	closeErr := output.Close()
	streamErr := errors.Join(finishErr, closeErr)
	// exec --json currently provides per-turn token usage but no authoritative
	// context-window size or remaining headroom. Keep ContextTelemetry unknown;
	// do not inspect Codex's private rollout files or estimate context use.
	result := harness.RunResult{
		SessionID: parser.session,
		SessionOutcome: func() harness.SessionOutcome {
			if parser.sessionInvalidated {
				return harness.SessionInvalidated
			}
			return harness.SessionReusable
		}(),
		ExitCode: process.exitCode,
		LogPath:  output.Path(),
	}
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
				return result, fmt.Errorf("run Codex: %w", processErr)
			}
		}
		return result, &ProcessError{
			ExitCode: process.exitCode,
			Stderr:   process.stderr,
			cause:    processErr,
		}
	}
	if sessionID == "" && request.RequireSession && result.SessionID == "" {
		return result, fmt.Errorf("%w: successful run did not report a thread ID", ErrProtocol)
	}
	return result, nil
}

// Cleanup applies the configured Codex terminal-session policy.
func (a *Adapter) Cleanup(ctx context.Context, sessionID string, policy harness.CleanupPolicy) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateSessionID(sessionID); err != nil {
		return err
	}
	var args []string
	switch policy {
	case harness.CleanupKeep:
		return nil
	case harness.CleanupDelete:
		args = []string{"delete", sessionID, "--force"}
	case harness.CleanupArchive:
		args = []string{"archive", sessionID}
	default:
		return fmt.Errorf("unknown Codex cleanup policy %q", policy)
	}
	process, err := a.run(ctx, processRequest{
		executable: a.executable,
		args:       args,
		env:        a.environ(),
		stdout:     io.Discard,
	})
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if err != nil || process.exitCode != 0 {
		if process.exitCode < 0 && err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				return fmt.Errorf("run Codex cleanup: %w", err)
			}
		}
		return &ProcessError{ExitCode: process.exitCode, Stderr: process.stderr, cause: err}
	}
	return nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("Codex context must not be nil")
	}
	return ctx.Err()
}

func validateRequest(request harness.RunRequest) error {
	for name, value := range map[string]string{
		"working directory": request.WorkingDir,
		"state directory":   request.StateDir,
		"role":              request.Role,
		"actor":             request.Actor,
		"ticket ID":         request.TicketID,
		"prompt":            request.Prompt,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Codex %s must not be empty", name)
		}
		if strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("Codex %s must not contain NUL", name)
		}
	}
	if strings.TrimSpace(request.OutputMode) == "" {
		return fmt.Errorf("Codex output mode must not be empty")
	}
	if strings.IndexFunc(request.Actor, unicode.IsSpace) >= 0 {
		return fmt.Errorf("Codex actor must not contain whitespace")
	}
	if strings.ContainsRune(request.Model, '\x00') || strings.ContainsRune(request.Reasoning, '\x00') {
		return fmt.Errorf("Codex model and reasoning must not contain NUL")
	}
	if request.CodexSandbox != "" && !oneOf(request.CodexSandbox, "read-only", "workspace-write", "danger-full-access") {
		return fmt.Errorf("Codex sandbox mode %q is not supported", request.CodexSandbox)
	}
	return nil
}

func validateQueueTarget(target string) error {
	if strings.TrimSpace(target) == "" {
		return fmt.Errorf("Codex queue target must not be empty")
	}
	if strings.IndexFunc(target, unicode.IsControl) >= 0 {
		return fmt.Errorf("Codex queue target must not contain control characters")
	}
	return nil
}

const maxDiagnosticBytes = 64 << 10

func validateSessionID(sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("Codex session ID must not be empty")
	}
	if strings.IndexFunc(sessionID, unicode.IsSpace) >= 0 || strings.ContainsRune(sessionID, '\x00') {
		return fmt.Errorf("Codex session ID must not contain whitespace or NUL")
	}
	return nil
}

func runProcess(ctx context.Context, request processRequest) (processResult, error) {
	result, err := harness.RunProcess(ctx, harness.ProcessRequest{
		Executable:  request.executable,
		Args:        request.args,
		Dir:         request.dir,
		Env:         request.env,
		Stdout:      request.stdout,
		StderrLimit: maxDiagnosticBytes,
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

func withoutTicketRouting(base []string) []string {
	env := make([]string, 0, len(base))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok && isTicketRoutingKey(key) {
			continue
		}
		env = append(env, entry)
	}
	return env
}

func withCodexHome(base []string, home string) []string {
	env := make([]string, 0, len(base)+1)
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok && sameEnvKey(key, "CODEX_HOME") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "CODEX_HOME="+home)
}

func isTicketRoutingKey(key string) bool {
	for _, blocked := range []string{"TICKET_ACTOR", "TICKET_REPOSITORY", "TICKET_ROOT", "TICKET_SCOPE", "TICKET_CONFIG", "TICKET_CURRENT"} {
		if sameEnvKey(key, blocked) {
			return true
		}
	}
	return false
}

func sameEnvKey(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}
