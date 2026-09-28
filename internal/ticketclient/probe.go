package ticketclient

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"unicode"
)

const maxProbeOutputBytes = 64 << 10

// ProbeError reports a bounded, safe failure from a one-shot Ticket info
// probe. It intentionally does not retain command arguments or raw output.
type ProbeError struct {
	Code    string
	Message string
}

func (e *ProbeError) Error() string {
	if e == nil || e.Code == "" {
		return "ticket info probe failed"
	}
	if e.Message == "" {
		return "ticket info probe failed: " + e.Code
	}
	return "ticket info probe failed: " + e.Code + ": " + e.Message
}

// ProbeInfo runs the same target-aware Ticket command used by persistent
// clients and returns repository metadata without mutating Ticket state.
func ProbeInfo(ctx context.Context, actor, workingDir string, target Target) (RepositoryInfo, error) {
	return ProbeInfoWithExecutable(ctx, actor, defaultExecutable, workingDir, target)
}

// ProbeInfoWithExecutable is the injectable form used by integrations and
// tests that need a specific Ticket executable.
func ProbeInfoWithExecutable(ctx context.Context, actor, executable, workingDir string, target Target) (RepositoryInfo, error) {
	if ctx == nil {
		return RepositoryInfo{}, &ProbeError{Code: "invalid_context"}
	}
	if strings.TrimSpace(actor) == "" || strings.IndexFunc(actor, unicode.IsSpace) >= 0 || !validActor(actor) {
		return RepositoryInfo{}, &ProbeError{Code: "invalid_actor"}
	}
	if strings.TrimSpace(executable) == "" {
		return RepositoryInfo{}, &ProbeError{Code: "invalid_executable"}
	}
	if err := target.validate(); err != nil {
		return RepositoryInfo{}, &ProbeError{Code: "invalid_target"}
	}
	args := append([]string{}, target.args()...)
	args = append(args, "info", "-j")
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = workingDir
	command.Env = ApplyTargetEnvironment(os.Environ(), actor, target)
	stdout := newBoundedBuffer(maxProbeOutputBytes)
	stderr := newBoundedBuffer(maxStderrBytes)
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return RepositoryInfo{}, ctxErr
	}
	if stdout.Truncated() {
		return RepositoryInfo{}, &ProbeError{Code: "output_too_large"}
	}
	if err != nil {
		if probeErr := probeErrorDetails(stdout.Bytes()); probeErr != nil {
			return RepositoryInfo{}, probeErr
		}
		return RepositoryInfo{}, &ProbeError{Code: "command_failed"}
	}
	var info RepositoryInfo
	if err := decodeOne(stdout.Bytes(), &info); err != nil {
		return RepositoryInfo{}, &ProbeError{Code: "malformed_output"}
	}
	if strings.TrimSpace(info.Path) == "" {
		return RepositoryInfo{}, &ProbeError{Code: "missing_path"}
	}
	if !ValidRepositoryID(info.ID) {
		return RepositoryInfo{}, &ProbeError{Code: "missing_repository_id"}
	}
	if target.Scope != "" {
		if info.Scope == nil || *info.Scope != target.Scope {
			return RepositoryInfo{}, &ProbeError{Code: "scope_mismatch"}
		}
	}
	return info, nil
}

func probeErrorDetails(data []byte) *ProbeError {
	var envelope struct {
		Error *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if decodeOne(data, &envelope) != nil || envelope.Error == nil {
		return nil
	}
	code := strings.TrimSpace(envelope.Error.Code)
	if code == "" || len(code) > 128 {
		return &ProbeError{Code: "command_failed"}
	}
	for _, r := range code {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' && r != '.' {
			return &ProbeError{Code: "command_failed"}
		}
	}
	message := strings.TrimSpace(envelope.Error.Message)
	if len(message) > 256 {
		message = message[:256]
	}
	for _, r := range message {
		if unicode.IsControl(r) {
			message = ""
			break
		}
	}
	return &ProbeError{Code: code, Message: message}
}
