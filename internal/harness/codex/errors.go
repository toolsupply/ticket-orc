package codex

import (
	"errors"
	"fmt"
	"strings"
)

// ErrProtocol identifies malformed or inconsistent Codex JSONL events.
var ErrProtocol = errors.New("Codex stream protocol error")

// ErrTurnFailed records an explicit provider failure event, without using its
// free-form message to infer authentication, configuration, or crash causes.
var ErrTurnFailed = errors.New("Codex reported a failed turn")

// ProcessError reports a Codex CLI process that exited unsuccessfully.
type ProcessError struct {
	ExitCode int
	Stderr   string
	cause    error
}

func (e *ProcessError) Error() string {
	detail := strings.TrimSpace(e.Stderr)
	if detail == "" && e.cause != nil {
		detail = e.cause.Error()
	}
	if detail == "" {
		detail = "Codex process failed"
	}
	return fmt.Sprintf("Codex exited with status %d: %s", e.ExitCode, detail)
}

func (e *ProcessError) Unwrap() error { return e.cause }
