package pi

import (
	"errors"
	"fmt"
	"strings"
)

var ErrProtocol = errors.New("Pi stream protocol error")

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
		detail = "Pi process failed"
	}
	return fmt.Sprintf("Pi exited with status %d: %s", e.ExitCode, detail)
}

func (e *ProcessError) Unwrap() error { return e.cause }
