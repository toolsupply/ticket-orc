package harness

import (
	"context"
	"fmt"
	"io"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
)

// CleanupPolicy controls disposal of a retained harness session.
type CleanupPolicy string

const (
	CleanupDelete  CleanupPolicy = "delete"
	CleanupArchive CleanupPolicy = "archive"
	CleanupKeep    CleanupPolicy = "keep"
)

// RunRequest contains the process settings shared by concrete harnesses.
type RunRequest struct {
	WorkerName           string
	WorkingDir           string
	Role                 string
	Actor                string
	TicketID             string
	Prompt               string
	Model                string
	Reasoning            string
	RequireSession       bool
	StateDir             string
	OutputMode           string
	CodexSandbox         string
	PiProvider           string
	ClaudePermissionMode string
	Operator             io.Writer
}

// OutputMode selects how a provider stream is rendered for the operator.
// Every mode still writes the original bytes to the private raw log.
type OutputMode string

const (
	OutputCompact OutputMode = "compact"
	OutputQuiet   OutputMode = "quiet"
	OutputJSON    OutputMode = "json"
)

// RunResult describes the harness process and parsed session identity.
type RunResult struct {
	SessionID           string
	SessionOutcome      SessionOutcome
	ContextTelemetry    contextheadroom.Telemetry
	ExitCode            int
	StreamEndedNormally bool
	LogPath             string
}

// SessionOutcome reports whether a turn left the session safe to resume. The
// zero value retains the historical reusable behavior.
type SessionOutcome uint8

const (
	SessionReusable SessionOutcome = iota
	SessionInvalidated
)

// Capabilities describes static features an adapter can provide without
// starting a ticket turn. Unsupported requested combinations must be rejected
// during startup rather than after a queue claim.
type Capabilities struct {
	Resume         bool
	Fresh          bool
	CleanupDelete  bool
	CleanupArchive bool
}

// PreflightConfig contains the generic settings an adapter must validate
// before Orc enters its claim loop.
type PreflightConfig struct {
	Model                string
	Reasoning            string
	SessionPolicy        string
	SessionCleanup       CleanupPolicy
	OutputMode           string
	CodexSandbox         string
	PiProvider           string
	ClaudePermissionMode string
}

// PreflightAdapter is implemented by concrete adapters that can report their
// static capabilities and validate a resolved invocation.
type PreflightAdapter interface {
	Capabilities() Capabilities
	Validate(PreflightConfig) error
}

// PreflightError identifies a failure while validating a harness invocation
// before the orchestrator begins claiming work.
type PreflightError struct {
	Cause error
}

func (e *PreflightError) Error() string {
	if e == nil || e.Cause == nil {
		return "harness preflight failed"
	}
	return "harness preflight: " + e.Cause.Error()
}

func (e *PreflightError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Preflight validates one selected adapter and its generic session/cleanup
// intent before any ticket operation can claim work.
func Preflight(adapter Harness, config PreflightConfig) error {
	validator, ok := adapter.(PreflightAdapter)
	if !ok {
		return &PreflightError{Cause: fmt.Errorf("harness adapter does not expose preflight capabilities")}
	}
	if err := validator.Validate(config); err != nil {
		return &PreflightError{Cause: err}
	}
	caps := validator.Capabilities()
	switch config.SessionPolicy {
	case "ticket":
		if !caps.Resume {
			return &PreflightError{Cause: fmt.Errorf("harness does not support retained ticket sessions")}
		}
	case "fresh":
		if !caps.Fresh {
			return &PreflightError{Cause: fmt.Errorf("harness does not support fresh sessions")}
		}
	default:
		return &PreflightError{Cause: fmt.Errorf("unknown session policy %q", config.SessionPolicy)}
	}
	switch config.SessionCleanup {
	case CleanupKeep:
	case CleanupDelete:
		if !caps.CleanupDelete {
			return &PreflightError{Cause: fmt.Errorf("harness does not support session cleanup policy %q", config.SessionCleanup)}
		}
	case CleanupArchive:
		if !caps.CleanupArchive {
			return &PreflightError{Cause: fmt.Errorf("harness does not support session cleanup policy %q", config.SessionCleanup)}
		}
	default:
		return &PreflightError{Cause: fmt.Errorf("unknown session cleanup policy %q", config.SessionCleanup)}
	}
	return nil
}

// PreflightIfSupported preserves the small Harness test seam for embedded
// adapters while ensuring concrete adapters with capabilities are checked.
func PreflightIfSupported(adapter Harness, config PreflightConfig) error {
	if _, ok := adapter.(PreflightAdapter); !ok {
		return nil
	}
	return Preflight(adapter, config)
}

// Harness is the narrow agent-process boundary used by orchestration policy.
type Harness interface {
	Run(context.Context, RunRequest) (RunResult, error)
	Resume(context.Context, string, RunRequest) (RunResult, error)
	Cleanup(context.Context, string, CleanupPolicy) error
}
