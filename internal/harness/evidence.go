package harness

import (
	"context"
	"os"
	"strings"
)

// ObservedProcess reports only facts from an OS wait result. Protocol events
// such as turn.completed cannot establish any of these exit facts.
func ObservedProcess(process *os.ProcessState) ProcessOutcome {
	outcome := ProcessOutcome{Start: "unknown", Cancellation: "unknown"}
	if process != nil {
		outcome.Start = "started"
		outcome.ExitObserved = true
		outcome.ExitCode = process.ExitCode()
		outcome.Signal = processSignal(process)
	}
	return outcome
}

// ProcessOutcome contains nested process exit facts, not worker-boundary
// cleanup evidence. Empty values mean unknown.
// Signal is the OS signal number, when available; it is never an OOM diagnosis.
type ProcessOutcome struct {
	Start        string `json:"start"`
	ExitObserved bool   `json:"exit_observed"`
	ExitCode     int    `json:"exit_code"`
	Signal       int    `json:"signal,omitempty"`
	Cancellation string `json:"cancellation"`
}

// ExecutionEvidence is the bounded public projection of a nested execution.
// Private session identity and full log paths belong to RunResult.
type ExecutionEvidence struct {
	// LogID names the private raw log within Orc's log store; it contains no
	// directory or session ID. The full private path stays in RunResult.
	LogID          string         `json:"log_id,omitempty"`
	Process        ProcessOutcome `json:"process"`
	Phase          string         `json:"phase"`
	ProtocolFailed bool           `json:"protocol_failed"`
	OutputFailed   bool           `json:"output_failed"`
	ProviderFailed bool           `json:"provider_failed"`
}

// Bounded copies evidence across the worker boundary using finite vocabularies.
// Unknown adapter facts remain unknown.
func (e ExecutionEvidence) Bounded() ExecutionEvidence {
	if len(e.LogID) > 512 || !strings.HasSuffix(e.LogID, ".jsonl") || strings.ContainsAny(e.LogID, "/\\") {
		e.LogID = ""
	}
	for _, r := range e.LogID {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-' && r != '.' {
			e.LogID = ""
			break
		}
	}
	e.Process.Start = evidenceValue(e.Process.Start, "not_started", "started")
	e.Process.Cancellation = evidenceValue(e.Process.Cancellation, "none", "canceled", "deadline")
	e.Phase = evidenceValue(e.Phase, "startup", "running", "protocol", "output")
	if e.Process.Signal < 0 || e.Process.Signal > 255 {
		e.Process.Signal = 0
	}
	return e
}

func evidenceValue(value string, allowed ...string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return "unknown"
}

// ProcessCancellation reports cancellation without interpreting private cause text.
func ProcessCancellation(ctx context.Context) string {
	if ctx.Err() == nil {
		return "none"
	}
	if ctx.Err() == context.DeadlineExceeded {
		return "deadline"
	}
	return "canceled"
}

// ExecutionError retains the private result even when callers also fail during
// protocol, logging, session registration or Ticket reconciliation. Public
// envelopes must project Evidence only, never serialize Result or Cause.
type ExecutionError struct {
	Harness string
	Result  RunResult
	Cause   error
}

func (e *ExecutionError) Error() string { return e.Cause.Error() }
func (e *ExecutionError) Unwrap() error { return e.Cause }
