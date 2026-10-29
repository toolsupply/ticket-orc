package cli

import (
	"errors"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestManagedHoldHarnessFailurePreservesEvidence(t *testing.T) {
	execution := &harness.ExecutionError{Harness: "codex", Cause: errors.New("execution failed"), Result: harness.RunResult{Evidence: &harness.ExecutionEvidence{
		Phase: "running", Process: harness.ProcessOutcome{Start: "started", ExitObserved: true, ExitCode: 7, Cancellation: "none"},
	}}}
	failure := roleFailureEnvelope(execution)
	if failure.Classification != "harness_process_failed" || failure.Execution == nil || failure.Execution.Process.ExitCode != 7 {
		t.Fatalf("harness failure overwritten: %#v", failure)
	}
}
