package supervisor

import (
	"path/filepath"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestFailedCoderClaimRequiresUnchangedExclusiveExecution(t *testing.T) {
	root := t.TempDir()
	worker := RunWorker{Name: "coder", Config: RoleConfig{Role: RoleCoder, Harness: "codex", Actor: "coder", WorkingDir: filepath.Join(root, "repo"), RepositoryIdentity: "repository"}}
	for _, name := range []string{"confirmed", "unknown cleanup", "changed target", "changed actor", "overlap", "same identity", "independent"} {
		t.Run(name, func(t *testing.T) {
			desired := worker
			var active []RunWorker
			confirmed := name != "unknown cleanup"
			switch name {
			case "changed target":
				desired.Config.RepositoryIdentity = "other"
			case "changed actor":
				desired.Config.Actor = "other"
			case "overlap":
				other := worker
				other.Config.Actor = "other"
				other.Config.WorkingDir = filepath.Join(root, "repo", "subdir")
				active = append(active, other)
			case "same identity":
				other := worker
				other.Config.WorkingDir = filepath.Join(root, "independent")
				active = append(active, other)
			case "independent":
				other := worker
				other.Config.Actor = "other"
				other.Config.WorkingDir = filepath.Join(root, "independent")
				active = append(active, other)
			}
			outcome := ExitOutcome{State: WorkerFailed, Failure: &WorkerFailure{AttemptID: 1, Execution: &harness.ExecutionEvidence{Process: harness.ProcessOutcome{Start: "started"}}}}
			got := PrepareFailedCoderClaim(worker, desired, true, active, confirmed, &outcome)
			if want := name == "confirmed" || name == "independent"; got != want {
				t.Fatalf("eligible=%v want=%v reason=%s", got, want, outcome.Failure.Remediation)
			}
			if outcome.BlockWorkspace != (name == "unknown cleanup") {
				t.Fatalf("workspace block=%v", outcome.BlockWorkspace)
			}
		})
	}
}

func TestFailedChildMissingEnvelopeStillProtectsUnconfirmedWorkspace(t *testing.T) {
	worker := RunWorker{Name: "coder", Config: RoleConfig{Role: RoleCoder, Harness: "codex", WorkingDir: t.TempDir()}}
	for _, test := range []struct {
		name      string
		state     WorkerState
		failure   *WorkerFailure
		wantBlock bool
	}{
		{name: "signal without envelope", failure: &WorkerFailure{Classification: "signal_exit", Signal: "terminated"}, wantBlock: true},
		{name: "exit without envelope", failure: &WorkerFailure{Classification: "child_exit", ExitCode: 17}, wantBlock: true},
		{name: "silent exit without envelope", state: WorkerStopped, failure: &WorkerFailure{Classification: "silent_exit"}, wantBlock: true},
		{name: "missing failure", wantBlock: true},
		{name: "partial identity", failure: &WorkerFailure{Ticket: "20261024-92236"}, wantBlock: true},
		{name: "partial execution", failure: &WorkerFailure{Execution: &harness.ExecutionEvidence{}}, wantBlock: true},
		{name: "unknown classification", failure: &WorkerFailure{Execution: &harness.ExecutionEvidence{Process: harness.ProcessOutcome{Start: "not_started"}}}, wantBlock: true},
		{name: "known pre-execution", failure: &WorkerFailure{Classification: "harness_start_failed", Execution: &harness.ExecutionEvidence{Process: harness.ProcessOutcome{Start: "not_started"}}}},
		{name: "lease refusal", failure: &WorkerFailure{Classification: "lease_conflict", Phase: "lease acquisition", Contained: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			workerState := test.state
			if workerState == "" {
				workerState = WorkerFailed
			}
			outcome := ExitOutcome{State: workerState, Failure: test.failure}
			if PrepareFailedCoderClaim(worker, worker, true, nil, false, &outcome) {
				t.Fatal("unconfirmed cleanup authorized release")
			}
			if outcome.BlockWorkspace != test.wantBlock {
				t.Fatalf("block=%v want=%v failure=%+v", outcome.BlockWorkspace, test.wantBlock, outcome.Failure)
			}
			if test.wantBlock && (outcome.Failure == nil || outcome.Failure.Remediation == "") {
				t.Fatalf("missing manual-action diagnostic: %+v", outcome.Failure)
			}
			if outcome.Failure != nil && (outcome.Failure.AttemptID != 0 || outcome.Failure.Actor != "") {
				t.Fatalf("fabricated claim identity: %+v", outcome.Failure)
			}
		})
	}
}
