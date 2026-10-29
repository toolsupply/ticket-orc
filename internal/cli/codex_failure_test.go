package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// This executable substitutes only for Codex. It never invokes a model or Ticket.
func TestManagedCodexFailureHelper(t *testing.T) {
	mode := os.Getenv("GO_WANT_CODEX_EVIDENCE")
	if mode == "" {
		return
	}
	if mode == "outer" {
		os.Exit(1)
	}
	if mode == "partial" {
		if err := os.WriteFile("partial-work.txt", []byte("preserve this partial work"), 0600); err != nil {
			os.Exit(98)
		}
		file, err := os.OpenFile("harness-calls.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(98)
		}
		fmt.Fprintln(file, "one invocation")
		file.Close()
	}
	fmt.Fprintln(os.Stdout, `{"type":"thread.started","thread_id":"private-session-identity"}`)
	if mode == "complete" {
		fmt.Fprintln(os.Stdout, `{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}`)
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, "authorization=private-stderr-token")
	if mode == "signal" {
		process, _ := os.FindProcess(os.Getpid())
		_ = process.Kill()
		os.Exit(99)
	}
	os.Exit(17)
}

type failureEvidenceTickets struct {
	ticket ticketclient.Ticket
	claims int
}

func (f *failureEvidenceTickets) WaitAndClaimImplementation(context.Context, ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	f.claims++
	if f.claims != 1 {
		return ticketclient.Ticket{}, errors.New("unexpected additional claim")
	}
	return f.ticket, nil
}
func (f *failureEvidenceTickets) Show(context.Context, string) (ticketclient.Ticket, error) {
	return f.ticket, nil
}
func (f *failureEvidenceTickets) Release(context.Context, string) (ticketclient.Transition, error) {
	return ticketclient.Transition{}, errors.New("unexpected release")
}

func TestManagedCodexFailureEvidenceThroughPublicStatus(t *testing.T) {
	for _, mode := range []string{"exit", "signal"} {
		t.Run(mode, func(t *testing.T) {
			const ticketID = "20261024-96942"
			stateDir := t.TempDir()
			store := state.NewForRepository(stateDir, joinTestRepositoryID)
			tickets := &failureEvidenceTickets{ticket: ticketclient.Ticket{ID: ticketID, State: "open", Assignee: "coder-actor"}}
			calls := 0
			adapter, err := codex.NewManaged(os.Args[0], append(os.Environ(), "GO_WANT_CODEX_EVIDENCE="+mode), func(ctx context.Context, req harness.ProcessRequest) (harness.ProcessResult, error) {
				calls++
				req.Args = []string{"-test.run=^TestManagedCodexFailureHelper$"}
				return harness.RunProcess(ctx, req)
			})
			if err != nil {
				t.Fatal(err)
			}
			err = orc.RunCoder(context.Background(), orc.CoderConfig{
				WorkerName: "stable-coder", RoleName: "backend", Actor: "coder-actor", Harness: "codex", MaxBounces: 6,
				SessionPolicy: "ticket", SessionCleanup: "keep", StateDir: stateDir, OutputMode: "quiet", WorkingDir: t.TempDir(), Operator: io.Discard,
			}, tickets, adapter, store, nil)
			var executionErr *harness.ExecutionError
			if !errors.As(err, &executionErr) || calls != 1 || tickets.claims != 1 {
				t.Fatalf("lost nested failure or automatically retried: %v calls=%d claims=%d", err, calls, tickets.claims)
			}
			result := executionErr.Result
			if result.SessionID != "private-session-identity" || result.LogPath == "" {
				t.Fatalf("lost private reconciliation references: %+v", result)
			}
			if _, statErr := os.Stat(result.LogPath); statErr != nil {
				t.Fatal(statErr)
			}
			loop, found, readErr := store.TicketLoop(context.Background(), ticketID)
			if readErr != nil || !found || loop.AttemptID == 0 {
				t.Fatalf("missing recorded attempt: %+v %v", loop, readErr)
			}
			// Encode the actual child failure event and consume it through the
			// same stream boundary used by the supervisor.
			var encoded bytes.Buffer
			emitRoleFailure(func(event orc.Event) {
				data, _ := json.Marshal(event)
				fmt.Fprintln(&encoded, orc.EventStreamPrefix+string(data))
			}, "stable-coder", "backend", orc.NewFailureContextError(orc.FailureContext{Origin: "backend", Operation: "worker execution", Phase: "worker execution"}, err))
			child := &runChild{}
			writer := workerEventWriter{dst: io.Discard, sink: func(event orc.Event) { child.captureFailure(event) }}
			if _, writeErr := writer.Write(encoded.Bytes()); writeErr != nil {
				t.Fatal(writeErr)
			}
			outer := exec.Command(os.Args[0], "-test.run=^TestManagedCodexFailureHelper$")
			outer.Env = append(os.Environ(), "GO_WANT_CODEX_EVIDENCE=outer")
			outerErr := outer.Run()
			failure := classifyManagedChildExit(child, outerErr).Failure
			if failure == nil || failure.Classification != "harness_process_failed" || failure.ExitCode != 1 || failure.Execution == nil {
				t.Fatalf("lost failure envelope or outer exit: %+v", failure)
			}
			if failure.RepositoryID != joinTestRepositoryID || failure.Worker != "stable-coder" || failure.Role != "backend" || failure.Actor != "coder-actor" || failure.Ticket != ticketID || failure.AttemptID != loop.AttemptID || failure.Origin != "codex" || failure.Operation != "managed turn" || failure.Phase != "running" {
				t.Fatalf("lost exact execution identity: %+v", failure)
			}
			e := failure.Execution
			if e.Process.Start != "started" || !e.Process.ExitObserved || e.LogID != filepath.Base(result.LogPath) {
				t.Fatalf("incorrect execution facts: %+v", e)
			}
			wantCode := 17
			if mode == "signal" {
				wantCode = -1
				if runtime.GOOS == "windows" {
					wantCode = e.Process.ExitCode // Windows supplies no Unix signal.
				} else if e.Process.Signal != 9 {
					t.Fatalf("lost OS signal: %+v", e.Process)
				}
			}
			if failure.NestedExitCode != wantCode || e.Process.ExitCode != wantCode || failure.Signal != "" {
				t.Fatalf("conflated nested and outer process: %+v, %+v", failure, e.Process)
			}
			worker := supervisor.RunWorker{Name: "stable-coder", Config: supervisor.RoleConfig{Role: RoleCoder, RoleName: "backend", RepositoryIdentity: joinTestRepositoryID}}
			runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
			if err := supervisor.SaveLatestFailure(t.Context(), stateDir, *failure); err != nil {
				t.Fatal(err)
			}
			runtimeState.SetDiagnosticDirectory(stateDir)
			runtimeState.SetFailure(worker.Name, failure)
			status := runtimeDaemonStatus(runtimeState, nil)
			if latest := status.Workers[0].LatestFailure; latest == nil || latest.Failure.OuterProcess == nil || latest.Failure.OuterProcess.ExitCode != 1 || latest.Failure.Execution == nil || latest.Failure.Execution.Process.ExitCode != wantCode {
				t.Fatalf("retention lost independent harness/outer evidence: %+v", latest)
			}
			publicEvent := daemon.RuntimeEventDTO(supervisor.RuntimeEvent{Type: "worker.failure", Worker: worker.Name, Failure: failure})
			public, _ := json.Marshal(struct {
				Status daemon.Status
				Event  daemon.Event
			}{status, publicEvent})
			for _, secret := range []string{"private-session-identity", "private-stderr-token", "authorization=", stateDir, "OOM", "segfault"} {
				if bytes.Contains(public, []byte(secret)) || bytes.Contains(encoded.Bytes(), []byte(secret)) {
					t.Fatalf("private data or invented diagnosis entered public output: %s", public)
				}
			}
			if len(status.Workers) != 1 || status.Workers[0].Failure.Execution.Process.ExitCode != wantCode || publicEvent.Failure.Execution.Process.Signal != e.Process.Signal {
				t.Fatalf("public boundary lost execution: %s", public)
			}
			if detail := consoleWorkerFailureDetail(status.Workers[0].Failure); !strings.Contains(detail, "harness_start=started") {
				t.Fatalf("operator diagnostics missing evidence: %s", detail)
			}
		})
	}
}

func TestGenericOuterExitDoesNotManufactureNestedEvidence(t *testing.T) {
	failure := classifyWorkerFailure(childExitCauseForTest(t))
	if failure.Execution != nil || failure.NestedExitCode != 0 || failure.Classification != "child_exit" {
		t.Fatalf("generic worker exit became nested crash evidence: %+v", failure)
	}
}
