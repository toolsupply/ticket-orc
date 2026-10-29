package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Only the model is substituted. This worker uses the public Ticket adapter,
// durable state and coder loop inside startRunChild's real outer boundary.
func runFailedClaimTestWorker() int {
	config, _, err := parseRoleConfig(RoleCoder, os.Args[2:], os.LookupEnv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 98
	}
	tickets, err := ticketclient.NewWithWorkingDirAndTarget(config.Actor, config.WorkingDir, ticketTarget(config))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 98
	}
	defer tickets.Close()
	env := setChildEnvironment(os.Environ(), "ORC_FAILED_CLAIM_WORKER", "0")
	env = setChildEnvironment(env, "GO_WANT_CODEX_EVIDENCE", "partial")
	agent, err := codex.NewManaged(os.Args[0], env, func(ctx context.Context, req harness.ProcessRequest) (harness.ProcessResult, error) {
		req.Args = []string{"-test.run=^TestManagedCodexFailureHelper$"}
		return harness.RunProcess(ctx, req)
	})
	if err != nil {
		return 98
	}
	store := state.NewForRepository(config.StateDir, config.RepositoryIdentity)
	err = orc.RunCoder(context.Background(), orc.CoderConfig{WorkerName: config.WorkerName, RoleName: config.RoleName, Actor: config.Actor,
		Harness: "codex", MaxBounces: 6, SessionPolicy: "ticket", SessionCleanup: "keep", StateDir: config.StateDir, OutputMode: "quiet", WorkingDir: config.WorkingDir,
		Operator: io.Discard, Diagnostics: io.Discard, EventSink: roleEventSink()}, tickets, agent, store, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	if mode := os.Getenv("ORC_FAILED_CLAIM_EXIT"); mode == "abrupt" || mode == "silent" {
		// Close the owned Ticket adapter before the deliberate abrupt death;
		// the real outer worker exits without running a final failure emitter.
		tickets.Close()
		if mode == "silent" {
			return 0
		}
		process, _ := os.FindProcess(os.Getpid())
		if err := process.Kill(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		return 99
	}
	emitRoleFailure(roleEventSink(), config.WorkerName, config.RoleName, err)
	return 17
}

func TestManagedFailureActualOuterExitPreservesClaimAndBlocksOverlap(t *testing.T) {
	for _, mode := range []string{"reported", "abrupt", "silent"} {
		t.Run(mode, func(t *testing.T) { testManagedFailureActualOuterExit(t, mode) })
	}
}

func testManagedFailureActualOuterExit(t *testing.T, mode string) {
	root := t.TempDir()
	working := filepath.Join(root, "working")
	if err := os.MkdirAll(working, 0700); err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(root, "tickets")
	target := ticketclient.Target{Repository: repository}
	// Routing changes belong only to this isolated fixture's subprocesses.
	init := exec.Command("ticket", "init", "-j")
	init.Dir, init.Env = working, ticketclient.ApplyTargetEnvironment(os.Environ(), "coder", target)
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("fixture init: %v %s", err, output)
	}
	info, err := ticketclient.ProbeInfo(t.Context(), "coder", working, target)
	if err != nil {
		t.Fatal(err)
	}
	tickets, err := ticketclient.NewWithWorkingDirAndTarget("coder", working, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tickets.Close() })
	created, err := tickets.CreateTicket(t.Context(), ticketclient.CreateInput{Title: "Controlled managed failure", Sections: map[string]string{"objective": "Preserve partial work", "handoff": "Original substantive handoff"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("ORC_FAILED_CLAIM_WORKER", "1")
	t.Setenv("ORC_FAILED_CLAIM_EXIT", mode)
	worker := supervisorTestWorker("stable-coder", filepath.Join(root, "state"))
	worker.Config.WorkingDir, worker.Config.Repository = working, repository
	worker.Config.Actor, worker.Config.RepositoryIdentity = "coder", info.ID
	worker.Config.RoleName, worker.Config.TicketQueue = "backend", "open"
	worker.Config.SessionPolicy = supervisor.SessionPolicyTicket
	worker.TicketInfo = &info
	worker.EventSink = func(orc.Event) {}
	workers := []supervisor.RunWorker{worker}
	runtime := NewRuntimeState(workers)
	var diagnostics bytes.Buffer
	manager := newWorkerManager(t.Context(), RunConfig{ConfigPath: supervisorTestConfigPath(t), StateDir: worker.Config.StateDir, Workers: workers, Runtime: runtime}, os.Args[0], io.Discard, &diagnostics,
		func(name string, s supervisor.WorkerState, err error) {
			runtime.Set(supervisor.WorkerTransition{Worker: name, State: s})
		})
	manager.eventSink = worker.EventSink
	if _, err := manager.start(t.Context(), worker.Name); err != nil {
		t.Fatal(err)
	}
	child, ok := manager.lifecycle.Child(worker.Name)
	if !ok {
		t.Fatal("no original child")
	}
	stopRunChildAtTestCleanup(t, child)
	awaitSupervisorSignal(t, child.waited, "actual outer worker exit and output drain")
	observation := manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false)
	wantState := WorkerFailed
	if mode == "silent" {
		wantState = WorkerStopped
	}
	if !observation.Current || observation.Outcome.State != wantState || child.boundaryResult().Confirmed {
		t.Fatalf("unexpected exit: %+v boundary=%+v diagnostics=%s", observation, child.boundaryResult(), diagnostics.String())
	}
	failure := observation.Outcome.Failure
	if failure == nil || failure.Remediation != orc.CleanupUnconfirmed || !observation.Outcome.BlockWorkspace {
		t.Fatalf("lost exact failure/refusal: %+v diagnostics=%s", failure, diagnostics.String())
	}
	if mode == "reported" && failure.Ticket != created.ID {
		t.Fatalf("lost reported claim identity: %+v", failure)
	}
	if mode != "reported" && (child.failureEnvelope() != nil || failure.Ticket != "" || failure.AttemptID != 0 || failure.Execution != nil) {
		t.Fatalf("abrupt exit emitted an envelope or fabricated claim identity: %+v", failure)
	}
	runtime.SetFailure(worker.Name, failure)
	status := runtimeDaemonStatus(runtime, nil)
	if status.Workers[0].State != string(wantState) || status.Workers[0].Failure.Ticket != failure.Ticket || status.Workers[0].Failure.Remediation != orc.CleanupUnconfirmed {
		t.Fatalf("missing operator status: %+v", status.Workers)
	}
	var rendered bytes.Buffer
	renderConsoleDetailStatus(&rendered, status)
	if !strings.Contains(rendered.String(), orc.CleanupUnconfirmed) || (mode == "reported" && !strings.Contains(rendered.String(), created.ID)) {
		t.Fatalf("operator cannot identify the claim and manual action: %s", rendered.String())
	}
	detail, err := tickets.ShowDetail(t.Context(), created.ID)
	if err != nil || detail.State != "open" || detail.Assignee != "coder" || strings.TrimSpace(detail.Sections["handoff"].Text) != "Original substantive handoff" {
		t.Fatalf("speculative Ticket mutation: %+v err=%v", detail, err)
	}
	if worklog, err := tickets.ShowWorkLog(t.Context(), created.ID); err != nil || strings.Contains(worklog.WorkLog, "Released") || strings.Contains(worklog.WorkLog, "unexpectedly") {
		t.Fatalf("speculative failure note: %+v err=%v", worklog, err)
	}
	store := state.NewForRepository(worker.Config.StateDir, info.ID)
	snapshot, err := store.Read(t.Context())
	loop, found, loopErr := store.TicketLoop(t.Context(), created.ID)
	if err != nil || !found || loopErr != nil || len(snapshot.Sessions) != 1 || !snapshot.Sessions[0].IsCurrent() || snapshot.Sessions[0].ID != "private-session-identity" || len(snapshot.Loops) != 1 || loop.ClaimActor != "coder" || loop.StallCount != 0 {
		t.Fatalf("speculative session/attempt mutation: %+v err=%v", snapshot, err)
	}
	before, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.start(t.Context(), worker.Name); err == nil || !strings.Contains(err.Error(), "workspace_cleanup_unconfirmed") {
		t.Fatalf("overlapping launch accepted: %v", err)
	}
	if _, err := manager.lifecycle.Restart(t.Context(), worker.Name); err == nil || !strings.Contains(err.Error(), "workspace_cleanup_unconfirmed") {
		t.Fatalf("overlapping restart accepted: %v", err)
	}
	nested := worker
	nested.Name, nested.Config.WorkerName, nested.Config.Actor = "nested", "nested", "nested-actor"
	nested.Config.WorkingDir = filepath.Join(working, "nested")
	if err := os.Mkdir(nested.Config.WorkingDir, 0700); err != nil {
		t.Fatal(err)
	}
	manager.lifecycle.SetWorker(nested.Name, nested)
	if _, err := manager.start(t.Context(), nested.Name); err == nil || !strings.Contains(err.Error(), "workspace_cleanup_unconfirmed") {
		t.Fatalf("nested launch accepted: %v", err)
	}
	if _, err := manager.lifecycle.Restart(t.Context(), nested.Name); err == nil || !strings.Contains(err.Error(), "workspace_cleanup_unconfirmed") {
		t.Fatalf("nested restart accepted: %v", err)
	}
	if manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false).Current {
		t.Fatal("duplicate exit accepted")
	}
	afterSnapshot, _ := store.Read(t.Context())
	after, _ := json.Marshal(afterSnapshot)
	if !bytes.Equal(before, after) {
		t.Fatal("refused launch changed durable evidence")
	}
	if data, err := os.ReadFile(filepath.Join(working, "partial-work.txt")); err != nil || string(data) != "preserve this partial work" {
		t.Fatalf("partial work lost: %s %v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(working, "harness-calls.txt")); err != nil || string(data) != "one invocation\n" {
		t.Fatalf("unexpected second harness: %s %v", data, err)
	}
	logs, err := os.ReadDir(filepath.Join(worker.Config.StateDir, "logs"))
	if err != nil || len(logs) != 1 {
		t.Fatalf("log evidence lost: %v %v", logs, err)
	}
	// An unrelated workspace still runs through the real outer launcher. Its
	// simple signal-aware helper performs no Ticket or model work.
	t.Setenv("ORC_FAILED_CLAIM_WORKER", "0")
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", t.TempDir())
	independent := worker
	independent.Name, independent.Config.WorkerName, independent.Config.Actor = "independent", "independent", "independent-actor"
	independent.Config.WorkingDir = t.TempDir()
	manager.lifecycle.SetWorker(independent.Name, independent)
	if _, err := manager.start(t.Context(), independent.Name); err != nil {
		t.Fatalf("independent launch blocked: %v", err)
	}
	independentChild, _ := manager.lifecycle.Child(independent.Name)
	stopRunChildAtTestCleanup(t, independentChild)
	awaitSupervisorSignal(t, independentChild.ready, "independent worker readiness")
}

func TestFailedCoderPolicyWithTicketAdapterPreservesHandoff(t *testing.T) {
	// This is a controlled domain-policy case with NO production execution or
	// cleanup assertion. Real outer-boundary capability remains unconfirmed.
	root := t.TempDir()
	target := ticketclient.Target{Repository: filepath.Join(root, "tickets")}
	init := exec.Command("ticket", "init", "-j")
	init.Dir, init.Env = root, ticketclient.ApplyTargetEnvironment(os.Environ(), "coder", target)
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("fixture init: %v %s", err, output)
	}
	info, err := ticketclient.ProbeInfo(t.Context(), "coder", root, target)
	if err != nil {
		t.Fatal(err)
	}
	tickets, err := ticketclient.NewWithWorkingDirAndTarget("coder", root, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tickets.Close() })
	created, err := tickets.CreateTicket(t.Context(), ticketclient.CreateInput{Title: "Controlled domain policy", Sections: map[string]string{"objective": "Test public release mutation", "handoff": "Preserve substantive context"}})
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewForRepository(filepath.Join(root, "state"), info.ID)
	if _, _, err := store.Participate(t.Context(), created.ID, 6); err != nil {
		t.Fatal(err)
	}
	holder := containmentTicketClient{actor: orc.ContainmentActor("fixture"), workingDir: root, target: target}
	expected := orc.FailureContext{RepositoryID: info.ID, Worker: "stable", Actor: "coder", Role: "coder"}
	for attempt := 1; attempt <= 2; attempt++ {
		// Ticket selects both attempts through its ordinary public queue. The
		// second claim proves the controlled release restored queue eligibility.
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		claimed, err := tickets.WaitAndClaimImplementation(ctx, ticketclient.QueueFilters{})
		cancel()
		if err != nil || claimed.ID != created.ID || claimed.State != "open" || claimed.Assignee != "coder" {
			t.Fatalf("ordinary claim %d: %+v err=%v", attempt, claimed, err)
		}
		loop, err := store.RecordClaim(t.Context(), created.ID, "coder", "open")
		if err != nil {
			t.Fatal(err)
		}
		if loop.StallCount != attempt-1 {
			t.Fatalf("metadata reset prior progress: %+v", loop)
		}
		if err := store.SetSession(t.Context(), state.Session{Ticket: created.ID, Role: "coder", Harness: "codex", Owner: "stable", ID: fmt.Sprintf("private-interrupted-session-%d", attempt)}); err != nil {
			t.Fatal(err)
		}
		failure := &orc.Failure{RepositoryID: info.ID, Worker: "stable", Actor: "coder", Role: "coder", Ticket: created.ID, AttemptID: loop.AttemptID, Origin: "codex", Operation: "managed turn", Classification: "harness_process_failed", Execution: &harness.ExecutionEvidence{LogID: "safe-log.jsonl", Phase: "running", Process: harness.ProcessOutcome{Start: "started", ExitObserved: true, ExitCode: 17, Cancellation: "none"}}}
		reason, err := orc.ReconcileFailedCoderClaim(t.Context(), expected, failure, true, store, tickets, holder)
		if err != nil || !strings.Contains(reason, "claim released") {
			t.Fatalf("reason=%s err=%v", reason, err)
		}
		loop, _, err = store.TicketLoop(t.Context(), created.ID)
		if err != nil || loop.StallCount != attempt || loop.ClaimState != "" {
			t.Fatalf("ended attempt not reconciled: %+v %v", loop, err)
		}
		snapshot, err := store.Read(t.Context())
		if err != nil || len(snapshot.Sessions) != attempt {
			t.Fatalf("lost interrupted sessions: %+v err=%v", snapshot, err)
		}
		for _, session := range snapshot.Sessions {
			if session.IsCurrent() || session.SupersededReason != "unexpected_execution_termination" {
				t.Fatalf("ordinary attempt could resume failed session: %+v", session)
			}
		}
		detail, err := tickets.ShowReadiness(t.Context(), created.ID)
		if err != nil || detail.Assignee != "" || !strings.Contains(detail.Sections["handoff"].Text, "Preserve substantive context") || !strings.Contains(detail.Sections["handoff"].Text, "safe-log.jsonl") {
			t.Fatalf("release/handoff not visible through Ticket: %+v %v", detail, err)
		}
		if attempt == 1 {
			if detail.State != "open" || detail.Readiness == nil || !detail.Readiness.Ready {
				t.Fatalf("first ended attempt did not remain ready: %+v", detail)
			}
			if _, err := tickets.UpdateTicket(t.Context(), created.ID, ticketclient.UpdateInput{Sections: map[string]string{"handoff": detail.Sections["handoff"].Text + "\nValidation still pending."}}); err != nil {
				t.Fatal(err)
			}
		} else if detail.State != "hold" || loop.Phase != state.TicketLoopHeld || loop.HeldFrom != "open" || !strings.Contains(detail.Sections["handoff"].Text, "Validation still pending.") {
			t.Fatalf("two unchanged ended attempts did not reach the existing hold: ticket=%+v loop=%+v", detail, loop)
		}
		worklog, err := tickets.ShowWorkLog(t.Context(), created.ID)
		if err != nil || strings.Count(worklog.WorkLog, "Managed worker stable ended attempt") != attempt || strings.Contains(worklog.WorkLog, "private-interrupted-session") {
			t.Fatalf("audit message=%+v err=%v", worklog, err)
		}
	}
	if ready, err := tickets.HasReady(t.Context(), "open", ticketclient.QueueFilters{}); err != nil || ready {
		t.Fatalf("held ticket remained in the ordinary queue: ready=%v err=%v", ready, err)
	}
}
