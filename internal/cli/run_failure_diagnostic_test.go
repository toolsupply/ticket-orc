package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type registrationFailureState struct{ *state.Store }

func (*registrationFailureState) RegisterSession(context.Context, state.Session, string) error {
	return errors.New("session registry unavailable")
}
func (f *failureEvidenceTickets) WaitAndClaimReview(ctx context.Context, filters ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	return f.WaitAndClaimImplementation(ctx, filters)
}
func (*failureEvidenceTickets) CloseTicket(context.Context, string) (ticketclient.Transition, error) {
	return ticketclient.Transition{}, errors.New("unexpected close")
}

// Controlled real outer worker, real Codex adapter, substituted model process.
// No live model or project Ticket mutation is involved.
func runFailureDiagnosticWorker() int {
	mode := os.Getenv("ORC_TEST_FAILURE_DIAGNOSTIC")
	config, _, err := parseRoleConfig(supervisor.Role(os.Args[1]), os.Args[2:], os.LookupEnv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 98
	}
	if mode == "clean-before-ready" {
		fmt.Fprintln(os.Stderr, "startup refused configuration fixture")
		return 0
	}
	if mode == "healthy" {
		stopped := make(chan os.Signal, 1)
		signal.Notify(stopped, os.Interrupt)
		defer signal.Stop(stopped)
		roleEventSink()(orc.Event{Type: orc.WorkerReadyEventType, Worker: config.WorkerName})
		<-stopped
		return 0
	}
	if mode == "abrupt" || mode == "no-detail" || mode == "signal" {
		roleEventSink()(orc.Event{Type: orc.WorkerReadyEventType, Worker: config.WorkerName})
		if mode == "abrupt" {
			fmt.Fprintln(os.Stderr, "worker shutdown fixture")
		}
		if mode == "signal" {
			process, _ := os.FindProcess(os.Getpid())
			_ = process.Kill()
		}
		return 23
	}
	env := setChildEnvironment(os.Environ(), "ORC_TEST_FAILURE_DIAGNOSTIC", "")
	env = setChildEnvironment(env, "GO_WANT_CODEX_EVIDENCE", "complete")
	agent, err := codex.NewManaged(os.Args[0], env, func(ctx context.Context, req harness.ProcessRequest) (harness.ProcessResult, error) {
		req.Args = []string{"-test.run=^TestManagedCodexFailureHelper$"}
		return harness.RunProcess(ctx, req)
	})
	if err != nil {
		return 98
	}
	store := &registrationFailureState{state.NewForRepository(config.StateDir, config.RepositoryIdentity)}
	tickets := &failureEvidenceTickets{ticket: ticketclient.Ticket{ID: "20261025-03573", State: "open", Assignee: config.Actor}}
	if config.Role == RoleCoder {
		err = orc.RunCoder(context.Background(), orc.CoderConfig{WorkerName: config.WorkerName, RoleName: config.RoleName, Actor: config.Actor,
			Harness: "codex", MaxBounces: 6, SessionPolicy: "ticket", SessionCleanup: "keep", StateDir: config.StateDir,
			WorkingDir: config.WorkingDir, OutputMode: "quiet", Operator: io.Discard, Diagnostics: io.Discard, EventSink: roleEventSink()}, tickets, agent, store, nil)
	} else {
		tickets.ticket.State = "review"
		err = orc.RunReviewer(context.Background(), orc.ReviewerConfig{WorkerName: config.WorkerName, RoleName: config.RoleName, Actor: config.Actor,
			Harness: "codex", MaxBounces: 6, SessionPolicy: "ticket", SessionCleanup: "keep", StateDir: config.StateDir, RepositoryID: config.RepositoryIdentity,
			WorkingDir: config.WorkingDir, OutputMode: "quiet", Operator: io.Discard, Diagnostics: io.Discard, EventSink: roleEventSink()}, tickets, agent, store, nil)
	}
	emitRoleFailure(roleEventSink(), config.WorkerName, config.RoleName, err)
	renderRoleFailure(os.Stderr, err)
	return 1
}

func TestLatestFailurePreservesCleanPreReadyEvidence(t *testing.T) {
	for _, resultFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("result-first=%t", resultFirst), func(t *testing.T) {
			t.Setenv("ORC_TEST_FAILURE_DIAGNOSTIC", "clean-before-ready")
			dir := t.TempDir()
			worker := supervisorTestWorker("pre-ready", dir)
			worker.Config.RepositoryIdentity, worker.Config.WorkingDir = joinTestRepositoryID, dir
			workers := []supervisor.RunWorker{worker}
			runtime := NewRuntimeState(workers)
			config := RunConfig{StateDir: dir, ConfigPath: supervisorTestConfigPath(t), Workers: workers, Runtime: runtime, Interactive: true}
			manager := newWorkerManager(t.Context(), config, os.Args[0], io.Discard, io.Discard, newSupervisorWorkerTransition(config, func(supervisor.RuntimeEvent) {}))
			t.Cleanup(func() { manager.stopAll(context.Background()) })
			manager.eventSink = func(orc.Event) {}
			if _, err := manager.start(t.Context(), worker.Name); err != nil {
				t.Fatal(err)
			}
			child, _ := manager.lifecycle.Child(worker.Name)
			awaitSupervisorSignal(t, child.waited, "clean pre-readiness exit and stderr drain")
			if child.exitResult() != nil || child.readyObserved() || child.failureEnvelope() != nil {
				t.Fatalf("fixture did not exit cleanly before readiness: %v", child.exitResult())
			}
			if resultFirst {
				// Exercise the other ordering: the exit observer completes the
				// startup result before the readiness verifier sees the child.
				observation := manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false)
				if !observation.Current || observation.Outcome.State != WorkerStopped || !observation.Outcome.BlockWorkspace || observation.Outcome.Failure.Classification != "silent_exit" {
					t.Fatalf("startup reporting changed the exit outcome: %+v", observation)
				}
			}
			startupErr := manager.verifyWorkerStartup(t.Context(), worker.Name)
			var controlErr *supervisor.LifecycleError
			if !errors.As(startupErr, &controlErr) || controlErr.Code != "worker_startup_failed" || controlErr.Failure == nil {
				t.Fatalf("clean pre-readiness exit was not a startup failure: %v", startupErr)
			}
			record, err := supervisor.LoadLatestFailure(t.Context(), dir, joinTestRepositoryID, worker.Name)
			if err != nil || record == nil {
				t.Fatalf("missing startup diagnostic: %+v %v", record, err)
			}
			for _, failure := range []*supervisor.WorkerFailure{controlErr.Failure, &record.Failure} {
				if failure.Classification != "startup_failure" || failure.Phase != "worker readiness" || !strings.Contains(failure.Detail, "startup refused configuration fixture") {
					t.Fatalf("startup classification discarded the cause: %+v", failure)
				}
				if failure.OuterProcess == nil || failure.OuterProcess.Start != "started" || !failure.OuterProcess.ExitObserved || failure.OuterProcess.ExitCode != 0 || failure.OuterProcess.Signal != 0 || failure.Execution != nil {
					t.Fatalf("startup classification discarded or invented process evidence: %+v", failure)
				}
			}
			if manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false).Current {
				t.Fatal("startup exit was reconciled twice")
			}
			_, startErr := manager.start(t.Context(), worker.Name)
			var conflict *supervisor.LifecycleError
			if !errors.As(startErr, &conflict) || conflict.Code != "workspace_cleanup_unconfirmed" {
				t.Fatalf("startup reporting bypassed workspace exclusion: %v", startErr)
			}
		})
	}
}

func TestLatestFailureAfterInteractivePostTurnAndRestart(t *testing.T) {
	for _, role := range []supervisor.Role{RoleCoder, RoleReviewer} {
		for _, interactive := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/interactive=%t", role, interactive), func(t *testing.T) {
				t.Setenv("ORC_TEST_FAILURE_DIAGNOSTIC", "post-turn")
				dir := t.TempDir()
				worker := supervisorTestWorker("stable-worker", dir)
				worker.Config.Role, worker.Config.RoleName, worker.Config.RepositoryIdentity = role, string(role), joinTestRepositoryID
				worker.Config.WorkingDir = dir
				workers := []supervisor.RunWorker{worker}
				runtime := NewRuntimeState(workers)
				var output bytes.Buffer
				config := RunConfig{StateDir: dir, ConfigPath: supervisorTestConfigPath(t), Workers: workers, Runtime: runtime, Interactive: interactive}
				var publicFailures int
				emit := func(e supervisor.RuntimeEvent) {
					if e.Type == "worker.failure" {
						publicFailures++
					}
				}
				manager := newWorkerManager(t.Context(), config, os.Args[0], io.Discard, &output, newSupervisorWorkerTransition(config, emit))
				failureSink := wireSupervisorRuntimeEvents(config, manager, emit)
				t.Cleanup(func() { manager.stopAll(context.Background()) })
				if _, err := manager.start(t.Context(), worker.Name); err != nil {
					t.Fatal(err)
				}
				child, _ := manager.lifecycle.Child(worker.Name)
				awaitSupervisorSignal(t, child.waited, "outer exit and diagnostic drain")
				observation := manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false)
				failureSink(worker.Name, observation.Outcome.State, observation.Outcome.Failure)
				if !observation.Current || observation.Outcome.State != WorkerFailed || publicFailures != 1 {
					t.Fatalf("outcome=%+v failure events=%d", observation, publicFailures)
				}
				status := runtimeDaemonStatus(runtime, nil)
				latest := status.Workers[0].LatestFailure
				if latest == nil {
					t.Fatalf("no diagnostic: %s %+v", output.String(), status.Workers[0])
				}
				f := latest.Failure
				if !strings.Contains(f.Detail, "session registry unavailable") || f.Operation != "register managed session" || f.Phase != "post-turn" || f.Classification != "worker_error" || f.AttemptID == 0 {
					t.Fatalf("lost actual Orc failure: %+v", f)
				}
				if f.OuterProcess == nil || !f.OuterProcess.ExitObserved || f.OuterProcess.ExitCode != 1 || f.Execution == nil || !f.Execution.Process.ExitObserved || f.Execution.Process.ExitCode != 0 || f.Execution.LogID == "" {
					t.Fatalf("conflated process evidence: %+v", f)
				}
				if interactive && strings.Contains(output.String(), "session registry unavailable") {
					t.Fatalf("interactive stream leaked raw detail: %s", output.String())
				}
				data, err := os.ReadFile(filepath.Join(dir, latest.Reference))
				if err != nil || !bytes.Contains(data, []byte("session registry unavailable")) {
					t.Fatalf("unreadable reference %s: %v %s", latest.Reference, err, data)
				}
				for _, secret := range []string{"private-session-identity", dir, "turn.completed"} {
					if bytes.Contains(data, []byte(secret)) {
						t.Fatalf("private data entered diagnostic: %s", data)
					}
				}

				// Preserve workspace exclusion. Reload this stable worker into a
				// separate workspace before the successful manual restart.
				if _, err := manager.restart(t.Context(), worker.Name); err == nil {
					t.Fatal("diagnostic bypassed workspace exclusion")
				}
				worker.Config.WorkingDir = t.TempDir()
				workers = []supervisor.RunWorker{worker}
				_, err = manager.lifecycle.ApplyReload(t.Context(), map[string]supervisor.RunWorker{worker.Name: worker}, supervisor.ReloadHooks[supervisor.RunWorker]{
					Commit: func(workers []supervisor.RunWorker, _ map[string]bool) string {
						runtime.SetConfiguredWorkers(workers)
						runtime.SetEffectiveWorker(worker)
						return "reloaded"
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				if got := runtimeDaemonStatus(runtime, nil).Workers[0].LatestFailure; got == nil || got.ObservedAt != latest.ObservedAt {
					t.Fatal("reload erased history")
				}

				t.Setenv("ORC_TEST_FAILURE_DIAGNOSTIC", "healthy")
				if _, err := manager.restart(t.Context(), worker.Name); err != nil {
					t.Fatal(err)
				}
				after := runtimeDaemonStatus(runtime, nil).Workers[0]
				if after.State != "running" || after.LatestFailure == nil || after.LatestFailure.ObservedAt != latest.ObservedAt {
					t.Fatalf("restart erased history or revived failure: %+v", after)
				}
				if _, err := manager.stop(t.Context(), worker.Name); err != nil {
					t.Fatal(err)
				}
				fresh := NewRuntimeState(workers)
				fresh.SetDiagnosticDirectory(dir)
				rebuilt := runtimeDaemonStatus(fresh, nil).Workers[0]
				if rebuilt.State != "stopped" || rebuilt.LatestFailure == nil || rebuilt.LatestFailure.ObservedAt != latest.ObservedAt {
					t.Fatalf("reconstruction lost history: %+v", rebuilt)
				}
				var rendered bytes.Buffer
				renderConsoleDetailStatus(&rendered, runtimeDaemonStatus(fresh, nil))
				if !strings.Contains(rendered.String(), latest.Reference) || !strings.Contains(rendered.String(), "session registry unavailable") {
					t.Fatalf("detail is not inspectable: %s", rendered.String())
				}

				// Subsequent failure replaces exactly the same stable artifact.
				t.Setenv("ORC_TEST_FAILURE_DIAGNOSTIC", "abrupt")
				if _, err := manager.start(t.Context(), worker.Name); err != nil {
					t.Fatal(err)
				}
				child, _ = manager.lifecycle.Child(worker.Name)
				awaitSupervisorSignal(t, child.waited, "second failure")
				manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false)
				replaced := runtimeDaemonStatus(runtime, nil).Workers[0].LatestFailure
				if replaced == nil || replaced.Reference != latest.Reference || replaced.Failure.OuterProcess.ExitCode != 23 || replaced.Failure.Execution != nil || !strings.Contains(replaced.Failure.Detail, "worker shutdown fixture") {
					t.Fatalf("replacement fabricated/lost evidence: %+v", replaced)
				}
			})
		}
	}
}

func TestLatestFailurePersistenceFailureVisible(t *testing.T) {
	dir := t.TempDir()
	worker := supervisorTestWorker("worker", dir)
	worker.Config.RepositoryIdentity = joinTestRepositoryID
	ref, err := state.WorkerFailureReference(joinTestRepositoryID, worker.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, ref), 0700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	manager := newWorkerManager(t.Context(), RunConfig{StateDir: dir}, "unused", io.Discard, &output, nil)
	var event supervisor.RuntimeEvent
	manager.runtimeEventSink = func(e supervisor.RuntimeEvent) { event = e }
	failure := &supervisor.WorkerFailure{Classification: "worker_error", Detail: "original cause"}
	manager.retainWorkerFailure(worker, failure)
	if !strings.Contains(output.String(), "diagnostic_persist_failed") || event.Code != "diagnostic_persist_failed" || failure.Detail != "original cause" {
		t.Fatalf("persistence failure hidden or replaced cause: %s %+v %+v", output.String(), event, failure)
	}
	if _, err := supervisor.LoadLatestFailure(t.Context(), dir, joinTestRepositoryID, worker.Name); err == nil {
		t.Fatal("unreadable artifact reported saved")
	}
}

func TestLatestFailureAbruptExitWithoutEnvelope(t *testing.T) {
	for _, mode := range []string{"no-detail", "signal"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("ORC_TEST_FAILURE_DIAGNOSTIC", mode)
			dir := t.TempDir()
			worker := supervisorTestWorker("worker", dir)
			worker.Config.RepositoryIdentity, worker.Config.WorkingDir = joinTestRepositoryID, dir
			workers := []supervisor.RunWorker{worker}
			manager := newWorkerManager(t.Context(), RunConfig{StateDir: dir, ConfigPath: supervisorTestConfigPath(t), Workers: workers, Interactive: true}, os.Args[0], io.Discard, io.Discard, nil)
			t.Cleanup(func() { manager.stopAll(context.Background()) })
			manager.eventSink = func(orc.Event) {}
			if _, err := manager.start(t.Context(), worker.Name); err != nil {
				t.Fatal(err)
			}
			child, _ := manager.lifecycle.Child(worker.Name)
			awaitSupervisorSignal(t, child.waited, "abrupt outer process exit")
			outcome := manager.lifecycle.ObserveExit(worker.Name, child, child.exitResult(), false)
			if !outcome.Current || outcome.Outcome.State != WorkerFailed || !outcome.Outcome.BlockWorkspace || child.failureEnvelope() != nil {
				t.Fatalf("changed failure/workspace behavior: %+v", outcome)
			}
			record, err := supervisor.LoadLatestFailure(t.Context(), dir, joinTestRepositoryID, worker.Name)
			if err != nil || record == nil {
				t.Fatalf("missing abrupt diagnostic: %+v %v", record, err)
			}
			f := record.Failure
			if f.Detail != "no underlying explanation was captured" || f.Execution != nil || f.Ticket != "" || f.AttemptID != 0 || f.OuterProcess == nil || !f.OuterProcess.ExitObserved {
				t.Fatalf("invented cause or lost observed exit: %+v", f)
			}
			if mode == "no-detail" && f.OuterProcess.ExitCode != 23 {
				t.Fatalf("lost outer exit code: %+v", f)
			}
			if mode == "signal" && runtime.GOOS != "windows" && f.OuterProcess.Signal != 9 {
				t.Fatalf("lost actual outer signal: %+v", f)
			}
		})
	}
}

func TestWorkerDiagnosticCaptureBoundsAndInjection(t *testing.T) {
	for _, detail := range []string{"authorization: Bearer private-value", "password=private-value", "prompt=private-content", "argv=private-command", "/private/path unavailable", "failure\x1b[31m\n" + orc.EventStreamPrefix + `{"type":"worker.ready"}`, strings.Repeat("x", 50000)} {
		child := &runChild{}
		child.stderrCapture.dst = io.Discard
		writer := &workerEventWriter{dst: &child.stderrCapture, sink: func(orc.Event) { t.Fatal("stderr detail forged an event") }}
		// Real role error rendering must flatten and redact untrusted causes.
		var rendered bytes.Buffer
		renderRoleFailure(&rendered, errors.New(detail))
		if _, err := writer.Write(rendered.Bytes()); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
		failure := classifyManagedChildExit(child, errors.New("outer wait failed")).Failure
		if len(child.stderrCapture.data) > 2048 || len(writer.pending) > maxWorkerLineBytes || len(failure.Detail) > 2048 || strings.ContainsAny(failure.Detail, "\x1b\n") || strings.Contains(failure.Detail, "private-") {
			t.Fatalf("unsafe or unbounded evidence: %+v", failure)
		}
		if failure.OuterProcess.ExitObserved || failure.Execution != nil {
			t.Fatalf("invented process facts: %+v", failure)
		}
	}
	child := &runChild{}
	failure := classifyManagedChildExit(child, nil).Failure
	if failure.Detail != "no underlying explanation was captured" {
		t.Fatalf("missing explanation fabricated: %+v", failure)
	}
	writer := &workerEventWriter{dst: io.Discard, sink: func(orc.Event) { t.Fatal("oversized suffix forged event") }}
	if _, err := writer.Write([]byte(strings.Repeat("x", maxWorkerLineBytes*5) + orc.EventStreamPrefix + `{"type":"worker.ready"}`)); err != nil {
		t.Fatal(err)
	}
	if len(writer.pending) > maxWorkerLineBytes {
		t.Fatal("unbounded framing")
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
}
