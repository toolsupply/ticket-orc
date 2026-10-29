package orc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestCoderSessionRegistrationFailurePreservesExecutionEvidence(t *testing.T) {
	processErr := errors.New("nested process failed")
	stateErr := errors.New("session registration unavailable")
	store := newFakeCoderState()
	store.setErr = stateErr
	result := harness.RunResult{SessionID: "private-session", LogPath: "/private/log.jsonl", Evidence: &harness.ExecutionEvidence{
		Process: harness.ProcessOutcome{Start: "started", ExitObserved: true, ExitCode: 17},
	}}
	agent := &fakeCoderHarness{runRes: result, runErr: &harness.ExecutionError{Harness: "codex", Result: result, Cause: processErr}}
	config := coderConfig()
	config.SessionCleanup = "delete"
	turn, err := runCoderTurn(context.Background(), config, "20261024-96942", agent, store)
	var executionErr *harness.ExecutionError
	if err != nil || !errors.Is(turn.err, processErr) || !errors.Is(turn.err, stateErr) || !errors.As(turn.err, &executionErr) {
		t.Fatalf("session bookkeeping hid execution failure: %+v, %v", turn, err)
	}
	if executionErr.Result.SessionID != result.SessionID || executionErr.Result.LogPath != result.LogPath || executionErr.Result.Evidence.Process.ExitCode != 17 || len(agent.runs) != 1 {
		t.Fatalf("private result or original outcome lost: %+v", executionErr.Result)
	}
	if len(agent.cleanups) != 0 {
		t.Fatalf("deleted failed session evidence before outer cleanup: %v", agent.cleanups)
	}
}

func TestFailedCoderDefersEndedClaimHoldUntilOuterCleanup(t *testing.T) {
	store := newTestStateStore(t)
	id := "20261024-92236"
	if _, _, err := store.Participate(t.Context(), id, 6); err != nil {
		t.Fatal(err)
	}
	loop, err := store.RecordClaim(t.Context(), id, "previous-coder", "open")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordStall(t.Context(), id, loop.AttemptID); err != nil {
		t.Fatal(err)
	}
	tickets := &fakeCoderTickets{claims: []ticketclient.Ticket{{ID: id, State: "open", Assignee: "coder-1"}}, shows: map[string]ticketclient.Ticket{id: {ID: id, State: "open"}}}
	holder := &loopTicketFixture{}
	config := coderConfig()
	config.Diagnostics = io.Discard
	config.ContainmentTickets = holder
	agent := &fakeCoderHarness{runErr: errors.New("unexpected process exit"), runRes: harness.RunResult{SessionID: "interrupted", Evidence: &harness.ExecutionEvidence{Process: harness.ProcessOutcome{Start: "started"}}}}
	if err := RunCoder(t.Context(), config, tickets, agent, store, nil); err == nil {
		t.Fatal("failed turn continued")
	}
	loop, _, err = store.TicketLoop(t.Context(), id)
	if err != nil || loop.StallCount != 1 || loop.Phase != state.TicketLoopActive || loop.ClaimActor != config.Actor || holder.holds != 0 || len(tickets.releases) != 0 || len(agent.runs) != 1 {
		t.Fatalf("premature hold/release/replay: loop=%+v holds=%d runs=%d err=%v", loop, holder.holds, len(agent.runs), err)
	}
}
