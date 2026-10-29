package orc

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type failedClaimTickets struct {
	loopTicketFixture
	handoff       ticketclient.TicketSection
	options       []ticketclient.MutationOptions
	beforeRelease func()
	releaseErr    error
	readErr       error
	receipt       *ticketclient.MutationResult
	reads         int
}

func (f *failedClaimTickets) ShowReadiness(ctx context.Context, id string) (ticketclient.TicketDetail, error) {
	f.reads++
	if f.readErr != nil {
		return ticketclient.TicketDetail{}, f.readErr
	}
	detail, err := f.loopTicketFixture.ShowReadiness(ctx, id)
	detail.Sections = map[string]ticketclient.TicketSection{"handoff": f.handoff}
	return detail, err
}

func (f *failedClaimTickets) ReleaseTicket(ctx context.Context, id string, options ticketclient.MutationOptions) (ticketclient.MutationResult, error) {
	f.options = append(f.options, options)
	if f.beforeRelease != nil {
		f.beforeRelease()
	}
	if f.releaseErr != nil {
		return ticketclient.MutationResult{}, f.releaseErr
	}
	transition, err := f.Release(ctx, id)
	if err != nil {
		return ticketclient.MutationResult{}, err
	}
	f.handoff.Text = options.Handoff
	if f.receipt != nil {
		return *f.receipt, nil
	}
	return ticketclient.MutationResult{ID: id, Changed: true, State: transition.State}, nil
}

func failedClaimFixture(t *testing.T) (*state.Store, *failedClaimTickets, FailureContext, *Failure) {
	t.Helper()
	store := state.NewForRepository(t.TempDir(), "8d1268c4-6a64-4b9b-95c9-d5598a150e86")
	expected := FailureContext{RepositoryID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", Worker: "stable-coder", Actor: "coder", Role: "backend"}
	id := "20261024-92236"
	if _, _, err := store.Participate(t.Context(), id, 6); err != nil {
		t.Fatal(err)
	}
	loop, err := store.RecordClaim(t.Context(), id, expected.Actor, "open")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetSession(t.Context(), state.Session{Ticket: id, Role: "coder", Harness: "codex", Owner: expected.Worker, ID: "private-session"}); err != nil {
		t.Fatal(err)
	}
	tickets := &failedClaimTickets{loopTicketFixture: loopTicketFixture{ticket: ticketclient.Ticket{ID: id, State: "open", Assignee: expected.Actor}, actor: expected.Actor, ready: true}, handoff: ticketclient.TicketSection{Text: "Keep the partial parser fix; validation still pending."}}
	failure := &Failure{RepositoryID: expected.RepositoryID, Worker: expected.Worker, Actor: expected.Actor, Role: expected.Role, Ticket: id, AttemptID: loop.AttemptID, Classification: "harness_process_failed", Origin: "codex", Operation: "managed turn", Execution: &harness.ExecutionEvidence{LogID: "safe-log.jsonl", Phase: "running", Process: harness.ProcessOutcome{Start: "started", ExitObserved: true, ExitCode: 17, Cancellation: "none"}}}
	return store, tickets, expected, failure
}

func TestFailedCoderClaimRetiresBeforeReleaseAndUsesExistingStallBreaker(t *testing.T) {
	store, tickets, expected, failure := failedClaimFixture(t)
	partial := filepath.Join(t.TempDir(), "partial.go")
	if err := os.WriteFile(partial, []byte("partial work"), 0600); err != nil {
		t.Fatal(err)
	}
	tickets.beforeRelease = func() {
		if _, current, err := store.GetSession(t.Context(), failure.Ticket, "coder", "codex", expected.Worker); err != nil || current {
			t.Fatalf("release preceded retirement: current=%v err=%v", current, err)
		}
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if attempt == 2 {
			tickets.ticket.Assignee = expected.Actor
			loop, err := store.RecordClaim(t.Context(), failure.Ticket, expected.Actor, "open")
			if err != nil {
				t.Fatal(err)
			}
			failure.AttemptID = loop.AttemptID
		}
		reason, err := ReconcileFailedCoderClaim(t.Context(), expected, failure, true, store, tickets, tickets)
		if err != nil || !strings.Contains(reason, "claim released") {
			t.Fatalf("reason=%s err=%v", reason, err)
		}
		loop, _, err := store.TicketLoop(t.Context(), failure.Ticket)
		if err != nil || loop.StallCount != attempt || loop.ClaimState != "" {
			t.Fatalf("attempt=%d loop=%+v err=%v", attempt, loop, err)
		}
		if attempt == 1 && (tickets.ticket.Assignee != "" || tickets.ticket.State != "open") {
			t.Fatalf("not ordinary queue eligible: %+v", tickets.ticket)
		}
	}
	if tickets.holds != 1 || tickets.ticket.State != "hold" || len(tickets.options) != 2 {
		t.Fatalf("missing ordinary breaker: %+v", tickets)
	}
	for _, options := range tickets.options {
		if len(options.Handoff) > 12000 || !strings.Contains(options.Handoff, "partial parser fix") || !strings.Contains(options.Handoff, "safe-log.jsonl") || options.Message == "" || strings.Contains(options.Handoff, "private-session") {
			t.Fatalf("unsafe/lost handoff: %+v", options)
		}
	}
	snapshot, err := store.Read(t.Context())
	if err != nil || len(snapshot.Sessions) != 1 || snapshot.Sessions[0].IsCurrent() || snapshot.Sessions[0].SupersededReason != "unexpected_execution_termination" {
		t.Fatalf("lost session evidence: %+v %v", snapshot, err)
	}
	if data, err := os.ReadFile(partial); err != nil || string(data) != "partial work" {
		t.Fatalf("partial work changed: %s %v", data, err)
	}
}

func TestFailedCoderClaimRefusesUnsafeOrSupersededEvidence(t *testing.T) {
	cases := map[string]func(*state.Store, *failedClaimTickets, *FailureContext, *Failure){
		"owner changed": func(_ *state.Store, f *failedClaimTickets, _ *FailureContext, _ *Failure) {
			f.ticket.Assignee = "other"
		},
		"review":             func(_ *state.Store, f *failedClaimTickets, _ *FailureContext, _ *Failure) { f.ticket.State = "review" },
		"signoff":            func(_ *state.Store, f *failedClaimTickets, _ *FailureContext, _ *Failure) { f.ticket.State = "signoff" },
		"hold":               func(_ *state.Store, f *failedClaimTickets, _ *FailureContext, _ *Failure) { f.ticket.State = "hold" },
		"terminal":           func(_ *state.Store, f *failedClaimTickets, _ *FailureContext, _ *Failure) { f.ticket.State = "closed" },
		"repository changed": func(_ *state.Store, _ *failedClaimTickets, e *FailureContext, _ *Failure) { e.RepositoryID = "other" },
		"worker changed":     func(_ *state.Store, _ *failedClaimTickets, e *FailureContext, _ *Failure) { e.Worker = "other" },
		"actor changed":      func(_ *state.Store, _ *failedClaimTickets, e *FailureContext, _ *Failure) { e.Actor = "other" },
		"stale attempt":      func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) { f.AttemptID++ },
		"unknown category": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Classification = "worker_error"
		},
		"external operation unresolved": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Classification = "external_operation_unresolved"
		},
		"authentication": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Execution.ProviderFailed = true
		},
		"protocol": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Execution.ProtocolFailed = true
		},
		"output": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Execution.OutputFailed = true
		},
		"deliberate stop": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Execution.Process.Cancellation = "canceled"
		},
		"unknown cancellation": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Execution.Process.Cancellation = ""
		},
		"setup": func(_ *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			f.Execution.Process.Start = "not_started"
		},
		"handoff truncated": func(_ *state.Store, f *failedClaimTickets, _ *FailureContext, _ *Failure) { f.handoff.Truncated = true },
		"handoff oversized": func(_ *state.Store, f *failedClaimTickets, _ *FailureContext, _ *Failure) {
			f.handoff.Text = strings.Repeat("x", 8193)
		},
		"containment pending": func(s *state.Store, _ *failedClaimTickets, _ *FailureContext, f *Failure) {
			if _, err := s.MarkContainmentPending(t.Context(), f.Ticket); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			store, tickets, expected, failure := failedClaimFixture(t)
			change(store, tickets, &expected, failure)
			_, err := ReconcileFailedCoderClaim(t.Context(), expected, failure, true, store, tickets, tickets)
			if err != nil {
				t.Fatal(err)
			}
			if len(tickets.options) != 0 || tickets.holds != 0 {
				t.Fatalf("unsafe mutation: %+v", tickets)
			}
			if _, current, err := store.GetSession(t.Context(), failure.Ticket, "coder", "codex", "stable-coder"); err != nil || !current {
				t.Fatalf("unsafe retirement: current=%v err=%v", current, err)
			}
		})
	}
}

func TestFailedCoderClaimUnknownCleanupDoesNotReadOrMutate(t *testing.T) {
	// Nil boundaries make any speculative state/Ticket access fail immediately.
	reason, err := ReconcileFailedCoderClaim(t.Context(), FailureContext{}, nil, false, nil, nil, nil)
	if err != nil || reason != CleanupUnconfirmed {
		t.Fatalf("reason=%s err=%v", reason, err)
	}
}

func TestFailedCoderClaimAmbiguousReleaseNeverRetriesOrCountsStall(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not applied", true: "applied"}[applied], func(t *testing.T) {
			store, tickets, expected, failure := failedClaimFixture(t)
			tickets.releaseErr = io.ErrUnexpectedEOF
			tickets.beforeRelease = func() {
				if applied {
					tickets.ticket.Assignee = ""
				}
			}
			reason, err := ReconcileFailedCoderClaim(t.Context(), expected, failure, true, store, tickets, tickets)
			if !errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(reason, "uncertain") || len(tickets.options) != 1 {
				t.Fatalf("reason=%s err=%v mutations=%d", reason, err, len(tickets.options))
			}
			loop, _, _ := store.TicketLoop(t.Context(), failure.Ticket)
			if loop.StallCount != 0 || loop.ClaimState != "open" {
				t.Fatalf("manufactured release success: %+v", loop)
			}
			if applied {
				// Ordinary restart reconciliation consumes the original attempt;
				// metadata never constitutes workflow progress or another mutation.
				if _, err := ReconcileEndedClaim(t.Context(), store, tickets, tickets, failure.Ticket); err != nil {
					t.Fatal(err)
				}
				loop, _, _ = store.TicketLoop(t.Context(), failure.Ticket)
				if loop.StallCount != 1 || len(tickets.options) != 1 {
					t.Fatalf("restart reconciliation: %+v", loop)
				}
			}
		})
	}
}

func TestFailedCoderClaimReceiptRereadAndMissingSession(t *testing.T) {
	store, tickets, expected, failure := failedClaimFixture(t)
	session, _, _ := store.GetSession(t.Context(), failure.Ticket, "coder", "codex", expected.Worker)
	if _, _, err := store.SupersedeSession(t.Context(), session, "session_policy_fresh"); err != nil {
		t.Fatal(err)
	}
	tickets.receipt = &ticketclient.MutationResult{ID: failure.Ticket, Changed: true}
	if _, err := ReconcileFailedCoderClaim(t.Context(), expected, failure, true, store, tickets, tickets); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Read(t.Context())
	if len(snapshot.Sessions) != 1 || len(tickets.options) != 1 {
		t.Fatalf("created new session or duplicate mutation: %+v", snapshot)
	}
}

func TestFailedCoderClaimReadFailureAndAtomicOwnerChange(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(map[bool]string{false: "read failure", true: "atomic owner change"}[race], func(t *testing.T) {
			store, tickets, expected, failure := failedClaimFixture(t)
			if race {
				tickets.beforeRelease = func() { tickets.ticket.Assignee = "other" }
			} else {
				tickets.readErr = io.ErrUnexpectedEOF
			}
			if _, err := ReconcileFailedCoderClaim(t.Context(), expected, failure, true, store, tickets, tickets); err == nil {
				t.Fatal("uncertain read/ownership reported success")
			}
			if tickets.holds != 0 || tickets.ticket.Assignee == "" {
				t.Fatalf("changed foreign/uncertain claim: %+v", tickets)
			}
			if want := map[bool]int{false: 0, true: 1}[race]; len(tickets.options) != want {
				t.Fatalf("mutations=%d want=%d", len(tickets.options), want)
			}
			if !race {
				if _, current, err := store.GetSession(t.Context(), failure.Ticket, "coder", "codex", expected.Worker); err != nil || !current {
					t.Fatalf("read failure retired session: %v %v", current, err)
				}
			}
		})
	}
}

func TestFailedCoderClaimAlreadyEndedRetiresSessionWithoutDuplicateRelease(t *testing.T) {
	store, tickets, expected, failure := failedClaimFixture(t)
	tickets.ticket.Assignee = ""
	if _, err := ReconcileFailedCoderClaim(t.Context(), expected, failure, true, store, tickets, tickets); err != nil {
		t.Fatal(err)
	}
	loop, _, err := store.TicketLoop(t.Context(), failure.Ticket)
	if err != nil || loop.StallCount != 1 || len(tickets.options) != 0 {
		t.Fatalf("ended claim loop=%+v mutations=%d err=%v", loop, len(tickets.options), err)
	}
	if _, current, err := store.GetSession(t.Context(), failure.Ticket, "coder", "codex", expected.Worker); err != nil || current {
		t.Fatalf("ended attempt can resume interrupted session: %v %v", current, err)
	}
}
