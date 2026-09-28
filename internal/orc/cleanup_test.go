package orc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type fakeTicketReader struct {
	mu      sync.Mutex
	tickets map[string]ticketclient.Ticket
	errors  map[string]error
	shown   []string
}

func (f *fakeTicketReader) Show(_ context.Context, id string) (ticketclient.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shown = append(f.shown, id)
	if err := f.errors[id]; err != nil {
		return ticketclient.Ticket{}, err
	}
	ticket, ok := f.tickets[id]
	if !ok {
		return ticketclient.Ticket{}, fmt.Errorf("unknown ticket")
	}
	return ticket, nil
}

type fakeSessionCleaner struct {
	mu     sync.Mutex
	calls  []cleanupCall
	err    error
	onCall func(string)
}

type cleanupCall struct {
	session string
	policy  harness.CleanupPolicy
}

func (f *fakeSessionCleaner) Cleanup(_ context.Context, session string, policy harness.CleanupPolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onCall != nil {
		f.onCall(session)
	}
	f.calls = append(f.calls, cleanupCall{session: session, policy: policy})
	return f.err
}

func TestCleanupTicketRemovesStateBeforeBestEffortHarnessCleanup(t *testing.T) {
	ctx := context.Background()
	store := newTestStateStoreAt(t, filepath.Join(t.TempDir(), "state"))
	ticketID := "20260919-10001"
	sessions := []state.Session{
		{Ticket: ticketID, Role: "coder", Harness: "codex", ID: "coder-session"},
		{Ticket: ticketID, Role: "reviewer", Harness: "codex", ID: "reviewer-session"},
	}
	for _, session := range sessions {
		if err := store.SetSession(ctx, session); err != nil {
			t.Fatalf("SetSession: %v", err)
		}
	}
	if _, err := store.IncrementBounces(ctx, ticketID); err != nil {
		t.Fatalf("IncrementBounces: %v", err)
	}

	cleanupErr := errors.New("archive unavailable")
	harnessCleaner := &fakeSessionCleaner{err: cleanupErr}
	harnessCleaner.onCall = func(_ string) {
		snapshot, err := store.Read(ctx)
		if err != nil {
			t.Errorf("Read during cleanup: %v", err)
			return
		}
		if len(snapshot.Sessions) != 0 || len(snapshot.Bounces) != 0 {
			t.Errorf("state still present during harness cleanup: %#v", snapshot)
		}
	}
	tickets := &fakeTicketReader{tickets: map[string]ticketclient.Ticket{
		// Legacy persisted/test data is normalized at the cleanup boundary.
		ticketID: {ID: ticketID, State: ticketclient.StateLegacyCompleted},
	}}
	var diagnostics bytes.Buffer
	cleaner := newTestCleaner(t, store, tickets, harnessCleaner, harness.CleanupArchive, &diagnostics)

	result, err := cleaner.CleanupTicket(ctx, ticketID)
	if err != nil {
		t.Fatalf("CleanupTicket: %v", err)
	}
	if len(result.Removed) != 2 || len(result.Failures) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if result.State != ticketclient.StateClosed {
		t.Fatalf("result state = %q, want %q", result.State, ticketclient.StateClosed)
	}
	if len(harnessCleaner.calls) != 2 {
		t.Fatalf("cleanup calls = %#v", harnessCleaner.calls)
	}
	for _, call := range harnessCleaner.calls {
		if call.policy != harness.CleanupArchive {
			t.Errorf("cleanup policy = %q", call.policy)
		}
	}
	for _, want := range []string{ticketID, "coder-session", "reviewer-session", cleanupErr.Error()} {
		if !strings.Contains(diagnostics.String(), want) {
			t.Errorf("diagnostics %q do not contain %q", diagnostics.String(), want)
		}
	}

	result, err = cleaner.CleanupTicket(ctx, ticketID)
	if err != nil || len(result.Removed) != 0 || len(harnessCleaner.calls) != 2 {
		t.Fatalf("repeated cleanup = %#v, %v; calls %#v", result, err, harnessCleaner.calls)
	}
}

func TestCleanupSessionPolicyDefaultsByHarness(t *testing.T) {
	ctx := context.Background()
	store := newTestStateStoreAt(t, filepath.Join(t.TempDir(), "state"))
	ticketID := "20260919-10002"
	for _, session := range []state.Session{
		{Ticket: ticketID, Role: "coder", Harness: "codex", ID: "codex-session"},
		{Ticket: ticketID, Role: "coder", Harness: "pi", ID: "pi-session"},
		{Ticket: ticketID, Role: "coder", Harness: "claude", ID: "claude-session"},
	} {
		if err := store.SetSession(ctx, session); err != nil {
			t.Fatal(err)
		}
	}
	cleaners := map[string]*fakeSessionCleaner{"codex": {}, "pi": {}, "claude": {}}
	tickets := &fakeTicketReader{tickets: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: ticketclient.StateClosed}}}
	policy := func(session state.Session) harness.CleanupPolicy {
		if session.Harness == "pi" || session.Harness == "claude" {
			return harness.CleanupKeep
		}
		return harness.CleanupDelete
	}
	cleaner, err := NewCleanerWithSessionPolicy(store, tickets, map[string]SessionCleaner{
		"codex": cleaners["codex"], "pi": cleaners["pi"], "claude": cleaners["claude"],
	}, harness.CleanupDelete, policy, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cleaner.CleanupTicket(ctx, ticketID); err != nil {
		t.Fatal(err)
	}
	if got := cleaners["codex"].calls[0].policy; got != harness.CleanupDelete {
		t.Fatalf("Codex default = %q", got)
	}
	if got := cleaners["pi"].calls[0].policy; got != harness.CleanupKeep {
		t.Fatalf("Pi default = %q", got)
	}
	if got := cleaners["claude"].calls[0].policy; got != harness.CleanupKeep {
		t.Fatalf("Claude default = %q", got)
	}
}

func TestCleanupTicketRefusesNonterminalState(t *testing.T) {
	ctx := context.Background()
	store := newTestStateStore(t)
	ticketID := "20260919-10002"
	session := state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", ID: "session"}
	if err := store.SetSession(ctx, session); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	tickets := &fakeTicketReader{tickets: map[string]ticketclient.Ticket{
		ticketID: {ID: ticketID, State: "review"},
	}}
	harnessCleaner := &fakeSessionCleaner{}
	cleaner := newTestCleaner(t, store, tickets, harnessCleaner, harness.CleanupDelete, nil)

	if _, err := cleaner.CleanupTicket(ctx, ticketID); !errors.Is(err, ErrNotTerminal) {
		t.Fatalf("CleanupTicket error = %v, want ErrNotTerminal", err)
	}
	if got, ok, err := store.GetSession(ctx, session.Ticket, session.Role, session.Harness); err != nil || !ok || got.ID != session.ID {
		t.Fatalf("session after refused cleanup = %#v, %v, %v", got, ok, err)
	}
	if len(harnessCleaner.calls) != 0 {
		t.Fatalf("cleanup calls = %#v", harnessCleaner.calls)
	}
}

func TestConcurrentCleanupOnlyCleansRemovedSessionOnce(t *testing.T) {
	ctx := context.Background()
	store := newTestStateStore(t)
	ticketID := "20260919-10003"
	if err := store.SetSession(ctx, state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", ID: "once"}); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	tickets := &fakeTicketReader{tickets: map[string]ticketclient.Ticket{
		ticketID: {ID: ticketID, State: "rejected"},
	}}
	harnessCleaner := &fakeSessionCleaner{}
	cleaner := newTestCleaner(t, store, tickets, harnessCleaner, harness.CleanupDelete, nil)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, err := cleaner.CleanupTicket(ctx, ticketID)
			errs <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent CleanupTicket: %v", err)
		}
	}
	if len(harnessCleaner.calls) != 1 || harnessCleaner.calls[0].session != "once" {
		t.Fatalf("cleanup calls = %#v, want one", harnessCleaner.calls)
	}
}

func TestCleanupReportsMissingHarnessAfterRemovingMapping(t *testing.T) {
	ctx := context.Background()
	store := newTestStateStore(t)
	ticketID := "20260919-10008"
	if err := store.SetSession(ctx, state.Session{Ticket: ticketID, Role: "coder", Harness: "future", ID: "future-session"}); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	tickets := &fakeTicketReader{tickets: map[string]ticketclient.Ticket{
		ticketID: {ID: ticketID, State: ticketclient.StateClosed},
	}}
	var diagnostics bytes.Buffer
	cleaner, err := NewCleaner(store, tickets, nil, harness.CleanupDelete, &diagnostics)
	if err != nil {
		t.Fatalf("NewCleaner: %v", err)
	}
	result, err := cleaner.CleanupTicket(ctx, ticketID)
	if err != nil || len(result.Failures) != 1 {
		t.Fatalf("CleanupTicket = %#v, %v", result, err)
	}
	if !strings.Contains(diagnostics.String(), "future-session") || !strings.Contains(diagnostics.String(), "future") {
		t.Fatalf("diagnostics = %q", diagnostics.String())
	}
}

func TestCleanupSanitizesHarnessFailureDiagnosticsWithoutChangingRawError(t *testing.T) {
	ctx := context.Background()
	store := newTestStateStore(t)
	ticketID := "20260921-95799"
	if err := store.SetSession(ctx, state.Session{Ticket: ticketID, Role: "coder", Harness: "codex", ID: "session"}); err != nil {
		t.Fatal(err)
	}
	rawErr := errors.New("cleanup \x1b[31mfailed\x1b]2;spoof\a")
	cleanerImpl := &fakeSessionCleaner{err: rawErr}
	tickets := &fakeTicketReader{tickets: map[string]ticketclient.Ticket{ticketID: {ID: ticketID, State: ticketclient.StateClosed}}}
	var diagnostics bytes.Buffer
	cleaner := newTestCleaner(t, store, tickets, cleanerImpl, harness.CleanupDelete, &diagnostics)
	result, err := cleaner.CleanupTicket(ctx, ticketID)
	if err != nil || len(result.Failures) != 1 || !errors.Is(result.Failures[0].Err, rawErr) {
		t.Fatalf("cleanup result=%#v err=%v", result, err)
	}
	for _, r := range diagnostics.String() {
		if r != '\n' && (r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)) {
			t.Fatalf("unsafe diagnostics=%q", diagnostics.String())
		}
	}
	if !strings.Contains(diagnostics.String(), "cleanup [31mfailed]2;spoof") {
		t.Fatalf("sanitized diagnostics=%q", diagnostics.String())
	}
}

func newTestCleaner(t *testing.T, store *state.Store, tickets *fakeTicketReader, harnessCleaner SessionCleaner, policy harness.CleanupPolicy, diagnostics *bytes.Buffer) *Cleaner {
	t.Helper()
	var writer interface{ Write([]byte) (int, error) }
	if diagnostics != nil {
		writer = diagnostics
	}
	cleaner, err := NewCleaner(store, tickets, map[string]SessionCleaner{"codex": harnessCleaner}, policy, writer)
	if err != nil {
		t.Fatalf("NewCleaner: %v", err)
	}
	return cleaner
}
