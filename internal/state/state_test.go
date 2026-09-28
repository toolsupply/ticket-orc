package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
)

func newStateTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	return newStateTestStoreAt(t, dir)
}

func newStateTestStoreAt(t *testing.T, dir string) *Store {
	t.Helper()
	return NewForRepository(dir, filepath.Join(dir, "repo"))
}

func TestEmptyState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	store := NewAdministrative(dir)
	snapshot, err := store.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if snapshot.Version != currentVersion || len(snapshot.Sessions) != 0 || len(snapshot.Bounces) != 0 {
		t.Fatalf("unexpected empty state: %#v", snapshot)
	}
	if _, err := os.Stat(filepath.Join(store.dir, stateFileName)); !os.IsNotExist(err) {
		t.Fatalf("empty read created state file: %v", err)
	}
}

func TestSavedStateUsesInitialPublicSchema(t *testing.T) {
	ctx := context.Background()
	store := newStateTestStore(t)
	if err := store.SetSession(ctx, Session{Ticket: "ticket", Role: "coder", Harness: "codex", ID: "session"}); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(store.dir, stateFileName))
	if err != nil {
		t.Fatalf("read persisted state: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("decode persisted state: %v", err)
	}
	var version int
	if err := json.Unmarshal(wire["version"], &version); err != nil || version != 1 {
		t.Fatalf("persisted version = %d, err=%v; want initial public version 1", version, err)
	}
	for _, removed := range []string{"initializations", "reorientations", "stale_at", "nudge_count"} {
		if _, exists := wire[removed]; exists {
			t.Errorf("persisted state contains removed field %q", removed)
		}
	}
}

func TestOwnerScopedSessionsCanCoexist(t *testing.T) {
	ctx := context.Background()
	store := NewForRepository(t.TempDir(), filepath.Join(t.TempDir(), "repo"))
	ticket := "20260921-77097"
	for _, owner := range []string{"worker-a", "worker-b"} {
		if err := store.SetSession(ctx, Session{Ticket: ticket, Role: "coder", Harness: "codex", Owner: owner, ID: owner + "-session"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.IncrementBounces(ctx, ticket); err != nil {
		t.Fatal(err)
	}
	if got, found, err := store.GetSession(ctx, ticket, "coder", "codex", "worker-a"); err != nil || !found || got.Owner != "worker-a" {
		t.Fatalf("owner A session = %#v found=%v err=%v", got, found, err)
	}
	if got, found, err := store.GetSession(ctx, ticket, "coder", "codex", "worker-b"); err != nil || !found || got.Owner != "worker-b" {
		t.Fatalf("owner B session = %#v found=%v err=%v", got, found, err)
	}
	if count, err := store.BounceCount(ctx, ticket); err != nil || count != 1 {
		t.Fatalf("bounce = %d, %v", count, err)
	}
}

func TestRepositoryNamespacesSeparateManagedSessionsAndBounces(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	storeA := NewForRepository(stateDir, filepath.Join(root, "repo-a"))
	storeB := NewForRepository(stateDir, filepath.Join(root, "repo-b"))
	const ticket = "20260926-12345"
	for _, item := range []struct {
		store *Store
		id    string
	}{{storeA, "session-a"}, {storeB, "session-b"}} {
		if err := item.store.SetSession(ctx, Session{Ticket: ticket, Role: "coder", Harness: "codex", ID: item.id}); err != nil {
			t.Fatal(err)
		}
		if _, err := item.store.IncrementBounces(ctx, ticket); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct {
		store *Store
		id    string
	}{{storeA, "session-a"}, {storeB, "session-b"}} {
		snapshot, err := item.store.Read(ctx)
		if err != nil || len(snapshot.Sessions) != 1 || snapshot.Sessions[0].ID != item.id || len(snapshot.Bounces) != 1 {
			t.Fatalf("repository snapshot = %#v err=%v; want session %q and one bounce", snapshot, err, item.id)
		}
	}
	if removed, changed, err := storeA.RemoveTicket(ctx, ticket); err != nil || !changed || len(removed) != 1 || removed[0].ID != "session-a" {
		t.Fatalf("repository A cleanup removed=%#v changed=%t err=%v", removed, changed, err)
	}
	if snapshot, err := storeB.Read(ctx); err != nil || len(snapshot.Sessions) != 1 || snapshot.Sessions[0].ID != "session-b" || len(snapshot.Bounces) != 1 {
		t.Fatalf("repository B state changed after A cleanup: %#v err=%v", snapshot, err)
	}
}

func TestMalformedUnnamespacedTicketStateIsRejected(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	unscopedState := `{"version":1,"sessions":[{"ticket":"same","role":"coder","harness":"codex","id":"old"}],"bounces":{"same":1}}`
	if err := os.WriteFile(filepath.Join(stateDir, stateFileName), []byte(unscopedState), 0o600); err != nil {
		t.Fatal(err)
	}
	admin := NewAdministrative(stateDir)
	if _, err := admin.Read(ctx); !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), "repository") {
		t.Fatalf("unscoped state error = %v, want precise malformed repository identity", err)
	}
}

func TestUnsupportedStateVersionFailsClearly(t *testing.T) {
	for _, version := range []int{0, 2, 3, 4} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "state")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			legacy := fmt.Sprintf(`{"version":%d,"sessions":[],"bounces":{},"initializations":[],"reorientations":[]}`, version)
			if err := os.WriteFile(filepath.Join(dir, stateFileName), []byte(legacy), 0o600); err != nil {
				t.Fatal(err)
			}
			store := NewAdministrative(dir)
			if _, err := store.Read(context.Background()); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unsupported state version %d; expected %d", version, currentVersion)) {
				t.Fatalf("state version %d error = %v", version, err)
			}
		})
	}
}

func TestSessionsAreKeyedByTicketRoleAndHarness(t *testing.T) {
	ctx := context.Background()
	store := newStateTestStore(t)
	sessions := []Session{
		{Repository: store.repository, Ticket: "ticket-1", Role: "coder", Harness: "codex", ID: "coder-codex"},
		{Repository: store.repository, Ticket: "ticket-1", Role: "reviewer", Harness: "codex", ID: "reviewer-codex"},
		{Repository: store.repository, Ticket: "ticket-1", Role: "coder", Harness: "pi", ID: "coder-pi"},
	}
	for _, session := range sessions {
		if err := store.SetSession(ctx, session); err != nil {
			t.Fatalf("SetSession(%#v): %v", session, err)
		}
	}
	for _, want := range sessions {
		got, ok, err := store.GetSession(ctx, want.Ticket, want.Role, want.Harness)
		if err != nil || !ok || got != want {
			t.Fatalf("GetSession(%#v) = %#v, %v, %v", want, got, ok, err)
		}
	}

	replacement := Session{Repository: store.repository, Ticket: "ticket-1", Role: "coder", Harness: "codex", ID: "replacement"}
	if err := store.SetSession(ctx, replacement); err != nil {
		t.Fatalf("replace session: %v", err)
	}
	got, ok, err := store.GetSession(ctx, replacement.Ticket, replacement.Role, replacement.Harness)
	if err != nil || !ok || got != replacement {
		t.Fatalf("replacement = %#v, %v, %v", got, ok, err)
	}
}

func TestSupersededSessionCannotBeRegisteredAgain(t *testing.T) {
	ctx := context.Background()
	store := newStateTestStore(t)
	first := Session{Ticket: "ticket-session-history", Role: "coder", Harness: "codex", Owner: "worker-a", ID: "thread-old"}
	if err := store.RegisterSession(ctx, first, ""); err != nil {
		t.Fatal(err)
	}
	retired, changed, err := store.SupersedeSession(ctx, first, "harness_invalidated")
	if err != nil || !changed || retired.SupersededReason != "harness_invalidated" {
		t.Fatalf("supersede = %#v, %v, %v", retired, changed, err)
	}
	second := first
	second.ID = "thread-new"
	if err := store.RegisterSession(ctx, second, first.ID); err != nil {
		t.Fatalf("register replacement: %v", err)
	}
	if _, found, err := store.GetSession(ctx, first.Ticket, first.Role, first.Harness, first.Owner); err != nil || !found {
		t.Fatalf("current session missing: found=%v err=%v", found, err)
	}
	if err := store.RegisterSession(ctx, first, ""); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("resurrection error = %v", err)
	}
	snapshot, err := store.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Sessions) != 2 || snapshot.Sessions[0].ReplacedBy != second.ID {
		t.Fatalf("session history = %#v", snapshot.Sessions)
	}
}

func TestSessionContextTelemetryPersistsAndCanBeCleared(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newStateTestStoreAt(t, dir)
	session := Session{Ticket: "20260923-telemetry", Role: "coder", Harness: "codex", Owner: "worker", ID: "thread"}
	if err := store.RegisterSession(ctx, session, ""); err != nil {
		t.Fatal(err)
	}
	want := contextheadroom.Telemetry{Known: true, Used: 700, Window: 1000, Remaining: 300, ObservedAt: time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)}
	if changed, err := store.SetSessionContextTelemetry(ctx, session, want); err != nil || !changed {
		t.Fatalf("record context telemetry = %v, %v", changed, err)
	}
	restarted := NewForRepository(dir, store.repository)
	got, found, err := restarted.GetSession(ctx, session.Ticket, session.Role, session.Harness, session.Owner)
	if err != nil || !found || got.ContextTelemetry != want {
		t.Fatalf("persisted telemetry = %#v, found=%v, err=%v", got.ContextTelemetry, found, err)
	}
	if changed, err := restarted.SetSessionContextTelemetry(ctx, got, contextheadroom.Telemetry{}); err != nil || !changed {
		t.Fatalf("clear context telemetry = %v, %v", changed, err)
	}
	got, found, err = store.GetSession(ctx, session.Ticket, session.Role, session.Harness, session.Owner)
	if err != nil || !found || got.ContextTelemetry.Known || !got.ContextTelemetry.Valid() {
		t.Fatalf("cleared telemetry = %#v, found=%v, err=%v", got.ContextTelemetry, found, err)
	}
	malformed := contextheadroom.Telemetry{Known: true, Used: 200, Window: 100, Remaining: 10, ObservedAt: time.Now().UTC()}
	if _, err := store.SetSessionContextTelemetry(ctx, got, malformed); err == nil {
		t.Fatal("accepted malformed context telemetry")
	}
}

func TestBounceIncrement(t *testing.T) {
	ctx := context.Background()
	store := newStateTestStore(t)
	for want := 1; want <= 3; want++ {
		got, err := store.IncrementBounces(ctx, "ticket-1")
		if err != nil || got != want {
			t.Fatalf("IncrementBounces = %d, %v; want %d", got, err, want)
		}
	}
	got, err := store.BounceCount(ctx, "ticket-1")
	if err != nil || got != 3 {
		t.Fatalf("BounceCount = %d, %v; want 3", got, err)
	}
	got, err = store.BounceCount(ctx, "unknown")
	if err != nil || got != 0 {
		t.Fatalf("unknown BounceCount = %d, %v; want 0", got, err)
	}
}

func TestRemoveTicketIsAtomicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newStateTestStore(t)
	terminal := []Session{
		{Repository: store.repository, Ticket: "terminal", Role: "coder", Harness: "codex", ID: "one"},
		{Repository: store.repository, Ticket: "terminal", Role: "reviewer", Harness: "codex", ID: "two"},
	}
	active := Session{Repository: store.repository, Ticket: "active", Role: "coder", Harness: "codex", ID: "three"}
	for _, session := range append(terminal, active) {
		if err := store.SetSession(ctx, session); err != nil {
			t.Fatalf("SetSession: %v", err)
		}
	}
	if _, err := store.IncrementBounces(ctx, "terminal"); err != nil {
		t.Fatalf("increment terminal: %v", err)
	}
	if _, err := store.IncrementBounces(ctx, "active"); err != nil {
		t.Fatalf("increment active: %v", err)
	}

	removed, changed, err := store.RemoveTicket(ctx, "terminal")
	if err != nil {
		t.Fatalf("RemoveTicket: %v", err)
	}
	if !changed {
		t.Fatal("RemoveTicket did not report the state change")
	}
	if len(removed) != len(terminal) {
		t.Fatalf("removed = %#v", removed)
	}
	if _, ok, err := store.GetSession(ctx, "terminal", "coder", "codex"); err != nil || ok {
		t.Fatalf("terminal session remains: ok=%v err=%v", ok, err)
	}
	if count, err := store.BounceCount(ctx, "terminal"); err != nil || count != 0 {
		t.Fatalf("terminal bounces = %d, %v", count, err)
	}
	if got, ok, err := store.GetSession(ctx, active.Ticket, active.Role, active.Harness); err != nil || !ok || got != active {
		t.Fatalf("active session lost: %#v, %v, %v", got, ok, err)
	}
	if count, err := store.BounceCount(ctx, "active"); err != nil || count != 1 {
		t.Fatalf("active bounces = %d, %v", count, err)
	}
	removed, changed, err = store.RemoveTicket(ctx, "terminal")
	if err != nil || changed || len(removed) != 0 {
		t.Fatalf("idempotent removal = %#v, changed=%v, %v", removed, changed, err)
	}
}

func TestMalformedStateIsRejected(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"invalid JSON", "{"},
		{"unsupported version", `{"version":2,"sessions":[],"bounces":{}}`},
		{"removed initialization field", `{"version":1,"sessions":[],"bounces":{},"initializations":[]}`},
		{"unknown field", `{"version":1,"sessions":[],"bounces":{},"extra":true}`},
		{"negative bounce", `{"version":1,"sessions":[],"bounces":{"/repo\u0000ticket":-1}}`},
		{"unnamespaced bounce key", `{"version":1,"sessions":[],"bounces":{"ticket":1}}`},
		{"session missing repository", `{"version":1,"sessions":[{"ticket":"t","role":"coder","harness":"codex","id":"s"}],"bounces":{}}`},
		{"session noncanonical repository", `{"version":1,"sessions":[{"repository":"/repo/../repo","ticket":"t","role":"coder","harness":"codex","id":"s"}],"bounces":{}}`},
		{"duplicate session", `{"version":1,"sessions":[{"ticket":"t","role":"coder","harness":"codex","id":"1"},{"ticket":"t","role":"coder","harness":"codex","id":"2"}],"bounces":{}}`},
		{"trailing value", `{"version":1,"sessions":[],"bounces":{}} {}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, stateFileName), []byte(tt.data), 0o600); err != nil {
				t.Fatalf("write malformed state: %v", err)
			}
			_, err := NewAdministrative(dir).Read(context.Background())
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("Read error = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestReadRejectsSymlinkAndOversizedStateFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires elevated Windows privileges")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, stateFileName)
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte(`{"version":1,"sessions":[],"bounces":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAdministrative(dir).Read(context.Background()); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("symlink state accepted: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, maxStateBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAdministrative(dir).Read(context.Background()); err == nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized state accepted: %v", err)
	}
}

func TestReadRejectsPathReplacementBeforeOpen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink replacement requires elevated Windows privileges")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, stateFileName)
	outside := filepath.Join(t.TempDir(), "outside.json")
	data := []byte(`{"version":1,"sessions":[],"bounces":{},"role_deliveries":[],"control_deliveries":[]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outside, data, 0o600); err != nil {
		t.Fatal(err)
	}
	original := openStateFile
	openStateFile = func(name string) (*os.File, error) {
		if err := os.Remove(name); err != nil {
			return nil, err
		}
		if err := os.Symlink(outside, name); err != nil {
			return nil, err
		}
		return os.Open(name)
	}
	t.Cleanup(func() { openStateFile = original })
	if _, err := NewAdministrative(dir).Read(context.Background()); err == nil || !strings.Contains(err.Error(), "changed while opening") {
		t.Fatalf("replaced state file accepted: %v", err)
	}
}

func TestAtomicStateFileUsesSafePermissions(t *testing.T) {
	dir := t.TempDir()
	store := newStateTestStoreAt(t, dir)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		session := Session{Ticket: "ticket", Role: "coder", Harness: "codex", ID: fmt.Sprintf("session-%d", i)}
		if err := store.SetSession(ctx, session); err != nil {
			t.Fatalf("SetSession: %v", err)
		}
		if _, err := store.Read(ctx); err != nil {
			t.Fatalf("read published state: %v", err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".state-") {
			t.Fatalf("temporary state remains: %s", entry.Name())
		}
	}
	if runtime.GOOS != "windows" {
		for _, name := range []string{stateFileName, lockFileName} {
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("stat %s: %v", name, err)
			}
			if got := info.Mode().Perm(); got != 0o600 {
				t.Fatalf("%s permissions = %o, want 600", name, got)
			}
		}
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat state dir: %v", err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("state dir permissions = %o, want 700", got)
		}
	}
}

func TestOversizedUpdatePreservesReadableState(t *testing.T) {
	ctx := context.Background()
	store := newStateTestStore(t)
	wantSession := Session{Repository: store.repository, Ticket: "existing", Role: "coder", Harness: "codex", ID: "session"}
	if err := store.SetSession(ctx, wantSession); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := store.IncrementBounces(ctx, "existing"); err != nil {
		t.Fatalf("seed bounce: %v", err)
	}

	oversized := Session{
		Ticket:  "oversized",
		Role:    "coder",
		Harness: "codex",
		ID:      strings.Repeat("x", maxStateBytes),
	}
	if err := store.SetSession(ctx, oversized); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("SetSession error = %v, want ErrTooLarge", err)
	}

	snapshot, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read after rejected update: %v", err)
	}
	if len(snapshot.Sessions) != 1 || snapshot.Sessions[0] != wantSession {
		t.Fatalf("sessions changed after rejected update: %#v", snapshot.Sessions)
	}
	if got := snapshot.Bounces[store.bounceKey("existing")]; got != 1 {
		t.Fatalf("bounce count = %d, want 1", got)
	}
}

func TestConcurrentUpdatesDoNotLoseData(t *testing.T) {
	const workers = 6
	const increments = 12
	store := newStateTestStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			session := Session{
				Ticket:  fmt.Sprintf("ticket-%d", worker),
				Role:    "coder",
				Harness: "codex",
				ID:      fmt.Sprintf("session-%d", worker),
			}
			if err := store.SetSession(ctx, session); err != nil {
				errs <- err
				return
			}
			for i := 0; i < increments; i++ {
				if _, err := store.IncrementBounces(ctx, "shared"); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent update: %v", err)
	}
	snapshot, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(snapshot.Sessions) != workers {
		t.Fatalf("session count = %d, want %d", len(snapshot.Sessions), workers)
	}
	if got, want := snapshot.Bounces[store.bounceKey("shared")], workers*increments; got != want {
		t.Fatalf("bounce count = %d, want %d", got, want)
	}
}

func TestIndependentProcessesPreserveUpdatesAndCleanup(t *testing.T) {
	if os.Getenv("TICKET_ORC_STATE_HELPER") != "" {
		t.Skip("parent-only test")
	}
	const workers = 4
	const increments = 10
	dir := t.TempDir()
	store := newStateTestStoreAt(t, dir)
	ctx := context.Background()
	if err := store.SetSession(ctx, Session{Ticket: "terminal", Role: "coder", Harness: "codex", ID: "old"}); err != nil {
		t.Fatalf("seed terminal session: %v", err)
	}
	if _, err := store.IncrementBounces(ctx, "terminal"); err != nil {
		t.Fatalf("seed terminal bounce: %v", err)
	}

	type helperProcess struct {
		command *exec.Cmd
		output  bytes.Buffer
	}
	var helpers []*helperProcess
	for worker := 0; worker < workers; worker++ {
		helpers = append(helpers, &helperProcess{command: stateHelperCommand(dir, "update", worker, increments)})
	}
	helpers = append(helpers, &helperProcess{command: stateHelperCommand(dir, "cleanup", 0, increments)})
	for _, helper := range helpers {
		helper.command.Stdout = &helper.output
		helper.command.Stderr = &helper.output
		if err := helper.command.Start(); err != nil {
			t.Fatalf("start helper: %v", err)
		}
	}
	for _, helper := range helpers {
		if err := helper.command.Wait(); err != nil {
			t.Fatalf("helper failed: %v\n%s", err, helper.output.Bytes())
		}
	}

	snapshot, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got, want := snapshot.Bounces[store.bounceKey("active")], workers*increments; got != want {
		t.Fatalf("active bounce count = %d, want %d", got, want)
	}
	if _, exists := snapshot.Bounces[store.bounceKey("terminal")]; exists {
		t.Fatal("terminal bounce count was resurrected")
	}
	if len(snapshot.Sessions) != workers {
		t.Fatalf("session count = %d, want %d: %#v", len(snapshot.Sessions), workers, snapshot.Sessions)
	}
	for _, session := range snapshot.Sessions {
		if session.Ticket == "terminal" {
			t.Fatalf("terminal session was resurrected: %#v", session)
		}
	}
}

func TestStateProcessHelper(t *testing.T) {
	mode := os.Getenv("TICKET_ORC_STATE_HELPER")
	if mode == "" {
		return
	}
	dir := os.Getenv("TICKET_ORC_TEST_STATE_DIR")
	worker, err := strconv.Atoi(os.Getenv("TICKET_ORC_STATE_WORKER"))
	if err != nil {
		t.Fatalf("parse worker: %v", err)
	}
	iterations, err := strconv.Atoi(os.Getenv("TICKET_ORC_STATE_ITERATIONS"))
	if err != nil {
		t.Fatalf("parse iterations: %v", err)
	}
	store := newStateTestStoreAt(t, dir)
	ctx := context.Background()
	switch mode {
	case "update":
		for i := 0; i < iterations; i++ {
			session := Session{
				Ticket:  fmt.Sprintf("active-%d", worker),
				Role:    "coder",
				Harness: "codex",
				ID:      fmt.Sprintf("session-%d-%d", worker, i),
			}
			if err := store.SetSession(ctx, session); err != nil {
				t.Fatalf("SetSession: %v", err)
			}
			if _, err := store.IncrementBounces(ctx, "active"); err != nil {
				t.Fatalf("IncrementBounces: %v", err)
			}
		}
	case "cleanup":
		for i := 0; i < iterations; i++ {
			if _, _, err := store.RemoveTicket(ctx, "terminal"); err != nil {
				t.Fatalf("RemoveTicket: %v", err)
			}
		}
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
}

func TestLockWaitHonorsContext(t *testing.T) {
	dir := t.TempDir()
	if err := prepareDir(dir); err != nil {
		t.Fatalf("prepareDir: %v", err)
	}
	held, err := acquireLock(context.Background(), dir, time.Second)
	if err != nil {
		t.Fatalf("acquire held lock: %v", err)
	}
	defer held.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	store := NewAdministrative(dir)
	store.lockTimeout = time.Second
	if _, err := store.Read(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read error = %v, want context deadline", err)
	}
}

func TestTryAcquireLockFailsFastAndIsDirectoryScoped(t *testing.T) {
	firstDir := t.TempDir()
	first, err := TryAcquireLock(context.Background(), firstDir)
	if err != nil {
		t.Fatalf("acquire first ownership lock: %v", err)
	}
	defer first.Release()

	started := time.Now()
	if _, err := TryAcquireLock(context.Background(), firstDir); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("contended ownership lock error=%v, want immediate contention", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("contended ownership lock waited %s", elapsed)
	}

	second, err := TryAcquireLock(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("distinct state root could not acquire ownership lock: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release distinct ownership lock: %v", err)
	}
}

func TestCanceledContextDoesNotInitializeState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewAdministrative(dir).Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read error = %v, want context canceled", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("canceled read initialized state directory: %v", err)
	}
}

func TestSerializedSessionsHaveStableOrder(t *testing.T) {
	store := newStateTestStore(t)
	ctx := context.Background()
	input := []Session{
		{Ticket: "z", Role: "coder", Harness: "codex", ID: "1"},
		{Ticket: "a", Role: "reviewer", Harness: "codex", ID: "2"},
		{Ticket: "a", Role: "coder", Harness: "pi", ID: "3"},
		{Ticket: "a", Role: "coder", Harness: "codex", ID: "4"},
	}
	for _, session := range input {
		if err := store.SetSession(ctx, session); err != nil {
			t.Fatalf("SetSession: %v", err)
		}
	}
	snapshot, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	keys := make([]string, 0, len(snapshot.Sessions))
	for _, session := range snapshot.Sessions {
		keys = append(keys, sessionKey(session))
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("session keys are not sorted: %q", keys)
	}
}

func stateHelperCommand(dir, mode string, worker, iterations int) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestStateProcessHelper$")
	command.Env = append(os.Environ(),
		"TICKET_ORC_STATE_HELPER="+mode,
		"TICKET_ORC_TEST_STATE_DIR="+dir,
		"TICKET_ORC_STATE_WORKER="+strconv.Itoa(worker),
		"TICKET_ORC_STATE_ITERATIONS="+strconv.Itoa(iterations),
	)
	return command
}
