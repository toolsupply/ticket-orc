package spool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
)

func testRegistration() state.SteerRegistration {
	return state.SteerRegistration{
		RegistrationID: "0123456789abcdef0123456789abcdef",
		IncarnationID:  "abcdef0123456789abcdef0123456789",
		Harness:        "some-future-harness",
		SessionID:      "session:opaque/id",
		Transport:      state.SteerTransportRoute{Kind: Kind},
	}
}

func TestPrepareDerivesGenericEndpointAndWritesPrivateManifest(t *testing.T) {
	root := t.TempDir()
	registration := testRegistration()
	endpoint, err := New().Prepare(context.Background(), root, registration)
	if err != nil {
		t.Fatal(err)
	}
	wantRoot := filepath.Join(root, "steer-spool", registration.RegistrationID, registration.IncarnationID)
	if endpoint.Kind != Kind || endpoint.Protocol != ProtocolVersion || endpoint.Root != wantRoot ||
		endpoint.Pending != filepath.Join(wantRoot, "pending") || endpoint.Control != filepath.Join(wantRoot, "control") ||
		endpoint.Rejected != filepath.Join(wantRoot, "rejected") {
		t.Fatalf("endpoint = %#v", endpoint)
	}
	data, err := os.ReadFile(filepath.Join(wantRoot, "registration.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := decodeStrict(data, &manifest); err != nil || manifest != manifestFor(registration) {
		t.Fatalf("manifest = %#v, err=%v", manifest, err)
	}
	if runtime.GOOS != "windows" {
		for _, path := range []string{filepath.Join(root, "steer-spool"), filepath.Dir(wantRoot), wantRoot,
			endpoint.Pending, endpoint.Control, endpoint.Rejected} {
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0o700 {
				t.Errorf("directory %s permissions = %v, err=%v", path, info.Mode().Perm(), err)
			}
		}
		info, err := os.Stat(filepath.Join(wantRoot, "registration.json"))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("manifest permissions = %v, err=%v", info.Mode().Perm(), err)
		}
	}
	if _, err := New().Prepare(context.Background(), root, registration); err != nil {
		t.Fatalf("idempotent prepare: %v", err)
	}
}

func TestVerifyRejectsMissingEndpointWithoutRecreatingIt(t *testing.T) {
	root := t.TempDir()
	registration := testRegistration()
	transport := New()
	endpoint, err := transport.Prepare(context.Background(), root, registration)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(endpoint.Root); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Verify(context.Background(), root, registration); err == nil {
		t.Fatal("Verify accepted a missing endpoint")
	}
	if _, err := os.Lstat(endpoint.Root); !os.IsNotExist(err) {
		t.Fatalf("Verify recreated the missing endpoint: %v", err)
	}
}

func TestVerifyDoesNotRepairDirectoryPermissions(t *testing.T) {
	root := t.TempDir()
	registration := testRegistration()
	transport := New()
	endpoint, err := transport.Prepare(context.Background(), root, registration)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(endpoint.Root, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(endpoint.Root)
	if err != nil {
		t.Fatal(err)
	}
	_, verifyErr := transport.Verify(context.Background(), root, registration)
	if privateDirectoryMode(before) && verifyErr != nil {
		t.Fatalf("Verify rejected a private endpoint: %v", verifyErr)
	}
	if !privateDirectoryMode(before) && verifyErr == nil {
		t.Fatal("Verify accepted a directory without the required private mode")
	}
	after, err := os.Stat(endpoint.Root)
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("Verify changed endpoint mode from %04o to %04o", before.Mode().Perm(), after.Mode().Perm())
	}
}

func TestPrepareRejectsCallerPathsAndInvalidRegistrationIDs(t *testing.T) {
	for name, registration := range map[string]state.SteerRegistration{
		"parameters": func() state.SteerRegistration {
			r := testRegistration()
			r.Transport.Params = map[string]string{"path": "/caller/selected"}
			return r
		}(),
		"registration ID": func() state.SteerRegistration {
			r := testRegistration()
			r.RegistrationID = "../not-an-id"
			return r
		}(),
		"incarnation ID": func() state.SteerRegistration {
			r := testRegistration()
			r.IncarnationID = strings.Repeat("A", 32)
			return r
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New().Prepare(context.Background(), t.TempDir(), registration); err == nil {
				t.Fatal("invalid spool registration was accepted")
			}
		})
	}
}

func TestDeliverPublishesCompleteStrictProtocolAndSupersedesPending(t *testing.T) {
	root := t.TempDir()
	registration := testRegistration()
	transport := New()
	endpoint, err := transport.Prepare(context.Background(), root, registration)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Deliver(context.Background(), root, registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "first"}); err != nil {
		t.Fatal(err)
	}
	first := filesIn(t, endpoint.Pending)
	if len(first) != 1 {
		t.Fatalf("first pending files = %v", first)
	}
	if err := transport.Deliver(context.Background(), root, registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "second"}); err != nil {
		t.Fatal(err)
	}
	second := filesIn(t, endpoint.Pending)
	if len(second) != 1 || second[0] == first[0] {
		t.Fatalf("stale pending was not superseded: first=%v second=%v", first, second)
	}
	data, err := os.ReadFile(filepath.Join(endpoint.Pending, second[0]))
	if err != nil {
		t.Fatal(err)
	}
	message, err := DecodeMessage(data, registration)
	if err != nil || message.Protocol != 1 || message.Kind != steertransport.MessageSteer || message.Message != "second" {
		t.Fatalf("decoded message = %#v, err=%v", message, err)
	}
	if len(data) == 0 || data[len(data)-1] != '}' {
		t.Fatalf("published message is incomplete: %q", data)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(endpoint.Pending, second[0]))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("message permissions = %v, err=%v", info.Mode().Perm(), err)
		}
	}
}

func TestPublishUsesSameDirectoryTemporaryFileAndAtomicRename(t *testing.T) {
	dir := t.TempDir()
	want := []byte(`{"protocol":1,"message":"complete"}`)
	called := false
	err := publishWithRename(dir, "message.json", want, func(from, to string) error {
		called = true
		if filepath.Dir(from) != dir || filepath.Dir(to) != dir || !strings.HasPrefix(filepath.Base(from), ".spool-") || filepath.Ext(from) != ".tmp" {
			t.Fatalf("publication paths are not same-directory temp and target: from=%q to=%q", from, to)
		}
		if _, err := os.Lstat(to); !os.IsNotExist(err) {
			t.Fatalf("target was visible before atomic rename: %v", err)
		}
		data, err := os.ReadFile(from)
		if err != nil || !bytes.Equal(data, want) {
			t.Fatalf("temporary content=%q err=%v", data, err)
		}
		return os.Rename(from, to)
	})
	if err != nil || !called {
		t.Fatalf("atomic publication called=%t err=%v", called, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "message.json" {
		t.Fatalf("publication left partial files: entries=%v err=%v", entries, err)
	}
}

func TestDeliverRejectsOversizedEncodedMessage(t *testing.T) {
	root := t.TempDir()
	registration := testRegistration()
	transport := New()
	endpoint, err := transport.Prepare(context.Background(), root, registration)
	if err != nil {
		t.Fatal(err)
	}
	err = transport.Deliver(context.Background(), root, registration, steertransport.Message{
		Kind: steertransport.MessageSteer, Text: strings.Repeat("x", MaxMessageBytes),
	})
	if err == nil || !steertransport.IsRejected(err) {
		t.Fatalf("oversized delivery error=%v, want definite rejection", err)
	}
	if pending := filesIn(t, endpoint.Pending); len(pending) != 0 {
		t.Fatalf("oversized message was published: %v", pending)
	}
}

func TestPublishedMessageSurvivesTransportRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	candidate := testRegistration()
	candidate.RepositoryID = "d659917f-5939-4e93-bfde-6346a0f2bc50"
	candidate.RepositoryPath = root
	candidate.Actor = "worker"
	candidate.Role = "coder"
	registration, _, _, err := state.NewRegistrationStore(root).Join(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	transport := New()
	endpoint, err := transport.Prepare(ctx, root, registration)
	if err != nil {
		t.Fatal(err)
	}
	runtimeStore := state.NewSteerRuntimeStore(root)
	if err := runtimeStore.Reconcile(ctx, []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtimeStore.Update(ctx, registration, "sending", ""); err != nil || !updated {
		t.Fatalf("persist sending state updated=%t err=%v", updated, err)
	}
	if err := transport.Deliver(ctx, root, registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "work"}); err != nil {
		t.Fatal(err)
	}

	// A fresh transport and state store model Orc restarting after publication
	// but before it records the delivery completion.
	restarted := New()
	if _, err := restarted.Verify(ctx, root, registration); err != nil {
		t.Fatalf("restart could not verify active endpoint: %v", err)
	}
	snapshot, err := state.NewSteerRuntimeStore(root).Snapshot(ctx)
	if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].State != "sending" {
		t.Fatalf("restart runtime snapshot=%#v err=%v", snapshot, err)
	}
	pending := filesIn(t, endpoint.Pending)
	if len(pending) != 1 {
		t.Fatalf("published message did not survive restart: %v", pending)
	}
	data, err := os.ReadFile(filepath.Join(endpoint.Pending, pending[0]))
	if err != nil {
		t.Fatal(err)
	}
	message, err := DecodeMessage(data, registration)
	if err != nil || message.Message != "work" {
		t.Fatalf("restart message=%#v err=%v", message, err)
	}
}

func TestReconcileRemovesOnlyStaleSpoolIncarnations(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	candidate := testRegistration()
	candidate.RepositoryID = "d659917f-5939-4e93-bfde-6346a0f2bc50"
	candidate.RepositoryPath = root
	candidate.Actor = "worker"
	candidate.Role = "coder"
	store := state.NewRegistrationStore(root)
	current, _, _, err := store.Join(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	stale := current
	stale.IncarnationID = "33333333333333333333333333333333"
	other := current
	other.RegistrationID = "44444444444444444444444444444444"
	other.IncarnationID = "55555555555555555555555555555555"
	transport := New()
	currentEndpoint, err := transport.Prepare(ctx, root, current)
	if err != nil {
		t.Fatal(err)
	}
	staleEndpoint, err := transport.Prepare(ctx, root, stale)
	if err != nil {
		t.Fatal(err)
	}
	otherEndpoint, err := transport.Prepare(ctx, root, other)
	if err != nil {
		t.Fatal(err)
	}
	retired := filepath.Join(filepath.Dir(currentEndpoint.Root), ".retired-dead-incarnation")
	if err := os.Mkdir(retired, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := store.WithSnapshot(ctx, func(snapshot state.SteerSnapshot) error {
		return transport.Reconcile(ctx, root, snapshot.Registrations)
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{staleEndpoint.Root, otherEndpoint.Root, retired} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("stale endpoint %s remains: %v", path, err)
		}
	}
	if _, err := os.Stat(currentEndpoint.Root); err != nil {
		t.Fatalf("current endpoint was removed: %v", err)
	}
}

func TestStopUsesPriorityLaneAndOrdinaryDeliveryPreservesControl(t *testing.T) {
	root := t.TempDir()
	registration := testRegistration()
	transport := New()
	endpoint, err := transport.Prepare(context.Background(), root, registration)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Deliver(context.Background(), root, registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "stale"}); err != nil {
		t.Fatal(err)
	}
	if err := transport.Deliver(context.Background(), root, registration, steertransport.Message{Kind: steertransport.MessageStop, Text: "stop now"}); err != nil {
		t.Fatal(err)
	}
	if pending := filesIn(t, endpoint.Pending); len(pending) != 0 {
		t.Fatalf("stop left ordinary work pending: %v", pending)
	}
	control := filesIn(t, endpoint.Control)
	if len(control) != 1 {
		t.Fatalf("control files = %v", control)
	}
	if err := transport.Deliver(context.Background(), root, registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "later ordinary"}); err != nil {
		t.Fatal(err)
	}
	if got := filesIn(t, endpoint.Control); len(got) != 1 || got[0] != control[0] {
		t.Fatalf("ordinary delivery changed control lane: %v", got)
	}
	pending := filesIn(t, endpoint.Pending)
	if len(pending) != 1 {
		t.Fatalf("ordinary lane after stop=%v", pending)
	}
	var first WireMessage
	for _, lane := range []string{endpoint.Control, endpoint.Pending} {
		files := filesIn(t, lane)
		if len(files) == 0 {
			continue
		}
		data, err := os.ReadFile(filepath.Join(lane, files[0]))
		if err != nil {
			t.Fatal(err)
		}
		first, err = DecodeMessage(data, registration)
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if first.Kind != steertransport.MessageStop {
		t.Fatalf("consumer checking control before pending should select stop first: message=%#v", first)
	}
}

func TestStopStillPublishesControlWhenPendingCleanupFails(t *testing.T) {
	root := t.TempDir()
	registration := testRegistration()
	transport := New()
	endpoint, err := transport.Prepare(context.Background(), root, registration)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "missing-target"), filepath.Join(endpoint.Pending, "unsafe.json")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	err = transport.Deliver(context.Background(), root, registration, steertransport.Message{Kind: steertransport.MessageStop, Text: "stop now"})
	if err == nil || !steertransport.IsUncertain(err) {
		t.Fatalf("cleanup failure outcome = %v", err)
	}
	if control := filesIn(t, endpoint.Control); len(control) != 1 {
		t.Fatalf("stop was not published to priority lane: %v", control)
	}
}

func TestDecodeMessageRejectsMalformedUnsupportedAndMismatchedIdentity(t *testing.T) {
	registration := testRegistration()
	valid := WireMessage{Protocol: 1, MessageID: "11111111111111111111111111111111", Kind: steertransport.MessageSteer,
		RegistrationID: registration.RegistrationID, IncarnationID: registration.IncarnationID,
		Harness: registration.Harness, SessionID: registration.SessionID, Message: "hello"}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeMessage(encoded, registration); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
	unknown := strings.TrimSuffix(string(encoded), "}") + `,"unexpected":true}`
	badTrailing := string(encoded) + ` {}`
	oversize := strings.Repeat("x", MaxMessageBytes+1)
	tests := map[string][]byte{
		"unknown field": []byte(unknown), "trailing data": []byte(badTrailing),
		"oversize": []byte(oversize), "malformed": []byte(`{"protocol":`),
		"duplicate field": []byte(strings.TrimSuffix(string(encoded), "}") + `,"protocol":1}`),
	}
	mutations := map[string]func(*WireMessage){
		"protocol":      func(m *WireMessage) { m.Protocol = 2 },
		"message id":    func(m *WireMessage) { m.MessageID = "../escape" },
		"registration":  func(m *WireMessage) { m.RegistrationID = "ffffffffffffffffffffffffffffffff" },
		"incarnation":   func(m *WireMessage) { m.IncarnationID = "ffffffffffffffffffffffffffffffff" },
		"harness":       func(m *WireMessage) { m.Harness = "other" },
		"session":       func(m *WireMessage) { m.SessionID = "other" },
		"kind":          func(m *WireMessage) { m.Kind = "other" },
		"empty message": func(m *WireMessage) { m.Message = "" },
		"NUL message":   func(m *WireMessage) { m.Message = "bad\x00text" },
	}
	for name, mutate := range mutations {
		copy := valid
		mutate(&copy)
		data, _ := json.Marshal(copy)
		tests[name] = data
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeMessage(data, registration); err == nil {
				t.Fatal("invalid message accepted")
			}
		})
	}
}

func TestPrepareAndDeliverRejectSymlinkedSpoolComponents(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, "steer-spool")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := New().Prepare(context.Background(), root, testRegistration()); err == nil {
		t.Fatal("Prepare accepted a symlinked spool root")
	}

	root2 := t.TempDir()
	registration := testRegistration()
	endpoint, err := New().Prepare(context.Background(), root2, registration)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(endpoint.Pending); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, endpoint.Pending); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	err = New().Deliver(context.Background(), root2, registration, steertransport.Message{Kind: steertransport.MessageSteer, Text: "unsafe"})
	if err == nil || steertransport.IsUncertain(err) {
		t.Fatalf("Deliver through symlink: err=%v", err)
	}
}

func TestRetireRemovesOnlyExactIncarnation(t *testing.T) {
	root := t.TempDir()
	transport := New()
	old := testRegistration()
	current := old
	current.IncarnationID = "22222222222222222222222222222222"
	oldEndpoint, err := transport.Prepare(context.Background(), root, old)
	if err != nil {
		t.Fatal(err)
	}
	currentEndpoint, err := transport.Prepare(context.Background(), root, current)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Retire(context.Background(), root, old); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldEndpoint.Root); !os.IsNotExist(err) {
		t.Fatalf("retired endpoint still exists: %v", err)
	}
	if _, err := os.Stat(currentEndpoint.Root); err != nil {
		t.Fatalf("retiring old incarnation removed current endpoint: %v", err)
	}
}

func TestPublicationErrorAfterVisibleRenameIsUncertain(t *testing.T) {
	root := t.TempDir()
	messageID := strings.Repeat("f", 32)
	publicationErr := errors.New("simulated ambiguous rename error")
	err := publishWithRename(root, messageID+".json", []byte(`{"complete":true}`), func(from, to string) error {
		if err := os.Rename(from, to); err != nil {
			return err
		}
		return publicationErr
	})
	if err == nil || !steertransport.IsUncertain(err) || !errors.Is(err, publicationErr) {
		t.Fatalf("ambiguous publication error = %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(root, messageID+".json")); err != nil || string(data) != `{"complete":true}` {
		t.Fatalf("rename published data=%q err=%v", data, err)
	}
}

func filesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	result := make([]string, len(entries))
	for i, entry := range entries {
		result[i] = entry.Name()
	}
	return result
}

var _ steertransport.Transport = (*Transport)(nil)
