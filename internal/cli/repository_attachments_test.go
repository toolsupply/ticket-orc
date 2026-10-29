package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

const attachmentOtherRepo = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
const attachmentTestID = "20261025-00001"

// Public Ticket protocol fixture. Only supplementary storage directories are
// created by these tests; no managed Ticket metadata is read or written.
func runAttachmentTicket() int {
	repo := os.Getenv("TICKET_REPOSITORY")
	id := joinTestRepositoryID
	if filepath.Base(repo) == "second" {
		id = attachmentOtherRepo
	}
	log, err := os.OpenFile(os.Getenv("ORC_ATTACHMENT_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return 90
	}
	defer log.Close()
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		var req struct {
			Args []string `json:"args"`
		}
		if json.Unmarshal(scan.Bytes(), &req) != nil || len(req.Args) == 0 {
			return 91
		}
		json.NewEncoder(log).Encode(map[string]any{"actor": os.Getenv("TICKET_ACTOR"), "repository": repo, "args": req.Args})
		var result any
		switch req.Args[0] {
		case "info":
			result = map[string]any{"id": id, "path": repo}
		case "show", "path":
			if len(req.Args) < 2 {
				return 92
			}
			if req.Args[1] != attachmentTestID {
				fmt.Fprintln(os.Stdout, `{"error":{"code":"not_found","message":"private/path"}}`)
				continue
			}
			result = map[string]any{"id": attachmentTestID, "path": "fixture/TASK.md", "archived": false}
		default:
			return 93 // Claim/update/lifecycle commands are forbidden.
		}
		json.NewEncoder(os.Stdout).Encode(result)
	}
	return 0
}

func attachmentGatewayFixture(t *testing.T) (*workerManager, daemon.RepositoryGateway, string, string) {
	t.Helper()
	// Attachment traversal rejects symlinked ancestors. macOS TempDir paths
	// commonly pass through /var -> /private/var; route the fixture by its
	// physical path, just as the attachment domain fixtures do.
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	name := "ticket"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, name), binary, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("ORC_TEST_ATTACHMENTS", "1")
	log := filepath.Join(dir, "calls.jsonl")
	t.Setenv("ORC_ATTACHMENT_LOG", log)
	repos := map[string]supervisor.ConfiguredRepository{}
	for id, key := range map[string]string{joinTestRepositoryID: "first", attachmentOtherRepo: "second"} {
		root := filepath.Join(dir, key)
		if err = os.MkdirAll(filepath.Join(root, "fixture"), 0700); err != nil {
			t.Fatal(err)
		}
		repos[id] = supervisor.ConfiguredRepository{ID: id, Key: key, Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: root}}
	}
	m := &workerManager{repositoriesByID: repos}
	return m, m.repositoryGateway(), dir, log
}

func TestAttachmentGatewayComposition(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable")
	}
	m, g, dir, log := attachmentGatewayFixture(t)
	ctx := context.Background()
	var events []supervisor.RuntimeEvent
	m.runtimeEventSink = func(e supervisor.RuntimeEvent) { events = append(events, e) }
	create := attachments.Condition{Create: true}
	for _, repo := range []string{joinTestRepositoryID, attachmentOtherRepo} {
		listing, err := g.ListAttachments(ctx, repo, attachmentTestID, daemon.RepositoryAttachmentQuery{})
		if err != nil || len(listing.Entries) != 0 {
			t.Fatalf("absent listing=%+v err=%v", listing, err)
		}
		r, err := g.WriteAttachment(ctx, repo, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "binary.zip", Condition: create}, strings.NewReader("opaque bytes"), 12, attachmentTestCommitContext)
		if err != nil || !r.MutationApplied || r.Actor != "ui-user" || r.RepositoryID != repo {
			t.Fatalf("receipt=%+v err=%v", r, err)
		}
		d, err := g.ReadAttachment(ctx, repo, attachmentTestID, "binary.zip")
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(d.Content)
		closeErr := d.Content.Close()
		if readErr != nil || closeErr != nil || string(data) != "opaque bytes" {
			t.Fatalf("download=%q err=%v close=%v", data, readErr, closeErr)
		}
	}
	if len(events) != 2 {
		t.Fatalf("events=%+v", events)
	}
	for i, e := range events {
		public := daemon.RuntimeEventDTO(e)
		if public.Actor != "ui-user" || public.RepositoryID != e.RepositoryID || !public.Applied {
			t.Fatalf("public invalidation=%+v", public)
		}
		repo := joinTestRepositoryID
		if i == 1 {
			repo = attachmentOtherRepo
		}
		if e.Type != "ticket.repository_changed" || e.Code != "attachments_changed" || e.RepositoryID != repo || e.Ticket != attachmentTestID || e.Actor != "ui-user" || !e.Applied {
			t.Fatalf("event=%+v", e)
		}
	}
	// File size and tree/path bounds retain distinct HTTP classifications.
	_, err := g.WriteAttachment(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "oversized", Condition: create}, strings.NewReader(""), attachments.V1Limits().MaxFileBytes+1, attachmentTestCommitContext)
	assertAttachmentMutationError(t, err, "limit_exceeded", 413, false, true)
	// A rejected upload must not publish or replace content.
	_, err = g.WriteAttachment(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "binary.zip", Condition: create}, strings.NewReader("bad"), 3, attachmentTestCommitContext)
	assertAttachmentMutationError(t, err, "revision_conflict", 412, false, true)
	if len(events) != 2 {
		t.Fatalf("false commit events=%+v", events)
	}
	// Simulate a lost event: reconcile solely from fresh list/read and use its
	// authoritative revision for subsequent operations.
	m.runtimeEventSink = nil
	folder, err := g.CreateAttachmentDirectory(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "folder", Condition: create})
	if err != nil {
		t.Fatal(err)
	}
	listing, err := g.ListAttachments(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentQuery{})
	if err != nil || len(listing.Entries) != 2 {
		t.Fatalf("reconcile=%+v %v", listing, err)
	}
	var file attachments.Entry
	for _, e := range listing.Entries {
		if e.Kind == "file" {
			file = e
		}
	}
	moved, err := g.MoveAttachment(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentMoveRequest{Actor: "ui-user", From: file.Path, To: "folder/new.zip", Revision: file.Revision, Condition: create})
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.DeleteAttachment(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: moved.Entry.Path, Condition: attachments.Condition{Revision: moved.Entry.Revision}})
	if err != nil {
		t.Fatal(err)
	}
	// The directory revision changes with every mutation: refresh after event loss.
	listing, err = g.ListAttachments(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentQuery{})
	if err != nil {
		t.Fatal(err)
	}
	folder.Entry = &listing.Entries[0]
	_, err = g.DeleteAttachment(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "folder", Condition: attachments.Condition{Revision: folder.Entry.Revision}})
	if err != nil {
		t.Fatal(err)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(calls)), "\n") {
		var c struct {
			Actor, Repository string
			Args              []string
		}
		if json.Unmarshal([]byte(line), &c) != nil {
			t.Fatal(line)
		}
		if c.Actor != "ui-user" && c.Actor != repositoryGatewayActor {
			t.Fatalf("actor=%q", c.Actor)
		}
		if c.Repository != filepath.Join(dir, "first") && c.Repository != filepath.Join(dir, "second") {
			t.Fatalf("routing=%q", c.Repository)
		}
	}
}

func assertAttachmentMutationError(t *testing.T, err error, code string, status int, applied, known bool) {
	t.Helper()
	var failure *daemon.RepositoryMutationError
	if !errors.As(err, &failure) || failure.Code != code || failure.Status != status || failure.Applied != applied || failure.AppliedKnown != known {
		t.Fatalf("failure=%+v want %s %d applied=%v known=%v", err, code, status, applied, known)
	}
}
func TestAttachmentGatewayFailures(t *testing.T) {
	m, g, dir, _ := attachmentGatewayFixture(t)
	ctx := context.Background()
	for _, tc := range []struct {
		repo, id, code string
		status         int
	}{
		{"dynamic", attachmentTestID, "repository_not_found", 404},
		{joinTestRepositoryID, "partial", "invalid_ticket", 400},
		{joinTestRepositoryID, "20261025-00002", "ticket_not_found", 404},
	} {
		_, err := g.ListAttachments(ctx, tc.repo, tc.id, daemon.RepositoryAttachmentQuery{})
		var failure *daemon.RepositoryReadError
		if !errors.As(err, &failure) || failure.Code != tc.code || failure.Status != tc.status {
			t.Fatalf("failure=%+v", err)
		}
	}
	_, err := g.CreateAttachmentDirectory(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "bad actor", Path: "a", Condition: attachments.Condition{Create: true}})
	assertAttachmentMutationError(t, err, "invalid_actor", 400, false, true)
	// Missing resolved local roots fail closed without creating a ticket directory.
	if err = os.Remove(filepath.Join(dir, "first", "fixture")); err != nil {
		t.Fatal(err)
	}
	_, err = g.ListAttachments(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentQuery{})
	var failure *daemon.RepositoryReadError
	if !errors.As(err, &failure) || failure.Code != "entry_not_found" {
		t.Fatalf("missing ticket dir: %v", err)
	}
	m.repositoriesByID = nil
	_, err = g.DeleteAttachment(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user"})
	assertAttachmentMutationError(t, err, "repository_not_found", 404, false, true)
}
func TestAttachmentErrorCertaintyAndCommittedInvalidation(t *testing.T) {
	for code, status := range map[string]int{"invalid_path": 400, "entry_not_found": 404, "revision_conflict": 412, "directory_not_empty": 409, "precondition_required": 428, "limit_exceeded": 422, "capability_unavailable": 422, "attachment_busy": 503, "attachment_storage_failed": 502, "mutation_outcome_unknown": 503} {
		for _, certainty := range []*bool{nil, boolPointer(false), boolPointer(true)} {
			err := attachmentFailure(&attachments.Error{Code: code, MutationApplied: certainty}, true, false)
			assertAttachmentMutationError(t, err, code, status, certainty != nil && *certainty, certainty != nil)
		}
	}
	var events []supervisor.RuntimeEvent
	m := &workerManager{runtimeEventSink: func(e supervisor.RuntimeEvent) { events = append(events, e) }}
	r := attachments.Receipt{RepositoryID: joinTestRepositoryID, TicketID: attachmentTestID, MutationApplied: true}
	got, err := m.attachmentMutationResult(joinTestRepositoryID, attachmentTestID, "ui-user", r, errors.New("private/path"))
	assertAttachmentMutationError(t, err, "attachment_storage_failed", 502, true, true)
	if !got.MutationApplied || len(events) != 1 {
		t.Fatalf("receipt=%+v events=%+v", got, events)
	}
	if strings.Contains(err.Error(), "private") {
		t.Fatalf("unsafe error: %v", err)
	}
	_, err = m.attachmentMutationResult(joinTestRepositoryID, attachmentTestID, "ui-user", attachments.Receipt{}, &attachments.Error{Code: "mutation_outcome_unknown"})
	assertAttachmentMutationError(t, err, "mutation_outcome_unknown", 503, false, false)
	if len(events) != 1 {
		t.Fatal("unknown result emitted committed event")
	}
}
func boolPointer(v bool) *bool { return &v }

func TestAttachmentGatewayTransferAdmissionAndCleanup(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable")
	}
	m, g, _, _ := attachmentGatewayFixture(t)
	ctx := context.Background()
	_, err := g.WriteAttachment(ctx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "file", Condition: attachments.Condition{Create: true}}, strings.NewReader("data"), 4, attachmentTestCommitContext)
	if err != nil {
		t.Fatal(err)
	}
	// Resolution failures must release transfer admission.
	for i := 0; i < attachments.V1Limits().MaxActiveTransfers+1; i++ {
		_, err = g.ReadAttachment(ctx, joinTestRepositoryID, "20261025-00002", "file")
		var failure *daemon.RepositoryReadError
		if !errors.As(err, &failure) || failure.Code != "ticket_not_found" {
			t.Fatalf("resolution cleanup: %v", err)
		}
	}
	var downloads []io.ReadCloser
	defer func() {
		for _, d := range downloads {
			d.Close()
		}
	}()
	for i := 0; i < attachments.V1Limits().MaxActiveTransfers; i++ {
		d, err := g.ReadAttachment(ctx, joinTestRepositoryID, attachmentTestID, "file")
		if err != nil {
			t.Fatal(err)
		}
		downloads = append(downloads, d.Content)
	}
	// Reconstructing a gateway must not create another daemon transfer pool.
	other := m.repositoryGateway()
	_, err = other.ReadAttachment(ctx, joinTestRepositoryID, attachmentTestID, "file")
	var failure *daemon.RepositoryReadError
	if !errors.As(err, &failure) || failure.Code != "attachment_busy" {
		t.Fatalf("admission: %v", err)
	}
	downloads[0].Close()
	d, err := other.ReadAttachment(ctx, joinTestRepositoryID, attachmentTestID, "file")
	if err != nil {
		t.Fatal(err)
	}
	d.Content.Close()
}

func attachmentTestCommitContext() (context.Context, context.CancelFunc, error) {
	return context.Background(), func() {}, nil
}

func TestAttachmentPublishedCancellationKeepsReceiptAndInvalidation(t *testing.T) {
	var events []supervisor.RuntimeEvent
	m := &workerManager{runtimeEventSink: func(e supervisor.RuntimeEvent) { events = append(events, e) }}
	receipt := attachments.Receipt{RepositoryID: joinTestRepositoryID, TicketID: attachmentTestID, Actor: "ui-user", Operation: "upload", MutationApplied: true}
	got, err := m.attachmentMutationResult(joinTestRepositoryID, attachmentTestID, "ui-user", receipt, context.Canceled)
	assertAttachmentMutationError(t, err, "attachment_storage_failed", 502, true, true)
	if got != receipt || len(events) != 1 || !events[0].Applied || events[0].Code != "attachments_changed" {
		t.Fatalf("published receipt=%+v events=%+v", got, events)
	}
}
