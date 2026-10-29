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
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// This fixture uses the installed public Ticket CLI, never a replacement binary
// or direct managed-file construction. Only child processes select test routing.
func realAttachmentRepository(t *testing.T, binary, root string) (string, string) {
	t.Helper()
	run := func(args ...string) map[string]any {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), binary, args...)
		for _, e := range os.Environ() {
			if !strings.HasPrefix(e, "TICKET_") {
				cmd.Env = append(cmd.Env, e)
			}
		}
		cmd.Env = append(cmd.Env, "TICKET_REPOSITORY="+root, "TICKET_ACTOR=attachment-test")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("ticket %v: %v: %s", args, err, out)
		}
		var result map[string]any
		if err = json.Unmarshal(out, &result); err != nil {
			t.Fatalf("ticket JSON: %s: %v", out, err)
		}
		return result
	}
	run("init", "-j")
	info := run("info", "-j")
	created := run("create", "Attachment integration fixture", "Controlled temporary repository validation", "-j")
	repo, ok := info["id"].(string)
	if !ok {
		t.Fatalf("info=%v", info)
	}
	id, ok := created["id"].(string)
	if !ok {
		t.Fatalf("create=%v", created)
	}
	return repo, id
}
func TestAttachmentClientRealTicketEndToEnd(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable on this platform")
	}
	binary, err := exec.LookPath("ticket")
	if err != nil {
		t.Skip("installed Ticket CLI unavailable")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo, id := realAttachmentRepository(t, binary, filepath.Join(root, "first"))
	other, otherID := realAttachmentRepository(t, binary, filepath.Join(root, "second"))
	m := &workerManager{repositoriesByID: map[string]supervisor.ConfiguredRepository{}}
	for rid, name := range map[string]string{repo: "first", other: "second"} {
		m.repositoriesByID[rid] = supervisor.ConfiguredRepository{ID: rid, Key: name, Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: filepath.Join(root, name)}}
	}
	s, err := daemon.NewServer(daemon.Config{EndpointKey: testDeleteKey(t), StateDir: t.TempDir(), RepositoryGateway: m.repositoryGateway()})
	if err != nil {
		t.Fatal(err)
	}
	m.runtimeEventSink = func(e supervisor.RuntimeEvent) { s.PublishEvent(daemon.RuntimeEventDTO(e)) }
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	if err = s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); waitForTestDaemonStopped(t, s) })
	c, err := daemonclient.NewWithEndpoint("", s.Endpoint().CapabilityURL(), "")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := c.Events(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	list := func(rid, tid, path string) attachments.Listing {
		t.Helper()
		l, e := c.ListRepositoryAttachments(ctx, rid, tid, daemon.RepositoryAttachmentQuery{Path: path})
		if e != nil {
			t.Fatal(e)
		}
		return l
	}
	if l := list(repo, id, ""); len(l.Entries) != 0 {
		t.Fatal(l)
	}
	create := attachments.Condition{Create: true}
	req := daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "folder", Condition: create}
	folder, err := c.CreateRepositoryAttachmentDirectory(ctx, repo, id, req)
	if err != nil {
		t.Fatal(err)
	}
	if !folder.Created || folder.Actor != "ui-user" {
		t.Fatal(folder)
	}
	event, err := stream.Next()
	if err != nil || event.Code != "attachments_changed" || event.RepositoryID != repo || event.Actor != "ui-user" || !event.Applied {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	// ZIP is deliberately opaque, including bytes that are not a valid archive.
	payload := append([]byte{'P', 'K', 3, 4, 0, 255}, bytes.Repeat([]byte{0, 1, 128, 255}, (2<<20)/4+1)...)
	req.Path = "folder/archive ü.zip"
	uploaded, err := c.WriteRepositoryAttachment(ctx, repo, id, req, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	read := func(path string) []byte {
		t.Helper()
		d, e := c.ReadRepositoryAttachment(ctx, repo, id, path)
		if e != nil {
			t.Fatal(e)
		}
		defer d.Content.Close()
		b, e := io.ReadAll(d.Content)
		if e != nil || d.Length != int64(len(b)) || d.ETag != `"`+d.Entry.Revision+`"` || d.Entry.Name != "archive ü.zip" {
			t.Fatalf("download=%+v err=%v", d, e)
		}
		return b
	}
	if !bytes.Equal(read(req.Path), payload) {
		t.Fatal("binary changed")
	}
	if len(list(other, otherID, "").Entries) != 0 {
		t.Fatal("repository scope crossed")
	}
	if _, err = c.CreateRepositoryAttachmentDirectory(ctx, other, otherID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "folder", Condition: create}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.WriteRepositoryAttachment(ctx, other, otherID, req, strings.NewReader("other repository"), 16); err != nil {
		t.Fatal(err)
	}
	isolated, err := c.ReadRepositoryAttachment(ctx, other, otherID, req.Path)
	if err != nil {
		t.Fatal(err)
	}
	isolatedBytes, err := io.ReadAll(isolated.Content)
	isolated.Content.Close()
	if err != nil || string(isolatedBytes) != "other repository" || !bytes.Equal(read(req.Path), payload) {
		t.Fatal("matching paths crossed repository scope")
	}
	assertError := func(e error, code string, applied bool) {
		t.Helper()
		var ce *daemonclient.Error
		if !errors.As(e, &ce) || ce.Code != code || !ce.AppliedKnown || ce.Applied != applied {
			t.Fatalf("want %s applied=%v: %+v", code, applied, e)
		}
	}
	req.Condition = attachments.Condition{Revision: uploaded.Entry.Revision}
	replaced, err := c.WriteRepositoryAttachment(ctx, repo, id, req, bytes.NewReader(payload), -1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.WriteRepositoryAttachment(ctx, repo, id, req, strings.NewReader("stale"), 5)
	assertError(err, "revision_conflict", false)
	_, err = c.DeleteRepositoryAttachment(ctx, repo, id, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "folder", Condition: attachments.Condition{Revision: list(repo, id, "").Entries[0].Revision}})
	assertError(err, "directory_not_empty", false)
	// Typed relisting is the recovery operation after lost/disconnected events.
	stream.Close()
	current := list(repo, id, "folder")
	if current.Entries[0].Revision != replaced.Entry.Revision {
		t.Fatal("reconciliation revision mismatch")
	}
	moved, err := c.MoveRepositoryAttachment(ctx, repo, id, daemon.RepositoryAttachmentMoveRequest{Actor: "ui-user", From: req.Path, To: "renamed.zip", Revision: current.Entries[0].Revision, Condition: create})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.DeleteRepositoryAttachment(ctx, repo, id, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "renamed.zip", Condition: attachments.Condition{Revision: moved.Entry.Revision}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.DeleteRepositoryAttachment(ctx, repo, id, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "folder", Condition: attachments.Condition{Revision: list(repo, id, "").Entries[0].Revision}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list(repo, id, "").Entries) != 0 {
		t.Fatal("delete incomplete")
	}
	req.Path = "too-large"
	req.Condition = create
	_, err = c.WriteRepositoryAttachment(ctx, repo, id, req, io.LimitReader(attachmentZeroReader{}, attachments.V1Limits().MaxFileBytes+1), attachments.V1Limits().MaxFileBytes+1)
	assertError(err, "limit_exceeded", false)
	req.Path = "invalid-condition"
	req.Condition = attachments.Condition{Create: true, Revision: replaced.Entry.Revision}
	_, err = c.WriteRepositoryAttachment(ctx, repo, id, req, strings.NewReader(""), 0)
	assertError(err, "invalid_precondition", false)
	req.Condition = create
	req.Path = "../private"
	_, err = c.WriteRepositoryAttachment(ctx, repo, id, req, strings.NewReader(""), 0)
	assertError(err, "invalid_path", false)
	_, err = c.WriteRepositoryAttachment(ctx, repo, otherID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "cross", Condition: create}, strings.NewReader("x"), 1)
	assertError(err, "ticket_not_found", false)
}

func TestAttachmentClientMaximumEscapedListing(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable on this platform")
	}
	binary, err := exec.LookPath("ticket")
	if err != nil {
		t.Skip("installed Ticket CLI unavailable")
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "repository")
	repo, id := realAttachmentRepository(t, binary, root)
	m := &workerManager{repositoriesByID: map[string]supervisor.ConfiguredRepository{
		repo: {ID: repo, Key: "boundary", Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: root}},
	}}
	s, err := daemon.NewServer(daemon.Config{EndpointKey: testDeleteKey(t), StateDir: t.TempDir(), RepositoryGateway: m.repositoryGateway()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	if err = s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); waitForTestDaemonStopped(t, s) })
	c, err := daemonclient.NewWithEndpoint("", s.Endpoint().CapabilityURL(), "")
	if err != nil {
		t.Fatal(err)
	}
	limits := attachments.V1Limits()
	// Maximal components at depth 15 leave one component for immediate files at
	// depth 16. Ampersands expand from one byte to six in Go's normal JSON encoder.
	directory := ""
	component := strings.Repeat("&", limits.MaxNameBytes)
	for depth := 1; depth < limits.MaxDepth; depth++ {
		if directory != "" {
			directory += "/"
		}
		directory += component
		_, err = c.CreateRepositoryAttachmentDirectory(ctx, repo, id, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: directory, Condition: attachments.Condition{Create: true}})
		if err != nil {
			t.Fatalf("mkdir depth %d: %v", depth, err)
		}
	}
	names := make([]string, limits.MaxPageEntries+1)
	for i := range names {
		names[i] = strings.Repeat("&", limits.MaxNameBytes-3) + fmt.Sprintf("%03d", i)
		path := directory + "/" + names[i]
		if err = attachments.ValidatePath(path, false); err != nil {
			t.Fatal(err)
		}
		_, err = c.WriteRepositoryAttachment(ctx, repo, id, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: path, Condition: attachments.Condition{Create: true}}, strings.NewReader(""), 0)
		if err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
	}
	query := daemon.RepositoryAttachmentQuery{Path: directory, Limit: limits.MaxPageEntries}
	page, err := c.ListRepositoryAttachments(ctx, repo, id, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != limits.MaxPageEntries || page.NextCursor == nil || len(*page.NextCursor) > limits.MaxCursorBytes {
		t.Fatalf("page entries=%d cursor=%v", len(page.Entries), page.NextCursor)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= 2<<20 {
		t.Fatalf("boundary fixture no longer exceeds ordinary JSON budget: %d", len(encoded))
	}
	t.Logf("maximum escaped listing: %d bytes, %d entries, %d-byte cursor", len(encoded), len(page.Entries), len(*page.NextCursor))
	for i, entry := range page.Entries {
		if entry.Name != names[i] || entry.Path != directory+"/"+names[i] || entry.Kind != "file" || entry.SizeBytes == nil || *entry.SizeBytes != 0 {
			t.Fatalf("entry %d lost metadata", i)
		}
	}
	query.Cursor = *page.NextCursor
	next, err := c.ListRepositoryAttachments(ctx, repo, id, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Entries) != 1 || next.Entries[0].Name != names[len(names)-1] || next.NextCursor != nil || next.Revision != page.Revision {
		t.Fatal("continuation lost boundary entry or snapshot")
	}
}
