package attachments

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T, exists bool) (Location, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	loc := Location{"8d1268c4-6a64-4b9b-95c9-d5598a150e86", "20261025-12345", root, "archive/20261025-12345"}
	dir := filepath.Join(root, filepath.FromSlash(loc.TicketDirectory))
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	attachments := filepath.Join(dir, "attachments")
	if exists {
		if err = os.Mkdir(attachments, 0700); err != nil {
			t.Fatal(err)
		}
	}
	return loc, attachments
}
func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func mkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
}
func code(t *testing.T, err error, want string) {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != want {
		t.Fatalf("error=%v, want %s", err, want)
	}
}
func list(t *testing.T, loc Location, dir string, limit int, token string) Listing {
	t.Helper()
	result, err := List(context.Background(), loc, dir, limit, token)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func download(t *testing.T, s *Store, loc Location, path string) *Download {
	t.Helper()
	d, err := s.BeginDownload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if err = d.Open(loc, path); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestValidatePath(t *testing.T) {
	for _, path := range []string{"/a", "C:/a", "a\\b", "a//b", "a/", ".", "..", "a/../b", "a/./b", "x\x00", "x\n", "x\u0085", "%2e%2e", "a%252f", ".TICKET-ORC/a", "a/.ticket-orc", "CON.txt", "lpt9", "a.", "a ", string([]byte{0xff})} {
		if err := ValidatePath(path, false); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	for _, path := range []string{"Mixed/é日本.zip", "quoted\"<&>", "COM0", "com10", "a b", ".hidden"} {
		if err := ValidatePath(path, false); err != nil {
			t.Errorf("%q: %v", path, err)
		}
	}
	if err := ValidatePath("", true); err != nil {
		t.Fatal(err)
	}
	code(t, ValidatePath("", false), "invalid_path")
	code(t, ValidatePath(strings.Repeat("a/", 16)+"b", false), "limit_exceeded")
	code(t, ValidatePath(strings.Repeat("a", 256), false), "limit_exceeded")
}

func TestTraversalRejectsPrivateObjectAfterBootstrap(t *testing.T) {
	loc, root := fixture(t, true)
	mkdir(t, filepath.Join(root, "user"))
	owned, absent, err := openTree(context.Background(), loc)
	if err != nil || absent {
		t.Fatalf("bootstrap: absent=%v err=%v", absent, err)
	}
	defer owned.close()
	// Bypass the lexical path validator: the traversal primitive itself must
	// protect the actual private object, including through alternate spellings.
	_, err = owned.descend(owned.root, ".ticket-orc")
	code(t, err, "unsafe_attachment")
	_, err = owned.descend(owned.private, ".")
	code(t, err, "unsafe_attachment")
	if _, err = owned.descend(owned.root, "user"); err != nil {
		t.Fatal(err)
	}
	if err = owned.verify(); err != nil {
		t.Fatal(err)
	}
}

func TestAbsentRootAndIsolation(t *testing.T) {
	loc, root := fixture(t, false)
	first := list(t, loc, "", 0, "")
	if len(first.Entries) != 0 || first.NextCursor != nil || len(first.Revision) != 46 {
		t.Fatal(first)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("read created root", err)
	}
	s := NewStore()
	d, err := s.BeginDownload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	code(t, d.Open(loc, "missing"), "entry_not_found")
	_, err = List(context.Background(), loc, "missing", 0, "")
	code(t, err, "entry_not_found")
	other := loc
	other.TicketID = "20261025-54321"
	if list(t, other, "", 0, "").Revision == first.Revision {
		t.Fatal("revision did not bind ticket")
	}
	other = loc
	other.RepositoryID = "8d1268c4-6a64-4b9b-95c9-d5598a150e87"
	if list(t, other, "", 0, "").Revision == first.Revision {
		t.Fatal("revision did not bind repository")
	}
	mkdir(t, root)
	if got := list(t, loc, "", 0, "").Revision; got != first.Revision {
		t.Fatal("bootstrap changed virtual revision")
	}
}

func TestListingPaginationGenerationAndLazyDepth(t *testing.T) {
	loc, root := fixture(t, true)
	mkdir(t, filepath.Join(root, "z-dir"))
	mkdir(t, filepath.Join(root, "A-dir"))
	write(t, filepath.Join(root, "Z"), []byte("Z"))
	write(t, filepath.Join(root, "a"), []byte("a"))
	// A descendant may be unreadable/unsafe without affecting a root listing.
	deep := filepath.Join(root, "z-dir", "deep")
	mkdir(t, deep)
	write(t, filepath.Join(deep, "CON"), nil)
	var names []string
	token := ""
	for {
		page := list(t, loc, "", 1, token)
		for _, e := range page.Entries {
			names = append(names, e.Name)
		}
		if page.NextCursor == nil {
			break
		}
		token = *page.NextCursor
	}
	if !reflect.DeepEqual(names, []string{"A-dir", "z-dir", "Z", "a"}) {
		t.Fatal(names)
	}
	first := list(t, loc, "", 1, "")
	other := loc
	other.TicketID = "20261025-54321"
	_, err := List(context.Background(), other, "", 1, *first.NextCursor)
	code(t, err, "invalid_cursor")
	_, err = List(context.Background(), loc, "", 2, *first.NextCursor)
	code(t, err, "invalid_cursor")
	_, err = List(context.Background(), loc, "z-dir", 1, *first.NextCursor)
	code(t, err, "invalid_cursor")
	_, err = List(context.Background(), loc, "", 1, *first.NextCursor+"=")
	code(t, err, "invalid_cursor")
	gen := bytes.Repeat([]byte{7}, 16)
	write(t, filepath.Join(root, ".ticket-orc", "generation.json"), []byte(`{"version":1,"generation":"`+base64.RawURLEncoding.EncodeToString(gen)+`"}`))
	_, err = List(context.Background(), loc, "", 1, *first.NextCursor)
	code(t, err, "cursor_stale")
	if list(t, loc, "", 1, "").Revision == first.Revision {
		t.Fatal("generation not reflected")
	}
	if list(t, loc, "z-dir", 1, "").Entries[0].Name != "deep" {
		t.Fatal("deep listing")
	}
	_, err = List(context.Background(), loc, "z-dir/deep", 1, "")
	code(t, err, "unsafe_attachment")
}

func TestMaximumCursorSerialization(t *testing.T) {
	loc, root := fixture(t, true)
	// 15 directory components leave room for immediate children at depth 16.
	component := strings.Repeat("\"<&>", 63) + "abc"
	if runtime.GOOS == "windows" {
		component = strings.Repeat("x", 255)
	} // Windows forbids these filename bytes; wire budget is tested separately below.
	parts := make([]string, 15)
	for i := range parts {
		parts[i] = component
	}
	directory := strings.Join(parts, "/")
	current, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	for _, part := range parts {
		if err = current.Mkdir(part, 0700); err != nil {
			t.Fatal(err)
		}
		next, openErr := current.OpenRoot(part)
		current.Close()
		if openErr != nil {
			t.Fatal(openErr)
		}
		current = next
	}
	defer current.Close()
	for _, name := range []string{component, "z"} {
		file, err := current.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		file.Close()
	}
	page := list(t, loc, directory, 1, "")
	if page.NextCursor == nil || len(*page.NextCursor) > V1Limits().MaxCursorBytes {
		t.Fatal("cursor budget", page.NextCursor)
	}
	if got := list(t, loc, directory, 1, *page.NextCursor); len(got.Entries) != 1 || got.Entries[0].Name != "z" {
		t.Fatal(got)
	}
	data, _ := base64.RawURLEncoding.DecodeString(*page.NextCursor)
	duplicate := bytes.Replace(data, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
	_, err = List(context.Background(), loc, directory, 1, base64.RawURLEncoding.EncodeToString(duplicate))
	code(t, err, "invalid_cursor")
	_, err = List(context.Background(), loc, directory, 1, strings.Repeat("a", V1Limits().MaxCursorBytes+1))
	code(t, err, "invalid_cursor")
}

func TestMaximumEscapedCursorWireBudget(t *testing.T) {
	component := strings.Repeat("\"<&>", 63) + "abc"
	parts := make([]string, 15)
	for i := range parts {
		parts[i] = component
	}
	directory := strings.Join(parts, "/")
	loc := Location{RepositoryID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", TicketID: "20261025-12345"}
	rev := revision(loc, directory, "directory", [16]byte{})
	data, err := json.Marshal(cursor{1, loc.RepositoryID, loc.TicketID, directory, rev, 1, "file", component})
	if err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(data)
	if len(token) > V1Limits().MaxCursorBytes {
		t.Fatal("wire budget exceeded", len(token))
	}
	if _, err = parseCursor(token, loc, directory, 1, rev); err != nil {
		t.Fatal(err)
	}
}

func TestBinaryDownloadPinsObjectAndBoundsRead(t *testing.T) {
	loc, root := fixture(t, true)
	data := bytes.Repeat([]byte{0, 0xff, 1, 2, 3, 4, 5}, 500000) // above Ticket's 2 MiB frame
	write(t, filepath.Join(root, "payload.zip"), data)
	d := download(t, NewStore(), loc, "payload.zip")
	if *d.Entry.SizeBytes != int64(len(data)) || d.Entry.Path != "payload.zip" {
		t.Fatal(d.Entry)
	}
	// Download releases the ticket lock before the first byte is consumed.
	_ = list(t, loc, "", 0, "")
	write(t, filepath.Join(root, "replacement"), []byte("new"))
	// Exercise the rooted rename used by attachment publication. Ordinary
	// os.Rename on Windows cannot replace this open download's destination.
	publicationRoot, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer publicationRoot.Close()
	if err := publicationRoot.Rename("replacement", "payload.zip"); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(d)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("stable stream %d: %v", len(got), err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if current, err := os.ReadFile(filepath.Join(root, "payload.zip")); err != nil || string(current) != "new" {
		t.Fatalf("replacement bytes %q: %v", current, err)
	}
	// An append after pinning must not extend the advertised stream length.
	write(t, filepath.Join(root, "append"), []byte("old"))
	d2 := download(t, NewStore(), loc, "append")
	f, err := os.OpenFile(filepath.Join(root, "append"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte("extra"))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(d2)
	if err != nil || string(got) != "old" {
		t.Fatalf("bounded stream %q %v", got, err)
	}
}

func TestAdmissionCancellationAndLockContention(t *testing.T) {
	loc, root := fixture(t, true)
	write(t, filepath.Join(root, "file"), []byte("data"))
	s := NewStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readers []*Download
	for i := 0; i < V1Limits().MaxActiveTransfers; i++ {
		d, err := s.BeginDownload(ctx)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, d)
		defer d.Close()
	}
	_, err := s.BeginDownload(ctx)
	code(t, err, "attachment_busy")
	if err = readers[0].Open(loc, "file"); err != nil {
		t.Fatal(err)
	}
	opened := readers[0].file
	cancel()
	for _, d := range readers {
		select {
		case <-d.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation did not release download")
		}
	}
	// Read checks os.File's closed state on every platform. Windows Stat
	// instead calls GetFileType on the retired handle and returns a native
	// invalid-handle error, which does not wrap os.ErrClosed.
	var probe [1]byte
	if _, err = opened.Read(probe[:]); !errors.Is(err, os.ErrClosed) {
		t.Fatal("descriptor not closed", err)
	}
	d, err := s.BeginDownload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	tree, _, err := openTree(context.Background(), loc)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.close()
	_, err = List(context.Background(), loc, "", 0, "")
	code(t, err, "attachment_busy")
	code(t, d.Open(loc, "file"), "attachment_busy")
}

func TestPrivateMetadataValidation(t *testing.T) {
	for _, record := range []string{`{}`, `{"version":2,"generation":"AAAAAAAAAAAAAAAAAAAAAA"}`, `{"version":1,"version":1,"generation":"AAAAAAAAAAAAAAAAAAAAAA"}`, `{"version":1,"generation":"bad"}`, strings.Repeat(" ", 4097)} {
		loc, root := fixture(t, true)
		list(t, loc, "", 0, "")
		write(t, filepath.Join(root, ".ticket-orc", "generation.json"), []byte(record))
		_, err := List(context.Background(), loc, "", 0, "")
		code(t, err, "unsafe_attachment")
	}
	for _, name := range []string{"unknown", "upload.tmp", "generation.tmp"} {
		loc, root := fixture(t, true)
		list(t, loc, "", 0, "")
		write(t, filepath.Join(root, ".ticket-orc", name), nil)
		_, err := List(context.Background(), loc, "", 0, "")
		code(t, err, "unsafe_attachment")
	}
}

func TestImmediateChildBound(t *testing.T) {
	loc, root := fixture(t, true)
	for i := 0; i <= V1Limits().MaxEntries; i++ {
		name, _ := json.Marshal(i)
		write(t, filepath.Join(root, string(name)), nil)
	}
	_, err := List(context.Background(), loc, "", 1, "")
	code(t, err, "limit_exceeded")
}

func TestAttachmentLockProcessHelper(t *testing.T) {
	raw := os.Getenv("ORC_ATTACHMENT_LOCK_TEST")
	if raw == "" {
		return
	}
	var loc Location
	if err := json.Unmarshal([]byte(raw), &loc); err != nil {
		t.Fatal(err)
	}
	_, err := List(context.Background(), loc, "", 0, "")
	if os.Getenv("ORC_ATTACHMENT_LOCK_BUSY") == "1" {
		code(t, err, "attachment_busy")
	} else if err != nil {
		t.Fatal(err)
	}
}

func TestTicketLockCoordinatesIndependentProcesses(t *testing.T) {
	loc, _ := fixture(t, true)
	owned, _, err := openTree(context.Background(), loc)
	if err != nil {
		t.Fatal(err)
	}
	defer owned.close()
	raw, _ := json.Marshal(loc)
	run := func(busy string) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestAttachmentLockProcessHelper$")
		cmd.Env = append(os.Environ(), "ORC_ATTACHMENT_LOCK_TEST="+string(raw), "ORC_ATTACHMENT_LOCK_BUSY="+busy)
		if data, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("lock subprocess: %v %s", err, data)
		}
	}
	run("1")
	owned.close()
	run("0")
}
