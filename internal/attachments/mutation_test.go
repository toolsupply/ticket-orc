package attachments

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

var create = Condition{Create: true}

func put(t *testing.T, s *Store, loc Location, p string, c Condition, b []byte) Receipt {
	t.Helper()
	u, err := s.BeginUpload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if err = u.Stage(loc, "tester", p, c, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
	r, err := u.Commit(testCommitContext(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	if !r.MutationApplied || r.Entry == nil {
		t.Fatal(r)
	}
	return r
}
func rejected(t *testing.T, err error, want string) {
	t.Helper()
	code(t, err, want)
	var e *Error
	errors.As(err, &e)
	if e.MutationApplied == nil || *e.MutationApplied {
		t.Fatalf("certainty: %+v", e)
	}
}
func TestMutationLifecycle(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	loc, root := fixture(t, false)
	s := NewStore()
	ctx := context.Background()
	initial := list(t, loc, "", 0, "")
	r := put(t, s, loc, "Binary.ZIP", create, []byte{0, 255, 1, 0})
	if !r.Created || r.RootRevision == initial.Revision {
		t.Fatal(r)
	}
	// Revisions invalidate across every path, including directory tokens.
	folder, err := Mkdir(ctx, loc, "tester", "Design", create)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Delete(ctx, loc, "tester", "Binary.ZIP", Condition{Revision: r.Entry.Revision}, false)
	rejected(t, err, "revision_conflict")
	u, _ := s.BeginUpload(ctx)
	err = u.Stage(loc, "tester", "Binary.ZIP", Condition{Revision: r.Entry.Revision}, strings.NewReader("new"), 3)
	rejected(t, err, "revision_conflict")
	current := list(t, loc, "", 0, "")
	var rev string
	for _, e := range current.Entries {
		if e.Name == "Binary.ZIP" {
			rev = e.Revision
		}
	}
	r = put(t, s, loc, "Binary.ZIP", Condition{Revision: rev}, nil)
	if r.Created || *r.Entry.SizeBytes != 0 {
		t.Fatal(r)
	}
	_, err = Mkdir(ctx, loc, "tester", "Design", create)
	rejected(t, err, "revision_conflict")
	_, err = Mkdir(ctx, loc, "tester", "absent/child", create)
	rejected(t, err, "entry_not_found")
	_, err = Mkdir(ctx, loc, "tester", "Design/sub", Condition{})
	rejected(t, err, "precondition_required")
	_, err = Mkdir(ctx, loc, "tester", "Design/sub", Condition{Revision: r.Entry.Revision})
	rejected(t, err, "invalid_precondition")
	_, err = Move(ctx, loc, "tester", "Binary.ZIP", "Design/file", r.Entry.Revision, create)
	if err != nil {
		t.Fatal(err)
	}
	current = list(t, loc, "", 0, "")
	folder.Entry = &current.Entries[0]
	_, err = Delete(ctx, loc, "tester", "Design", Condition{Revision: folder.Entry.Revision}, false)
	rejected(t, err, "directory_not_empty")
	child := list(t, loc, "Design", 0, "").Entries[0]
	_, err = Delete(ctx, loc, "tester", "Design/file", Condition{Revision: child.Revision}, true)
	rejected(t, err, "capability_unavailable")
	del, err := Delete(ctx, loc, "tester", "Design/file", Condition{Revision: child.Revision}, false)
	if err != nil || !del.MutationApplied {
		t.Fatal(del, err)
	}
	dir := list(t, loc, "", 0, "").Entries[0]
	_, err = Delete(ctx, loc, "tester", "Design", Condition{Revision: dir.Revision}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(list(t, loc, "", 0, "").Entries) != 0 {
		t.Fatal("entries remain")
	}
	for _, p := range []string{"Design", "Binary.ZIP"} {
		if _, err = os.Stat(filepath.Join(root, p)); !os.IsNotExist(err) {
			t.Fatal(p, err)
		}
	}
}
func TestMoveValidationAndRevisionABA(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	ctx := context.Background()
	loc, _ := fixture(t, false)
	s := NewStore()
	_, err := Mkdir(ctx, loc, "tester", "a", create)
	if err != nil {
		t.Fatal(err)
	}
	r := put(t, s, loc, "a/file", create, []byte("one"))
	for _, to := range []string{"a/file", "a/FILE"} {
		_, err = Move(ctx, loc, "tester", "a/file", to, r.Entry.Revision, create)
		rejected(t, err, "invalid_path")
	}
	for _, to := range []string{"a/sub", "a"} {
		rev := list(t, loc, "", 0, "").Entries[0].Revision
		_, err = Move(ctx, loc, "tester", "a", to, rev, create)
		rejected(t, err, "invalid_path")
	}
	_, err = Move(ctx, loc, "tester", "a/file", "missing/file", r.Entry.Revision, create)
	rejected(t, err, "entry_not_found")
	other := put(t, s, loc, "other", create, nil)
	old := list(t, loc, "a", 0, "").Entries[0]
	_, err = Move(ctx, loc, "tester", "a/file", "other", old.Revision, create)
	rejected(t, err, "revision_conflict")
	_, err = Delete(ctx, loc, "tester", "other", Condition{Revision: other.Entry.Revision}, false)
	if err != nil {
		t.Fatal(err)
	}
	original := list(t, loc, "a", 0, "").Entries[0]
	_, err = Delete(ctx, loc, "tester", "a/file", Condition{Revision: original.Revision}, false)
	if err != nil {
		t.Fatal(err)
	}
	fresh := put(t, s, loc, "a/file", create, []byte("one"))
	if fresh.Entry.Revision == original.Revision {
		t.Fatal("ABA token reused")
	}
	// Validate resulting paths for an entire directory subtree before rename.
	deep := strings.Repeat("d/", 14) + "d"
	// Existing parents are explicitly created one at a time.
	p := ""
	for _, component := range strings.Split(deep, "/") {
		if p != "" {
			p += "/"
		}
		p += component
		if _, err = Mkdir(ctx, loc, "tester", p, create); err != nil {
			t.Fatal(err)
		}
	}
	rev := list(t, loc, "", 0, "").Entries[0].Revision
	_, err = Move(ctx, loc, "tester", "a", deep+"/a", rev, create)
	rejected(t, err, "limit_exceeded")
}

type brokenReader struct{ sent bool }

func (b *brokenReader) Read(p []byte) (int, error) {
	if b.sent {
		return 0, io.ErrUnexpectedEOF
	}
	b.sent = true
	return copy(p, []byte("partial")), nil
}

type zeros struct{ remaining int64 }

func (z *zeros) Read(p []byte) (int, error) {
	if z.remaining == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), z.remaining))
	clear(p[:n])
	z.remaining -= int64(n)
	return n, nil
}
func TestUploadLimitsFailureAndCleanup(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	ctx := context.Background()
	loc, root := fixture(t, false)
	s := NewStore()
	original := put(t, s, loc, "file", create, []byte("old"))
	tests := []struct {
		name   string
		r      io.Reader
		length int64
		want   string
	}{
		{"truncated", strings.NewReader("x"), 2, "invalid_request"},
		{"interrupted", &brokenReader{}, -1, "attachment_storage_failed"},
		{"declared oversize", strings.NewReader(""), V1Limits().MaxFileBytes + 1, "limit_exceeded"},
		{"actual oversize", &zeros{V1Limits().MaxFileBytes + 1}, -1, "limit_exceeded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := s.BeginUpload(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer u.Close()
			err = u.Stage(loc, "tester", "file", Condition{Revision: original.Entry.Revision}, tt.r, tt.length)
			rejected(t, err, tt.want)
			b, err := os.ReadFile(filepath.Join(root, "file"))
			if err != nil || string(b) != "old" {
				t.Fatal(string(b), err)
			}
			if _, err = os.Stat(filepath.Join(root, ".ticket-orc/upload.tmp")); !os.IsNotExist(err) {
				t.Fatal("stage remains", err)
			}
		})
	}
	boundary := put(t, s, loc, "file", Condition{Revision: original.Entry.Revision}, []byte{0, 255, 0})
	u, _ := s.BeginUpload(ctx)
	err := u.Stage(loc, "tester", "file", Condition{Revision: boundary.Entry.Revision}, &zeros{V1Limits().MaxFileBytes}, -1)
	if err != nil {
		t.Fatal(err)
	}
	r, err := u.Commit(testCommitContext(ctx))
	if err != nil || *r.Entry.SizeBytes != V1Limits().MaxFileBytes {
		t.Fatal(r, err)
	}
	if _, err = Delete(ctx, loc, "tester", "file", Condition{Revision: r.Entry.Revision}, false); err != nil {
		t.Fatal(err)
	}
}
func TestCommitRevalidatesAndPreservesVisibleFile(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	for _, kind := range []string{"parent moved", "generation changed", "stage redirected", "publication failure"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			loc, root := fixture(t, false)
			s := NewStore()
			_, err := Mkdir(ctx, loc, "tester", "dir", create)
			if err != nil {
				t.Fatal(err)
			}
			old := put(t, s, loc, "dir/file", create, []byte("old"))
			u, _ := s.BeginUpload(ctx)
			defer u.Close()
			if err = u.Stage(loc, "tester", "dir/file", Condition{Revision: old.Entry.Revision}, strings.NewReader("new"), 3); err != nil {
				t.Fatal(err)
			}
			want := "unsafe_attachment"
			switch kind {
			case "parent moved":
				if err = os.Rename(filepath.Join(root, "dir"), filepath.Join(root, "moved")); err != nil {
					t.Fatal(err)
				}
			case "generation changed":
				if _, err = u.tree.rotate(ctx); err != nil {
					t.Fatal(err)
				}
				want = "revision_conflict"
			case "stage redirected":
				if err = os.Remove(filepath.Join(root, ".ticket-orc/upload.tmp")); err != nil {
					t.Fatal(err)
				}
				mkdir(t, filepath.Join(root, ".ticket-orc/upload.tmp"))
			case "publication failure":
				// The real OS boundary is an immutable destination type changed after
				// validation. A directory must never be replaced by file rename.
				if err = os.Remove(filepath.Join(root, "dir/file")); err != nil {
					t.Fatal(err)
				}
				mkdir(t, filepath.Join(root, "dir/file"))
				want = "entry_type_conflict"
			}
			_, err = u.Commit(testCommitContext(ctx))
			rejected(t, err, want)
			if kind != "publication failure" {
				p := "dir/file"
				if kind == "parent moved" {
					p = "moved/file"
				}
				b, err := os.ReadFile(filepath.Join(root, p))
				if err != nil || string(b) != "old" {
					t.Fatal(string(b), err)
				}
			}
		})
	}
}
func TestStagingRecoveryAndQuotaEdges(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	ctx := context.Background()
	loc, root := fixture(t, true)
	s := NewStore()
	put(t, s, loc, "zero", create, nil)
	// Crash leftovers are valid only alongside a valid generation record.
	write(t, filepath.Join(root, ".ticket-orc/upload.tmp"), []byte("abandoned"))
	write(t, filepath.Join(root, ".ticket-orc/generation.tmp"), []byte("interrupted metadata"))
	put(t, s, loc, "recovered", create, nil)
	for _, name := range []string{"upload.tmp", "generation.tmp"} {
		if _, err := os.Stat(filepath.Join(root, ".ticket-orc", name)); !os.IsNotExist(err) {
			t.Fatal(name, err)
		}
	}
	// Sparse existing files exercise aggregate quota without large buffers.
	for i := 0; i < 8; i++ {
		name := filepath.Join(root, string(rune('a'+i)))
		f, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		err = f.Truncate(V1Limits().MaxFileBytes)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	u, _ := s.BeginUpload(ctx)
	err := u.Stage(loc, "tester", "extra", create, strings.NewReader("x"), 1)
	rejected(t, err, "limit_exceeded")
	current := list(t, loc, "", 0, "")
	var first Entry
	for _, e := range current.Entries {
		if e.Name == "a" {
			first = e
		}
	}
	put(t, s, loc, "a", Condition{Revision: first.Revision}, nil)
	// Oversized total: a reducing replacement succeeds, growth/mkdir fail.
	f, err := os.OpenFile(filepath.Join(root, "b"), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(2 * V1Limits().MaxTotalBytes)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	_, err = Mkdir(ctx, loc, "tester", "over", create)
	rejected(t, err, "limit_exceeded")
	var b Entry
	for _, e := range list(t, loc, "", 0, "").Entries {
		if e.Name == "b" {
			b = e
		}
	}
	put(t, s, loc, "b", Condition{Revision: b.Revision}, nil)
	// Bound entry enumeration and include directories in quota accounting.
	for i := 0; i < V1Limits().MaxEntries; i++ {
		// Arbitrary Unicode code points include names rejected by macOS
		// filesystems. This fixture needs distinct entries, not Unicode coverage.
		name := filepath.Join(root, "entry"+strconv.Itoa(i))
		write(t, name, nil)
	}
	_, err = Mkdir(ctx, loc, "tester", "full", create)
	rejected(t, err, "limit_exceeded")
	// Deletion remains available to reduce an over-limit tree.
	_, err = Delete(ctx, loc, "tester", "recovered", Condition{Revision: revision(loc, "recovered", "file", mustGeneration(t, loc))}, false)
	if err != nil {
		t.Fatal(err)
	}
}
func mustGeneration(t *testing.T, loc Location) [16]byte {
	t.Helper()
	tree, _, err := openTree(context.Background(), loc)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.close()
	g, err := generation(tree)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestUploadCrossProcessLock(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	if os.Getenv("ORC_ATTACHMENT_UPLOAD_CHILD") == "1" {
		loc := Location{RepositoryID: os.Getenv("ORC_ATTACHMENT_REPO"), TicketID: os.Getenv("ORC_ATTACHMENT_TICKET"), RepositoryRoot: os.Getenv("ORC_ATTACHMENT_ROOT"), TicketDirectory: os.Getenv("ORC_ATTACHMENT_DIR")}
		u, err := NewStore().BeginUpload(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer u.Close()
		condition := create
		if rev := os.Getenv("ORC_ATTACHMENT_REVISION"); rev != "" {
			condition = Condition{Revision: rev}
		}
		err = u.Stage(loc, "other", "file", condition, strings.NewReader("other"), 5)
		want := os.Getenv("ORC_ATTACHMENT_EXPECT")
		code(t, err, want)
		return
	}
	loc, _ := fixture(t, false)
	ctx := context.Background()
	s := NewStore()
	u, _ := s.BeginUpload(ctx)
	defer u.Close()
	if err := u.Stage(loc, "tester", "file", create, strings.NewReader("winner"), 6); err != nil {
		t.Fatal(err)
	}
	precondition := ""
	run := func(want string) {
		t.Helper()
		guard, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(guard, os.Args[0], "-test.run=^TestUploadCrossProcessLock$")
		cmd.Env = append(os.Environ(), "ORC_ATTACHMENT_UPLOAD_CHILD=1", "ORC_ATTACHMENT_REPO="+loc.RepositoryID, "ORC_ATTACHMENT_TICKET="+loc.TicketID, "ORC_ATTACHMENT_ROOT="+loc.RepositoryRoot, "ORC_ATTACHMENT_DIR="+loc.TicketDirectory, "ORC_ATTACHMENT_EXPECT="+want, "ORC_ATTACHMENT_REVISION="+precondition)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %s %v", out, err)
		}
	}
	run("attachment_busy")
	// A second Store represents a different Orc home; the lock is ticket-local.
	second, _ := NewStore().BeginUpload(ctx)
	defer second.Close()
	rejected(t, second.Stage(loc, "other", "file", create, strings.NewReader("other"), 5), "attachment_busy")
	if _, err := u.Commit(testCommitContext(ctx)); err != nil {
		t.Fatal(err)
	}
	run("revision_conflict")
	precondition = list(t, loc, "", 0, "").Entries[0].Revision
	replacement, err := s.BeginUpload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err = replacement.Stage(loc, "tester", "file", Condition{Revision: precondition}, strings.NewReader("replacement"), 11); err != nil {
		t.Fatal(err)
	}
	run("attachment_busy")
	if _, err = replacement.Commit(testCommitContext(ctx)); err != nil {
		t.Fatal(err)
	}
	run("revision_conflict")
}

func TestUploadAdmissionCancellationAndCommitQuota(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	ctx := context.Background()
	s := NewStore()
	loc, root := fixture(t, false)
	cancelCtx, cancel := context.WithCancel(ctx)
	u, err := s.BeginUpload(cancelCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if err = u.Stage(loc, "tester", "cancelled", create, strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-u.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not release stage")
	}
	if _, err = os.Stat(filepath.Join(root, ".ticket-orc/upload.tmp")); !os.IsNotExist(err) {
		t.Fatal("canceled stage remains", err)
	}
	if len(list(t, loc, "", 0, "").Entries) != 0 {
		t.Fatal("cancelled upload published")
	}
	// Uploads and downloads share the four slots, including resolution time.
	var uploads []*Upload
	for i := 0; i < V1Limits().MaxActiveTransfers; i++ {
		u, err := s.BeginUpload(ctx)
		if err != nil {
			t.Fatal(err)
		}
		uploads = append(uploads, u)
		t.Cleanup(func() { u.Close() })
	}
	_, err = s.BeginUpload(ctx)
	rejected(t, err, "attachment_busy")
	_, err = s.BeginDownload(ctx)
	code(t, err, "attachment_busy")
	for _, u := range uploads {
		u.Close()
	}
	staged, err := s.BeginUpload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	if err = staged.Stage(loc, "tester", "file", create, strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	// Quiescent external-edit simulation at the transfer/commit boundary:
	// the commit must recount, even though the initial quota passed.
	for i := 0; i < 8; i++ {
		f, err := os.Create(filepath.Join(root, string(rune('a'+i))))
		if err != nil {
			t.Fatal(err)
		}
		err = f.Truncate(V1Limits().MaxFileBytes)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = staged.Commit(testCommitContext(ctx))
	rejected(t, err, "limit_exceeded")
	if _, err = os.Stat(filepath.Join(root, "file")); !os.IsNotExist(err) {
		t.Fatal("over-quota upload published")
	}
}

type gatedReader struct {
	ctx     context.Context
	entered chan struct{}
	once    bool
}

func (g *gatedReader) Read(p []byte) (int, error) {
	if !g.once {
		g.once = true
		close(g.entered)
	}
	<-g.ctx.Done()
	return 0, g.ctx.Err()
}
func TestSlowTransferDoesNotQueueOtherOperations(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loc, _ := fixture(t, false)
	s := NewStore()
	u, err := s.BeginUpload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	g := &gatedReader{ctx: u.Context(), entered: make(chan struct{})}
	// Register cancellation and a completion barrier before starting work.
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("reader did not finish")
		}
		u.Close()
	})
	go func() { done <- u.Stage(loc, "tester", "file", create, g, -1) }()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reader not entered")
	}
	_, err = Mkdir(context.Background(), loc, "tester", "other", create)
	rejected(t, err, "attachment_busy")
	_, err = List(context.Background(), loc, "", 0, "")
	code(t, err, "attachment_busy")
	cancel()
	// Done is the authoritative cleanup milestone, not merely the read handoff.
	select {
	case <-u.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not release lock")
	}
	if _, err = Mkdir(context.Background(), loc, "tester", "after", create); err != nil {
		t.Fatal(err)
	}
}
func TestAppliedReceiptSurvivesPostPublicationFailure(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable on this platform")
	}
	loc, root := fixture(t, true)
	ctx := context.Background()
	tree, _, err := openTree(ctx, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.close()
	if _, err = tree.prepare(); err != nil {
		t.Fatal(err)
	}
	gen, err := tree.rotate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = tree.root.Mkdir("published", 0700); err != nil {
		t.Fatal(err)
	}
	// Real OS fault after publication: the opened root can no longer supply
	// receipt metadata or flush. Publication itself remains known.
	tree.root.Close()
	r, err := tree.finish(loc, "tester", "mkdir", "published", "", gen, true)
	var domain *Error
	if !errors.As(err, &domain) || domain.MutationApplied == nil || !*domain.MutationApplied || !r.MutationApplied {
		t.Fatal(r, err)
	}
	if info, err := os.Stat(filepath.Join(root, "published")); err != nil || !info.IsDir() {
		t.Fatal(info, err)
	}
}

func TestMutationInputAndOutcomeCertainty(t *testing.T) {
	ctx := context.Background()
	loc, _ := fixture(t, false)
	for _, c := range []Condition{{Revision: "weak"}, {Create: true, Revision: strings.Repeat("x", 46)}} {
		_, err := Mkdir(ctx, loc, "tester", "dir", c)
		rejected(t, err, "invalid_precondition")
	}
	_, err := Mkdir(ctx, loc, "bad actor", "dir", create)
	rejected(t, err, "invalid_actor")
	for _, p := range []string{"", "../other", ".ticket-orc"} {
		_, err = Delete(ctx, loc, "tester", p, Condition{Revision: revision(loc, "", "directory", [16]byte{})}, false)
		rejected(t, err, "invalid_path")
	}
	unknown := mutationError(failure("mutation_outcome_unknown"), false)
	var typed *Error
	if !errors.As(unknown, &typed) || typed.MutationApplied != nil {
		t.Fatal("lost uncertainty", unknown)
	}
	yes := true
	known := mutationError(&Error{Code: "attachment_storage_failed", MutationApplied: &yes}, false)
	if !errors.As(known, &typed) || typed.MutationApplied == nil || !*typed.MutationApplied {
		t.Fatal("lost publication evidence", known)
	}
}

func TestUnsupportedMutationPlatformFailsClosed(t *testing.T) {
	if safeMutationSupported {
		t.Skip("safe atomic publication supported")
	}
	loc, root := fixture(t, false)
	ctx := context.Background()
	_, err := NewStore().BeginUpload(ctx)
	rejected(t, err, "capability_unavailable")
	_, err = Mkdir(ctx, loc, "tester", "dir", create)
	rejected(t, err, "capability_unavailable")
	rev := revision(loc, "file", "file", [16]byte{})
	_, err = Move(ctx, loc, "tester", "file", "to", rev, create)
	rejected(t, err, "capability_unavailable")
	_, err = Delete(ctx, loc, "tester", "file", Condition{Revision: rev}, false)
	rejected(t, err, "capability_unavailable")
	if _, err = os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("unsupported mutation created root", err)
	}
}

func TestDownloadRemainsStableDuringAtomicReplacement(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable")
	}
	loc, _ := fixture(t, false)
	s := NewStore()
	old := put(t, s, loc, "file", create, []byte("opened bytes"))
	d := download(t, s, loc, "file")
	fresh := put(t, s, loc, "file", Condition{Revision: old.Entry.Revision}, []byte("replacement"))
	b, err := io.ReadAll(d)
	if err != nil || string(b) != "opened bytes" {
		t.Fatal(string(b), err)
	}
	if d.Entry.Revision == fresh.Entry.Revision {
		t.Fatal("opened revision changed")
	}
	d.Close()
	next := download(t, s, loc, "file")
	b, err = io.ReadAll(next)
	if err != nil || string(b) != "replacement" {
		t.Fatal(string(b), err)
	}
}

func TestUploadRejectsTicketRelocationWithoutRecreatingIt(t *testing.T) {
	if !safeMutationSupported {
		t.Skip("safe atomic publication unavailable")
	}
	loc, root := fixture(t, false)
	s := NewStore()
	ctx := context.Background()
	u, err := s.BeginUpload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if err = u.Stage(loc, "tester", "file", create, strings.NewReader("data"), 4); err != nil {
		t.Fatal(err)
	}
	ticket := filepath.Dir(root)
	moved := ticket + "-moved"
	if err = os.Rename(ticket, moved); err != nil {
		t.Fatal(err)
	}
	_, err = u.Commit(testCommitContext(ctx))
	rejected(t, err, "unsafe_attachment")
	if _, err = os.Stat(ticket); !os.IsNotExist(err) {
		t.Fatal("ticket recreated", err)
	}
	if _, err = os.Stat(filepath.Join(moved, "attachments", "file")); !os.IsNotExist(err) {
		t.Fatal("stale location published", err)
	}
}

func testCommitContext(ctx context.Context) CommitContext {
	return func() (context.Context, context.CancelFunc, error) { return ctx, func() {}, nil }
}
