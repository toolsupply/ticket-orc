package attachments

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Condition is transport-independent: Create means If-None-Match: *, while
// Revision is an unquoted strong token. Exactly one must be supplied.
type Condition struct {
	Create   bool
	Revision string
}

// Validate checks a transport-decoded precondition against the storage contract.
func (c Condition) Validate(createOnly bool) error { return c.validate(createOnly) }

func (c Condition) validate(createOnly bool) error {
	if !c.Create && c.Revision == "" {
		return failure("precondition_required")
	}
	if c.Create && c.Revision != "" || createOnly && !c.Create {
		return failure("invalid_precondition")
	}
	if c.Revision != "" && !validRevision(c.Revision) {
		return failure("invalid_precondition")
	}
	return nil
}
func validRevision(s string) bool {
	if len(s) != 46 || !strings.HasPrefix(s, "a1_") {
		return false
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(s[3:])
	return e == nil && len(b) == 32 && base64.RawURLEncoding.EncodeToString(b) == s[3:]
}

// Receipt records known publication. Created selects the creation response;
// deletion has no Entry. Errors after publication retain MutationApplied=true.
type Receipt struct {
	RepositoryID    string `json:"repository_id"`
	TicketID        string `json:"ticket_id"`
	Actor           string `json:"actor"`
	Operation       string `json:"operation"`
	MutationApplied bool   `json:"mutation_applied"`
	Entry           *Entry `json:"entry,omitempty"`
	RootRevision    string `json:"root_revision"`
	From            string `json:"from,omitempty"`
	Created         bool   `json:"-"`
}

func mutationError(err error, applied bool) error {
	if err == nil {
		return nil
	}
	code := "attachment_storage_failed"
	var domain *Error
	if errors.As(err, &domain) {
		code = domain.Code
		if code == "mutation_outcome_unknown" {
			return &Error{Code: code}
		}
		if domain.MutationApplied != nil && *domain.MutationApplied {
			applied = true
		}
	}
	return &Error{Code: code, MutationApplied: &applied, FileSizeLimit: domain != nil && domain.FileSizeLimit}
}
func validateMutation(actor, p string, c Condition, createOnly bool) error {
	if ticketclient.ValidateActor(actor) != nil {
		return failure("invalid_actor")
	}
	if err := ValidatePath(p, false); err != nil {
		return err
	}
	return c.validate(createOnly)
}

// openWritable creates only the literal attachment root below a pinned,
// existing Ticket directory. A concurrent bootstrap may win the mkdir.
func openWritable(ctx context.Context, loc Location) (*tree, error) {
	if !safeMutationSupported {
		return nil, failure("capability_unavailable")
	}
	t, absent, err := openTree(ctx, loc)
	if err != nil {
		return nil, err
	}
	if !absent {
		return t, nil
	}
	defer t.close()
	if err = t.verify(); err != nil {
		return nil, err
	}
	ticket := t.roots[len(t.roots)-1]
	if err = ticket.Mkdir("attachments", 0700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, storageError(err)
	}
	if err = t.verify(); err != nil {
		return nil, err
	}
	returnTree, stillAbsent, err := openTree(ctx, loc)
	if err == nil && stillAbsent {
		returnTree.close()
		return nil, failure("unsafe_attachment")
	}
	if err == nil {
		err = t.verify()
		if err != nil {
			returnTree.close()
			return nil, err
		}
	}
	return returnTree, err
}
func (t *tree) parent(p string) (*os.Root, string, error) {
	parts := strings.Split(p, "/")
	dir := t.root
	for _, part := range parts[:len(parts)-1] {
		var err error
		dir, err = t.descend(dir, part)
		if err != nil {
			return nil, "", storageError(err)
		}
	}
	return dir, parts[len(parts)-1], nil
}
func (t *tree) entry(loc Location, p string, gen [16]byte) (*Entry, error) {
	dir, name, err := t.parent(p)
	if err != nil {
		return nil, err
	}
	info, err := dir.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, storageError(err)
	}
	e := &Entry{Name: name, Path: p, Kind: "directory"}
	if info.Mode().IsRegular() {
		f, err := openRegular(dir, name, os.O_RDONLY)
		if err != nil {
			return nil, storageError(err)
		}
		info, err = f.Stat()
		f.Close()
		if err != nil {
			return nil, storageError(err)
		}
		size := info.Size()
		e.Kind = "file"
		e.SizeBytes = &size
	} else if !info.IsDir() || redirected(info) {
		return nil, failure("unsafe_attachment")
	}
	if e.Kind == "directory" {
		private, err := t.private.Stat(".")
		if err != nil || os.SameFile(private, info) {
			return nil, failure("unsafe_attachment")
		}
	}
	e.Revision = revision(loc, p, e.Kind, gen)
	return e, nil
}
func checkCondition(e *Entry, c Condition, fileOnly bool) error {
	if c.Create {
		if e != nil {
			return failure("revision_conflict")
		}
		return nil
	}
	if e == nil {
		return failure("revision_conflict")
	}
	if fileOnly && e.Kind != "file" {
		return failure("entry_type_conflict")
	}
	if e.Revision != c.Revision {
		return failure("revision_conflict")
	}
	return nil
}

// writeGeneration always runs under the stable ticket lock. Rotation precedes
// publication: an unsuccessful publication may invalidate tokens, never reuse
// them. A lock-only directory is initialized to zero before staging exists.
func (t *tree) writeGeneration(gen [16]byte) error {
	data, _ := json.Marshal(struct {
		Version    int    `json:"version"`
		Generation string `json:"generation"`
	}{1, base64.RawURLEncoding.EncodeToString(gen[:])})
	f, err := openRegular(t.private, "generation.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return storageError(err)
	}
	defer t.private.Remove("generation.tmp")
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = t.verify()
	}
	if err == nil {
		err = t.private.Rename("generation.tmp", "generation.json")
	}
	if err == nil {
		err = syncDirectory(t.private)
	}
	return storageErrorOrNil(err)
}
func storageErrorOrNil(err error) error {
	if err == nil {
		return nil
	}
	return storageError(err)
}
func (t *tree) prepare() ([16]byte, error) {
	gen, err := generation(t)
	if err != nil {
		return gen, err
	}
	// generation validates every known temp before removal. Never clean a live
	// upload: exclusive cross-process ownership was acquired before this call.
	for _, name := range []string{"upload.tmp", "generation.tmp"} {
		if err = t.private.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return gen, storageError(err)
		}
	}
	if _, err = t.private.Lstat("generation.json"); errors.Is(err, fs.ErrNotExist) {
		err = t.writeGeneration(gen)
	}
	return gen, storageErrorOrNil(err)
}
func (t *tree) rotate(ctx context.Context) ([16]byte, error) {
	var gen [16]byte
	if err := ctx.Err(); err != nil {
		return gen, err
	}
	if err := t.verify(); err != nil {
		return gen, err
	}
	for gen == ([16]byte{}) {
		if _, err := rand.Read(gen[:]); err != nil {
			return gen, err
		}
	}
	return gen, t.writeGeneration(gen)
}
func syncDirectory(dir *os.Root) error {
	f, err := dir.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return syncDir(f)
}
func (t *tree) finish(loc Location, actor, operation, p, from string, gen [16]byte, created bool) (Receipt, error) {
	r := Receipt{RepositoryID: loc.RepositoryID, TicketID: loc.TicketID, Actor: actor, Operation: operation, MutationApplied: true, RootRevision: revision(loc, "", "directory", gen), From: from, Created: created}
	var err error
	if operation != "delete" {
		r.Entry, err = t.entry(loc, p, gen)
	}
	if err == nil {
		err = syncDirectory(t.root)
	}
	// Nested publication requires syncing the changed parent, not just root.
	for _, changed := range []string{p, from} {
		if changed == "" {
			continue
		}
		dir, _, e := t.parent(changed)
		if e == nil {
			e = syncDirectory(dir)
		}
		if err == nil {
			err = e
		}
	}
	if err == nil {
		err = syncDirectory(t.private)
	}
	return r, mutationError(err, true)
}

// walk accounts only user metadata, with bounded descriptors and enumeration.
// Move matches the resolved source object, then carries the destination prefix
// through its descendants. Caller spelling is not filesystem identity (a volume
// may accept case aliases); revision tokens still bind the exact caller path.
func (t *tree) usage(ctx context.Context, from os.FileInfo, to string, replacing os.FileInfo) (int64, int, error) {
	var bytes int64
	count := 0
	var walk func(*os.Root, string, string) error
	walk = func(dir *os.Root, prefix, destinationPrefix string) error {
		f, err := dir.Open(".")
		if err != nil {
			return storageError(err)
		}
		defer f.Close()
		for {
			if err = ctx.Err(); err != nil {
				return err
			}
			names, e := f.Readdirnames(1)
			if e != nil && e != io.EOF {
				return storageError(e)
			}
			if len(names) == 0 {
				return nil
			}
			name := names[0]
			if prefix == "" && name == ".ticket-orc" {
				continue
			}
			p := name
			if prefix != "" {
				p = prefix + "/" + name
			}
			if err = ValidatePath(p, false); err != nil {
				return err
			}
			info, err := dir.Lstat(name)
			if err != nil {
				return storageError(err)
			}
			mapped := name
			if destinationPrefix != "" {
				mapped = destinationPrefix + "/" + name
			}
			if from != nil && os.SameFile(from, info) {
				mapped = to
			}
			if err = ValidatePath(mapped, false); err != nil {
				return err
			}
			count++
			if count > V1Limits().MaxEntries {
				return failure("limit_exceeded")
			}
			if info.IsDir() && !redirected(info) {
				// Close subtree handles as we return; do not retain a handle per entry.
				n := len(t.roots)
				pins := len(t.pins)
				child, err := t.descend(dir, name)
				if err != nil {
					return storageError(err)
				}
				err = walk(child, p, mapped)
				if err == nil {
					err = t.verify()
				}
				child.Close()
				t.roots = t.roots[:n]
				t.pins = t.pins[:pins]
				if err != nil {
					return err
				}
			} else {
				f, err := openRegular(dir, name, os.O_RDONLY)
				if err != nil {
					return storageError(err)
				}
				info, err = f.Stat()
				f.Close()
				if err != nil {
					return storageError(err)
				}
				if info.Size() > V1Limits().MaxFileBytes && !os.SameFile(info, replacing) || info.Size() > math.MaxInt64-bytes {
					return failure("limit_exceeded")
				}
				bytes += info.Size()
			}
		}
	}
	err := walk(t.root, "", "")
	return bytes, count, err
}
func quota(before, after int64, limit int64) bool {
	return after <= limit || before > limit && after < before
}
func (t *tree) uploadQuota(ctx context.Context, old *Entry, size int64) error {
	var replacing os.FileInfo
	if old != nil {
		var err error
		replacing, err = t.root.Lstat(old.Path)
		if err != nil {
			return storageError(err)
		}
	}
	bytes, count, err := t.usage(ctx, nil, "", replacing)
	if err != nil {
		return err
	}
	after := bytes
	if old != nil {
		after -= *old.SizeBytes
	}
	if after > math.MaxInt64-size {
		return failure("limit_exceeded")
	}
	after += size
	entries := count + 1
	if old != nil {
		entries = count
	}
	if !quota(bytes, after, V1Limits().MaxTotalBytes) || entries > V1Limits().MaxEntries {
		return failure("limit_exceeded")
	}
	return nil
}

func Mkdir(ctx context.Context, loc Location, actor, p string, c Condition) (r Receipt, err error) {
	defer func() { err = mutationError(err, r.MutationApplied) }()
	if err = validateMutation(actor, p, c, true); err != nil {
		return
	}
	t, e := openWritable(ctx, loc)
	if e != nil {
		err = e
		return
	}
	defer t.close()
	gen, e := t.prepare()
	if e != nil {
		err = e
		return
	}
	entry, e := t.entry(loc, p, gen)
	if e != nil {
		err = e
		return
	}
	if err = checkCondition(entry, c, false); err != nil {
		return
	}
	bytes, count, e := t.usage(ctx, nil, "", nil)
	if e != nil {
		err = e
		return
	}
	if count >= V1Limits().MaxEntries || bytes > V1Limits().MaxTotalBytes {
		err = failure("limit_exceeded")
		return
	}
	dir, name, e := t.parent(p)
	if e != nil {
		err = e
		return
	}
	commit, cancel := context.WithTimeout(ctx, V1Limits().CommitTimeout)
	defer cancel()
	gen, err = t.rotate(commit)
	if err != nil {
		return
	}
	if err = dir.Mkdir(name, 0700); err != nil {
		err = storageError(err)
		return
	}
	return t.finish(loc, actor, "mkdir", p, "", gen, true)
}
func Move(ctx context.Context, loc Location, actor, from, to, rev string, c Condition) (r Receipt, err error) {
	defer func() { err = mutationError(err, r.MutationApplied) }()
	if err = validateMutation(actor, to, c, true); err != nil {
		return
	}
	if err = ValidatePath(from, false); err != nil {
		return
	}
	if err = (Condition{Revision: rev}).validate(false); err != nil {
		return
	}
	if strings.EqualFold(from, to) || strings.HasPrefix(to, from+"/") {
		err = failure("invalid_path")
		return
	}
	if !safeMutationSupported {
		err = failure("capability_unavailable")
		return
	}
	t, absent, e := openTree(ctx, loc)
	if e != nil {
		err = e
		return
	}
	defer t.close()
	if absent {
		err = failure("revision_conflict")
		return
	}
	gen, e := t.prepare()
	if e != nil {
		err = e
		return
	}
	source, e := t.entry(loc, from, gen)
	if e != nil {
		err = e
		return
	}
	if err = checkCondition(source, Condition{Revision: rev}, false); err != nil {
		return
	}
	dest, e := t.entry(loc, to, gen)
	if e != nil {
		err = e
		return
	}
	if err = checkCondition(dest, c, false); err != nil {
		return
	}
	sourceInfo, e := t.root.Lstat(from)
	if e != nil {
		err = storageError(e)
		return
	}
	if bytes, _, e := t.usage(ctx, sourceInfo, to, nil); e != nil {
		err = e
		return
	} else if bytes > V1Limits().MaxTotalBytes {
		err = failure("limit_exceeded")
		return
	}
	if source.Kind == "directory" {
		if _, _, err = t.parent(to); err != nil {
			return
		}
		for _, root := range t.roots {
			current, e := root.Stat(".")
			if e != nil {
				err = storageError(e)
				return
			}
			if os.SameFile(sourceInfo, current) {
				err = failure("invalid_path")
				return
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return
	}
	if _, _, err = t.parent(to); err != nil {
		return
	}
	commit, cancel := context.WithTimeout(ctx, V1Limits().CommitTimeout)
	defer cancel()
	gen, err = t.rotate(commit)
	if err != nil {
		return
	}
	if err = t.root.Rename(from, to); err != nil {
		err = storageError(err)
		return
	}
	return t.finish(loc, actor, "move", to, from, gen, false)
}
func Delete(ctx context.Context, loc Location, actor, p string, c Condition, recursive bool) (r Receipt, err error) {
	defer func() { err = mutationError(err, r.MutationApplied) }()
	if recursive {
		err = failure("capability_unavailable")
		return
	}
	if err = validateMutation(actor, p, c, false); err != nil {
		return
	}
	if c.Create {
		err = failure("invalid_precondition")
		return
	}
	if !safeMutationSupported {
		err = failure("capability_unavailable")
		return
	}
	t, absent, e := openTree(ctx, loc)
	if e != nil {
		err = e
		return
	}
	defer t.close()
	if absent {
		err = failure("revision_conflict")
		return
	}
	gen, e := t.prepare()
	if e != nil {
		err = e
		return
	}
	entry, e := t.entry(loc, p, gen)
	if e != nil {
		err = e
		return
	}
	if err = checkCondition(entry, c, false); err != nil {
		return
	}
	dir, name, e := t.parent(p)
	if e != nil {
		err = e
		return
	}
	if entry.Kind == "directory" {
		child, e := t.descend(dir, name)
		if e != nil {
			err = e
			return
		}
		f, e := child.Open(".")
		if e != nil {
			err = storageError(e)
			return
		}
		names, e := f.Readdirnames(1)
		f.Close()
		if e != nil && e != io.EOF {
			err = storageError(e)
			return
		}
		if len(names) > 0 {
			err = failure("directory_not_empty")
			return
		}
	}
	commit, cancel := context.WithTimeout(ctx, V1Limits().CommitTimeout)
	defer cancel()
	gen, err = t.rotate(commit)
	if err != nil {
		return
	}
	if err = dir.Remove(name); err != nil {
		err = storageError(err)
		return
	}
	return t.finish(loc, actor, "delete", p, "", gen, false)
}
