package attachments

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Each opened component remains pinned until publication. os.Root enforces
// confinement during lookup, even if a component is swapped between Lstat and
// open. Identity checks additionally reject in-root symlink redirection. Never
// replace these with Lstat followed by an unconfined absolute-path open.
type pinned struct {
	parent *os.Root
	name   string
	info   os.FileInfo
}
type tree struct {
	roots    []*os.Root
	pins     []pinned
	root     *os.Root
	lock     state.Lock
	lockFile *os.File
	private  *os.Root
}

func (t *tree) close() {
	if t.lock != nil {
		_ = t.lock.Release()
	}
	for i := len(t.roots) - 1; i >= 0; i-- {
		_ = t.roots[i].Close()
	}
}
func storageError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var domain *Error
	if errors.As(err, &domain) {
		return err
	}
	if errors.Is(err, fs.ErrNotExist) {
		return failure("entry_not_found")
	}
	return failure("attachment_storage_failed")
}
func (t *tree) descend(parent *os.Root, name string) (*os.Root, error) {
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || redirected(before) {
		return nil, failure("unsafe_attachment")
	}
	child, err := parent.OpenRoot(name)
	if err != nil {
		return nil, failure("unsafe_attachment")
	}
	after, err := child.Stat(".")
	if err != nil || !os.SameFile(before, after) {
		child.Close()
		return nil, failure("unsafe_attachment")
	}
	// User traversal must not enter private storage through another spelling
	// of the same directory (notably an actual Windows 8.3 alias). The private
	// handle is installed after intentional bootstrap, before user traversal.
	if t.private != nil {
		privateInfo, err := t.private.Stat(".")
		if err != nil || os.SameFile(privateInfo, after) {
			child.Close()
			return nil, failure("unsafe_attachment")
		}
	}
	t.roots = append(t.roots, child)
	t.pins = append(t.pins, pinned{parent, name, after})
	return child, nil
}
func (t *tree) verify() error {
	for _, p := range t.pins {
		current, err := p.parent.Lstat(p.name)
		if err != nil || !current.IsDir() || redirected(current) || !os.SameFile(p.info, current) {
			return failure("unsafe_attachment")
		}
	}
	if t.lockFile != nil {
		opened, err := t.lockFile.Stat()
		current, pathErr := t.private.Lstat("lock")
		if err != nil || pathErr != nil || redirected(current) || !os.SameFile(opened, current) || checkRegular(t.lockFile, opened) != nil {
			return failure("unsafe_attachment")
		}
	}
	return nil
}
func openTree(ctx context.Context, loc Location) (_ *tree, absent bool, result error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if !ticketclient.ValidRepositoryID(loc.RepositoryID) || ticketclient.ValidateFullID(loc.TicketID) != nil {
		return nil, false, failure("invalid_ticket")
	}
	if !filepath.IsAbs(loc.RepositoryRoot) || filepath.Clean(loc.RepositoryRoot) != loc.RepositoryRoot || !safeRelative(loc.TicketDirectory) {
		return nil, false, failure("unsafe_attachment")
	}
	t := &tree{}
	defer func() {
		if result != nil {
			t.close()
		}
	}()
	volume := filepath.VolumeName(loc.RepositoryRoot)
	anchor, err := os.OpenRoot(volume + string(filepath.Separator))
	if err != nil {
		return nil, false, storageError(err)
	}
	t.roots = append(t.roots, anchor)
	current := anchor
	rest := strings.TrimPrefix(loc.RepositoryRoot, volume+string(filepath.Separator))
	parts := []string{}
	if rest != "" {
		parts = append(parts, strings.Split(rest, string(filepath.Separator))...)
	}
	parts = append(parts, strings.Split(loc.TicketDirectory, "/")...)
	for _, part := range parts {
		current, err = t.descend(current, part)
		if err != nil {
			return nil, false, storageError(err)
		}
	}
	root, err := t.descend(current, "attachments")
	if errors.Is(err, fs.ErrNotExist) {
		if err = t.verify(); err != nil {
			return nil, false, err
		}
		return t, true, nil
	}
	if err != nil {
		return nil, false, storageError(err)
	}
	t.root = root
	// Bootstrap creates only the protected directory, never an absent root.
	if err = root.Mkdir(".ticket-orc", 0700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, false, storageError(err)
	}
	private, err := t.descend(root, ".ticket-orc")
	if err != nil {
		return nil, false, storageError(err)
	}
	file, err := openRegular(private, "lock", os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, false, storageError(err)
	}
	lock, err := state.TryAcquireFileLock(ctx, file)
	if err != nil {
		file.Close()
		if errors.Is(err, state.ErrLockTimeout) {
			return nil, false, failure("attachment_busy")
		}
		return nil, false, storageError(err)
	}
	t.lock = lock
	t.lockFile = file
	t.private = private
	if err = t.verify(); err != nil {
		return nil, false, err
	}
	return t, false, nil
}
func safeRelative(value string) bool {
	if value == "" || strings.ContainsAny(value, "\\:\x00") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
func openRegular(root *os.Root, name string, flags int) (*os.File, error) {
	before, err := root.Lstat(name)
	if err != nil && !(errors.Is(err, fs.ErrNotExist) && flags&os.O_CREATE != 0) {
		return nil, err
	}
	if before != nil && (!before.Mode().IsRegular() || redirected(before)) {
		return nil, failure("unsafe_attachment")
	}
	file, err := root.OpenFile(name, flags|safeOpenFlags, 0600)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err == nil {
		err = checkRegular(file, opened)
	}
	after, statErr := root.Lstat(name)
	if err != nil || statErr != nil || !after.Mode().IsRegular() || redirected(after) || !os.SameFile(opened, after) || (before != nil && !os.SameFile(before, opened)) {
		file.Close()
		return nil, failure("unsafe_attachment")
	}
	return file, nil
}
