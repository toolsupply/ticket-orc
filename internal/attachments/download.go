package attachments

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
)

// Store owns bounded upload/download admission for one daemon. Reuse one instance.
type Store struct{ slots chan struct{} }

func NewStore() *Store { return &Store{slots: make(chan struct{}, V1Limits().MaxActiveTransfers)} }

// Download owns admission, deadline and (after Open) a stable regular-file
// handle. Close is mandatory even when resolution/Open fails. Cancellation and
// the total transfer deadline also close it; no background body reader exists.
// Entry metadata describes the opened object. Only one Open is permitted.
type Download struct {
	Entry          Entry
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	file           *os.File
	remaining      int64
	closed, opened bool
	stop           func() bool
	release        func()
	done           chan struct{}
}

// BeginDownload must precede Ticket resolution; use Download.Context for that
// resolution and close resolver clients before Open. Close if resolution fails.
func (s *Store) BeginDownload(ctx context.Context) (*Download, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return nil, failure("attachment_busy")
	}
	ctx, cancel := context.WithTimeout(ctx, V1Limits().TransferTimeout)
	d := &Download{ctx: ctx, cancel: cancel, release: func() { <-s.slots }, done: make(chan struct{})}
	d.mu.Lock()
	d.stop = context.AfterFunc(ctx, func() { _ = d.Close() })
	d.mu.Unlock()
	return d, nil
}

func (d *Download) Open(loc Location, path string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ctx.Err(); err != nil {
		return err
	}
	if d.closed || d.opened {
		return failure("invalid_request")
	}
	d.opened = true
	if err := ValidatePath(path, false); err != nil {
		return err
	}
	t, absent, err := openTree(d.ctx, loc)
	if err != nil {
		return err
	}
	defer t.close()
	if absent {
		return failure("entry_not_found")
	}
	gen, err := generation(t)
	if err != nil {
		return err
	}
	parts := strings.Split(path, "/")
	dir := t.root
	for _, part := range parts[:len(parts)-1] {
		dir, err = t.descend(dir, part)
		if err != nil {
			return storageError(err)
		}
	}
	if info, err := dir.Lstat(parts[len(parts)-1]); err == nil && info.IsDir() && !redirected(info) {
		return failure("entry_type_conflict")
	}
	file, err := openRegular(dir, parts[len(parts)-1], os.O_RDONLY)
	if err != nil {
		return storageError(err)
	}
	info, err := file.Stat()
	if err == nil {
		err = t.verify()
	}
	if err == nil {
		err = d.ctx.Err()
	}
	if err != nil {
		file.Close()
		return storageError(err)
	}
	size := info.Size()
	d.Entry = Entry{parts[len(parts)-1], path, "file", &size, revision(loc, path, "file", gen)}
	d.file = file
	d.remaining = size
	return nil
}
func (d *Download) Read(p []byte) (int, error) {
	d.mu.Lock()
	if err := d.ctx.Err(); err != nil {
		d.mu.Unlock()
		return 0, err
	}
	if d.closed || d.file == nil {
		d.mu.Unlock()
		return 0, os.ErrClosed
	}
	if d.remaining == 0 {
		d.mu.Unlock()
		return 0, io.EOF
	}
	file := d.file
	size := min(int64(len(p)), d.remaining)
	d.mu.Unlock()
	n, err := file.Read(p[:size])
	d.mu.Lock()
	d.remaining -= int64(n)
	d.mu.Unlock()
	return n, err
}
func (d *Download) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	if d.stop != nil {
		d.stop()
	}
	d.cancel()
	var err error
	if d.file != nil {
		err = d.file.Close()
	}
	d.release()
	close(d.done)
	return err
}

// Done closes after cancellation/Close has released the file and admission.
func (d *Download) Done() <-chan struct{} { return d.done }

// Context includes the total transfer deadline, starting before resolution.
func (d *Download) Context() context.Context { return d.ctx }

var _ io.ReadCloser = (*Download)(nil)
