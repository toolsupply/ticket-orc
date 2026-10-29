package attachments

import (
	"context"
	"io"
	"os"
	"sync"
)

// Upload reserves one daemon transfer slot before Ticket resolution. Stage
// holds only the ticket's filesystem lock. The caller must make its reader
// obey Context (for HTTP, set the body's read deadline); no goroutine is spawned
// to read an arbitrary, potentially non-cancelable io.Reader. Close is required
// on every path. Commit obtains a server-owned context after accepting the body.
type Upload struct {
	stageInfo                          os.FileInfo
	mu                                 sync.Mutex
	ctx                                context.Context
	cancel                             context.CancelFunc
	stop                               func() bool
	release                            func()
	done                               chan struct{}
	closed, started, staged, ownsStage bool
	tree                               *tree
	loc                                Location
	actor, p                           string
	condition                          Condition
	size                               int64
}

func (s *Store) BeginUpload(ctx context.Context) (*Upload, error) {
	if !safeMutationSupported {
		return nil, mutationError(failure("capability_unavailable"), false)
	}
	if err := ctx.Err(); err != nil {
		return nil, mutationError(err, false)
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return nil, mutationError(failure("attachment_busy"), false)
	}
	ctx, cancel := context.WithTimeout(ctx, V1Limits().TransferTimeout)
	u := &Upload{ctx: ctx, cancel: cancel, release: func() { <-s.slots }, done: make(chan struct{})}
	u.mu.Lock()
	u.stop = context.AfterFunc(ctx, func() { u.Close() })
	u.mu.Unlock()
	return u, nil
}
func (u *Upload) Context() context.Context { return u.ctx }
func (u *Upload) Done() <-chan struct{}    { return u.done }

// Stage accepts -1 for an unknown length; otherwise it verifies exact length.
// Its fixed buffer and limit+1 read detect oversize without unbounded spooling.
// A stage failure cleans up synchronously and releases both lock and admission.
func (u *Upload) Stage(loc Location, actor, p string, c Condition, body io.Reader, length int64) (err error) {
	u.mu.Lock()
	defer func() {
		if err != nil {
			u.closeLocked()
		}
		u.mu.Unlock()
		err = mutationError(err, false)
	}()
	if u.closed || u.started {
		return failure("invalid_request")
	}
	u.started = true
	if err = u.ctx.Err(); err != nil {
		return err
	}
	if err = validateMutation(actor, p, c, false); err != nil {
		return err
	}
	if body == nil || length < -1 {
		return failure("invalid_request")
	}
	limit := min(V1Limits().MaxFileBytes, V1Limits().MaxStagingBytesPerTicket)
	if length > limit {
		return &Error{Code: "limit_exceeded", FileSizeLimit: true}
	}
	t, err := openWritable(u.ctx, loc)
	if err != nil {
		return err
	}
	u.tree = t
	gen, err := t.prepare()
	if err != nil {
		return err
	}
	old, err := t.entry(loc, p, gen)
	if err != nil {
		return err
	}
	if err = checkCondition(old, c, true); err != nil {
		return err
	}
	if err = t.uploadQuota(u.ctx, old, max(length, 0)); err != nil {
		return err
	}
	stage, err := openRegular(t.private, "upload.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return storageError(err)
	}
	defer stage.Close()
	u.stageInfo, err = stage.Stat()
	if err != nil {
		return storageError(err)
	}
	u.ownsStage = true
	var buffer [32 * 1024]byte
	var size int64
	emptyReads := 0
	for {
		if err = u.ctx.Err(); err != nil {
			return err
		}
		n, e := body.Read(buffer[:min(int64(len(buffer)), limit-size+1)])
		if n < 0 || n > len(buffer) {
			return failure("invalid_request")
		}
		if int64(n) > limit-size {
			return &Error{Code: "limit_exceeded", FileSizeLimit: true}
		}
		if n > 0 {
			emptyReads = 0
			written, werr := stage.Write(buffer[:n])
			if werr != nil {
				return storageError(werr)
			}
			if written != n {
				return failure("attachment_storage_failed")
			}
			size += int64(n)
		} else if e == nil {
			emptyReads++
			if emptyReads >= 100 {
				return failure("invalid_request")
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return storageError(e)
		}
	}
	if err = u.ctx.Err(); err != nil {
		return err
	}
	if length >= 0 && size != length {
		return failure("invalid_request")
	}
	if err = stage.Sync(); err != nil {
		return storageError(err)
	}
	if err = stage.Close(); err != nil {
		return storageError(err)
	}
	u.loc = loc
	u.actor = actor
	u.p = p
	u.condition = c
	u.size = size
	u.staged = true
	return nil
}

// CommitContext obtains the server-owned mutation lifetime after body acceptance.
// The daemon composes this with Server.MutationContext: client disconnect is
// independent, while server shutdown and the commit deadline remain effective.
// The factory must return promptly without IO or control waits; success supplies
// a non-nil context and cancel function, which Commit owns and calls on exit.
type CommitContext func() (context.Context, context.CancelFunc, error)

func (u *Upload) Commit(beginCommit CommitContext) (r Receipt, err error) {
	u.mu.Lock()
	defer func() { u.closeLocked(); u.mu.Unlock(); err = mutationError(err, r.MutationApplied) }()
	if u.closed || !u.staged || beginCommit == nil {
		return r, failure("invalid_request")
	}
	// Stop the body deadline before entering the bounded server-owned commit.
	// If cancellation already won, no namespace publication is attempted.
	if err = u.ctx.Err(); err != nil {
		return r, err
	}
	if !u.stop() {
		// The body cancellation callback already won acceptance.
		return r, u.ctx.Err()
	}
	ctx, release, err := beginCommit()
	if err != nil {
		return r, err
	}
	defer release()
	ctx, cancel := context.WithTimeout(ctx, V1Limits().CommitTimeout)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return r, err
	}
	t := u.tree
	if err = t.verify(); err != nil {
		return r, err
	}
	gen, err := generation(t)
	if err != nil {
		return r, err
	}
	old, err := t.entry(u.loc, u.p, gen)
	if err != nil {
		return r, err
	}
	if err = checkCondition(old, u.condition, true); err != nil {
		return r, err
	}
	if err = t.uploadQuota(ctx, old, u.size); err != nil {
		return r, err
	}
	// Reopen the staged object through the same no-link primitive at commit.
	stage, err := openRegular(t.private, "upload.tmp", os.O_RDONLY)
	if err != nil {
		return r, storageError(err)
	}
	info, err := stage.Stat()
	stage.Close()
	if err != nil {
		return r, storageError(err)
	}
	if info.Size() != u.size || !os.SameFile(u.stageInfo, info) {
		return r, failure("unsafe_attachment")
	}
	gen, err = t.rotate(ctx)
	if err != nil {
		return r, err
	}
	if err = ctx.Err(); err != nil {
		return r, err
	}
	if err = t.root.Rename(".ticket-orc/upload.tmp", u.p); err != nil {
		return r, storageError(err)
	}
	return t.finish(u.loc, u.actor, "upload", u.p, "", gen, old == nil)
}
func (u *Upload) closeLocked() {
	if u.closed {
		return
	}
	u.closed = true
	if u.stop != nil {
		u.stop()
	}
	u.cancel()
	if u.tree != nil {
		// Only remove a stage owned by this operation. prepare already validates
		// abandoned metadata; never remove an unsafe preexisting file on failure.
		if u.ownsStage {
			info, err := u.tree.private.Lstat("upload.tmp")
			if err == nil && info.Mode().IsRegular() && !redirected(info) && os.SameFile(u.stageInfo, info) {
				_ = u.tree.private.Remove("upload.tmp")
			}
		}
		u.tree.close()
	}
	u.release()
	close(u.done)
}
func (u *Upload) Close() error { u.mu.Lock(); defer u.mu.Unlock(); u.closeLocked(); return nil }
