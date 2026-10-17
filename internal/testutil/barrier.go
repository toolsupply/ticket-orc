package testutil

import (
	"context"
	"sync"
)

// Barrier is a one-shot gate. Arrive announces that the gate was reached and
// blocks until Release or context cancellation. Its channel handoffs are
// permanent, so callers may wait before or after the corresponding operation.
type Barrier struct {
	reached     chan struct{}
	release     chan struct{}
	reachedOnce sync.Once
	releaseOnce sync.Once
}

// NewBarrier creates a one-shot barrier.
func NewBarrier() *Barrier {
	return &Barrier{
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// Arrive signals that the barrier has been reached, then waits for Release or
// ctx cancellation. ctx must be non-nil.
func (b *Barrier) Arrive(ctx context.Context) error {
	b.reachedOnce.Do(func() { close(b.reached) })
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitReached waits for Arrive to signal the barrier or ctx to be canceled.
// ctx must be non-nil.
func (b *Barrier) WaitReached(ctx context.Context) error {
	select {
	case <-b.reached:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release opens the gate. It is safe to call more than once, including from
// cleanup after normal release.
func (b *Barrier) Release() {
	b.releaseOnce.Do(func() { close(b.release) })
}
