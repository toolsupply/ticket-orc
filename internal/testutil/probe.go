// Package testutil provides small synchronization primitives for tests.
package testutil

import (
	"context"
	"sync"
)

// Probe records ordered observations and lets callers wait for a condition on
// the recorded history. Its zero value is ready for use.
//
// Probe copies the event slice, not values reachable through T. Values with
// mutable maps, slices, or pointers must remain immutable after Record, or be
// copied by the caller before recording.
type Probe[T any] struct {
	mu      sync.Mutex
	values  []T
	changed chan struct{}
}

// Record appends value and wakes every waiter to recheck its predicate.
func (p *Probe[T]) Record(value T) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.values = append(p.values, value)
	if p.changed == nil {
		p.changed = make(chan struct{})
	}
	close(p.changed)
	p.changed = make(chan struct{})
}

// Snapshot returns a copy of the recorded event slice in record order.
func (p *Probe[T]) Snapshot() []T {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.values)
}

// Wait returns the first snapshot for which predicate is true, or ctx's error.
// Predicate runs outside the Probe lock and receives an independent slice.
// ctx and predicate must be non-nil.
func (p *Probe[T]) Wait(ctx context.Context, predicate func([]T) bool) ([]T, error) {
	for {
		p.mu.Lock()
		if p.changed == nil {
			p.changed = make(chan struct{})
		}
		values := clone(p.values)
		changed := p.changed
		p.mu.Unlock()

		if predicate(values) {
			return values, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func clone[T any](values []T) []T {
	return append([]T(nil), values...)
}
