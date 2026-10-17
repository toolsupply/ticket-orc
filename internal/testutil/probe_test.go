package testutil

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

const testGuard = 2 * time.Second

func TestProbeWakesAllWaitersAndPreservesEventOrder(t *testing.T) {
	const waiters = 8
	var probe Probe[int]
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, waiters)
	results := make(chan []int, waiters)
	errs := make(chan error, waiters)
	checking := make(chan struct{}, waiters)
	t.Cleanup(func() {
		cancel()
		for range waiters {
			receive(t, done)
		}
	})
	for range waiters {
		go func() {
			values, err := probe.Wait(ctx, func(values []int) bool {
				if len(values) == 0 {
					checking <- struct{}{}
				}
				return len(values) == 2 && values[1] == 2
			})
			results <- values
			errs <- err
			done <- struct{}{}
		}()
	}
	for range waiters {
		receive(t, checking)
	}

	probe.Record(1)
	probe.Record(2)
	for range waiters {
		if err := receive(t, errs); err != nil {
			t.Fatalf("Wait error = %v", err)
		}
		if got := receive(t, results); !reflect.DeepEqual(got, []int{1, 2}) {
			t.Fatalf("Wait snapshot = %v, want [1 2]", got)
		}
	}
}

func TestProbeDoesNotLoseRecordBetweenPredicateAndWait(t *testing.T) {
	var probe Probe[int]
	ctx, cancel := context.WithTimeout(context.Background(), testGuard)
	defer cancel()
	values, err := probe.Wait(ctx, func(values []int) bool {
		if len(values) == 0 {
			// Record after Wait captured its empty snapshot/channel but before it
			// has selected on that channel.
			probe.Record(7)
			return false
		}
		return values[0] == 7
	})
	if err != nil || !reflect.DeepEqual(values, []int{7}) {
		t.Fatalf("Wait = %v, %v; want [7], nil", values, err)
	}
}

func TestProbePredicateRunsOutsideLock(t *testing.T) {
	var probe Probe[int]
	entered := make(chan struct{})
	continuePredicate := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	var releaseOnce sync.Once
	releasePredicate := func() { releaseOnce.Do(func() { close(continuePredicate) }) }
	type result struct {
		values []int
		err    error
	}
	resultCh := make(chan result, 1)
	waitDone := make(chan struct{})
	recordDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		releasePredicate()
		receive(t, recordDone)
		receive(t, waitDone)
	})
	go func() {
		defer close(waitDone)
		values, err := probe.Wait(ctx, func(values []int) bool {
			if len(values) == 0 {
				close(entered)
				<-continuePredicate
				return false
			}
			return values[0] == 1
		})
		resultCh <- result{values: values, err: err}
	}()
	go func() {
		defer close(recordDone)
		select {
		case <-entered:
			probe.Record(1)
		case <-ctx.Done():
		}
	}()
	// Record runs concurrently with the blocked predicate. It must complete
	// before the gate opens, proving the predicate does not hold Probe.mu.
	receive(t, recordDone)
	releasePredicate()
	got := receive(t, resultCh)
	if got.err != nil || !reflect.DeepEqual(got.values, []int{1}) {
		t.Fatalf("Wait = %v, %v; want [1], nil", got.values, got.err)
	}
}

func TestProbeSnapshotIsAnIndependentOrderedSlice(t *testing.T) {
	var probe Probe[string]
	probe.Record("first")
	probe.Record("second")

	snapshot := probe.Snapshot()
	snapshot[0] = "changed"
	if got := probe.Snapshot(); !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("Snapshot after caller mutation = %v", got)
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(testGuard)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for test synchronization")
		var zero T
		return zero
	}
}
