package testutil

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBarrierReleaseBeforeArrivalAndIdempotentRelease(t *testing.T) {
	barrier := NewBarrier()
	t.Cleanup(barrier.Release)
	barrier.Release()
	barrier.Release()

	if err := barrier.Arrive(context.Background()); err != nil {
		t.Fatalf("Arrive after Release: %v", err)
	}
	if err := barrier.WaitReached(context.Background()); err != nil {
		t.Fatalf("WaitReached after Arrive: %v", err)
	}
}

func TestBarrierWaitsForReleaseAfterArrival(t *testing.T) {
	barrier := NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	arrived := make(chan error, 1)
	arriveDone := make(chan struct{})
	t.Cleanup(func() {
		cleanupBarrierArrive(t, barrier, cancel, arriveDone)
		cancel()
	})
	go func() {
		defer close(arriveDone)
		arrived <- barrier.Arrive(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), testGuard)
	defer waitCancel()
	if err := barrier.WaitReached(waitCtx); err != nil {
		t.Fatalf("WaitReached: %v", err)
	}
	select {
	case err := <-arrived:
		t.Fatalf("Arrive returned before Release: %v", err)
	default:
	}

	barrier.Release()
	barrier.Release()
	if err := receive(t, arrived); err != nil {
		t.Fatalf("Arrive after Release: %v", err)
	}
}

func TestBarrierWaitReachedCanWaitBeforeArrival(t *testing.T) {
	barrier := NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	reached := make(chan error, 1)
	reachedDone := make(chan struct{})
	arrived := make(chan error, 1)
	arriveDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		barrier.Release()
		receive(t, arriveDone)
		receive(t, reachedDone)
	})
	go func() {
		defer close(reachedDone)
		reached <- barrier.WaitReached(ctx)
	}()
	go func() {
		defer close(arriveDone)
		arrived <- barrier.Arrive(ctx)
	}()
	if err := receive(t, reached); err != nil {
		t.Fatalf("WaitReached: %v", err)
	}
	barrier.Release()
	if err := receive(t, arrived); err != nil {
		t.Fatalf("Arrive: %v", err)
	}
}

func TestBarrierSupportsCancellation(t *testing.T) {
	t.Run("wait reached", func(t *testing.T) {
		barrier := NewBarrier()
		t.Cleanup(barrier.Release)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := barrier.WaitReached(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitReached error = %v, want context.Canceled", err)
		}
	})

	t.Run("arrive", func(t *testing.T) {
		barrier := NewBarrier()
		t.Cleanup(barrier.Release)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := barrier.Arrive(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Arrive error = %v, want context.Canceled", err)
		}
	})
}

func TestBarrierCleanupReleaseUnblocksArrive(t *testing.T) {
	barrier := NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	arrived := make(chan error, 1)
	arriveDone := make(chan struct{})
	t.Cleanup(func() {
		cleanupBarrierArrive(t, barrier, cancel, arriveDone)
		if err := receive(t, arrived); err != nil {
			t.Errorf("Arrive after cleanup Release: %v", err)
		}
		cancel()
	})
	go func() {
		defer close(arriveDone)
		arrived <- barrier.Arrive(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), testGuard)
	defer waitCancel()
	if err := barrier.WaitReached(waitCtx); err != nil {
		t.Fatalf("WaitReached: %v", err)
	}
	// Cleanup releases the barrier and joins Arrive without a normal Release.
}

func cleanupBarrierArrive(t *testing.T, barrier *Barrier, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	barrier.Release()
	timer := time.NewTimer(testGuard)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		cancel()
		t.Errorf("Arrive did not finish after Release; canceled for cleanup")
		receive(t, done)
	}
}
