package testutil

import (
	"fmt"
	"testing"
	"time"
)

// Receive waits for one value from ch. timeout is only a failure guard; use
// Probe, Barrier, or another completion signal for in-process coordination.
// description identifies the operation being awaited. A closed channel before
// a value arrives is a failure, not a zero-value result.
func Receive[T any](t *testing.T, timeout time.Duration, description string, ch <-chan T) T {
	t.Helper()
	value, err := receiveValue(timeout, description, ch)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func receiveValue[T any](timeout time.Duration, description string, ch <-chan T) (T, error) {
	var zero T
	if timeout <= 0 {
		return zero, fmt.Errorf("receive %s: timeout must be positive", description)
	}
	if description == "" {
		description = "channel value"
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case value, ok := <-ch:
		if !ok {
			return zero, fmt.Errorf("receive %s: channel closed before a value arrived", description)
		}
		return value, nil
	case <-timer.C:
		return zero, fmt.Errorf("timed out after %s waiting for %s", timeout, description)
	}
}
