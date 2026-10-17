package testutil

import (
	"fmt"
	"testing"
	"time"
)

const externalAwaitPollInterval = 10 * time.Millisecond

// Await polls condition until it succeeds or timeout expires. Use it only for
// genuine process, filesystem, or OS-visible boundaries (for example, a child
// process publishing a file). It is not a completion mechanism for in-process
// goroutines; prefer Probe, Barrier, or an explicit signal there. timeout is a
// failure guard, not evidence that the operation completed.
func Await(t *testing.T, timeout time.Duration, description string, condition func() bool, diagnostics func() string) {
	t.Helper()
	if err := awaitCondition(timeout, description, condition, diagnostics); err != nil {
		t.Fatal(err)
	}
}

func awaitCondition(timeout time.Duration, description string, condition func() bool, diagnostics func() string) error {
	if timeout <= 0 {
		return fmt.Errorf("await %s: timeout must be positive", description)
	}
	if condition == nil {
		return fmt.Errorf("await %s: condition must not be nil", description)
	}
	if description == "" {
		description = "external condition"
	}
	if condition() {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(externalAwaitPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if condition() {
				return nil
			}
		case <-timer.C:
			if condition() {
				return nil
			}
			message := fmt.Sprintf("timed out after %s waiting for %s", timeout, description)
			if diagnostics != nil {
				if details := diagnostics(); details != "" {
					message += ": " + details
				}
			}
			return fmt.Errorf("%s", message)
		}
	}
}
