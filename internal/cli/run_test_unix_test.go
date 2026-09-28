//go:build !windows

package cli

import (
	"testing"
	"time"
)

// Unix requests a graceful interrupt first, so the signal-ignoring test child
// must remain alive until the stop timeout's forced-kill path runs.
func assertPartialStartCleanupDuration(t *testing.T, elapsed, timeout time.Duration) {
	t.Helper()
	if elapsed < timeout {
		t.Fatalf("partial start cleanup took %s, want at least timeout %s", elapsed, timeout)
	}
}
