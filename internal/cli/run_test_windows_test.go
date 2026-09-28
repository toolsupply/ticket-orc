//go:build windows

package cli

import (
	"testing"
	"time"
)

// Windows has no portable child-interrupt signal; requestChildStop kills the
// child immediately. The shared test still verifies every child is reaped once
// and all workers reach terminal states, while Unix alone asserts the timeout.
func assertPartialStartCleanupDuration(t *testing.T, elapsed, timeout time.Duration) {
	t.Helper()
}
