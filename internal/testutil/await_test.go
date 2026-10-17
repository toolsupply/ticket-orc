package testutil

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAwaitConditionReturnsWhenExternalConditionIsMet(t *testing.T) {
	var checks atomic.Int32
	Await(t, time.Second, "fake process marker", func() bool {
		return checks.Add(1) == 2
	}, nil)
	if got := checks.Load(); got < 2 {
		t.Fatalf("condition checks = %d, want at least 2", got)
	}
}

func TestAwaitConditionTimeoutIncludesDescriptionAndDiagnostics(t *testing.T) {
	err := awaitCondition(10*time.Millisecond, "child readiness file", func() bool {
		return false
	}, func() string {
		return "pid=1234, last status=starting"
	})
	if err == nil {
		t.Fatal("awaitCondition succeeded for a false condition")
	}
	for _, want := range []string{"timed out", "child readiness file", "pid=1234", "last status=starting"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("awaitCondition error %q does not contain %q", err, want)
		}
	}
}

func TestAwaitConditionOmitsEmptyDiagnostics(t *testing.T) {
	err := awaitCondition(10*time.Millisecond, "external marker", func() bool { return false }, func() string { return "" })
	if err == nil || strings.Contains(err.Error(), ": ") {
		t.Fatalf("awaitCondition error = %v, want concise timeout without empty diagnostics", err)
	}
}
