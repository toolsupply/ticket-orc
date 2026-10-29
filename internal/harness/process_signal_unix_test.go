//go:build unix

package harness

import (
	"context"
	"os"
	"syscall"
	"testing"
)

func TestRunProcessSignalEvidence(t *testing.T) {
	if os.Getenv("GO_WANT_HARNESS_SIGNAL") == "1" {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		os.Exit(99)
	}
	result, err := RunProcess(context.Background(), ProcessRequest{
		Executable: os.Args[0], Args: []string{"-test.run=^TestRunProcessSignalEvidence$"},
		Env: append(os.Environ(), "GO_WANT_HARNESS_SIGNAL=1"),
	})
	if err == nil || result.ExitCode != -1 || result.Outcome.Signal != int(syscall.SIGKILL) {
		t.Fatalf("signal evidence = %+v, %v", result, err)
	}
}
