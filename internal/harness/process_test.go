package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestRunProcessStreamsStdoutAndHonorsStderrLimit(t *testing.T) {
	var stdout bytes.Buffer
	result, err := RunProcess(context.Background(), ProcessRequest{
		Executable:  os.Args[0],
		Args:        []string{"-test.run=TestHarnessProcessHelper", "--"},
		Env:         append(os.Environ(), "GO_WANT_HARNESS_PROCESS=1"),
		Stdout:      &stdout,
		StderrLimit: 3,
	})
	if err == nil || result.ExitCode != 7 {
		t.Fatalf("process result = %#v, %v; want exit code 7", result, err)
	}
	if stdout.String() != "output" {
		t.Errorf("stdout = %q, want %q", stdout.String(), "output")
	}
	if result.Stderr != "abc" {
		t.Errorf("stderr = %q, want bounded content %q", result.Stderr, "abc")
	}
	if result.Outcome.Start != "started" || !result.Outcome.ExitObserved || result.Outcome.ExitCode != 7 {
		t.Fatalf("lost process facts or invented descendant cleanup: %+v", result.Outcome)
	}
}

type failedProcessOutput struct{ err error }

func (w failedProcessOutput) Write([]byte) (int, error) { return 0, w.err }

func TestRunProcessJoinsOutputAndOriginalWaitOutcome(t *testing.T) {
	outputErr := errors.New("output unavailable")
	result, err := RunProcess(context.Background(), ProcessRequest{
		Executable: os.Args[0], Args: []string{"-test.run=TestHarnessProcessHelper", "--"},
		Env: append(os.Environ(), "GO_WANT_HARNESS_PROCESS=1"), Stdout: failedProcessOutput{outputErr},
	})
	var exitErr *exec.ExitError
	if !errors.Is(err, outputErr) || !errors.As(err, &exitErr) {
		t.Fatalf("output failure hid wait outcome: %+v, %v", result, err)
	}
	if !result.Outcome.ExitObserved || result.ExitCode != exitErr.ExitCode() || result.Outcome.ExitCode != exitErr.ExitCode() {
		t.Fatalf("reported output failure instead of process outcome: %+v, %v", result, err)
	}
}

func TestRunProcessNotStarted(t *testing.T) {
	result, err := RunProcess(context.Background(), ProcessRequest{Executable: t.TempDir() + "/missing"})
	if err == nil || result.Outcome.Start != "not_started" || result.Outcome.ExitObserved {
		t.Fatalf("startup evidence = %+v, %v", result, err)
	}
}

func TestHarnessProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HARNESS_PROCESS") != "1" {
		return
	}
	_, _ = fmt.Fprint(os.Stdout, "output")
	_, _ = fmt.Fprint(os.Stderr, "abcdef")
	os.Exit(7)
}
