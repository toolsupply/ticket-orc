package harness

import (
	"bytes"
	"context"
	"fmt"
	"os"
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
}

func TestHarnessProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HARNESS_PROCESS") != "1" {
		return
	}
	_, _ = fmt.Fprint(os.Stdout, "output")
	_, _ = fmt.Fprint(os.Stderr, "abcdef")
	os.Exit(7)
}
