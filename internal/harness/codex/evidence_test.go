package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestExecutionEvidenceKeepsIndependentFailures(t *testing.T) {
	processErr := errors.New("original process outcome")
	outputErr := errors.New("output unavailable")
	for _, test := range []struct {
		name, stream string
		code         int
		output       io.Writer
		protocol     bool
		outputFailed bool
		provider     bool
	}{
		{name: "crash after session", stream: "{\"type\":\"thread.started\",\"thread_id\":\"private-session\"}\n", code: 17},
		{name: "completion then crash", stream: "{\"type\":\"turn.completed\"}\n", code: 17},
		{name: "malformed and crash", stream: "{\n", code: 17, protocol: true},
		{name: "output and crash", stream: "{\"type\":\"turn.completed\"}\n", code: 17, output: failingEvidenceWriter{outputErr}, outputFailed: true},
		{name: "protocol output and crash", stream: "{\n", code: 17, output: failingEvidenceWriter{outputErr}, protocol: true, outputFailed: true},
		{name: "explicit provider failure", stream: "{\"type\":\"turn.failed\",\"error\":{\"message\":\"private-token\"}}\n", provider: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, err := NewManaged("test-codex", nil, func(_ context.Context, request harness.ProcessRequest) (harness.ProcessResult, error) {
				_, streamErr := io.WriteString(request.Stdout, test.stream)
				var cause error
				if test.code != 0 {
					cause = processErr
				}
				return harness.ProcessResult{ExitCode: test.code, Stderr: "private-token", Outcome: harness.ProcessOutcome{Start: "started", ExitObserved: true, ExitCode: test.code, Cancellation: "none"}}, errors.Join(cause, streamErr)
			})
			if err != nil {
				t.Fatal(err)
			}
			operator := test.output
			if operator == nil {
				operator = io.Discard
			}
			request := validRequest(t, operator)
			if test.output != nil {
				request.OutputMode = "json"
			}
			result, err := adapter.Run(context.Background(), request)
			var executionErr *harness.ExecutionError
			if !errors.As(err, &executionErr) || executionErr.Result.LogPath != result.LogPath || executionErr.Result.SessionID != result.SessionID {
				t.Fatalf("private evidence lost: %+v, %v", result, err)
			}
			if test.code != 0 && !errors.Is(err, processErr) {
				t.Fatalf("lost original process outcome: %v", err)
			}
			if test.outputFailed && !errors.Is(err, outputErr) {
				t.Fatalf("lost output failure: %v", err)
			}
			if test.protocol && !errors.Is(err, ErrProtocol) {
				t.Fatalf("lost protocol failure: %v", err)
			}
			e := result.Evidence
			if e == nil || !e.Process.ExitObserved || e.Process.ExitCode != test.code || e.ProtocolFailed != test.protocol || e.OutputFailed != test.outputFailed || e.ProviderFailed != test.provider {
				t.Fatalf("incorrect evidence: %+v", e)
			}
			raw, readErr := os.ReadFile(result.LogPath)
			if readErr != nil || string(raw) != test.stream || e.LogID == "" {
				t.Fatalf("lost private log: %q, %v", raw, readErr)
			}
			public, _ := json.Marshal(e.Bounded())
			for _, secret := range []string{"private-session", "private-token", request.StateDir} {
				if strings.Contains(string(public), secret) {
					t.Fatalf("private data entered evidence: %s", public)
				}
			}
		})
	}
}

type failingEvidenceWriter struct{ err error }

func (w failingEvidenceWriter) Write([]byte) (int, error) { return 0, w.err }

func TestExecutionEvidenceStartupAndCancellation(t *testing.T) {
	adapter, err := NewWithExecutable(t.TempDir() + "/missing")
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Run(context.Background(), validRequest(t, io.Discard))
	if err == nil || result.Evidence.Process.Start != "not_started" || result.Evidence.Process.ExitObserved || result.Evidence.Phase != "startup" {
		t.Fatalf("startup evidence: %+v, %v", result, err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	adapter, err = NewManaged("test-codex", nil, func(_ context.Context, request harness.ProcessRequest) (harness.ProcessResult, error) {
		_, _ = io.WriteString(request.Stdout, "{\"type\":\"thread.started\",\"thread_id\":\"session\"}\n")
		cancel(errors.New("private shutdown cause"))
		return harness.ProcessResult{ExitCode: -1, Outcome: harness.ProcessOutcome{Start: "started", ExitObserved: true, ExitCode: -1}}, context.Canceled
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err = adapter.Run(ctx, validRequest(t, io.Discard))
	if !errors.Is(err, context.Canceled) || result.Evidence.Process.Cancellation != "canceled" {
		t.Fatalf("cancellation evidence: %+v, %v", result, err)
	}
}
