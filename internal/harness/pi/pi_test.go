package pi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

type fakeProcessRunner struct {
	chunks [][]byte
	result processResult
	err    error
	calls  []processRequest
}

func (f *fakeProcessRunner) run(_ context.Context, request processRequest) (processResult, error) {
	copyRequest := request
	copyRequest.args = append([]string(nil), request.args...)
	copyRequest.env = append([]string(nil), request.env...)
	copyRequest.stdout = nil
	f.calls = append(f.calls, copyRequest)
	for _, chunk := range f.chunks {
		if _, err := request.stdout.Write(chunk); err != nil {
			return processResult{exitCode: -1}, err
		}
	}
	return f.result, f.err
}

func testAdapter(t *testing.T, environ []string, runner *fakeProcessRunner) *Adapter {
	t.Helper()
	if environ == nil {
		environ = []string{"PATH=/tools"}
	}
	adapter, err := newAdapter("/test/pi", func() []string { return environ }, runner.run)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func validRequest(t *testing.T, operator io.Writer) harness.RunRequest {
	t.Helper()
	return harness.RunRequest{WorkingDir: "/work/project", Role: "coder", Actor: "worker", TicketID: "20260919-12345", Prompt: "implement ticket", StateDir: t.TempDir(), OutputMode: "compact", Operator: operator}
}

func TestRunBuildsArgumentsStreamsSessionAndRenders(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{
		[]byte(`{"type":"session","id":"session-1","version":3}` + "\n"),
		[]byte(`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}` + "\n"),
		[]byte(`{"type":"turn_end"}` + "\n"),
	}}
	adapter := testAdapter(t, []string{"PATH=/tools", "TICKET_ACTOR=old", "KEEP=value"}, runner)
	var output bytes.Buffer
	request := validRequest(t, &output)
	request.Model = "model-x"
	request.Reasoning = "high"
	request.PiProvider = "openai"
	request.RequireSession = true
	result, err := adapter.Run(context.Background(), request)
	if err != nil || result.SessionID != "session-1" || result.ExitCode != 0 || !result.StreamEndedNormally {
		t.Fatalf("Run = %#v, %v", result, err)
	}
	want := []string{"--mode", "json", "--model", "model-x", "--thinking", "high", "--provider", "openai", "implement ticket"}
	if !reflect.DeepEqual(runner.calls[0].args, want) {
		t.Fatalf("args = %#v, want %#v", runner.calls[0].args, want)
	}
	if !contains(runner.calls[0].env, "KEEP=value") || countPrefix(runner.calls[0].env, "TICKET_ACTOR=") != 1 || !contains(runner.calls[0].env, "TICKET_ACTOR=worker") {
		t.Fatalf("environment = %#v", runner.calls[0].env)
	}
	if output.String() != "done\n[pi] turn complete\n" {
		t.Fatalf("operator output = %q", output.String())
	}
	raw, err := os.ReadFile(result.LogPath)
	if err != nil || !strings.Contains(string(raw), `"type":"session"`) {
		t.Fatalf("raw log = %q, %v", raw, err)
	}
}

func TestFreshAndResumeArguments(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"turn_end"}`)}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	result, err := adapter.Run(context.Background(), request)
	if err != nil || result.SessionID != "" {
		t.Fatalf("fresh Run = %#v, %v", result, err)
	}
	if want := []string{"--mode", "json", "--no-session", "implement ticket"}; !reflect.DeepEqual(runner.calls[0].args, want) {
		t.Fatalf("fresh args = %#v, want %#v", runner.calls[0].args, want)
	}
	runner.chunks = [][]byte{[]byte(`{"type":"session","id":"session-1"}`)}
	request.RequireSession = true
	if _, err := adapter.Resume(context.Background(), "session-1", request); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if want := []string{"--mode", "json", "--session", "session-1", "implement ticket"}; !reflect.DeepEqual(runner.calls[1].args, want) {
		t.Fatalf("resume args = %#v, want %#v", runner.calls[1].args, want)
	}
}

func TestResumeRejectsChangedSessionAndMalformedStream(t *testing.T) {
	for _, stream := range []string{`{"type":"session","id":"other"}`, "{"} {
		runner := &fakeProcessRunner{chunks: [][]byte{[]byte(stream)}}
		adapter := testAdapter(t, nil, runner)
		_, err := adapter.Resume(context.Background(), "session-1", validRequest(t, io.Discard))
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("error = %v, want protocol error", err)
		}
	}
}

func TestNewSessionIDUsesBoundedOpaqueValidation(t *testing.T) {
	for _, id := range []string{strings.Repeat("x", 513), "session with space"} {
		runner := &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"session","id":"` + id + `"}`)}}
		adapter := testAdapter(t, nil, runner)
		_, err := adapter.Run(context.Background(), validRequest(t, io.Discard))
		if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "invalid session ID") {
			t.Fatalf("id %q error = %v, want bounded ID protocol error", id, err)
		}
	}
}

func TestPiCapabilitiesAndValidation(t *testing.T) {
	adapter := New()
	if got := adapter.Capabilities(); got.CleanupDelete || got.CleanupArchive || !got.Resume || !got.Fresh {
		t.Fatalf("capabilities = %#v", got)
	}
	if err := adapter.Validate(harness.PreflightConfig{Reasoning: "minimal", OutputMode: "compact"}); err != nil {
		t.Fatalf("minimal reasoning rejected: %v", err)
	}
	if err := adapter.Validate(harness.PreflightConfig{Reasoning: "ultra", OutputMode: "compact"}); err == nil || !strings.Contains(err.Error(), "thinking") {
		t.Fatalf("invalid reasoning = %v", err)
	}
	if err := adapter.Cleanup(context.Background(), "session-1", harness.CleanupDelete); err == nil {
		t.Fatal("Pi accepted unsupported delete cleanup")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func countPrefix(values []string, prefix string) int {
	count := 0
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			count++
		}
	}
	return count
}
