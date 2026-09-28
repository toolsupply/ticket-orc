package claude

import (
	"bytes"
	"context"
	"encoding/json"
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
	adapter, err := newAdapter("/test/claude", func() []string { return environ }, runner.run)
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
		[]byte(`{"type":"system","subtype":"init","session_id":"session-1"}` + "\n"),
		[]byte(`{"type":"assistant","session_id":"session-1","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}` + "\n"),
		[]byte(`{"type":"result","subtype":"success","session_id":"session-1","result":"done"}` + "\n"),
	}}
	adapter := testAdapter(t, []string{"PATH=/tools", "TICKET_ACTOR=old", "KEEP=value"}, runner)
	var output bytes.Buffer
	request := validRequest(t, &output)
	request.Model = "model-x"
	request.Reasoning = "high"
	request.ClaudePermissionMode = "acceptEdits"
	request.RequireSession = true
	result, err := adapter.Run(context.Background(), request)
	if err != nil || result.SessionID != "session-1" || result.ExitCode != 0 || !result.StreamEndedNormally {
		t.Fatalf("Run = %#v, %v", result, err)
	}
	want := []string{"-p", "--output-format", "stream-json", "--verbose", "--model", "model-x", "--effort", "high", "--permission-mode", "acceptEdits", "implement ticket"}
	if !reflect.DeepEqual(runner.calls[0].args, want) {
		t.Fatalf("args = %#v, want %#v", runner.calls[0].args, want)
	}
	if !contains(runner.calls[0].env, "KEEP=value") || countPrefix(runner.calls[0].env, "TICKET_ACTOR=") != 1 || !contains(runner.calls[0].env, "TICKET_ACTOR=worker") {
		t.Fatalf("environment = %#v", runner.calls[0].env)
	}
	if output.String() != "done\n[claude] turn complete\n" {
		t.Fatalf("operator output = %q", output.String())
	}
	raw, err := os.ReadFile(result.LogPath)
	if err != nil || !strings.Contains(string(raw), `"type":"system"`) {
		t.Fatalf("raw log = %q, %v", raw, err)
	}
}

func TestFreshAndResumeArguments(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"result","subtype":"success","session_id":"session-1"}`)}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	result, err := adapter.Run(context.Background(), request)
	if err != nil || result.SessionID != "session-1" {
		t.Fatalf("fresh Run = %#v, %v", result, err)
	}
	if want := []string{"-p", "--output-format", "stream-json", "--verbose", "--no-session-persistence", "implement ticket"}; !reflect.DeepEqual(runner.calls[0].args, want) {
		t.Fatalf("fresh args = %#v, want %#v", runner.calls[0].args, want)
	}
	runner.chunks = [][]byte{[]byte(`{"type":"system","subtype":"init","session_id":"session-1"}` + "\n"), []byte(`{"type":"result","subtype":"success","session_id":"session-1"}`)}
	request.RequireSession = true
	if _, err := adapter.Resume(context.Background(), "session-1", request); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if want := []string{"-p", "--output-format", "stream-json", "--verbose", "--resume", "session-1", "implement ticket"}; !reflect.DeepEqual(runner.calls[1].args, want) {
		t.Fatalf("resume args = %#v, want %#v", runner.calls[1].args, want)
	}
}

func TestResumeRejectsChangedSessionAndMalformedStream(t *testing.T) {
	for _, stream := range []string{`{"type":"system","subtype":"init","session_id":"other"}`, "{"} {
		runner := &fakeProcessRunner{chunks: [][]byte{[]byte(stream)}}
		adapter := testAdapter(t, nil, runner)
		_, err := adapter.Resume(context.Background(), "session-1", validRequest(t, io.Discard))
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("error = %v, want protocol error", err)
		}
	}
}

func TestResultFailureAndMissingInitIdentityAreProtocolErrors(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"system","subtype":"init","session_id":"session-1"}` + "\n"), []byte(`{"type":"result","subtype":"error_during_execution","is_error":true,"session_id":"session-1"}` + "\n")}}
	adapter := testAdapter(t, nil, runner)
	result, err := adapter.Run(context.Background(), validRequest(t, io.Discard))
	if !errors.Is(err, ErrProtocol) || result.SessionID != "session-1" {
		t.Fatalf("failure result = %#v, %v", result, err)
	}
	runner = &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"system","subtype":"init"}`)}}
	adapter = testAdapter(t, nil, runner)
	_, err = adapter.Run(context.Background(), validRequest(t, io.Discard))
	if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "no session_id") {
		t.Fatalf("missing init identity error = %v", err)
	}
}

func TestClaudeRequiresTerminalResultForFreshAndRetainedRuns(t *testing.T) {
	streams := []string{
		``,
		`{"type":"system","subtype":"init","session_id":"session-1"}`,
		`{"type":"system","subtype":"init","session_id":"session-1"}` + "\n" + `{"type":"result","subtype":"success","session_id":"session-1"}` + "\n" + `{"type":"assistant","session_id":"session-1"}`,
	}
	for _, requireSession := range []bool{false, true} {
		for _, stream := range streams {
			runner := &fakeProcessRunner{chunks: [][]byte{[]byte(stream)}}
			adapter := testAdapter(t, nil, runner)
			request := validRequest(t, io.Discard)
			request.RequireSession = requireSession
			_, err := adapter.Run(context.Background(), request)
			if !errors.Is(err, ErrProtocol) {
				t.Errorf("require session %t stream %q error = %v, want protocol error", requireSession, stream, err)
			}
		}
	}
}

func TestClaudeCapturedStreamFixtures(t *testing.T) {
	for _, test := range []struct {
		name       string
		fixture    string
		wantErr    bool
		wantOutput string
	}{
		{name: "success", fixture: "testdata/stream-success.jsonl", wantOutput: "I will inspect the project file.\nThe project is ready.\n[claude] turn complete\n"},
		{name: "failure", fixture: "testdata/stream-failure.jsonl", wantErr: true, wantOutput: "[claude error] tool execution failed\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream, err := os.ReadFile(test.fixture)
			if err != nil {
				t.Fatal(err)
			}
			runner := &fakeProcessRunner{chunks: [][]byte{stream}}
			adapter := testAdapter(t, nil, runner)
			var output bytes.Buffer
			result, runErr := adapter.Run(context.Background(), validRequest(t, &output))
			if (runErr != nil) != test.wantErr {
				t.Fatalf("Run error = %v, want error %t", runErr, test.wantErr)
			}
			if !test.wantErr && result.SessionID != "captured-session-1" {
				t.Fatalf("session ID = %q", result.SessionID)
			}
			if output.String() != test.wantOutput {
				t.Fatalf("operator output = %q, want %q", output.String(), test.wantOutput)
			}
		})
	}
}

func TestClaudeValidatesPermissionModesAndEmittedSessionIDs(t *testing.T) {
	for _, mode := range []string{"default", "acceptEdits", "plan", "auto", "dontAsk", "bypassPermissions", "manual"} {
		if err := New().Validate(harness.PreflightConfig{ClaudePermissionMode: mode, OutputMode: "compact"}); err != nil {
			t.Errorf("permission mode %q rejected: %v", mode, err)
		}
	}
	for _, sessionID := range []string{strings.Repeat("x", 513), "session with spaces", "session\x00id"} {
		line, marshalErr := json.Marshal(map[string]string{"type": "system", "subtype": "init", "session_id": sessionID})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		runner := &fakeProcessRunner{chunks: [][]byte{line}}
		adapter := testAdapter(t, nil, runner)
		_, err := adapter.Run(context.Background(), validRequest(t, io.Discard))
		if !errors.Is(err, ErrProtocol) {
			t.Errorf("session ID %q error = %v, want protocol error", sessionID, err)
		}
	}
}

func TestClaudeCapabilitiesAndValidation(t *testing.T) {
	adapter := New()
	if got := adapter.Capabilities(); got.CleanupDelete || got.CleanupArchive || !got.Resume || !got.Fresh {
		t.Fatalf("capabilities = %#v", got)
	}
	if err := adapter.Validate(harness.PreflightConfig{Reasoning: "low", OutputMode: "compact"}); err != nil {
		t.Fatalf("low effort rejected: %v", err)
	}
	if err := adapter.Validate(harness.PreflightConfig{Reasoning: "ultra", OutputMode: "compact"}); err == nil || !strings.Contains(err.Error(), "effort") {
		t.Fatalf("invalid reasoning = %v", err)
	}
	if err := adapter.Cleanup(context.Background(), "session-1", harness.CleanupDelete); err == nil {
		t.Fatal("Claude accepted unsupported delete cleanup")
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
