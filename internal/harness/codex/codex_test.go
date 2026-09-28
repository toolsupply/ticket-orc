package codex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func canonicalTestDirectory(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func canonicalTestDirectoryWithSpace(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(canonicalTestDirectory(t), "recorded codex home")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
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

func TestRunBuildsArgumentsStreamsEventsAndFindsSession(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{
		[]byte(`{"type":"thread.`),
		[]byte("started\",\"thread_id\":\"thread-1\"}\n"),
		[]byte("{\"type\":\"turn.completed\"}\n"),
	}}
	adapter := testAdapter(t, []string{"PATH=/tools", "TICKET_ACTOR=old", "KEEP=value"}, runner)
	var stream bytes.Buffer
	request := validRequest(t, &stream)
	request.Model = "gpt-test"
	request.Reasoning = "high"
	request.CodexSandbox = "workspace-write"
	request.RequireSession = true

	result, err := adapter.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.SessionID != "thread-1" || result.ExitCode != 0 || !result.StreamEndedNormally {
		t.Fatalf("result = %#v", result)
	}
	if result.LogPath == "" {
		t.Fatal("Run did not report the raw log path")
	}
	wantArgs := []string{
		"exec", "--json",
		"-m", "gpt-test",
		"-c", `model_reasoning_effort="high"`,
		"-s", "workspace-write",
		"implement ticket",
	}
	if len(runner.calls) != 1 || !reflect.DeepEqual(runner.calls[0].args, wantArgs) {
		t.Fatalf("args = %q, want %q", runner.calls[0].args, wantArgs)
	}
	if runner.calls[0].executable != "/test/codex" || runner.calls[0].dir != "/work/project" {
		t.Fatalf("process request = %#v", runner.calls[0])
	}
	if !contains(runner.calls[0].env, "KEEP=value") || countPrefix(runner.calls[0].env, "TICKET_ACTOR=") != 1 ||
		!contains(runner.calls[0].env, "TICKET_ACTOR=worker") {
		t.Fatalf("environment = %q", runner.calls[0].env)
	}
	wantRaw := "{\"type\":\"thread.started\",\"thread_id\":\"thread-1\"}\n{\"type\":\"turn.completed\"}\n"
	raw, err := os.ReadFile(result.LogPath)
	if err != nil {
		t.Fatalf("read raw log: %v", err)
	}
	if string(raw) != wantRaw {
		t.Fatalf("raw log = %q, want %q", raw, wantRaw)
	}
	if stream.String() != "[codex] turn complete\n" {
		t.Fatalf("operator output = %q", stream.String())
	}
}

func TestModelChangeDoesNotChangeManagedWorkerLogIdentity(t *testing.T) {
	stateDir := t.TempDir()
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte("{\"type\":\"turn.completed\"}\n")}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	request.StateDir = stateDir
	request.WorkerName = "project-coder"
	request.Model = "model-one"
	first, err := adapter.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	request.Model = "model-two"
	second, err := adapter.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	prefix := "20260919-12345.coder.codex.project-coder."
	for _, path := range []string{first.LogPath, second.LogPath} {
		if !strings.HasPrefix(filepath.Base(path), prefix) {
			t.Fatalf("model changed or omitted worker log identity: %q", path)
		}
	}
	if first.LogPath == second.LogPath || len(runner.calls) != 2 {
		t.Fatalf("log paths = %q, %q calls=%d", first.LogPath, second.LogPath, len(runner.calls))
	}
}

func TestRunFreshSessionDoesNotRequireThreadID(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte("{\"type\":\"turn.completed\"}\n")}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	request.RequireSession = false
	result, err := adapter.Run(context.Background(), request)
	if err != nil || result.SessionID != "" || !result.StreamEndedNormally {
		t.Fatalf("Run = %#v, %v", result, err)
	}
}

func TestExecJSONUsageDoesNotInventContextTelemetry(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte(
		"{\"type\":\"thread.started\",\"thread_id\":\"thread-usage\"}\n" +
			"{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":1000,\"cached_input_tokens\":300,\"output_tokens\":80}}\n",
	)}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	result, err := adapter.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.ContextTelemetry.Known || !result.ContextTelemetry.Valid() {
		t.Fatalf("Codex adapter inferred context telemetry from per-turn usage: %#v", result.ContextTelemetry)
	}
}

func TestRunRequiredSessionMustBeReported(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte("{\"type\":\"turn.completed\"}\n")}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	request.RequireSession = true
	result, err := adapter.Run(context.Background(), request)
	if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "did not report") {
		t.Fatalf("Run = %#v, %v; want missing-session protocol error", result, err)
	}
}

func TestRunFindsSessionInFinalEventWithoutNewline(t *testing.T) {
	for _, requireSession := range []bool{false, true} {
		name := "optional"
		if requireSession {
			name = "required"
		}
		t.Run(name, func(t *testing.T) {
			runner := &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"thread.started","thread_id":"thread-final"}`)}}
			adapter := testAdapter(t, nil, runner)
			request := validRequest(t, io.Discard)
			request.RequireSession = requireSession
			result, err := adapter.Run(context.Background(), request)
			if err != nil || result.SessionID != "thread-final" || !result.StreamEndedNormally {
				t.Fatalf("Run = %#v, %v", result, err)
			}
		})
	}
}

func TestResumeUsesExistingSessionAndVerifiesStreamIdentity(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"thread.started","thread_id":"thread-1"}`)}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	request.CodexSandbox = "workspace-write"
	result, err := adapter.Resume(context.Background(), "thread-1", request)
	if err != nil || result.SessionID != "thread-1" || !result.StreamEndedNormally {
		t.Fatalf("Resume = %#v, %v", result, err)
	}
	want := []string{"exec", "--json", "-s", "workspace-write", "resume", "thread-1", "implement ticket"}
	if !reflect.DeepEqual(runner.calls[0].args, want) {
		t.Fatalf("args = %q, want %q", runner.calls[0].args, want)
	}
}

func TestResumeRejectsDifferentThreadIdentity(t *testing.T) {
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte("{\"type\":\"thread.started\",\"thread_id\":\"other\"}\n")}}
	adapter := testAdapter(t, nil, runner)
	result, err := adapter.Resume(context.Background(), "thread-1", validRequest(t, io.Discard))
	if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "expected") || result.SessionOutcome != harness.SessionInvalidated {
		t.Fatalf("Resume = %#v, %v; want mismatch protocol error", result, err)
	}
}

func TestMalformedOrChangingStreamsFail(t *testing.T) {
	tests := []struct {
		name   string
		stream string
		want   string
	}{
		{"malformed JSON", "{\n", "malformed JSONL"},
		{"missing thread ID", "{\"type\":\"thread.started\"}\n", "no thread_id"},
		{"changed thread", "{\"type\":\"thread.started\",\"thread_id\":\"one\"}\n{\"type\":\"thread.started\",\"thread_id\":\"two\"}\n", "changed thread"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &fakeProcessRunner{chunks: [][]byte{[]byte(tt.stream)}}
			adapter := testAdapter(t, nil, runner)
			_, err := adapter.Run(context.Background(), validRequest(t, io.Discard))
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want protocol error containing %q", err, tt.want)
			}
		})
	}
}

func TestProcessFailurePreservesDiscoveredSession(t *testing.T) {
	runner := &fakeProcessRunner{
		chunks: [][]byte{[]byte("{\"type\":\"thread.started\",\"thread_id\":\"thread-1\"}\n")},
		result: processResult{exitCode: 9, stderr: "command failed"},
		err:    errors.New("exit status 9"),
	}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	request.RequireSession = true
	result, err := adapter.Run(context.Background(), request)
	var processErr *ProcessError
	if !errors.As(err, &processErr) || processErr.ExitCode != 9 {
		t.Fatalf("Run error = %T %v", err, err)
	}
	if result.SessionID != "thread-1" || !result.StreamEndedNormally || result.ExitCode != 9 {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunProcessPreservesFullStderr(t *testing.T) {
	want := strings.Repeat("diagnostic-", 1000)
	result, err := runProcess(context.Background(), processRequest{
		executable: os.Args[0],
		args:       []string{"-test.run=TestCodexHelperProcess", "--"},
		env:        append(os.Environ(), "GO_WANT_CODEX_HELPER_PROCESS=1"),
		stdout:     io.Discard,
	})
	if err == nil || result.exitCode != 3 {
		t.Fatalf("runProcess = %#v, %v; want exit 3", result, err)
	}
	if result.stderr != want {
		t.Fatalf("stderr length = %d, want %d", len(result.stderr), len(want))
	}
}

func TestCodexHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_CODEX_HELPER_PROCESS") != "1" {
		return
	}
	_, _ = fmt.Fprint(os.Stderr, strings.Repeat("diagnostic-", 1000))
	os.Exit(3)
}

func TestCleanupMapsPoliciesToCodexCommands(t *testing.T) {
	runner := &fakeProcessRunner{}
	adapter := testAdapter(t, []string{"KEEP=value"}, runner)
	ctx := context.Background()
	if err := adapter.Cleanup(ctx, "thread-1", harness.CleanupDelete); err != nil {
		t.Fatalf("delete cleanup: %v", err)
	}
	if err := adapter.Cleanup(ctx, "thread-2", harness.CleanupArchive); err != nil {
		t.Fatalf("archive cleanup: %v", err)
	}
	if err := adapter.Cleanup(ctx, "thread-3", harness.CleanupKeep); err != nil {
		t.Fatalf("keep cleanup: %v", err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("cleanup call count = %d, want 2", len(runner.calls))
	}
	if !reflect.DeepEqual(runner.calls[0].args, []string{"delete", "thread-1", "--force"}) {
		t.Fatalf("delete args = %q", runner.calls[0].args)
	}
	if !reflect.DeepEqual(runner.calls[1].args, []string{"archive", "thread-2"}) {
		t.Fatalf("archive args = %q", runner.calls[1].args)
	}
}

func TestCleanupFailureAndValidation(t *testing.T) {
	runner := &fakeProcessRunner{result: processResult{exitCode: 4, stderr: "cannot archive"}, err: errors.New("exit status 4")}
	adapter := testAdapter(t, nil, runner)
	err := adapter.Cleanup(context.Background(), "thread-1", harness.CleanupArchive)
	var processErr *ProcessError
	if !errors.As(err, &processErr) || processErr.ExitCode != 4 {
		t.Fatalf("Cleanup error = %T %v", err, err)
	}
	if err := adapter.Cleanup(context.Background(), "thread-1", harness.CleanupPolicy("purge")); err == nil {
		t.Fatal("Cleanup accepted unknown policy")
	}
	if err := adapter.Cleanup(context.Background(), " ", harness.CleanupKeep); err == nil {
		t.Fatal("Cleanup accepted empty session")
	}
}

func TestQueueUsesRecordedHomeAndIgnoresCallerWorkingDirectory(t *testing.T) {
	runner := &fakeProcessRunner{}
	adapter := testAdapter(t, []string{
		"TICKET_ACTOR=old", "TICKET_REPOSITORY=/old/repo", "TICKET_SCOPE=old",
		"TICKET_CONFIG=/old/config", "TICKET_ROOT=/old/root", "TICKET_CURRENT=old-ticket",
		"CODEX_HOME=/inherited/home", "KEEP=value",
	}, runner)
	firstDir, secondDir := t.TempDir(), t.TempDir()
	codexHome := canonicalTestDirectoryWithSpace(t)
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Errorf("restore caller working directory: %v", err)
		}
	})
	for _, dir := range []string{firstDir, secondDir} {
		if err := os.Chdir(dir); err != nil {
			t.Fatal(err)
		}
		if err := adapter.Queue(context.Background(), codexHome, "thread-uuid", "exact notification"); err != nil {
			t.Fatalf("Queue from %q: %v", dir, err)
		}
	}
	if len(runner.calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(runner.calls))
	}
	for _, call := range runner.calls {
		want := []string{"queue", "--thread", "thread-uuid", "--message", "exact notification"}
		if !reflect.DeepEqual(call.args, want) {
			t.Fatalf("args = %#v, want %#v", call.args, want)
		}
		if call.dir != codexHome {
			t.Fatalf("queue working directory = %q, want recorded Codex home", call.dir)
		}
		if !contains(call.env, "CODEX_HOME="+codexHome) || !contains(call.env, "KEEP=value") || countPrefix(call.env, "CODEX_HOME=") != 1 {
			t.Fatalf("queue environment = %#v", call.env)
		}
		for _, key := range []string{"TICKET_ACTOR", "TICKET_REPOSITORY", "TICKET_SCOPE", "TICKET_CONFIG", "TICKET_ROOT", "TICKET_CURRENT"} {
			if countPrefix(call.env, key+"=") != 0 {
				t.Fatalf("queue environment contains %s: %#v", key, call.env)
			}
		}
	}
	if !reflect.DeepEqual(runner.calls[0].args, runner.calls[1].args) || !reflect.DeepEqual(runner.calls[0].env, runner.calls[1].env) {
		t.Fatal("queue invocation changed with caller working directory")
	}
}

func TestQueueRejectsInvalidHomeAndMessage(t *testing.T) {
	runner := &fakeProcessRunner{}
	adapter := testAdapter(t, nil, runner)
	validHome := canonicalTestDirectory(t)
	for _, test := range []struct {
		name, home, message, wantError string
	}{
		{name: "relative home", home: "relative", message: "message", wantError: "absolute canonical path"},
		{name: "empty message", home: validHome, message: "", wantError: "queue message must not be empty or contain NUL"},
		{name: "NUL message", home: validHome, message: "bad\x00message", wantError: "queue message must not be empty or contain NUL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := adapter.Queue(context.Background(), test.home, "thread-uuid", test.message)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Queue(%q, %q) error = %v, want error containing %q", test.home, test.message, err, test.wantError)
			}
		})
	}
	if len(runner.calls) != 0 {
		t.Fatalf("invalid queue calls = %d, want none", len(runner.calls))
	}
}

func TestQueueReportsSubprocessFailure(t *testing.T) {
	runner := &fakeProcessRunner{result: processResult{exitCode: 7, stderr: "target unavailable"}, err: errors.New("exit status 7")}
	adapter := testAdapter(t, nil, runner)
	err := adapter.Queue(context.Background(), canonicalTestDirectory(t), "thread-1", "prompt")
	var processErr *ProcessError
	if !errors.As(err, &processErr) || processErr.ExitCode != 7 || processErr.Stderr != "target unavailable" {
		t.Fatalf("Queue error = %T %v, want process exit 7", err, err)
	}
}

func TestQueueCancellationStopsBeforeTransport(t *testing.T) {
	called := false
	adapter, err := newAdapter("/test/codex", func() []string { return nil }, func(context.Context, processRequest) (processResult, error) {
		called = true
		return processResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = adapter.Queue(ctx, canonicalTestDirectory(t), "thread-1", "prompt")
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("Queue error=%v called=%v, want cancellation without transport", err, called)
	}
}

func TestQueueCancellationStopsRunningTransport(t *testing.T) {
	called := false
	ctx, cancel := context.WithCancel(context.Background())
	adapter, err := newAdapter("/test/codex", func() []string { return nil }, func(ctx context.Context, _ processRequest) (processResult, error) {
		called = true
		cancel()
		return processResult{exitCode: -1}, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.Queue(ctx, canonicalTestDirectory(t), "thread-1", "prompt")
	if !errors.Is(err, context.Canceled) || !called {
		t.Fatalf("Queue error=%v called=%v, want cancellation from transport", err, called)
	}
}

func TestStreamWriterFailureStopsRun(t *testing.T) {
	wantErr := errors.New("disk full")
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte("{\"type\":\"turn.completed\"}\n")}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, errorWriter{err: wantErr})
	result, err := adapter.Run(context.Background(), request)
	if !errors.Is(err, wantErr) || result.StreamEndedNormally {
		t.Fatalf("Run = %#v, %v", result, err)
	}
}

func TestCoalescedOutputFailurePreservesEarlierSession(t *testing.T) {
	wantErr := errors.New("broken pipe")
	runner := &fakeProcessRunner{chunks: [][]byte{[]byte(
		"{\"type\":\"thread.started\",\"thread_id\":\"thread-1\"}\n" +
			"{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"done\"}}\n",
	)}}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, errorWriter{err: wantErr})
	request.RequireSession = true

	result, err := adapter.Run(context.Background(), request)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run error = %v, want operator error", err)
	}
	if result.SessionID != "thread-1" {
		t.Fatalf("Run result = %#v, want preserved session", result)
	}
	raw, readErr := os.ReadFile(result.LogPath)
	if readErr != nil {
		t.Fatalf("read raw log: %v", readErr)
	}
	if !strings.Contains(string(raw), `"thread_id":"thread-1"`) || !strings.Contains(string(raw), `"text":"done"`) {
		t.Fatalf("raw log did not preserve coalesced events: %q", raw)
	}
}

func TestCanceledContextDoesNotStartProcess(t *testing.T) {
	runner := &fakeProcessRunner{}
	adapter := testAdapter(t, nil, runner)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Run(ctx, validRequest(t, io.Discard)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context canceled", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("canceled run started process: %#v", runner.calls)
	}
}

func TestLogCreationFailureDoesNotStartProcess(t *testing.T) {
	runner := &fakeProcessRunner{}
	adapter := testAdapter(t, nil, runner)
	stateFile := t.TempDir() + "/state-file"
	if err := os.WriteFile(stateFile, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	request := validRequest(t, io.Discard)
	request.StateDir = stateFile

	if _, err := adapter.Run(context.Background(), request); err == nil || !strings.Contains(err.Error(), "real directory") {
		t.Fatalf("Run error = %v, want log directory error", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("log failure started process: %#v", runner.calls)
	}
}

func TestRequestAndAdapterValidation(t *testing.T) {
	if _, err := NewWithExecutable(" "); err == nil {
		t.Fatal("NewWithExecutable accepted empty path")
	}
	runner := &fakeProcessRunner{}
	adapter := testAdapter(t, nil, runner)
	request := validRequest(t, io.Discard)
	request.Actor = "two actors"
	if _, err := adapter.Run(context.Background(), request); err == nil {
		t.Fatal("Run accepted actor whitespace")
	}
	if _, err := adapter.Resume(context.Background(), "bad session", validRequest(t, io.Discard)); err == nil {
		t.Fatal("Resume accepted session whitespace")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("invalid requests started processes: %#v", runner.calls)
	}
}

func TestCapabilitiesAndPreflight(t *testing.T) {
	adapter := New()
	want := harness.Capabilities{Resume: true, Fresh: true, CleanupDelete: true, CleanupArchive: true}
	if got := adapter.Capabilities(); got != want {
		t.Fatalf("Capabilities = %#v, want %#v", got, want)
	}
	valid := harness.PreflightConfig{
		Reasoning:      "high",
		SessionPolicy:  "ticket",
		SessionCleanup: harness.CleanupArchive,
		OutputMode:     "compact",
		CodexSandbox:   "workspace-write",
	}
	if err := harness.Preflight(adapter, valid); err != nil {
		t.Fatalf("Preflight(valid) = %v", err)
	}
	for _, test := range []struct {
		name   string
		config harness.PreflightConfig
		want   string
	}{
		{"reasoning", harness.PreflightConfig{Reasoning: "minimal", OutputMode: "compact"}, "reasoning"},
		{"sandbox", harness.PreflightConfig{CodexSandbox: "unsafe", OutputMode: "compact"}, "sandbox"},
		{"output", harness.PreflightConfig{OutputMode: "pretty"}, "output"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := harness.Preflight(adapter, test.config); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.want)) {
				t.Fatalf("Preflight = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCurrentTargetUsesCodexThreadEnvironment(t *testing.T) {
	adapter := &Adapter{executable: "codex", environ: func() []string { return []string{"CODEX_THREAD_ID=current-thread"} }}
	if target, err := adapter.CurrentTarget(); err != nil || target != "current-thread" {
		t.Fatalf("CurrentTarget() = %q, %v", target, err)
	}
	adapter.environ = func() []string { return nil }
	if _, err := adapter.CurrentTarget(); err == nil || !strings.Contains(err.Error(), "CODEX_THREAD_ID is unavailable") {
		t.Fatalf("missing target error = %v", err)
	}
	adapter.environ = func() []string { return []string{"CODEX_THREAD_ID=has space"} }
	if _, err := adapter.CurrentTarget(); err == nil {
		t.Fatal("invalid target accepted")
	}
}

func validRequest(t *testing.T, operator io.Writer) harness.RunRequest {
	t.Helper()
	return harness.RunRequest{
		WorkingDir: "/work/project",
		StateDir:   t.TempDir(),
		Role:       "coder",
		Actor:      "worker",
		TicketID:   "20260919-12345",
		Prompt:     "implement ticket",
		OutputMode: string(OutputCompact),
		Operator:   operator,
	}
}

func testAdapter(t *testing.T, env []string, runner *fakeProcessRunner) *Adapter {
	t.Helper()
	adapter, err := newAdapter("/test/codex", func() []string {
		return append([]string(nil), env...)
	}, runner.run)
	if err != nil {
		t.Fatalf("newAdapter: %v", err)
	}
	return adapter
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

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
