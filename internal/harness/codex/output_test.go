package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestExecutionOutputSanitizesHumanTextButPreservesRawAndJSON(t *testing.T) {
	message := "ansi\x1b[31mred\x1b[0m osc\x1b]52;c;secret\a\nnext\x00\x1f\u0085✓"
	event := map[string]any{"type": "item.completed", "item": map[string]any{"type": "agent_message", "text": message}}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	raw := string(encoded) + "\n"

	var human bytes.Buffer
	output, err := OpenExecutionOutput(t.TempDir(), "ticket", "coder", OutputCompact, &human)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := output.Write([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(human.String(), "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f\u0085") || !strings.Contains(human.String(), "ansi[31mred[0m") || !strings.Contains(human.String(), "\nnext") {
		t.Fatalf("unsafe human output: %q", human.String())
	}
	assertRawLog(t, output.Path(), raw)

	var machine bytes.Buffer
	jsonOutput, err := OpenExecutionOutput(t.TempDir(), "ticket", "coder", OutputJSON, &machine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jsonOutput.Write([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	if err := jsonOutput.Close(); err != nil {
		t.Fatal(err)
	}
	if machine.String() != raw {
		t.Fatalf("JSON output changed raw event: got %q want %q", machine.String(), raw)
	}
	assertRawLog(t, jsonOutput.Path(), raw)
}

const outputFixture = "" +
	`{"type":"thread.started","thread_id":"thread-1"}` + "\n" +
	`{"type":"item.completed","item":{"type":"agent_message","text":"I changed the parser."}}` + "\n" +
	`{"type":"item.started","item":{"type":"command_execution","command":"go test ./..."}}` + "\n" +
	`{"type":"item.completed","item":{"type":"command_execution","command":"go test ./...","exit_code":2}}` + "\n" +
	`{"type":"item.completed","item":{"type":"file_change","changes":[{"kind":"update","path":"main.go"},{"kind":"create","path":"main_test.go"}]}}` + "\n" +
	`{"type":"item.completed","item":{"type":"error","message":"tool failed"}}` + "\n" +
	`{"type":"error","message":"stream failed"}` + "\n" +
	`{"type":"turn.failed","error":{"message":"model failed"}}` + "\n" +
	`{"type":"turn.completed","usage":{"input_tokens":20,"cached_input_tokens":5,"output_tokens":9}}` + "\n"

func TestExecutionOutputModes(t *testing.T) {
	tests := []struct {
		name       string
		mode       OutputMode
		want       []string
		notWanted  []string
		wantRawOut bool
	}{
		{
			name: "compact",
			mode: OutputCompact,
			want: []string{
				"I changed the parser.",
				"  $ go test ./...",
				"  [exit 2] go test ./...",
				"  [files] update main.go, create main_test.go",
				"[codex error] tool failed",
				"[codex error] stream failed",
				"[codex failed] model failed",
				"[codex] turn complete (in=20, cached=5, out=9)",
			},
		},
		{
			name:      "quiet",
			mode:      OutputQuiet,
			want:      []string{"I changed the parser.", "[codex error] tool failed", "[codex error] stream failed", "[codex failed] model failed", "[codex] turn complete"},
			notWanted: []string{"$ go test", "[exit 2]", "[files]"},
		},
		{name: "json", mode: OutputJSON, wantRawOut: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var operator bytes.Buffer
			output, err := OpenExecutionOutput(t.TempDir(), "20260919-12345", "coder", tt.mode, &operator)
			if err != nil {
				t.Fatalf("OpenExecutionOutput: %v", err)
			}
			fixture := []byte(outputFixture)
			cut := len(fixture) / 3
			for _, chunk := range [][]byte{fixture[:cut], fixture[cut : cut*2], fixture[cut*2:]} {
				if _, err := output.Write(chunk); err != nil {
					t.Fatalf("Write: %v", err)
				}
			}
			if err := output.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			raw, err := os.ReadFile(output.Path())
			if err != nil {
				t.Fatalf("read raw log: %v", err)
			}
			if string(raw) != outputFixture {
				t.Fatalf("raw log differs from input\ngot:  %q\nwant: %q", raw, outputFixture)
			}
			if tt.wantRawOut {
				if operator.String() != outputFixture {
					t.Fatalf("JSON output differs from input\ngot:  %q\nwant: %q", operator.String(), outputFixture)
				}
				return
			}
			for _, want := range tt.want {
				if !strings.Contains(operator.String(), want) {
					t.Errorf("operator output %q does not contain %q", operator.String(), want)
				}
			}
			for _, unwanted := range tt.notWanted {
				if strings.Contains(operator.String(), unwanted) {
					t.Errorf("operator output %q contains %q", operator.String(), unwanted)
				}
			}
		})
	}
}

func TestExecutionOutputRendersFinalUnterminatedEvent(t *testing.T) {
	var operator bytes.Buffer
	output, err := OpenExecutionOutput(t.TempDir(), "ticket", "reviewer", OutputQuiet, &operator)
	if err != nil {
		t.Fatalf("OpenExecutionOutput: %v", err)
	}
	raw := `{"type":"item.completed","item":{"type":"agent_message","text":"final message"}}`
	if _, err := io.WriteString(output, raw); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := output.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if operator.String() != "final message\n" {
		t.Fatalf("operator output = %q", operator.String())
	}
	assertRawLog(t, output.Path(), raw)
}

func TestExecutionOutputBoundsDisplaysButNotDiagnostics(t *testing.T) {
	var operator bytes.Buffer
	output, err := OpenExecutionOutput(t.TempDir(), "ticket", "coder", OutputCompact, &operator)
	if err != nil {
		t.Fatalf("OpenExecutionOutput: %v", err)
	}
	longDisplay := strings.Repeat("界", displayLimit+50)
	longDiagnostic := strings.Repeat("failure-detail-", 200)
	raw := `{"type":"item.completed","item":{"type":"agent_message","text":"` + longDisplay + `"}}` + "\n" +
		`{"type":"error","message":"` + longDiagnostic + `"}` + "\n"
	if _, err := io.WriteString(output, raw); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := output.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !strings.Contains(operator.String(), strings.Repeat("界", displayLimit)+"…") || strings.Contains(operator.String(), longDisplay) {
		t.Fatalf("display was not truncated at rune boundary: %q", operator.String())
	}
	if !strings.Contains(operator.String(), longDiagnostic) {
		t.Fatal("diagnostic was truncated")
	}
	assertRawLog(t, output.Path(), raw)
}

func TestExecutionOutputMalformedEventIsLoggedAndReported(t *testing.T) {
	output, err := OpenExecutionOutput(t.TempDir(), "ticket", "coder", OutputCompact, io.Discard)
	if err != nil {
		t.Fatalf("OpenExecutionOutput: %v", err)
	}
	raw := "{not-json}\n"
	_, writeErr := io.WriteString(output, raw)
	if !errors.Is(writeErr, ErrProtocol) {
		t.Fatalf("Write error = %v, want protocol error", writeErr)
	}
	if closeErr := output.Close(); !errors.Is(closeErr, ErrProtocol) {
		t.Fatalf("Close error = %v, want protocol error", closeErr)
	}
	assertRawLog(t, output.Path(), raw)
}

func TestExecutionOutputPathsPermissionsAndCollisions(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	now := time.Date(2026, 9, 19, 12, 30, 45, 123, time.UTC)
	first, err := openExecutionOutputAt(stateDir, "ticket", "coder", OutputCompact, io.Discard, now)
	if err != nil {
		t.Fatalf("open first output: %v", err)
	}
	second, err := openExecutionOutputAt(stateDir, "ticket", "coder", OutputCompact, io.Discard, now)
	if err != nil {
		t.Fatalf("open second output: %v", err)
	}
	if first.Path() == second.Path() {
		t.Fatalf("colliding outputs used %q", first.Path())
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first output: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second output: %v", err)
	}
	if runtime.GOOS != "windows" {
		for _, path := range []string{stateDir, filepath.Join(stateDir, "logs")} {
			info, err := os.Stat(path)
			if err != nil {
				t.Errorf("stat directory %q: %v", path, err)
				continue
			}
			if info.Mode().Perm() != 0o700 {
				t.Errorf("directory %q mode = %v; want 0700", path, info.Mode().Perm())
			}
		}
		info, err := os.Stat(first.Path())
		if err != nil {
			t.Fatalf("stat log: %v", err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("log mode = %v; want 0600", info.Mode().Perm())
		}
	}
}

func TestExecutionOutputNamesHarnessAndWorker(t *testing.T) {
	stateDir := t.TempDir()
	now := time.Date(2026, 9, 19, 12, 30, 45, 123, time.UTC)
	first, err := openExecutionOutputAt(stateDir, "ticket", "coder", OutputCompact, io.Discard, now, "codex", "worker-one")
	if err != nil {
		t.Fatalf("open first output: %v", err)
	}
	second, err := openExecutionOutputAt(stateDir, "ticket", "coder", OutputCompact, io.Discard, now, "codex", "worker-two")
	if err != nil {
		t.Fatalf("open second output: %v", err)
	}
	defer first.Close()
	defer second.Close()
	for _, test := range []struct{ path, want string }{{first.Path(), "ticket.coder.codex.worker-one"}, {second.Path(), "ticket.coder.codex.worker-two"}} {
		if !strings.Contains(filepath.Base(test.path), test.want) {
			t.Errorf("log path %q does not contain %q", test.path, test.want)
		}
	}
}

func TestExecutionOutputRejectsUnsafeConfiguration(t *testing.T) {
	stateDir := t.TempDir()
	for _, test := range []struct {
		name   string
		ticket string
		role   string
		mode   OutputMode
		parts  []string
	}{
		{name: "ticket separator", ticket: "../ticket", role: "coder", mode: OutputCompact},
		{name: "role separator", ticket: "ticket", role: "co/der", mode: OutputCompact},
		{name: "unknown mode", ticket: "ticket", role: "coder", mode: "verbose"},
		{name: "worker separator", ticket: "ticket", role: "coder", mode: OutputCompact, parts: []string{"codex", "worker/name"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if output, err := OpenExecutionOutput(stateDir, test.ticket, test.role, test.mode, io.Discard, test.parts...); err == nil {
				_ = output.Close()
				t.Fatal("OpenExecutionOutput accepted unsafe configuration")
			}
		})
	}
}

func assertRawLog(t *testing.T, path, want string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read raw log: %v", err)
	}
	if string(raw) != want {
		t.Fatalf("raw log = %q, want %q", raw, want)
	}
}
