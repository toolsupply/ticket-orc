package harness_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/claude"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/harness/pi"
)

func TestProviderSemanticRecords(t *testing.T) {
	zero, failure := 0, 7
	tests := []struct {
		name      string
		interpret func([]byte) ([]harness.LogRecord, error)
		event     string
		want      []harness.LogRecord
	}{
		{"codex message", codex.InterpretEvent, `{"type":"item.completed","item":{"type":"agent_message","text":"**hello**\n世界"}}`, []harness.LogRecord{{Kind: "message", Text: "**hello**\n世界"}}},
		{"codex command start", codex.InterpretEvent, `{"type":"item.started","item":{"type":"command_execution","command":"go test ./..."}}`, []harness.LogRecord{{Kind: "command", Phase: "started", Command: "go test ./..."}}},
		{"codex zero exit", codex.InterpretEvent, `{"type":"item.completed","item":{"type":"command_execution","command":"true","exit_code":0}}`, []harness.LogRecord{{Kind: "command", Phase: "completed", Command: "true", ExitCode: &zero}}},
		{"codex absent exit", codex.InterpretEvent, `{"type":"item.completed","item":{"type":"command_execution","command":"true"}}`, []harness.LogRecord{{Kind: "command", Phase: "completed", Command: "true"}}},
		{"codex failed command", codex.InterpretEvent, `{"type":"item.completed","item":{"type":"command_execution","command":"exit 7","exit_code":7,"aggregated_output":"secret dump"}}`, []harness.LogRecord{{Kind: "command", Phase: "completed", Command: "exit 7", ExitCode: &failure}}},
		{"codex files", codex.InterpretEvent, `{"type":"item.completed","item":{"type":"file_change","changes":[{"kind":"update","path":"世界.go"},{"kind":"delete","path":"old.go"}]}}`, []harness.LogRecord{{Kind: "file_change", Changes: []harness.FileChange{{Kind: "update", Path: "世界.go"}, {Kind: "delete", Path: "old.go"}}}}},
		{"codex item error", codex.InterpretEvent, `{"type":"item.completed","item":{"type":"error","message":"bad item"}}`, []harness.LogRecord{{Kind: "diagnostic", Level: "error", Code: "item_error", Text: "bad item"}}},
		{"codex global error", codex.InterpretEvent, `{"type":"error","message":"bad stream"}`, []harness.LogRecord{{Kind: "diagnostic", Level: "error", Code: "error", Text: "bad stream"}}},
		{"codex failure", codex.InterpretEvent, `{"type":"turn.failed","error":{"message":"bad turn"}}`, []harness.LogRecord{{Kind: "diagnostic", Level: "error", Code: "turn_failed", Text: "bad turn"}}},
		{"codex usage", codex.InterpretEvent, `{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3}}`, []harness.LogRecord{{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: "turn complete (in=10, cached=2, out=3)"}}},
		{"codex completion", codex.InterpretEvent, `{"type":"turn.completed"}`, []harness.LogRecord{{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: "turn complete"}}},
		{"claude blocks and tool", claude.InterpretEvent, `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"first"},{"type":"thinking","thinking":"private"},{"type":"tool_use","name":"Bash","input":{"command":"not exposed"}},{"type":"text","text":"second"}]}}`, []harness.LogRecord{{Kind: "message", Text: "first"}, {Kind: "command", Phase: "started", ToolName: "Bash"}, {Kind: "message", Text: "second"}}},
		{"claude success", claude.InterpretEvent, `{"type":"result","subtype":"success","result":"duplicate text"}`, []harness.LogRecord{{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: "turn complete"}}},
		{"claude failure", claude.InterpretEvent, `{"type":"result","subtype":"error","is_error":true,"result":"bad turn"}`, []harness.LogRecord{{Kind: "diagnostic", Level: "error", Code: "turn_failed", Text: "bad turn"}}},
		{"pi blocks", pi.InterpretEvent, `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"first"},{"type":"thinking","thinking":"private"},{"type":"text","text":"second"}]}}`, []harness.LogRecord{{Kind: "message", Text: "first"}, {Kind: "message", Text: "second"}}},
		{"pi tool start", pi.InterpretEvent, `{"type":"tool_execution_start","toolName":"bash","args":{"command":"not exposed"}}`, []harness.LogRecord{{Kind: "command", Phase: "started", ToolName: "bash"}}},
		{"pi tool end", pi.InterpretEvent, `{"type":"tool_execution_end","toolName":"bash","result":{"content":[{"type":"text","text":"secret dump"}]}}`, []harness.LogRecord{{Kind: "command", Phase: "completed", ToolName: "bash"}}},
		{"pi tool error", pi.InterpretEvent, `{"type":"tool_execution_end","toolName":"bash","isError":true}`, []harness.LogRecord{{Kind: "command", Phase: "completed", ToolName: "bash"}, {Kind: "diagnostic", Level: "error", Code: "tool_error", Text: "bash"}}},
		{"pi turn", pi.InterpretEvent, `{"type":"turn_end"}`, []harness.LogRecord{{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: "turn complete"}}},
		{"pi agent", pi.InterpretEvent, `{"type":"agent_end"}`, []harness.LogRecord{{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: "turn complete"}}},
		{"pi error string", pi.InterpretEvent, `{"type":"error","error":" bad stream\n "}`, []harness.LogRecord{{Kind: "diagnostic", Level: "error", Code: "error", Text: " bad stream\n "}}},
		{"pi error object", pi.InterpretEvent, `{"type":"error","error":{"message":"bad stream"}}`, []harness.LogRecord{{Kind: "diagnostic", Level: "error", Code: "error", Text: "bad stream"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.interpret([]byte(tt.event))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("records = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestRecordExitCodeJSON(t *testing.T) {
	zero := 0
	for _, tt := range []struct {
		code *int
		want string
	}{{nil, `{"kind":"command"}`}, {&zero, `{"kind":"command","exit_code":0}`}} {
		got, err := json.Marshal(harness.LogRecord{Kind: "command", ExitCode: tt.code})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.want {
			t.Fatalf("JSON = %s, want %s", got, tt.want)
		}
	}
}

func TestProviderInterpretationSuppressesNoiseAndReportsInvalidEvents(t *testing.T) {
	for _, provider := range []struct {
		name      string
		interpret func([]byte) ([]harness.LogRecord, error)
		protocol  error
		noise     []string
	}{
		{"codex", codex.InterpretEvent, codex.ErrProtocol, []string{`{"type":"thread.started"}`, `{"type":"item.completed","item":{"type":"reasoning","text":"secret"}}`, `{"type":"item.updated","item":{"type":"agent_message","text":"delta"}}`}},
		{"claude", claude.InterpretEvent, claude.ErrProtocol, []string{`{"type":"system","subtype":"init"}`, `{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"secret"}}}`, `{"type":"user","message":{"content":[{"type":"tool_result","content":"secret"}]}}`}},
		{"pi", pi.InterpretEvent, pi.ErrProtocol, []string{`{"type":"session"}`, `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"secret"}}`, `{"type":"message_end","message":{"role":"toolResult","content":[{"type":"text","text":"secret"}]}}`}},
	} {
		t.Run(provider.name, func(t *testing.T) {
			for _, event := range append(provider.noise, `{"type":"future_event"}`, " \t") {
				got, err := provider.interpret([]byte(event))
				if err != nil || len(got) != 0 {
					t.Fatalf("suppressed event: records=%#v err=%v", got, err)
				}
			}
			for _, event := range []string{`{bad`, `{"type":123}`, strings.Repeat(" ", harness.MaxEventBytes+1)} {
				if _, err := provider.interpret([]byte(event)); !errors.Is(err, provider.protocol) {
					t.Fatalf("invalid event error = %v", err)
				}
			}
		})
	}
}

type executionOutput interface {
	io.WriteCloser
	Path() string
}

func TestSemanticTextSurvivesWhileRenderingRemainsSafe(t *testing.T) {
	text := "**世界**\n\x1b[31m" + strings.Repeat("界", harness.DisplayLimit) + "\nlast"
	encoded, _ := json.Marshal(text)
	for _, provider := range []struct {
		name      string
		event     string
		interpret func([]byte) ([]harness.LogRecord, error)
		render    func(harness.OutputMode, []byte, io.Writer) error
		open      func(string, harness.OutputMode, io.Writer) (executionOutput, error)
	}{
		{"codex", fmt.Sprintf(`{"type":"item.completed","item":{"type":"agent_message","text":%s}}`, encoded), codex.InterpretEvent, codex.RenderEvent, func(dir string, mode harness.OutputMode, w io.Writer) (executionOutput, error) {
			return codex.OpenExecutionOutput(dir, "ticket", "coder", mode, w)
		}},
		{"claude", fmt.Sprintf(`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":%s},{"type":"text","text":"second"}]}}`, encoded), claude.InterpretEvent, claude.RenderEvent, func(dir string, mode harness.OutputMode, w io.Writer) (executionOutput, error) {
			return claude.OpenExecutionOutput(dir, "ticket", "coder", mode, w)
		}},
		{"pi", fmt.Sprintf(`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":%s},{"type":"text","text":"second"}]}}`, encoded), pi.InterpretEvent, pi.RenderEvent, func(dir string, mode harness.OutputMode, w io.Writer) (executionOutput, error) {
			return pi.OpenExecutionOutput(dir, "ticket", "coder", mode, w)
		}},
	} {
		t.Run(provider.name, func(t *testing.T) {
			records, err := provider.interpret([]byte(provider.event))
			if err != nil {
				t.Fatal(err)
			}
			if records[0].Text != text {
				t.Fatalf("semantic text changed: %q", records[0].Text)
			}
			for _, mode := range []harness.OutputMode{harness.OutputCompact, harness.OutputQuiet, harness.OutputJSON} {
				var live, callable bytes.Buffer
				output, err := provider.open(t.TempDir(), mode, &live)
				if err != nil {
					t.Fatal(err)
				}
				raw := provider.event + "\n"
				if _, err := output.Write([]byte(raw)); err != nil {
					t.Fatal(err)
				}
				if err := output.Close(); err != nil {
					t.Fatal(err)
				}
				logged, err := os.ReadFile(output.Path())
				if err != nil {
					t.Fatal(err)
				}
				if string(logged) != raw {
					t.Fatal("raw log changed")
				}
				if mode == harness.OutputJSON {
					if live.String() != raw {
						t.Fatal("raw JSON changed")
					}
					continue
				}
				if err := provider.render(mode, []byte(provider.event), &callable); err != nil {
					t.Fatal(err)
				}
				if live.String() != callable.String() {
					t.Fatal("live and callable rendering differ")
				}
				if strings.Contains(live.String(), "\x1b") || strings.Contains(live.String(), "last") || !strings.Contains(live.String(), "…") || utf8.RuneCountInString(live.String()) > harness.DisplayLimit+15 {
					t.Fatalf("unsafe or unbounded terminal output: %q", live.String())
				}
				if provider.name != "codex" && !strings.HasSuffix(live.String(), "second\n") {
					t.Fatal("lost second text block")
				}
			}
		})
	}
}

func TestProviderTerminalVisibilityAndFormatting(t *testing.T) {
	for _, tt := range []struct {
		name           string
		render         func(harness.OutputMode, []byte, io.Writer) error
		events         []string
		compact, quiet string
	}{
		{"codex", codex.RenderEvent, []string{
			`{"type":"item.started","item":{"type":"command_execution","command":"false"}}`,
			`{"type":"item.completed","item":{"type":"command_execution","command":"false","exit_code":1}}`,
			`{"type":"item.completed","item":{"type":"command_execution","command":"true","exit_code":0}}`,
			`{"type":"item.completed","item":{"type":"file_change","changes":[{"kind":"update","path":"a.go"}]}}`,
			`{"type":"error","message":"oops"}`, `{"type":"turn.failed","error":{"message":"failed"}}`, `{"type":"turn.completed"}`,
		}, "  $ false\n  [exit 1] false\n  [files] update a.go\n[codex error] oops\n[codex failed] failed\n[codex] turn complete\n", "[codex error] oops\n[codex failed] failed\n[codex] turn complete\n"},
		{"claude", claude.RenderEvent, []string{
			`{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"Bash"}]}}`,
			`{"type":"result","subtype":"error","result":"oops"}`, `{"type":"result","subtype":"success"}`,
		}, "[claude error] oops\n[claude] turn complete\n", "[claude error] oops\n[claude] turn complete\n"},
		{"pi", pi.RenderEvent, []string{
			`{"type":"tool_execution_start","toolName":"bash"}`, `{"type":"tool_execution_end","toolName":"bash","isError":true}`,
			`{"type":"error","error":" oops "}`, `{"type":"turn_end"}`,
		}, "  $ bash\n  [tool error] bash\n[pi error] oops\n[pi] turn complete\n", "[pi error] oops\n[pi] turn complete\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, mode := range []harness.OutputMode{harness.OutputCompact, harness.OutputQuiet} {
				var got bytes.Buffer
				for _, event := range tt.events {
					if err := tt.render(mode, []byte(event), &got); err != nil {
						t.Fatal(err)
					}
				}
				want := tt.compact
				if mode == harness.OutputQuiet {
					want = tt.quiet
				}
				if got.String() != want {
					t.Fatalf("%s: output=%q want=%q", mode, got.String(), want)
				}
			}
		})
	}
}
