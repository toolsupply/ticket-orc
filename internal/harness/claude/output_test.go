package claude

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestExecutionOutputSanitizesHumanTextButPreservesRawAndJSON(t *testing.T) {
	message := "ansi\x1b[31mred\x1b[0m osc\x1b]52;c;secret\a\nnext\x00\x1f\u0085✓"
	event := map[string]any{"type": "assistant", "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": message}}}}
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
	assertSafeHumanOutput(t, human.String())
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

func assertSafeHumanOutput(t *testing.T, value string) {
	t.Helper()
	for _, r := range value {
		if r != '\n' && (r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)) {
			t.Fatalf("unsafe human output: %q", value)
		}
	}
	if !strings.Contains(value, "ansi[31mred[0m") || !strings.Contains(value, "\nnext") {
		t.Fatalf("human output lost ordinary text: %q", value)
	}
}

func assertRawLog(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("raw log changed: got %q want %q", got, want)
	}
}
