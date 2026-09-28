package terminaltext

import (
	"strings"
	"testing"
)

func TestSanitizeRemovesTerminalControls(t *testing.T) {
	input := "ansi\x1b[31mred\x1b[0m osc\x1b]52;c;secret\a\nnext\r\t\x00\x1f\x7f\u0085✓"
	got := Sanitize(input, true)
	if strings.ContainsAny(got, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f") {
		// Newline is intentionally retained by multiline output; check the
		// other controls separately so ordinary line structure is allowed.
		for _, r := range got {
			if r != '\n' && (r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)) {
				t.Fatalf("unsafe output %q", got)
			}
		}
	}
	if !strings.Contains(got, "ansi[31mred[0m osc]52;c;secret\nnext") || !strings.Contains(got, "✓") {
		t.Fatalf("ordinary text or line structure lost: %q", got)
	}
	if single := Sanitize(input, false); strings.Contains(single, "\n") || strings.Contains(single, "\r") || strings.Contains(single, "\t") {
		t.Fatalf("single-line output contains layout controls: %q", single)
	}
}
