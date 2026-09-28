package harness

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

// DisplayLimit is the maximum number of runes shown from a provider message.
const DisplayLimit = 1000

// WriteOperatorLine sanitizes and writes one provider message as one line.
func WriteOperatorLine(provider string, writer io.Writer, value string) error {
	value = terminaltext.Sanitize(value, true)
	if !strings.HasSuffix(value, "\n") {
		value += "\n"
	}
	written, err := io.WriteString(writer, value)
	if err != nil {
		return fmt.Errorf("write %s operator output: %w", provider, err)
	}
	if written != len(value) {
		return fmt.Errorf("write %s operator output: %w", provider, io.ErrShortWrite)
	}
	return nil
}

// TruncateDisplay limits text to DisplayLimit Unicode code points.
func TruncateDisplay(value string) string {
	if utf8.RuneCountInString(value) <= DisplayLimit {
		return value
	}
	runes := []rune(value)
	return string(runes[:DisplayLimit]) + "…"
}
