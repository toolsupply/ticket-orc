package harness

import (
	"bytes"
	"errors"
)

// MaxEventBytes bounds a single newline-delimited provider event.
const MaxEventBytes = 8 << 20

// ErrEventTooLarge identifies a JSONL frame that exceeds MaxEventBytes.
var ErrEventTooLarge = errors.New("JSONL event is too large")

// LineFramer splits an arbitrarily chunked byte stream into bounded lines.
// It does not interpret the provider-specific contents of each line.
type LineFramer struct {
	pending []byte
}

// Write delivers complete lines in order and retains one bounded partial line.
func (f *LineFramer) Write(data []byte, consume func([]byte) error) error {
	for len(data) > 0 {
		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			if len(f.pending)+len(data) > MaxEventBytes {
				return ErrEventTooLarge
			}
			f.pending = append(f.pending, data...)
			return nil
		}
		if len(f.pending)+newline > MaxEventBytes {
			return ErrEventTooLarge
		}
		line := append(f.pending, data[:newline]...)
		f.pending = nil
		if err := consume(line); err != nil {
			return err
		}
		data = data[newline+1:]
	}
	return nil
}

// Finish delivers the final unterminated line, if it contains non-whitespace.
func (f *LineFramer) Finish(consume func([]byte) error) error {
	line := f.pending
	f.pending = nil
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	if len(line) > MaxEventBytes {
		return ErrEventTooLarge
	}
	return consume(line)
}
