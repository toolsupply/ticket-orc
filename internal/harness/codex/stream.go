package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

const maxEventBytes = harness.MaxEventBytes

type streamParser struct {
	output             io.Writer
	expected           string
	session            string
	sessionInvalidated bool
	framer             harness.LineFramer
	err                error
}

func newStreamParser(output io.Writer, expected string) *streamParser {
	if output == nil {
		output = io.Discard
	}
	return &streamParser{output: output, expected: expected, session: expected}
}

func (p *streamParser) Write(data []byte) (int, error) {
	if p.err != nil {
		return 0, p.err
	}
	parseErr := p.parse(data)
	written, outputErr := p.output.Write(data)
	if outputErr != nil {
		outputErr = fmt.Errorf("write Codex stream: %w", outputErr)
	} else if written != len(data) {
		outputErr = io.ErrShortWrite
	}
	p.err = errors.Join(parseErr, outputErr)
	if p.err != nil {
		return written, p.err
	}
	return len(data), nil
}

func (p *streamParser) parse(data []byte) error {
	err := p.framer.Write(data, p.parseLine)
	if errors.Is(err, harness.ErrEventTooLarge) {
		return fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, maxEventBytes)
	}
	return err
}

func (p *streamParser) Finish() error {
	if p.err != nil {
		return p.err
	}
	if err := p.framer.Finish(p.parseLine); err != nil {
		if errors.Is(err, harness.ErrEventTooLarge) {
			err = fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, maxEventBytes)
		}
		p.err = err
		return err
	}
	return nil
}

func (p *streamParser) parseLine(line []byte) error {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	if len(line) > maxEventBytes {
		return fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, maxEventBytes)
	}
	var event struct {
		Type     string `json:"type"`
		ThreadID string `json:"thread_id"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return fmt.Errorf("%w: malformed JSONL event: %v", ErrProtocol, err)
	}
	if event.Type != "thread.started" {
		return nil
	}
	if strings.TrimSpace(event.ThreadID) == "" {
		return fmt.Errorf("%w: thread.started has no thread_id", ErrProtocol)
	}
	if p.expected != "" && event.ThreadID != p.expected {
		p.sessionInvalidated = true
		return fmt.Errorf("%w: resumed thread %q, expected %q", ErrProtocol, event.ThreadID, p.expected)
	}
	if p.session != "" && event.ThreadID != p.session {
		return fmt.Errorf("%w: stream changed thread from %q to %q", ErrProtocol, p.session, event.ThreadID)
	}
	p.session = event.ThreadID
	return nil
}
