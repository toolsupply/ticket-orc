package claude

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
	result             bool
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
		outputErr = fmt.Errorf("write Claude stream: %w", outputErr)
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
	if !p.result {
		p.err = fmt.Errorf("%w: stream ended without terminal result", ErrProtocol)
		return p.err
	}
	return nil
}

func (p *streamParser) parseLine(line []byte) error {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	if p.result {
		return fmt.Errorf("%w: event appeared after terminal result", ErrProtocol)
	}
	if len(line) > maxEventBytes {
		return fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, maxEventBytes)
	}
	var event struct {
		Type      string `json:"type"`
		Subtype   string `json:"subtype"`
		SessionID string `json:"session_id"`
		IsError   bool   `json:"is_error"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return fmt.Errorf("%w: malformed JSONL event: %v", ErrProtocol, err)
	}
	if event.Type == "system" && event.Subtype == "init" && strings.TrimSpace(event.SessionID) == "" {
		return fmt.Errorf("%w: system init has no session_id", ErrProtocol)
	}
	if event.Type == "result" && strings.TrimSpace(event.SessionID) == "" {
		return fmt.Errorf("%w: result has no session_id", ErrProtocol)
	}
	if event.SessionID == "" {
		return nil
	}
	if strings.TrimSpace(event.SessionID) == "" {
		return fmt.Errorf("%w: Claude event has empty session_id", ErrProtocol)
	}
	if err := validateSessionID(event.SessionID); err != nil {
		return fmt.Errorf("%w: invalid session ID: %v", ErrProtocol, err)
	}
	if p.expected != "" && event.SessionID != p.expected {
		p.sessionInvalidated = true
		return fmt.Errorf("%w: resumed session %q, expected %q", ErrProtocol, event.SessionID, p.expected)
	}
	if p.session != "" && event.SessionID != p.session {
		return fmt.Errorf("%w: stream changed session from %q to %q", ErrProtocol, p.session, event.SessionID)
	}
	p.session = event.SessionID
	if event.Type == "result" {
		if event.IsError || event.Subtype != "success" {
			return fmt.Errorf("%w: result subtype %q reports failure", ErrProtocol, event.Subtype)
		}
		p.result = true
	}
	return nil
}
