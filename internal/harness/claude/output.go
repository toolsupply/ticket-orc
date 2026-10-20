package claude

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

const displayLimit = harness.DisplayLimit

type OutputMode = harness.OutputMode

const (
	OutputCompact = harness.OutputCompact
	OutputQuiet   = harness.OutputQuiet
	OutputJSON    = harness.OutputJSON
)

type ExecutionOutput struct {
	log      *harness.RawLog
	operator io.Writer
	mode     OutputMode
	framer   harness.LineFramer
	err      error
	closed   bool
}

func OpenExecutionOutput(stateDir, ticket, role string, mode OutputMode, operator io.Writer, components ...string) (*ExecutionOutput, error) {
	if mode != OutputCompact && mode != OutputQuiet && mode != OutputJSON {
		return nil, fmt.Errorf("unknown Claude output mode %q", mode)
	}
	if operator == nil {
		operator = io.Discard
	}
	log, err := harness.OpenRawLog("Claude", stateDir, ticket, role, time.Now(), components...)
	if err != nil {
		return nil, err
	}
	return &ExecutionOutput{log: log, operator: operator, mode: mode}, nil
}

func (o *ExecutionOutput) Path() string { return o.log.Path() }

func (o *ExecutionOutput) Write(data []byte) (int, error) {
	if o.closed {
		return 0, fmt.Errorf("Claude execution output is closed")
	}
	if o.err != nil {
		return 0, o.err
	}
	written, err := o.log.Write(data)
	if err != nil {
		o.err = err
		return written, o.err
	}
	if o.mode == OutputJSON {
		written, err := o.operator.Write(data)
		if err != nil {
			o.err = fmt.Errorf("write Claude operator output: %w", err)
		} else if written != len(data) {
			o.err = io.ErrShortWrite
		}
		return len(data), o.err
	}
	if err := o.framer.Write(data, o.renderLine); err != nil {
		if errors.Is(err, harness.ErrEventTooLarge) {
			err = fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, harness.MaxEventBytes)
		}
		o.err = err
		return len(data), err
	}
	return len(data), nil
}

func (o *ExecutionOutput) Close() error {
	if o.closed {
		return o.err
	}
	o.closed = true
	if o.err == nil && o.mode != OutputJSON {
		if err := o.framer.Finish(o.renderLine); err != nil {
			if errors.Is(err, harness.ErrEventTooLarge) {
				err = fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, harness.MaxEventBytes)
			}
			o.err = err
		}
	}
	o.err = errors.Join(o.err, o.log.Close())
	return o.err
}

func (o *ExecutionOutput) renderLine(line []byte) error {
	return RenderEvent(o.mode, line, o.operator)
}

// InterpretEvent returns semantic records for one framed Claude JSON event.
// Blank and unknown events produce no records; malformed or oversized events
// return ErrProtocol. All assistant text blocks retain their original content.
func InterpretEvent(line []byte) ([]harness.LogRecord, error) {
	if len(line) > harness.MaxEventBytes {
		return nil, fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, harness.MaxEventBytes)
	}
	if len(bytes.TrimSpace(line)) == 0 {
		return nil, nil
	}
	var event struct {
		Type    string          `json:"type"`
		Message json.RawMessage `json:"message"`
		Result  string          `json:"result"`
		Subtype string          `json:"subtype"`
		IsError bool            `json:"is_error"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return nil, fmt.Errorf("%w: malformed JSONL output event: %v", ErrProtocol, err)
	}
	var records []harness.LogRecord
	switch event.Type {
	case "assistant":
		var message struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
				Name string `json:"name"`
			} `json:"content"`
		}
		if err := json.Unmarshal(event.Message, &message); err != nil {
			return nil, fmt.Errorf("%w: malformed assistant message: %v", ErrProtocol, err)
		}
		if message.Role == "assistant" {
			for _, content := range message.Content {
				switch content.Type {
				case "text":
					if content.Text != "" {
						records = append(records, harness.LogRecord{Kind: "message", Text: content.Text})
					}
				case "tool_use":
					records = append(records, harness.LogRecord{Kind: "command", Phase: "started", ToolName: content.Name})
				}
			}
		}
	case "result":
		record := harness.LogRecord{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: "turn complete"}
		if event.IsError || event.Subtype != "success" {
			message := event.Result
			if message == "" {
				message = event.Subtype
			}
			if message == "" {
				message = "Claude stream error"
			}
			record = harness.LogRecord{Kind: "diagnostic", Level: "error", Code: "turn_failed", Text: message}
		}
		records = append(records, record)
	}
	return records, nil
}

// RenderEvent renders one framed Claude event in compact or quiet mode using
// InterpretEvent, with terminal sanitization and display limits applied here.
func RenderEvent(mode OutputMode, line []byte, operator io.Writer) error {
	if mode != OutputCompact && mode != OutputQuiet {
		return fmt.Errorf("unknown Claude event output mode %q", mode)
	}
	if operator == nil {
		operator = io.Discard
	}
	records, err := InterpretEvent(line)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := RenderRecord(mode, record, operator); err != nil {
			return err
		}
	}
	return nil
}

// RenderRecord formats one semantic record for terminal display.
func RenderRecord(mode OutputMode, record harness.LogRecord, operator io.Writer) error {
	if mode != OutputCompact && mode != OutputQuiet {
		return fmt.Errorf("unknown Claude event output mode %q", mode)
	}
	if operator == nil {
		operator = io.Discard
	}

	value := ""
	switch record.Kind {
	case "message":
		value = truncateDisplay(record.Text)
	case "diagnostic":
		if record.Code == "turn_completed" {
			value = "[claude] " + record.Text
		} else {
			value = "[claude error] " + truncateDisplay(record.Text)
		}
	}
	if value != "" {
		if err := writeLine(operator, value); err != nil {
			return err
		}
	}
	return nil
}

func writeLine(writer io.Writer, value string) error {
	return harness.WriteOperatorLine("Claude", writer, value)
}

func truncateDisplay(value string) string { return harness.TruncateDisplay(value) }
