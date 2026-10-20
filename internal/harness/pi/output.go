package pi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
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
		return nil, fmt.Errorf("unknown Pi output mode %q", mode)
	}
	if operator == nil {
		operator = io.Discard
	}
	log, err := harness.OpenRawLog("Pi", stateDir, ticket, role, time.Now(), components...)
	if err != nil {
		return nil, err
	}
	return &ExecutionOutput{log: log, operator: operator, mode: mode}, nil
}

func (o *ExecutionOutput) Path() string { return o.log.Path() }

func (o *ExecutionOutput) Write(data []byte) (int, error) {
	if o.closed {
		return 0, fmt.Errorf("Pi execution output is closed")
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
			o.err = fmt.Errorf("write Pi operator output: %w", err)
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

// InterpretEvent returns semantic records for one framed Pi JSON event.
// Blank and unknown events produce no records; malformed or oversized events
// return ErrProtocol. Tool names are retained separately from shell commands.
func InterpretEvent(line []byte) ([]harness.LogRecord, error) {
	if len(line) > harness.MaxEventBytes {
		return nil, fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, harness.MaxEventBytes)
	}
	if len(bytes.TrimSpace(line)) == 0 {
		return nil, nil
	}
	var event struct {
		Type     string          `json:"type"`
		Message  json.RawMessage `json:"message"`
		Error    json.RawMessage `json:"error"`
		ToolName string          `json:"toolName"`
		IsError  bool            `json:"isError"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return nil, fmt.Errorf("%w: malformed JSONL output event: %v", ErrProtocol, err)
	}
	var records []harness.LogRecord
	switch event.Type {
	case "message_end":
		var message struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(event.Message, &message); err != nil {
			return nil, fmt.Errorf("%w: malformed assistant message: %v", ErrProtocol, err)
		}
		if message.Role == "assistant" {
			for _, content := range message.Content {
				if content.Type == "text" && content.Text != "" {
					records = append(records, harness.LogRecord{Kind: "message", Text: content.Text})
				}
			}
		}
	case "turn_end", "agent_end":
		records = append(records, harness.LogRecord{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: "turn complete"})
	case "tool_execution_start":
		records = append(records, harness.LogRecord{Kind: "command", Phase: "started", ToolName: event.ToolName})
	case "tool_execution_end":
		records = append(records, harness.LogRecord{Kind: "command", Phase: "completed", ToolName: event.ToolName})
		if event.IsError {
			records = append(records, harness.LogRecord{Kind: "diagnostic", Level: "error", Code: "tool_error", Text: event.ToolName})
		}
	case "error":
		message := ""
		var text string
		if json.Unmarshal(event.Error, &text) == nil {
			message = text
		} else {
			var detail struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(event.Error, &detail) == nil {
				message = detail.Message
			}
		}
		if strings.TrimSpace(message) == "" {
			message = "Pi stream error"
		}
		records = append(records, harness.LogRecord{Kind: "diagnostic", Level: "error", Code: "error", Text: message})
	}
	return records, nil
}

// RenderEvent renders one framed Pi event in compact or quiet mode using
// InterpretEvent, applying terminal visibility, sanitization and display limits.
func RenderEvent(mode OutputMode, line []byte, operator io.Writer) error {
	if mode != OutputCompact && mode != OutputQuiet {
		return fmt.Errorf("unknown Pi event output mode %q", mode)
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
		return fmt.Errorf("unknown Pi event output mode %q", mode)
	}
	if operator == nil {
		operator = io.Discard
	}

	value := ""
	switch record.Kind {
	case "message":
		value = truncateDisplay(record.Text)
	case "command":
		if mode == OutputCompact && record.Phase == "started" && record.ToolName != "" {
			value = "  $ " + truncateDisplay(record.ToolName)
		}
	case "diagnostic":
		switch record.Code {
		case "turn_completed":
			value = "[pi] " + record.Text
		case "tool_error":
			if mode == OutputCompact {
				value = "  [tool error] " + truncateDisplay(record.Text)
			}
		default:
			value = "[pi error] " + strings.TrimSpace(record.Text)
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
	return harness.WriteOperatorLine("Pi", writer, value)
}

func truncateDisplay(value string) string { return harness.TruncateDisplay(value) }
