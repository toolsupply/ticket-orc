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
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	var event struct {
		Type    string          `json:"type"`
		Message json.RawMessage `json:"message"`
		Result  string          `json:"result"`
		Subtype string          `json:"subtype"`
		IsError bool            `json:"is_error"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return fmt.Errorf("%w: malformed JSONL output event: %v", ErrProtocol, err)
	}
	switch event.Type {
	case "assistant":
		var message struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(event.Message, &message); err == nil && message.Role == "assistant" {
			for _, content := range message.Content {
				if content.Type == "text" && content.Text != "" {
					return writeLine(o.operator, truncateDisplay(content.Text))
				}
			}
		}
	case "result":
		if event.IsError || event.Subtype != "success" {
			message := event.Result
			if message == "" {
				message = event.Subtype
			}
			if message == "" {
				message = "Claude stream error"
			}
			return writeLine(o.operator, "[claude error] "+truncateDisplay(message))
		}
		return writeLine(o.operator, "[claude] turn complete")
	}
	return nil
}

func writeLine(writer io.Writer, value string) error {
	return harness.WriteOperatorLine("Claude", writer, value)
}

func truncateDisplay(value string) string { return harness.TruncateDisplay(value) }
