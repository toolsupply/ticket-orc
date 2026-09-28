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
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	var event struct {
		Type      string          `json:"type"`
		Message   json.RawMessage `json:"message"`
		Error     json.RawMessage `json:"error"`
		Assistant struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
		} `json:"assistantMessageEvent"`
		ToolName string `json:"toolName"`
		IsError  bool   `json:"isError"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return fmt.Errorf("%w: malformed JSONL output event: %v", ErrProtocol, err)
	}
	switch event.Type {
	case "message_end":
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
	case "turn_end", "agent_end":
		return writeLine(o.operator, "[pi] turn complete")
	case "tool_execution_start":
		if o.mode == OutputCompact && event.ToolName != "" {
			return writeLine(o.operator, "  $ "+truncateDisplay(event.ToolName))
		}
	case "tool_execution_end":
		if o.mode == OutputCompact && event.IsError {
			return writeLine(o.operator, "  [tool error] "+truncateDisplay(event.ToolName))
		}
	case "error":
		message := ""
		var text string
		if json.Unmarshal(event.Error, &text) == nil {
			message = strings.TrimSpace(text)
		} else {
			var detail struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(event.Error, &detail) == nil {
				message = strings.TrimSpace(detail.Message)
			}
		}
		if message == "" {
			message = "Pi stream error"
		}
		return writeLine(o.operator, "[pi error] "+message)
	}
	return nil
}

func writeLine(writer io.Writer, value string) error {
	return harness.WriteOperatorLine("Pi", writer, value)
}

func truncateDisplay(value string) string { return harness.TruncateDisplay(value) }
