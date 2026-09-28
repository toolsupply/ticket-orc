package codex

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

// OutputMode controls operator rendering while every raw event is logged.
type OutputMode = harness.OutputMode

const (
	OutputCompact = harness.OutputCompact
	OutputQuiet   = harness.OutputQuiet
	OutputJSON    = harness.OutputJSON
)

// ExecutionOutput persists one raw stream and renders selected events.
type ExecutionOutput struct {
	log      *harness.RawLog
	operator io.Writer
	mode     OutputMode
	framer   harness.LineFramer
	err      error
	closed   bool
}

// OpenExecutionOutput creates the required raw log before an agent starts.
func OpenExecutionOutput(stateDir, ticket, role string, mode OutputMode, operator io.Writer, components ...string) (*ExecutionOutput, error) {
	return openExecutionOutputAt(stateDir, ticket, role, mode, operator, time.Now().UTC(), components...)
}

func openExecutionOutputAt(stateDir, ticket, role string, mode OutputMode, operator io.Writer, now time.Time, components ...string) (*ExecutionOutput, error) {
	if mode != OutputCompact && mode != OutputQuiet && mode != OutputJSON {
		return nil, fmt.Errorf("unknown Codex output mode %q", mode)
	}
	if operator == nil {
		operator = io.Discard
	}
	log, err := harness.OpenRawLog("Codex", stateDir, ticket, role, now, components...)
	if err != nil {
		return nil, err
	}
	return &ExecutionOutput{log: log, operator: operator, mode: mode}, nil
}

// Path returns the raw JSONL log path.
func (o *ExecutionOutput) Path() string { return o.log.Path() }

// Write stores raw bytes before rendering them for the operator.
func (o *ExecutionOutput) Write(data []byte) (int, error) {
	if o.closed {
		return 0, fmt.Errorf("Codex execution output is closed")
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
		operatorWritten, err := o.operator.Write(data)
		if err != nil {
			o.err = fmt.Errorf("write Codex operator output: %w", err)
			return len(data), o.err
		}
		if operatorWritten != len(data) {
			o.err = fmt.Errorf("write Codex operator output: %w", io.ErrShortWrite)
			return len(data), o.err
		}
		return len(data), nil
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

// Close renders a final unterminated event, syncs the log, and closes it.
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

type outputEvent struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error"`
	Usage *struct {
		InputTokens       int64 `json:"input_tokens"`
		CachedInputTokens int64 `json:"cached_input_tokens"`
		OutputTokens      int64 `json:"output_tokens"`
	} `json:"usage"`
	Item *struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		Command  string `json:"command"`
		ExitCode int    `json:"exit_code"`
		Message  string `json:"message"`
		Changes  []struct {
			Kind string `json:"kind"`
			Path string `json:"path"`
		} `json:"changes"`
	} `json:"item"`
}

func (o *ExecutionOutput) renderLine(line []byte) error {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	if len(line) > maxEventBytes {
		return fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, maxEventBytes)
	}
	var event outputEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return fmt.Errorf("%w: malformed JSONL output event: %v", ErrProtocol, err)
	}

	switch {
	case event.Type == "item.completed" && event.Item != nil && event.Item.Type == "agent_message":
		return o.writeDisplay(truncateDisplay(event.Item.Text))
	case event.Type == "item.completed" && event.Item != nil && event.Item.Type == "error":
		return o.writeDiagnostic("[codex error] " + defaultText(event.Item.Message, "unknown error"))
	case event.Type == "error":
		return o.writeDiagnostic("[codex error] " + defaultText(event.Message, "unknown error"))
	case event.Type == "turn.failed":
		message := "turn failed"
		if event.Error != nil {
			message = defaultText(event.Error.Message, message)
		}
		return o.writeDiagnostic("[codex failed] " + message)
	case event.Type == "turn.completed":
		message := "[codex] turn complete"
		if event.Usage != nil {
			message += fmt.Sprintf(" (in=%d, cached=%d, out=%d)", event.Usage.InputTokens, event.Usage.CachedInputTokens, event.Usage.OutputTokens)
		}
		return o.writeDiagnostic(message)
	case o.mode == OutputCompact && event.Type == "item.started" && event.Item != nil && event.Item.Type == "command_execution":
		return o.writeDisplay("  $ " + truncateDisplay(defaultText(event.Item.Command, "(command)")))
	case o.mode == OutputCompact && event.Type == "item.completed" && event.Item != nil && event.Item.Type == "command_execution" && event.Item.ExitCode != 0:
		return o.writeDisplay(fmt.Sprintf("  [exit %d] %s", event.Item.ExitCode, truncateDisplay(defaultText(event.Item.Command, "(command)"))))
	case o.mode == OutputCompact && event.Type == "item.completed" && event.Item != nil && event.Item.Type == "file_change":
		changes := make([]string, 0, len(event.Item.Changes))
		for _, change := range event.Item.Changes {
			changes = append(changes, defaultText(change.Kind, "change")+" "+defaultText(change.Path, "?"))
		}
		return o.writeDisplay("  [files] " + truncateDisplay(strings.Join(changes, ", ")))
	default:
		return nil
	}
}

func (o *ExecutionOutput) writeDisplay(value string) error {
	if value == "" {
		return nil
	}
	return writeLine(o.operator, value)
}

func (o *ExecutionOutput) writeDiagnostic(value string) error {
	return writeLine(o.operator, value)
}

func writeLine(writer io.Writer, value string) error {
	return harness.WriteOperatorLine("Codex", writer, value)
}

func truncateDisplay(value string) string { return harness.TruncateDisplay(value) }

func defaultText(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
