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
		ID       string `json:"id"`
		Type     string `json:"type"`
		Text     string `json:"text"`
		Command  string `json:"command"`
		ExitCode *int   `json:"exit_code"`
		Message  string `json:"message"`
		Changes  []struct {
			Kind string `json:"kind"`
			Path string `json:"path"`
		} `json:"changes"`
	} `json:"item"`
}

// InterpretEvent returns semantic records for one framed Codex JSON event.
// Blank and unknown events produce no records; invalid or oversized JSON
// returns ErrProtocol. Text is preserved without terminal display limits.
func InterpretEvent(line []byte) ([]harness.LogRecord, error) {
	if len(line) > harness.MaxEventBytes {
		return nil, fmt.Errorf("%w: event exceeds %d bytes", ErrProtocol, harness.MaxEventBytes)
	}
	if len(bytes.TrimSpace(line)) == 0 {
		return nil, nil
	}
	var event outputEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return nil, fmt.Errorf("%w: malformed JSONL output event: %v", ErrProtocol, err)
	}
	record := harness.LogRecord{}
	switch {
	case event.Type == "item.completed" && event.Item != nil && event.Item.Type == "agent_message":
		record = harness.LogRecord{Kind: "message", Text: event.Item.Text}
	case event.Type == "item.completed" && event.Item != nil && event.Item.Type == "error":
		record = harness.LogRecord{Kind: "diagnostic", Level: "error", Code: "item_error", Text: defaultText(event.Item.Message, "unknown error")}
	case event.Type == "error":
		record = harness.LogRecord{Kind: "diagnostic", Level: "error", Code: "error", Text: defaultText(event.Message, "unknown error")}
	case event.Type == "turn.failed":
		message := "turn failed"
		if event.Error != nil {
			message = defaultText(event.Error.Message, message)
		}
		record = harness.LogRecord{Kind: "diagnostic", Level: "error", Code: "turn_failed", Text: message}
	case event.Type == "turn.completed":
		message := "turn complete"
		if event.Usage != nil {
			message += fmt.Sprintf(" (in=%d, cached=%d, out=%d)", event.Usage.InputTokens, event.Usage.CachedInputTokens, event.Usage.OutputTokens)
		}
		record = harness.LogRecord{Kind: "diagnostic", Level: "info", Code: "turn_completed", Text: message}
	case (event.Type == "item.started" || event.Type == "item.completed") && event.Item != nil && event.Item.Type == "command_execution":
		phase := "started"
		if event.Type == "item.completed" {
			phase = "completed"
		}
		record = harness.LogRecord{Kind: "command", Phase: phase, Command: event.Item.Command, ExitCode: event.Item.ExitCode}
	case event.Type == "item.completed" && event.Item != nil && event.Item.Type == "file_change":
		record.Kind = "file_change"
		for _, change := range event.Item.Changes {
			record.Changes = append(record.Changes, harness.FileChange{Kind: change.Kind, Path: change.Path})
		}
	default:
		return nil, nil
	}
	if event.Item != nil {
		record.ItemID = event.Item.ID
	}
	return []harness.LogRecord{record}, nil
}

// RenderEvent renders one framed Codex event in compact or quiet mode.
// It applies terminal formatting to the same records returned by InterpretEvent.
func RenderEvent(mode OutputMode, line []byte, operator io.Writer) error {
	if mode != OutputCompact && mode != OutputQuiet {
		return fmt.Errorf("unknown Codex event output mode %q", mode)
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
		return fmt.Errorf("unknown Codex event output mode %q", mode)
	}
	if operator == nil {
		operator = io.Discard
	}

	value := ""
	switch record.Kind {
	case "message":
		value = truncateDisplay(record.Text)
	case "diagnostic":
		prefix := "[codex error] "
		if record.Code == "turn_completed" {
			prefix = "[codex] "
		}
		if record.Code == "turn_failed" {
			prefix = "[codex failed] "
		}
		value = prefix + record.Text
	case "command":
		if mode == OutputCompact {
			if record.Phase == "started" {
				value = "  $ " + truncateDisplay(defaultText(record.Command, "(command)"))
			}
			if record.Phase == "completed" && record.ExitCode != nil && *record.ExitCode != 0 {
				value = fmt.Sprintf("  [exit %d] %s", *record.ExitCode, truncateDisplay(defaultText(record.Command, "(command)")))
			}
		}
	case "file_change":
		if mode == OutputCompact {
			changes := make([]string, 0, len(record.Changes))
			for _, change := range record.Changes {
				changes = append(changes, defaultText(change.Kind, "change")+" "+defaultText(change.Path, "?"))
			}
			value = "  [files] " + truncateDisplay(strings.Join(changes, ", "))
		}
	}
	if value != "" {
		if err := writeLine(operator, value); err != nil {
			return err
		}
	}
	return nil
}

func (o *ExecutionOutput) renderLine(line []byte) error {
	return RenderEvent(o.mode, line, o.operator)
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
