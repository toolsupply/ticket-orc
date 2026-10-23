package harness

import (
	"errors"
	"fmt"
)

// OutputGranularity describes live delivery, independently of terminal rendering.
type OutputGranularity string

// OutputCompleteMessages emits provider-completed items, never token deltas.
const OutputCompleteMessages OutputGranularity = "complete_messages"

const MaxOutputBuffer = 32

var (
	ErrOutputUnsupported  = errors.New("harness does not support live semantic output")
	ErrOutputBackpressure = errors.New("live semantic output buffer is full")
	ErrOutputIdentity     = errors.New("live semantic output item has missing or oversized identity")
)

// OutputEvent is the conversation-safe subset of interpreted provider output.
// Kind is message or activity. Messages preserve complete text and provider item
// identity. Activity exposes only a fixed tool name, phase, and optional exit
// status; commands, arguments, paths, tool output, and diagnostics stay private.
// Text is bounded by MaxEventBytes, identity by 256 bytes.
//
// The caller supplies a buffered channel (1..MaxOutputBuffer), drains it during
// Run/Resume, and owns its lifetime. Do not close it until Run/Resume returns;
// the adapter never closes it. Delivery is ordered and nonblocking. A full buffer
// fails the run with ErrOutputBackpressure and stops the child through the normal
// stream failure path; accepted events remain readable. Callers must report this
// incomplete outcome and use RunResult for session state, never infer completion
// from an event or channel closure. Downstream persistence failures should cancel
// the run context. No consumer callbacks or consumer goroutines are owned here.
type OutputEvent struct {
	Kind     string `json:"kind"`
	ItemID   string `json:"item_id"`
	Text     string `json:"text,omitempty"`
	ToolName string `json:"tool_name,omitempty"`
	Phase    string `json:"phase,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
}

func ValidateOutputEvents(events chan<- OutputEvent) error {
	if events != nil && (cap(events) < 1 || cap(events) > MaxOutputBuffer) {
		return fmt.Errorf("live semantic output requires a buffer of 1..%d events", MaxOutputBuffer)
	}
	return nil
}

// PublishOutputEvent projects a semantic record without exposing private fields.
// It must only be used by adapters advertising complete-message live output.
func PublishOutputEvent(events chan<- OutputEvent, record LogRecord) error {
	if events == nil {
		return nil
	}
	event := OutputEvent{ItemID: record.ItemID}
	switch record.Kind {
	case "message":
		event.Kind, event.Text = "message", record.Text
	case "command":
		event.Kind, event.ToolName, event.Phase = "activity", "command_execution", record.Phase
		event.ExitCode = record.ExitCode
	case "file_change":
		event.Kind, event.ToolName, event.Phase = "activity", "file_change", "completed"
	default:
		return nil
	}
	if event.ItemID == "" || len(event.ItemID) > 256 {
		return ErrOutputIdentity
	}
	select {
	case events <- event:
		return nil
	default:
		return ErrOutputBackpressure
	}
}
