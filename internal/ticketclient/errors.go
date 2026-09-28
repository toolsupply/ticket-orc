package ticketclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrProtocol identifies malformed or internally inconsistent ticket JSON.
var ErrProtocol = errors.New("ticket JSON protocol error")

// ErrTransport identifies a broken persistent ticket process or pipe.
var ErrTransport = errors.New("ticket JSONL transport error")

// TransportError describes a bounded failure of the persistent ticket
// process or its pipes. Category and process metadata are safe for callers to
// retain; the bounded stderr detail is kept only for the local error string
// and is never exposed as structured worker metadata.
type TransportError struct {
	Category string
	ExitCode int
	Signal   string

	detail string
	cause  error
}

func (e *TransportError) Error() string {
	message := "ticket JSONL transport failure"
	if e.Category != "" {
		message += " (" + e.Category + ")"
	}
	if e.ExitCode >= 0 {
		message += fmt.Sprintf(" with status %d", e.ExitCode)
	} else if e.Signal != "" {
		message += " with signal " + e.Signal
	}
	if e.detail != "" {
		message += "; stderr: " + e.detail
	}
	return message
}

func (e *TransportError) Unwrap() error {
	if e.cause == nil {
		return ErrTransport
	}
	return errors.Join(ErrTransport, e.cause)
}

// ErrClosed identifies operations attempted after explicit client shutdown.
var ErrClosed = errors.New("ticket client is closed")

// CommandError is one ticket command's JSON error response. A persistent
// stream has no per-command process exit status, so ExitCode is -1 for streamed
// responses. Details preserves ticket's public error data so callers can
// reconcile uncertain mutations without replaying them.
type CommandError struct {
	ExitCode int
	Code     string
	TicketID string
	Message  string
	Details  map[string]json.RawMessage
	Stderr   string
	cause    error
}

// ActorQueueConflictError identifies Ticket's safe conflict response when an
// actor already owns active work in the other lifecycle queue.
type ActorQueueConflictError struct {
	RequestedQueue   string
	ConflictingQueue string
	TicketID         string
	Cause            error
}

func (e *ActorQueueConflictError) Error() string {
	message := "ticket actor owns work in another queue"
	if e.ConflictingQueue != "" {
		message += " (" + e.ConflictingQueue
		if e.TicketID != "" {
			message += " ticket " + e.TicketID
		}
		message += ")"
	}
	return message
}

func (e *ActorQueueConflictError) Unwrap() error { return e.Cause }

// ActorQueueConflict extracts only Ticket's specific cross-queue ownership
// conflict. Other conflict codes remain ordinary command failures.
func ActorQueueConflict(err error, requestedQueue string) *ActorQueueConflictError {
	var conflict *ActorQueueConflictError
	if !errors.As(err, &conflict) || conflict == nil || conflict.RequestedQueue != requestedQueue {
		return nil
	}
	return conflict
}

func actorQueueConflictCandidate(err error, requestedQueue string) *ActorQueueConflictError {
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr == nil || commandErr.Code != "conflict" {
		return nil
	}
	conflictingQueue := ""
	switch requestedQueue {
	case "open":
		conflictingQueue = "review"
	case "review":
		conflictingQueue = "open"
	}
	ticketID := commandErr.TicketID
	if ticketID == "" {
		ticketID = commandErrorTicketID(commandErr.Details)
	}
	if ticketID == "" {
		return nil
	}
	return &ActorQueueConflictError{RequestedQueue: requestedQueue, ConflictingQueue: conflictingQueue, TicketID: ticketID, Cause: err}
}

func commandErrorTicketID(details map[string]json.RawMessage) string {
	raw, ok := details["id"]
	if !ok {
		return ""
	}
	var id string
	if err := json.Unmarshal(raw, &id); err != nil || ValidateFullID(id) != nil {
		return ""
	}
	return id
}

func (e *CommandError) Error() string {
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = strings.TrimSpace(e.Stderr)
	}
	if message == "" && e.cause != nil {
		message = e.cause.Error()
	}
	if message == "" {
		message = "ticket command failed"
	}
	if e.ExitCode < 0 && e.Code != "" {
		return fmt.Sprintf("ticket command failed (%s): %s", e.Code, message)
	}
	if e.ExitCode < 0 {
		return fmt.Sprintf("ticket command failed: %s", message)
	}
	if e.Code != "" {
		return fmt.Sprintf("ticket command failed with exit %d (%s): %s", e.ExitCode, e.Code, message)
	}
	return fmt.Sprintf("ticket command failed with exit %d: %s", e.ExitCode, message)
}

func (e *CommandError) Unwrap() error { return e.cause }

// MutationApplied reports ticket's explicit mutation_applied error detail.
// Callers must reconcile authoritative ticket state instead of retrying when
// this is true.
func (e *CommandError) MutationApplied() bool {
	raw, ok := e.Details["mutation_applied"]
	if !ok {
		return false
	}
	var applied bool
	return json.Unmarshal(raw, &applied) == nil && applied
}

// MutationApplied reports whether err is a ticket command error explicitly
// marked as having applied its local mutation.
func MutationApplied(err error) bool {
	var commandErr *CommandError
	return errors.As(err, &commandErr) && commandErr.MutationApplied()
}
