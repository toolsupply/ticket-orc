package daemonclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

// ErrEventGap reports that the bounded event stream dropped one or more
// notifications. Callers must resynchronize from Status before rendering
// subsequent events as a complete history.
var ErrEventGap = errors.New("daemon event stream gap")

// EventStream consumes the daemon's authenticated SSE event stream. It keeps
// only the last sequence number and one bounded event frame in memory.
type EventStream struct {
	response       *http.Response
	reader         *bufio.Reader
	lastSeq        uint64
	failureContext string
	gap            bool
	closed         bool
}

// Events opens the authenticated daemon event stream. The request context
// controls cancellation of a blocked Next call.
func (c *Client) Events(ctx context.Context) (*EventStream, error) {
	if c == nil || c.http == nil {
		return nil, &Error{Kind: ErrorTransport, Message: "daemon client is unavailable"}
	}
	if ctx == nil {
		return nil, &Error{Kind: ErrorTransport, Message: "daemon request context is unavailable"}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint.CapabilityURL()+"/v1/events", nil)
	if err != nil {
		return nil, &Error{Kind: ErrorProtocol, Message: "build daemon event request", Cause: err}
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, &Error{Kind: ErrorTransport, Message: c.discoveryMessage("daemon event stream is unreachable"), Cause: err}
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusNotFound {
		_ = response.Body.Close()
		return nil, &Error{Kind: ErrorAuth, Code: "unauthorized", Message: c.discoveryMessage("daemon authentication failed"), StatusCode: response.StatusCode}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
		_ = response.Body.Close()
		if readErr != nil {
			return nil, &Error{Kind: ErrorTransport, Message: c.discoveryMessage("read daemon event error"), Cause: readErr, StatusCode: response.StatusCode}
		}
		return nil, c.applicationError(response.StatusCode, data)
	}
	if !strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		_ = response.Body.Close()
		return nil, &Error{Kind: ErrorProtocol, Message: "daemon event stream has an unsupported content type", StatusCode: response.StatusCode}
	}
	return &EventStream{response: response, reader: bufio.NewReader(io.LimitReader(response.Body, maxResponseBytes+1)), failureContext: c.discoveryContext()}, nil
}

// Next returns the next classified event. Heartbeats and comments are
// ignored. A sequence gap is surfaced once through ErrEventGap after the
// event frame is decoded; callers can inspect EventStream.Event afterward.
func (s *EventStream) Next() (daemon.Event, error) {
	if s == nil || s.closed || s.reader == nil {
		return daemon.Event{}, io.EOF
	}
	var event daemon.Event
	var data bytes.Buffer
	var sequence uint64
	for {
		line, err := s.reader.ReadString('\n')
		if err != nil && len(line) == 0 {
			if errors.Is(err, io.EOF) {
				return daemon.Event{}, io.EOF
			}
			return daemon.Event{}, &Error{Kind: ErrorTransport, Message: "read daemon event stream" + s.failureContext, Cause: err}
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		switch {
		case line == "":
			if data.Len() == 0 {
				if err != nil {
					return daemon.Event{}, io.EOF
				}
				continue
			}
			if err := json.Unmarshal(data.Bytes(), &event); err != nil {
				return daemon.Event{}, &Error{Kind: ErrorProtocol, Message: "decode daemon event", Cause: err}
			}
			if sequence != 0 && event.Seq == 0 {
				event.Seq = sequence
			}
			if s.lastSeq != 0 && event.Seq != s.lastSeq+1 {
				s.gap = true
			}
			s.lastSeq = event.Seq
			if s.gap {
				s.gap = false
				return event, ErrEventGap
			}
			return event, nil
		case strings.HasPrefix(line, ":"):
			continue
		case strings.HasPrefix(line, "id:"):
			value, parseErr := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "id:")), 10, 64)
			if parseErr == nil {
				sequence = value
			}
		case strings.HasPrefix(line, "data:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data.Len()+len(value) > maxResponseBytes {
				return daemon.Event{}, &Error{Kind: ErrorProtocol, Message: "daemon event is too large"}
			}
			data.WriteString(value)
		case strings.HasPrefix(line, "event:"):
			// The JSON envelope's type is authoritative; the SSE label is
			// intentionally ignored if an older daemon omits it.
		default:
			return daemon.Event{}, &Error{Kind: ErrorProtocol, Message: fmt.Sprintf("unsupported daemon event field %q", line)}
		}
		if err != nil {
			return daemon.Event{}, io.EOF
		}
	}
}

// Close releases the underlying response body. It is safe to call repeatedly.
func (s *EventStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	if s.response == nil || s.response.Body == nil {
		return nil
	}
	return s.response.Body.Close()
}
