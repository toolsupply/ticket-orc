package daemonclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

// LogsQuery combines repeated values with OR within each field, AND across
// fields. Empty Workers/RepositoryIDs selects all managed workers; empty Kinds
// selects messages and diagnostics. Every connection starts at existing EOFs.
type LogsQuery struct {
	Workers       []string
	RepositoryIDs []string
	Kinds         []string
}

// LogsMessage contains exactly one ready, record, or terminal error payload.
// Logs have no lifecycle sequence, replay cursor, or continuity guarantee.
type LogsMessage struct {
	Ready  *daemon.LogsReady
	Record *daemon.LogRecord
	Error  *daemon.LogsError
}

type LogStream struct {
	body      io.ReadCloser
	reader    *bufio.Reader
	ready     bool
	terminal  bool
	closeOnce sync.Once
	closeErr  error
}

// Logs opens a live-only subscription. ctx cancels both opening and blocked
// reads. Close may be called concurrently with Next; serialize calls to Next.
func (c *Client) Logs(ctx context.Context, query LogsQuery) (*LogStream, error) {
	if c == nil || c.http == nil {
		return nil, &Error{Kind: ErrorTransport, Message: "daemon client is unavailable"}
	}
	if ctx == nil {
		return nil, &Error{Kind: ErrorTransport, Message: "daemon request context is unavailable"}
	}
	values := url.Values{}
	for _, value := range query.Workers {
		values.Add("worker", value)
	}
	for _, value := range query.RepositoryIDs {
		values.Add("repository_id", value)
	}
	for _, value := range query.Kinds {
		values.Add("kind", value)
	}
	path := c.endpoint.CapabilityURL() + "/v1/logs"
	if len(values) > 0 {
		path += "?" + values.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, &Error{Kind: ErrorProtocol, Message: "build daemon log request"}
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, &Error{Kind: ErrorTransport, Message: "daemon log stream is unreachable", Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
		if err != nil {
			return nil, &Error{Kind: ErrorTransport, Message: "read daemon log error", Cause: err}
		}
		if len(data) > maxResponseBytes {
			return nil, &Error{Kind: ErrorProtocol, Message: "daemon log error is too large"}
		}
		// An authenticated 404 may mean an unknown filter. Preserve the server's
		// structured code instead of conflating it with capability authentication.
		return nil, c.applicationError(response.StatusCode, data)
	}
	if !strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		response.Body.Close()
		return nil, &Error{Kind: ErrorProtocol, Message: "daemon log stream has an unsupported content type"}
	}
	return &LogStream{body: response.Body, reader: bufio.NewReader(response.Body)}, nil
}

const maxLogFrameBytes = daemon.MaxLogRecordBytes + (16 << 10)

func logProtocolError(message string) error { return &Error{Kind: ErrorProtocol, Message: message} }

// Next returns ready first, followed by semantic records or a terminal error
// message. Heartbeat comments are ignored. After logs.error it returns EOF.
func (s *LogStream) Next() (LogsMessage, error) {
	if s == nil || s.reader == nil || s.terminal {
		return LogsMessage{}, io.EOF
	}
	message, err := s.next()
	if err != nil || message.Error != nil {
		s.terminal = true
		_ = s.Close()
	}
	return message, err
}

func (s *LogStream) next() (LogsMessage, error) {
	var data bytes.Buffer
	event := ""
	frameBytes := 0
	for {
		var line []byte
		for {
			fragment, err := s.reader.ReadSlice('\n')
			frameBytes += len(fragment)
			if frameBytes > maxLogFrameBytes {
				return LogsMessage{}, logProtocolError("daemon log frame is too large")
			}
			line = append(line, fragment...)
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					if frameBytes != 0 {
						return LogsMessage{}, logProtocolError("incomplete daemon log frame")
					}
					return LogsMessage{}, io.EOF
				}
				return LogsMessage{}, &Error{Kind: ErrorTransport, Message: "read daemon log stream", Cause: err}
			}
			break
		}
		line = bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
		if len(line) == 0 {
			if data.Len() == 0 {
				if event != "" {
					return LogsMessage{}, logProtocolError("empty daemon log event")
				}
				frameBytes = 0
				continue
			}
			if data.Len() > daemon.MaxLogRecordBytes {
				return LogsMessage{}, logProtocolError("daemon log payload is too large")
			}
			var result LogsMessage
			switch event {
			case "logs.ready":
				if s.ready {
					return result, logProtocolError("duplicate daemon logs.ready")
				}
				result.Ready = &daemon.LogsReady{}
				if err := json.Unmarshal(data.Bytes(), result.Ready); err != nil || result.Ready.SchemaVersion != 1 || result.Ready.Replay {
					return LogsMessage{}, logProtocolError("invalid daemon logs.ready")
				}
				s.ready = true
			case "log":
				if !s.ready {
					return result, logProtocolError("daemon log arrived before logs.ready")
				}
				result.Record = &daemon.LogRecord{}
				if err := json.Unmarshal(data.Bytes(), result.Record); err != nil {
					return LogsMessage{}, logProtocolError("invalid daemon log record")
				}
			case "logs.error":
				if !s.ready {
					return result, logProtocolError("daemon logs.error arrived before logs.ready")
				}
				result.Error = &daemon.LogsError{}
				if err := json.Unmarshal(data.Bytes(), result.Error); err != nil || result.Error.Code == "" {
					return LogsMessage{}, logProtocolError("invalid daemon logs.error")
				}
			default:
				return result, logProtocolError("unsupported daemon log event")
			}
			return result, nil
		}
		if line[0] == ':' {
			continue
		}
		field, value, found := bytes.Cut(line, []byte(":"))
		if !found {
			return LogsMessage{}, logProtocolError("unsupported daemon log field")
		}
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			if event != "" {
				return LogsMessage{}, logProtocolError("duplicate daemon log event field")
			}
			event = string(value)
		case "data":
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.Write(value)
		default:
			return LogsMessage{}, logProtocolError("unsupported daemon log field")
		}
	}
}

// Close is idempotent and interrupts a blocked Next call.
func (s *LogStream) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.body != nil {
			s.closeErr = s.body.Close()
		}
	})
	return s.closeErr
}
