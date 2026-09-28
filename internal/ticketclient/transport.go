package ticketclient

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const maxRequestFrameBytes = 2 << 20

const maxResponseFrameBytes = 2 << 20

const maxStderrBytes = 4096

const startupResponseWait = 250 * time.Millisecond

type processOutcome struct {
	err      error
	exitCode int
}

type responseResult struct {
	raw json.RawMessage
	err error
}

func (c *Client) decodeResponse(response responseResult, output any) error {
	if response.err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(response.err, &syntaxErr) {
			protocolErr := fmt.Errorf("%w: decode response: %v", ErrProtocol, response.err)
			c.terminate(protocolErr)
			return protocolErr
		}
		if stoppedErr := c.stoppedError(); stoppedErr != nil {
			return stoppedErr
		}
		transportErr := c.transportError("read response", response.err)
		c.terminate(transportErr)
		return transportErr
	}
	var envelope struct {
		Error *struct {
			Code    string                     `json:"code"`
			Message string                     `json:"message"`
			Details map[string]json.RawMessage `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.raw, &envelope); err != nil {
		protocolErr := fmt.Errorf("%w: decode response envelope: %v", ErrProtocol, err)
		c.terminate(protocolErr)
		return protocolErr
	}
	if envelope.Error != nil {
		return &CommandError{
			ExitCode: -1,
			Code:     envelope.Error.Code,
			TicketID: commandErrorTicketID(envelope.Error.Details),
			Message:  envelope.Error.Message,
			Details:  envelope.Error.Details,
			Stderr:   c.stderr.String(),
		}
	}
	if err := decodeOne(response.raw, output); err != nil {
		protocolErr := fmt.Errorf("%w: decode success response: %v", ErrProtocol, err)
		c.terminate(protocolErr)
		return protocolErr
	}
	return nil
}

// recoverExitedResponse handles Ticket's startup failure convention: it may
// emit one ordinary JSON error envelope and then terminate before accepting a
// request. The process waiter can observe the exit before invoke reads that
// envelope, so inspect the already-closed stream once before reporting the
// generic transport exit error.
func (c *Client) recoverExitedResponse(output any) (error, bool) {
	c.stateMu.Lock()
	if c.startupResponseRead || (c.stopped && !errors.Is(c.stopCause, ErrTransport)) {
		c.stateMu.Unlock()
		return nil, false
	}
	c.startupResponseRead = true
	c.stateMu.Unlock()

	responseDone := make(chan responseResult, 1)
	go func() {
		var raw json.RawMessage
		raw, err := c.readResponse()
		responseDone <- responseResult{raw: raw, err: err}
	}()
	timer := time.NewTimer(startupResponseWait)
	defer timer.Stop()
	var response responseResult
	select {
	case response = <-responseDone:
	case <-c.done:
		response = <-responseDone
	case <-timer.C:
		return nil, false
	}
	if response.err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(response.err, &syntaxErr) {
			protocolErr := fmt.Errorf("%w: decode response: %v", ErrProtocol, response.err)
			return protocolErr, true
		}
		return nil, false
	}
	return c.decodeResponse(response, output), true
}

func (c *Client) readResponse() (json.RawMessage, error) {
	var frame []byte
	for {
		chunk, err := c.reader.ReadSlice('\n')
		frame = append(frame, chunk...)
		if len(frame) > maxResponseFrameBytes {
			return nil, fmt.Errorf("ticket JSONL response exceeds %d bytes", maxResponseFrameBytes)
		}
		if err == nil {
			return bytes.TrimSpace(frame), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) && len(frame) > 0 {
			return bytes.TrimSpace(frame), nil
		}
		return nil, err
	}
}

func (c *Client) wait() {
	err := c.command.Wait()
	exitCode := 0
	if err != nil {
		exitCode = -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
	}
	c.stateMu.Lock()
	c.outcome = processOutcome{err: err, exitCode: exitCode}
	if !c.stopped {
		c.stopped = true
		c.stopCause = c.processExitError(c.outcome)
	}
	c.stateMu.Unlock()
	close(c.done)
}

func (c *Client) terminate(cause error) {
	c.stateMu.Lock()
	if c.stopped {
		c.stateMu.Unlock()
		return
	}
	c.stopped = true
	c.stopCause = cause
	stdin := c.stdin
	process := c.command.Process
	c.stateMu.Unlock()

	_ = stdin.Close()
	if process != nil {
		if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			c.stateMu.Lock()
			c.stopCause = errors.Join(c.stopCause, fmt.Errorf("stop ticket JSONL session: %w", err))
			c.stateMu.Unlock()
		}
	}
}

func (c *Client) stoppedError() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if !c.stopped {
		return nil
	}
	if c.stopCause != nil {
		return c.stopCause
	}
	return ErrClosed
}

func (c *Client) transportError(action string, cause error) error {
	if errors.Is(cause, io.EOF) {
		select {
		case <-c.done:
			c.stateMu.Lock()
			outcome := c.outcome
			c.stateMu.Unlock()
			return c.processExitError(outcome)
		default:
		}
	}
	detail := strings.TrimSpace(c.stderr.String())
	return &TransportError{Category: "pipe_error", detail: detail, cause: fmt.Errorf("%s: %w", action, cause)}
}

func (c *Client) processExitError(outcome processOutcome) error {
	detail := strings.TrimSpace(c.stderr.String())
	signal := ""
	if outcome.exitCode < 0 && outcome.err != nil {
		signal = "terminated"
	}
	return &TransportError{Category: "process_exit", ExitCode: outcome.exitCode, Signal: signal, detail: detail, cause: outcome.err}
}

func decodeOne(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return fmt.Errorf("trailing data: %w", err)
	}
	return nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

type boundedBuffer struct {
	mu        sync.Mutex
	data      []byte
	limit     int
	truncated bool
}

func newBoundedBuffer(limit int) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	available := b.limit - len(b.data)
	if available > 0 {
		if available > len(data) {
			available = len(data)
		}
		b.data = append(b.data, data[:available]...)
	}
	if available < len(data) {
		b.truncated = true
	}
	return len(data), nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	value := strings.TrimSpace(string(b.data))
	if b.truncated {
		value += "…"
	}
	return value
}

func (b *boundedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...)
}

func (b *boundedBuffer) Truncated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.truncated
}
