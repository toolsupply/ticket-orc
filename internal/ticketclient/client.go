package ticketclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"unicode"
)

// Client owns one actor-bound ticket JSONL process. Ticket dispatches stream
// requests sequentially, so Client deliberately permits only one outstanding
// request. A canceled request permanently stops the connection because replay
// could duplicate an uncertain claim or mutation.
type Client struct {
	actor string

	requestSlot chan struct{}
	stateMu     sync.Mutex
	stopped     bool
	stopCause   error

	command             *exec.Cmd
	stdin               io.WriteCloser
	reader              *bufio.Reader
	done                chan struct{}
	outcome             processOutcome
	stderr              *boundedBuffer
	startupResponseRead bool
}

// New starts an actor-bound ticket -i -j session located through PATH.
func New(actor string) (*Client, error) {
	return NewWithWorkingDirAndTarget(actor, "", Target{})
}

// NewWithTarget starts an actor-bound session using an explicit Ticket target.
func NewWithTarget(actor string, target Target) (*Client, error) {
	return NewWithWorkingDirAndTarget(actor, "", target)
}

// NewWithWorkingDir starts an actor-bound session with Ticket's process
// working directory set to workingDir. An empty directory preserves the
// caller's inherited directory.
func NewWithWorkingDir(actor, workingDir string) (*Client, error) {
	return NewWithWorkingDirAndTarget(actor, workingDir, Target{})
}

// NewWithWorkingDirAndRepository starts Ticket with independent source and
// repository contexts for this child process.
func NewWithWorkingDirAndRepository(actor, workingDir, repository string) (*Client, error) {
	return NewWithWorkingDirAndTarget(actor, workingDir, Target{Repository: repository})
}

// NewWithWorkingDirAndTarget starts Ticket with the resolved target supplied
// as explicit process arguments or isolated environment variables.
func NewWithWorkingDirAndTarget(actor, workingDir string, target Target) (*Client, error) {
	return newClient(actor, defaultExecutable, os.Environ, workingDir, target)
}

// NewWithExecutable starts a session using a specific executable. It is useful
// for executable-level tests without adding a public CLI option.
func NewWithExecutable(actor, executable string) (*Client, error) {
	return NewWithExecutableAndTarget(actor, executable, Target{})
}

// NewWithExecutableAndTarget is the injectable form of
// NewWithWorkingDirAndTarget.
func NewWithExecutableAndTarget(actor, executable string, target Target) (*Client, error) {
	return newClient(actor, executable, os.Environ, "", target)
}

// NewWithExecutableAndWorkingDir is the executable-injectable form of
// NewWithWorkingDir, used by executable-level tests.
func NewWithExecutableAndWorkingDir(actor, executable, workingDir string) (*Client, error) {
	return NewWithExecutableWorkingDirAndTarget(actor, executable, workingDir, Target{})
}

// NewWithExecutableWorkingDirAndRepository is the injectable form used by
// executable-level tests.
func NewWithExecutableWorkingDirAndRepository(actor, executable, workingDir, repository string) (*Client, error) {
	return NewWithExecutableWorkingDirAndTarget(actor, executable, workingDir, Target{Repository: repository})
}

// NewWithExecutableWorkingDirAndTarget is the executable-injectable form used
// by tests and integrations that need both a working directory and a target.
func NewWithExecutableWorkingDirAndTarget(actor, executable, workingDir string, target Target) (*Client, error) {
	return newClient(actor, executable, os.Environ, workingDir, target)
}

func newClient(actor, executable string, environ environFunc, workingDir string, target Target) (*Client, error) {
	if strings.TrimSpace(actor) == "" {
		return nil, fmt.Errorf("ticket actor must not be empty")
	}
	if strings.IndexFunc(actor, unicode.IsSpace) >= 0 {
		return nil, fmt.Errorf("ticket actor must not contain whitespace")
	}
	if !validActor(actor) {
		return nil, fmt.Errorf("ticket actor must start with an ASCII letter or digit and contain only letters, digits, '.', '_', or '-'")
	}
	if strings.TrimSpace(executable) == "" {
		return nil, fmt.Errorf("ticket executable must not be empty")
	}
	if environ == nil {
		return nil, fmt.Errorf("ticket process environment must not be nil")
	}
	if err := target.validate(); err != nil {
		return nil, err
	}

	command := exec.Command(executable, append(target.args(), "-i", "-j")...)
	command.Dir = workingDir
	command.Env = ApplyTargetEnvironment(environ(), actor, target)
	stderrBuffer := newBoundedBuffer(maxStderrBytes)
	command.Stderr = stderrBuffer
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open ticket stdin: %w", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("open ticket stdout: %w", err)
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start ticket JSONL session: %w", err)
	}

	client := &Client{
		actor:       actor,
		requestSlot: make(chan struct{}, 1),
		command:     command,
		stdin:       stdin,
		reader:      bufio.NewReader(stdout),
		done:        make(chan struct{}),
		stderr:      stderrBuffer,
	}
	go client.wait()
	return client, nil
}

// Close terminates the persistent ticket process and waits for its pipes and
// stderr drainer to finish. It is safe to call more than once and interrupts an
// outstanding wait command.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.terminate(ErrClosed)
	<-c.done
	return nil
}

func (c *Client) invoke(ctx context.Context, args []string, output any) error {
	return c.invokeWithStdin(ctx, args, nil, output)
}

func (c *Client) invokeWithStdin(ctx context.Context, args []string, stdin []byte, output any) error {
	if ctx == nil {
		return fmt.Errorf("ticket command context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.requestSlot <- struct{}{}:
		defer func() { <-c.requestSlot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.stoppedError(); err != nil {
		if responseErr, recovered := c.recoverExitedResponse(output); recovered {
			return responseErr
		}
		return err
	}

	var input *string
	if stdin != nil {
		value := string(stdin)
		input = &value
	}
	frame, err := json.Marshal(struct {
		Args  []string `json:"args"`
		Stdin *string  `json:"stdin,omitempty"`
	}{Args: args, Stdin: input})
	if err != nil {
		return fmt.Errorf("encode ticket request: %w", err)
	}
	if len(frame) > maxRequestFrameBytes {
		return fmt.Errorf("ticket JSONL request exceeds %d bytes", maxRequestFrameBytes)
	}
	frame = append(frame, '\n')

	writeDone := make(chan error, 1)
	go func() { writeDone <- writeAll(c.stdin, frame) }()
	select {
	case err := <-writeDone:
		if err != nil {
			if responseErr, recovered := c.recoverExitedResponse(output); recovered {
				return responseErr
			}
			transportErr := c.transportError("write request", err)
			c.terminate(transportErr)
			return transportErr
		}
	case <-ctx.Done():
		c.terminate(ctx.Err())
		<-writeDone
		return ctx.Err()
	}

	responseDone := make(chan responseResult, 1)
	go func() {
		var raw json.RawMessage
		raw, err := c.readResponse()
		responseDone <- responseResult{raw: raw, err: err}
	}()

	var response responseResult
	select {
	case response = <-responseDone:
	case <-ctx.Done():
		c.terminate(ctx.Err())
		response = <-responseDone
		responseErr := c.decodeResponse(response, output)
		if responseErr != nil {
			return errors.Join(ctx.Err(), responseErr)
		}
		return ctx.Err()
	}

	responseErr := c.decodeResponse(response, output)
	if contextErr := ctx.Err(); contextErr != nil {
		c.terminate(contextErr)
		if responseErr != nil {
			return errors.Join(contextErr, responseErr)
		}
		return contextErr
	}
	return responseErr
}
