package harness

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
)

// ProcessRequest is the protocol-neutral process contract shared by harness
// adapters. StderrLimit is zero for uncapped stderr, or a positive byte limit.
type ProcessRequest struct {
	Executable  string
	Args        []string
	Dir         string
	Env         []string
	Stdout      io.Writer
	StderrLimit int
}

// ProcessResult contains the subprocess exit code and trimmed stderr text.
type ProcessResult struct {
	ExitCode int
	Stderr   string
	Outcome  ProcessOutcome
}

// RunProcess runs one configured child, streams stdout to its adapter, and
// captures stderr. Provider-specific diagnostics remain with the adapter.
func RunProcess(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	outcome := ProcessOutcome{Start: "not_started", Cancellation: ProcessCancellation(ctx)}
	command := exec.CommandContext(ctx, request.Executable, request.Args...)
	command.Dir = request.Dir
	command.Env = request.Env
	stderrBuffer := &bytes.Buffer{}
	var stderr io.Writer = stderrBuffer
	var bounded *limitedBuffer
	if request.StderrLimit > 0 {
		bounded = &limitedBuffer{limit: request.StderrLimit}
		stderr = bounded
	}
	command.Stderr = stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		return ProcessResult{ExitCode: -1, Outcome: outcome}, err
	}
	if err := command.Start(); err != nil {
		outcome.Cancellation = ProcessCancellation(ctx)
		return ProcessResult{ExitCode: -1, Outcome: outcome}, err
	}
	outcome.Start = "started"
	copyErr := error(nil)
	output := request.Stdout
	if output == nil {
		output = io.Discard
	}
	if _, err := io.Copy(output, stdout); err != nil {
		copyErr = err
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	exitCode := -1
	if command.ProcessState != nil {
		exitCode = command.ProcessState.ExitCode()
		outcome.ExitObserved = true
		outcome.ExitCode = exitCode
		outcome.Signal = processSignal(command.ProcessState)
	}
	outcome.Cancellation = ProcessCancellation(ctx)
	var stderrText string
	if bounded != nil {
		stderrText = bounded.String()
	} else {
		stderrText = stderrBuffer.String()
	}
	return ProcessResult{ExitCode: exitCode, Stderr: strings.TrimSpace(stderrText), Outcome: outcome}, errors.Join(waitErr, copyErr, ctx.Err())
}

type limitedBuffer struct {
	data  bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	remaining := b.limit - b.data.Len()
	if remaining > 0 {
		if len(data) > remaining {
			_, _ = b.data.Write(data[:remaining])
		} else {
			_, _ = b.data.Write(data)
		}
	}
	return len(data), nil
}

func (b *limitedBuffer) String() string { return b.data.String() }
