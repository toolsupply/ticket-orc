//go:build !windows

package cli

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestConversationProcessContainsDescendantsBeforeDrainingOutput(t *testing.T) {
	for _, code := range []string{"0", "7"} {
		t.Run(code, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var output bytes.Buffer
			done := make(chan struct{})
			var result harness.ProcessResult
			var runErr error
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("conversation did not stop during cleanup")
				}
			})
			go func() {
				defer close(done)
				// The descendant inherits stdout and outlives the leader. EOF
				// requires containment, rather than just reaping the leader.
				result, runErr = runConversationProcess(ctx, harness.ProcessRequest{
					Executable: "sh", Args: []string{"-c", "sleep 30 & printf trailing-output; exit " + code}, Stdout: &output,
				})
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("leader exit left descendant output open")
			}
			if output.String() != "trailing-output" || (code == "0" && (runErr != nil || result.ExitCode != 0)) || (code == "7" && (runErr == nil || result.ExitCode != 7)) {
				t.Fatalf("result=%#v err=%v output=%q", result, runErr, output.String())
			}
		})
	}
}

func TestConversationProcessCancellationUnblocksExitObserver(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ready := make(chan struct{})
	var once sync.Once
	done := make(chan struct{})
	var result harness.ProcessResult
	var runErr error
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("canceled conversation retained a process or observer")
		}
	})
	go func() {
		defer close(done)
		result, runErr = runConversationProcess(ctx, harness.ProcessRequest{
			Executable: "sh", Args: []string{"-c", "sleep 30 & printf ready; wait"},
			Stdout: conversationReadyWriter{ready: func() { once.Do(func() { close(ready) }) }},
		})
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation child did not publish readiness")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not unblock the exit observer")
	}
	if runErr == nil || result.ExitCode == 0 {
		t.Fatalf("canceled conversation result=%#v err=%v", result, runErr)
	}
}

type conversationReadyWriter struct{ ready func() }

func (w conversationReadyWriter) Write(data []byte) (int, error) {
	w.ready()
	return len(data), nil
}
