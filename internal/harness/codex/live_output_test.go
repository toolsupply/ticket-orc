package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestLiveOutputProcessHelper(t *testing.T) {
	if os.Getenv("ORC_LIVE_OUTPUT_HELPER") != "1" {
		return
	}
	conn, err := net.Dial("tcp", os.Getenv("ORC_LIVE_OUTPUT_ADDRESS"))
	if err != nil {
		os.Exit(2)
	}
	defer conn.Close()
	fmt.Println(`{"type":"thread.started","thread_id":"live-thread"}`)
	encoded, _ := json.Marshal(strings.Repeat("hello 世界\n", 1000))
	fmt.Printf("{\"type\":\"item.completed\",\"item\":{\"id\":\"message-1\",\"type\":\"agent_message\",\"text\":%s}}\n", encoded)
	switch os.Getenv("ORC_LIVE_OUTPUT_MODE") {
	case "backpressure":
		fmt.Printf("{\"type\":\"item.completed\",\"item\":{\"id\":\"message-2\",\"type\":\"agent_message\",\"text\":%s}}\n", encoded)
	case "malformed":
		fmt.Println(`{broken`)
	case "invalidated":
		fmt.Println(`{"type":"thread.started","thread_id":"different-thread"}`)
	}
	// Only the parent can release process exit. EOF is also safe during cleanup.
	_, _ = conn.Read(make([]byte, 1))
	fmt.Println(`{"type":"turn.completed"}`)
	os.Exit(0)
}

func TestLiveOutputBeforeProcessExit(t *testing.T) {
	for _, mode := range []string{"compact", "quiet", "json", "resume", "backpressure", "malformed", "cancel", "invalidated"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var result harness.RunResult
			var runErr error
			var terminal bytes.Buffer
			events := make(chan harness.OutputEvent, 1)
			adapter, err := newAdapter(os.Args[0], os.Environ, func(ctx context.Context, request processRequest) (processResult, error) {
				request.args = []string{"-test.run=^TestLiveOutputProcessHelper$"}
				request.env = append(os.Environ(), "ORC_LIVE_OUTPUT_HELPER=1", "ORC_LIVE_OUTPUT_ADDRESS="+listener.Addr().String(), "ORC_LIVE_OUTPUT_MODE="+mode)
				return runProcess(ctx, request)
			})
			if err != nil {
				t.Fatal(err)
			}
			request := validRequest(t, &terminal)
			request.WorkingDir = t.TempDir()
			request.Events = events
			if mode == "json" || mode == "quiet" {
				request.OutputMode = mode
			}
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("process did not stop")
				}
			})
			go func() {
				if mode == "invalidated" || mode == "resume" {
					result, runErr = adapter.Resume(ctx, "live-thread", request)
				} else {
					result, runErr = adapter.Run(ctx, request)
				}
				close(done)
			}()
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if mode == "backpressure" {
				// Do not consume: the second complete message must fail instead of blocking.
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("full output blocked process termination")
				}
				if !errors.Is(runErr, harness.ErrOutputBackpressure) {
					t.Fatalf("error = %v", runErr)
				}
			}
			var event harness.OutputEvent
			select {
			case event = <-events:
			case <-time.After(10 * time.Second):
				t.Fatal("no live message")
			}
			want := strings.Repeat("hello 世界\n", 1000)
			if event.Kind != "message" || event.ItemID != "message-1" || event.Text != want {
				t.Fatalf("message identity/content changed: %#v", event)
			}
			if mode == "compact" || mode == "quiet" || mode == "json" || mode == "resume" {
				select {
				case <-done:
					t.Fatal("process exited before exit barrier was released")
				default:
				}
				if _, err := conn.Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "cancel" {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("process did not finish")
			}
			if result.SessionID != "live-thread" {
				t.Fatalf("session = %#v", result)
			}
			switch mode {
			case "compact", "quiet", "json", "resume":
				if runErr != nil || !result.StreamEndedNormally {
					t.Fatalf("result = %#v, %v", result, runErr)
				}
				if mode != "json" && terminal.String() != harness.TruncateDisplay(want)+"\n[codex] turn complete\n" {
					t.Fatalf("unexpected or duplicate rendering: %q", terminal.String())
				}
			case "cancel":
				if !errors.Is(runErr, context.Canceled) || result.StreamEndedNormally {
					t.Fatalf("result = %#v, %v", result, runErr)
				}
			case "malformed", "invalidated":
				if !errors.Is(runErr, ErrProtocol) || result.StreamEndedNormally {
					t.Fatalf("result = %#v, %v", result, runErr)
				}
			}
			if mode == "invalidated" && result.SessionOutcome != harness.SessionInvalidated {
				t.Fatalf("outcome = %#v", result)
			}
			raw, err := os.ReadFile(result.LogPath)
			if mode == "json" && terminal.String() != string(raw) {
				t.Fatal("JSON rendering differs from raw log")
			}
			if err != nil || !bytes.Contains(raw, []byte(`"message-1"`)) {
				t.Fatalf("private log missing message: %v", err)
			}
		})
	}
}

func TestLiveOutputProjectionAndFinalFrame(t *testing.T) {
	raw := `{"type":"item.started","item":{"id":"command-1","type":"command_execution","command":"TOKEN=secret env","aggregated_output":"credentials"}}
{"type":"item.completed","item":{"id":"command-1","type":"command_execution","command":"TOKEN=secret env","exit_code":0,"aggregated_output":"credentials"}}
{"type":"item.completed","item":{"id":"files-1","type":"file_change","changes":[{"kind":"update","path":"secret"}]}}
{"type":"item.completed","item":{"id":"reasoning-1","type":"reasoning","text":"private"}}
{"type":"error","message":"credential dump"}
{"type":"item.completed","item":{"id":"message-1","type":"agent_message","text":"complete"}}`
	for _, mode := range []string{"compact", "quiet", "json"} {
		t.Run(mode, func(t *testing.T) {
			runner := &fakeProcessRunner{chunks: [][]byte{[]byte(raw)}}
			request := validRequest(t, io.Discard)
			request.OutputMode = mode
			events := make(chan harness.OutputEvent, 4)
			request.Events = events
			_, err := testAdapter(t, nil, runner).Run(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 4 {
				t.Fatalf("events = %d", len(events))
			}
			for i := 0; i < 4; i++ {
				event := <-events
				encoded, _ := json.Marshal(event)
				if strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), "private") || strings.Contains(string(encoded), "credential") {
					t.Fatalf("unsafe activity: %s", encoded)
				}
				if i == 3 && (event.Text != "complete" || event.ItemID != "message-1") {
					t.Fatalf("final frame = %#v", event)
				}
			}
		})
	}
}

func TestLiveOutputRejectsMissingIdentityAndInvalidBuffer(t *testing.T) {
	for _, size := range []int{0, 1, harness.MaxOutputBuffer + 1} {
		request := validRequest(t, io.Discard)
		request.Events = make(chan harness.OutputEvent, size)
		runner := &fakeProcessRunner{chunks: [][]byte{[]byte(`{"type":"item.completed","item":{"type":"agent_message","text":"no ID"}}` + "\n")}}
		result, err := testAdapter(t, nil, runner).Run(context.Background(), request)
		if err == nil || result.StreamEndedNormally {
			t.Fatalf("size %d: %#v, %v", size, result, err)
		}
		if size == 1 && !errors.Is(err, harness.ErrOutputIdentity) {
			t.Fatalf("error = %v", err)
		}
		if size != 1 && len(runner.calls) != 0 {
			t.Fatal("invalid channel launched a process")
		}
	}
}

func TestLiveOutputStopsAtInvalidSession(t *testing.T) {
	const header = `{"type":"thread.started","thread_id":"expected-thread"}` + "\n"
	const prefix = `{"type":"item.completed","item":{"id":"valid-message","type":"agent_message","text":"valid prefix"}}` + "\n"
	const mismatch = `{"type":"thread.started","thread_id":"foreign-thread"}`
	const foreign = `{"type":"item.completed","item":{"id":"foreign-message","type":"agent_message","text":"foreign text"}}` + "\n" +
		`{"type":"item.started","item":{"id":"foreign-activity","type":"command_execution","command":"env"}}`
	for _, resume := range []bool{false, true} {
		for _, suffix := range []string{"\n" + foreign + "\n", "\n" + foreign, ""} {
			for _, mode := range []string{"compact", "quiet", "json"} {
				t.Run(fmt.Sprintf("resume=%t/suffix=%d/%s", resume, len(suffix), mode), func(t *testing.T) {
					raw := header + prefix + mismatch + suffix
					runner := &fakeProcessRunner{chunks: [][]byte{[]byte(raw)}}
					adapter := testAdapter(t, nil, runner)
					events := make(chan harness.OutputEvent, 8)
					var terminal bytes.Buffer
					request := validRequest(t, &terminal)
					request.Events, request.OutputMode = events, mode
					var result harness.RunResult
					var err error
					if resume {
						result, err = adapter.Resume(context.Background(), "expected-thread", request)
					} else {
						result, err = adapter.Run(context.Background(), request)
					}
					if !errors.Is(err, ErrProtocol) || result.SessionOutcome != harness.SessionInvalidated || result.StreamEndedNormally || result.SessionID != "expected-thread" {
						t.Fatalf("invalidated result = %#v, %v", result, err)
					}
					// Run/Resume returning is the completion barrier for all publication,
					// including final unterminated framing and output.Close.
					if len(events) != 1 {
						t.Fatalf("got %d events; foreign records escaped or valid prefix was lost", len(events))
					}
					event := <-events
					if event.Kind != "message" || event.ItemID != "valid-message" || event.Text != "valid prefix" {
						t.Fatalf("event = %#v", event)
					}
					logged, err := os.ReadFile(result.LogPath)
					if err != nil || string(logged) != raw {
						t.Fatalf("private raw log changed: %q, %v", logged, err)
					}
					if mode == "json" && terminal.String() != raw {
						t.Fatal("JSON rendering changed")
					}
				})
			}
		}
	}
}
