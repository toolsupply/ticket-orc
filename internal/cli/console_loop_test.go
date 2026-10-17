package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func TestInteractiveSupervisorConsoleExitRequestsShutdown(t *testing.T) {
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var output, errorsOut bytes.Buffer
	var exits atomic.Int32
	if err := runConsoleLoopWithSelectionPolicy(context.Background(), stateDir, strings.NewReader("quit\n"), &output, &errorsOut, nil, "", false, func() { exits.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if exits.Load() != 1 {
		t.Fatalf("shutdown callback count=%d output=%q errors=%q", exits.Load(), output.String(), errorsOut.String())
	}
	if !strings.Contains(output.String(), "stopping supervisor") {
		t.Fatalf("exit output=%q", output.String())
	}
	if !strings.HasPrefix(output.String(), "[ticket-orc] interactive console; type help for commands.\n") {
		t.Fatalf("console banner has unexpected spacing: %q", output.String())
	}
}

func TestInteractiveSupervisorConsoleSecondInterruptRequestsShutdown(t *testing.T) {
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	reader, writer := io.Pipe()
	interrupts := make(chan os.Signal, 2)
	var errorsOut bytes.Buffer
	var exits atomic.Int32
	prompts := make(chan struct{}, 4)
	shutdownMessage := make(chan struct{})
	consoleOutput := &consoleWatchStartWriter{prompted: prompts, matched: shutdownMessage, match: "press Ctrl-C again"}
	done := make(chan error, 1)
	go func() {
		done <- runConsoleLoopWithSelectionPolicy(context.Background(), stateDir, reader, consoleOutput, &errorsOut, interrupts, "", false, func() { exits.Add(1) })
	}()
	waitConsoleMilestone(t, prompts, "initial prompt")
	_, _ = io.WriteString(writer, "not-a-command\n")
	waitConsoleMilestone(t, prompts, "prompt after invalid command")
	interrupts <- os.Interrupt
	waitConsoleMilestone(t, shutdownMessage, "first interrupt prompt")
	waitConsoleMilestone(t, prompts, "prompt after first interrupt")
	interrupts <- os.Interrupt
	_ = writer.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second interrupt did not stop console")
	}
	if exits.Load() != 1 || !strings.Contains(consoleOutput.String(), "press Ctrl-C again") || !strings.Contains(consoleOutput.String(), "stopping supervisor") || !strings.Contains(errorsOut.String(), "command:") {
		t.Fatalf("interrupt output=%q exits=%d errors=%q", consoleOutput.String(), exits.Load(), errorsOut.String())
	}
}

func TestInteractiveSupervisorConsoleUsesItsOwnInstanceEndpoint(t *testing.T) {
	var localCalls, ambientCalls atomic.Int32
	newHandler := func(calls *atomic.Int32) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			switch r.URL.Path {
			case "/v1/status":
				_ = json.NewEncoder(w).Encode(daemon.Status{})
			case "/v1/events":
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, ": connected\n\n")
				<-r.Context().Done()
			default:
				http.NotFound(w, r)
			}
		})
	}
	localServer, localStateDir := newConsoleTestServer(t, newHandler(&localCalls))
	defer localServer.Close()
	ambientServer, ambientStateDir := newConsoleTestServer(t, newHandler(&ambientCalls))
	defer ambientServer.Close()
	data, err := os.ReadFile(daemon.EndpointPath(ambientStateDir))
	if err != nil {
		t.Fatal(err)
	}
	var ambientEndpoint daemon.Endpoint
	if err := json.Unmarshal(data, &ambientEndpoint); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TICKET_ORC_ENDPOINT", ambientEndpoint.CapabilityURL())

	var output, errorsOut bytes.Buffer
	var exits atomic.Int32
	if err := runInteractiveSupervisorConsoleWithSelection(context.Background(), localStateDir, strings.NewReader("quit\n"), &output, &errorsOut, "", func() { exits.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if localCalls.Load() == 0 || ambientCalls.Load() != 0 || exits.Load() != 1 {
		t.Fatalf("console endpoint calls local=%d ambient=%d exits=%d output=%q errors=%q", localCalls.Load(), ambientCalls.Load(), exits.Load(), output.String(), errorsOut.String())
	}
}

func TestConsoleRendersDetachedMessageAtInputEOF(t *testing.T) {
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	reader, writer := io.Pipe()
	prompts := make(chan struct{}, 1)
	detached := make(chan struct{})
	consoleOutput := &consoleWatchStartWriter{prompted: prompts, detached: detached}
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		done <- runConsoleLoopWithSelectionPolicy(context.Background(), stateDir, reader, consoleOutput, io.Discard, nil, "", false, nil)
	}()
	t.Cleanup(func() {
		_ = writer.Close()
		_ = reader.Close()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Errorf("console did not exit during cleanup")
		}
	})
	waitConsoleMilestone(t, prompts, "initial prompt")
	_ = writer.Close()
	waitConsoleMilestone(t, detached, "detached message")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(consoleOutput.String(), "detached") {
		t.Fatalf("console output omitted detached message: %q", consoleOutput.String())
	}
}
