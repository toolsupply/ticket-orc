package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

func TestConsoleEventTrackerSuppressesResyncAndDuplicateState(t *testing.T) {
	tracker := newConsoleEventTracker()
	tracker.seed(daemon.Status{
		Workers:      []daemon.WorkerStatus{{Name: "coder", State: "running"}},
		Repositories: []daemon.RepositoryStatus{{ID: "repo-id", Key: "orc", State: "healthy"}},
	})
	if tracker.accept(daemon.Event{Type: "status.resync", Worker: "coder", State: "running"}) {
		t.Fatal("status resync should stay internal")
	}
	if tracker.accept(daemon.Event{Type: "status.resync", RepositoryID: "repo-id", RepositoryKey: "orc", State: "healthy"}) {
		t.Fatal("repository status resync should stay internal")
	}
	if tracker.accept(daemon.Event{Type: "worker.state", Worker: "coder", State: "running"}) {
		t.Fatal("duplicate state should be coalesced")
	}
	if !tracker.accept(daemon.Event{Type: "worker.state", Worker: "coder", State: "paused"}) {
		t.Fatal("state transition should be rendered")
	}
	claim := daemon.Event{Type: "ticket.claim", Worker: "coder", Ticket: "20260926-00001"}
	if !tracker.accept(claim) || tracker.accept(claim) {
		t.Fatal("duplicate adjacent activity should be suppressed")
	}

	tracker = newConsoleEventTracker()
	ready := daemon.Event{Type: "worker.state", Worker: "coder", State: "running"}
	claimed := daemon.Event{Type: "ticket.claim", Worker: "coder", Ticket: "20260926-00002"}
	if !tracker.accept(ready) || !tracker.accept(claimed) || tracker.accept(claimed) {
		t.Fatal("ready and claimed events were not deduplicated")
	}
	failure := daemon.Event{Type: "worker.failure", Worker: "coder", State: "failed"}
	if !tracker.accept(failure) || !tracker.accept(failure) {
		t.Fatal("worker failure was hidden by duplicate coalescing")
	}
}

func TestConsoleEventsResynchronizeAfterReconnectAndGap(t *testing.T) {
	var eventCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte(": connected\n\n"))
			if eventCalls.Add(1) == 1 {
				_, _ = w.Write([]byte("id: 1\ndata: {\"type\":\"worker.state\",\"seq\":1,\"worker\":\"coder\",\"state\":\"running\"}\n\n"))
				return
			}
			_, _ = w.Write([]byte("id: 3\ndata: {\"type\":\"ticket.claim\",\"seq\":3,\"worker\":\"coder\",\"ticket\":\"20260926-12345\"}\n\n"))
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{
				Workers:      []daemon.WorkerStatus{{Name: "coder", State: "paused"}},
				Repositories: []daemon.RepositoryStatus{{Key: "orc", State: "healthy"}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events := make(chan daemon.Event, 16)
	errorsOut := make(chan error, 4)
	go readConsoleEvents(ctx, client, events, errorsOut)
	var sawResync, sawRepositoryResync, sawClaim bool
	for !sawResync || !sawRepositoryResync || !sawClaim {
		select {
		case event := <-events:
			if event.Type == "status.resync" {
				if event.Worker != "" {
					sawResync = true
				}
				if event.RepositoryKey == "orc" {
					sawRepositoryResync = true
				}
			}
			if event.Type == "ticket.claim" {
				sawClaim = true
			}
		case eventErr := <-errorsOut:
			if eventErr != nil {
				t.Fatalf("event stream error: %v", eventErr)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for resync=%t repository_resync=%t claim=%t", sawResync, sawRepositoryResync, sawClaim)
		}
	}
}

func TestConsoleEventsReadsMultipleEventsFromOneSubscription(t *testing.T) {
	var eventCalls atomic.Int32
	connected := make(chan struct{})
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" {
			http.NotFound(w, r)
			return
		}
		call := eventCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "id: 40\nevent: stream.sync\ndata: {\"seq\":40,\"type\":\"stream.sync\"}\n\n")
		for sequence, eventType := range []string{"worker.state", "ticket.claim", "repository.observer"} {
			frame := fmt.Sprintf("id: %d\ndata: {\"seq\":%d,\"type\":%q}\n\n", sequence+41, sequence+41, eventType)
			_, _ = io.WriteString(w, frame)
		}
		w.(http.Flusher).Flush()
		if call == 1 {
			close(connected)
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 8)
	errorsOut := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("event stream did not connect")
	}
	want := []string{"worker.state", "ticket.claim", "repository.observer"}
	for i, eventType := range want {
		event := nextConsoleTestEvent(t, events)
		if event.Type != eventType || event.Seq != uint64(i+41) {
			t.Fatalf("event[%d]=%#v, want type=%q seq=%d", i, event, eventType, i+41)
		}
	}
	if got := eventCalls.Load(); got != 1 {
		t.Fatalf("event subscriptions=%d, want one for all events", got)
	}
	select {
	case event := <-events:
		t.Fatalf("unexpected extra event, possibly synthetic stream.sync: %#v", event)
	default:
	}
	select {
	case err := <-errorsOut:
		t.Fatalf("event stream error: %v", err)
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader did not stop after cancellation")
	}
}

func TestConsoleEventGapResyncKeepsCurrentSubscription(t *testing.T) {
	var eventCalls, statusCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			statusCalls.Add(1)
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "paused"}}})
		case "/v1/events":
			eventCalls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "id: 5\nevent: stream.sync\ndata: {\"seq\":5,\"type\":\"stream.sync\"}\n\nid: 6\ndata: {\"seq\":6,\"type\":\"worker.state\",\"worker\":\"coder\"}\n\nid: 8\ndata: {\"seq\":8,\"type\":\"ticket.claim\",\"ticket\":\"20260928-12345\"}\n\nid: 9\ndata: {\"seq\":9,\"type\":\"worker.state\",\"worker\":\"coder\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 8)
	errorsOut := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	want := []struct {
		typeName string
		seq      uint64
	}{
		{typeName: "worker.state", seq: 6},
		{typeName: "status.resync"},
		{typeName: "ticket.claim", seq: 8},
		{typeName: "worker.state", seq: 9},
	}
	for i, expected := range want {
		event := nextConsoleTestEvent(t, events)
		if event.Type != expected.typeName || event.Seq != expected.seq {
			t.Fatalf("event[%d]=%#v, want type=%q seq=%d", i, event, expected.typeName, expected.seq)
		}
		if i == 1 && (event.Worker != "coder" || event.State != "paused") {
			t.Fatalf("resync event=%#v, want authoritative paused worker", event)
		}
	}
	if got := eventCalls.Load(); got != 1 {
		t.Fatalf("event subscriptions=%d, want current stream retained after gap", got)
	}
	if got := statusCalls.Load(); got != 1 {
		t.Fatalf("status reconciliations=%d, want one", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader did not stop after cancellation")
	}
}

func TestConsoleEventEOFReconnectsAndClosesPreviousStream(t *testing.T) {
	var eventCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
		case "/v1/events":
			call := eventCalls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			if call == 1 {
				_, _ = io.WriteString(w, "id: 100\nevent: stream.sync\ndata: {\"seq\":100,\"type\":\"stream.sync\"}\n\nid: 101\ndata: {\"seq\":101,\"type\":\"worker.state\",\"worker\":\"first\"}\n\n")
				w.(http.Flusher).Flush()
				return
			}
			_, _ = io.WriteString(w, "id: 200\nevent: stream.sync\ndata: {\"seq\":200,\"type\":\"stream.sync\"}\n\nid: 201\ndata: {\"seq\":201,\"type\":\"worker.state\",\"worker\":\"second\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	trackedBodies := make(chan *consoleTrackedEventBody, 4)
	httpClient := &http.Client{Transport: consoleEventTrackingTransport{base: http.DefaultTransport, bodies: trackedBodies}}
	client, err := daemonclient.NewWithHTTPClient(stateDir, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 8)
	errorsOut := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	firstBody := nextTrackedConsoleBody(t, trackedBodies)
	firstEvent := nextConsoleTestEvent(t, events)
	if firstEvent.Type != "worker.state" || firstEvent.Seq != 101 || firstEvent.Worker != "first" {
		t.Fatalf("first event=%#v", firstEvent)
	}
	select {
	case <-firstBody.closed:
	case <-time.After(time.Second):
		t.Fatal("previous event response body was not closed on EOF")
	}
	secondBody := nextTrackedConsoleBody(t, trackedBodies)
	var sawResync, sawSecond bool
	for !sawResync || !sawSecond {
		event := nextConsoleTestEvent(t, events)
		if event.Type == "status.resync" && event.Worker == "coder" {
			sawResync = true
		}
		if event.Type == "worker.state" && event.Seq == 201 && event.Worker == "second" {
			sawSecond = true
		}
	}
	if got := eventCalls.Load(); got != 2 {
		t.Fatalf("event subscriptions=%d, want reconnect with new sync baseline", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader did not stop after cancellation")
	}
	select {
	case <-secondBody.closed:
	case <-time.After(time.Second):
		t.Fatal("current event response body was not closed on cancellation")
	}
}

func TestConsoleEventReconnectFailuresRemainBounded(t *testing.T) {
	var eventCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" {
			http.NotFound(w, r)
			return
		}
		eventCalls.Add(1)
		http.Error(w, `{"error":{"code":"temporarily_unavailable","message":"unavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 1)
	errorsOut := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader exceeded its bounded reconnect attempts")
	}
	if got := eventCalls.Load(); got != 3 {
		t.Fatalf("event subscription attempts=%d, want 3", got)
	}
	select {
	case err := <-errorsOut:
		if err == nil {
			t.Fatal("terminal reconnect error is nil")
		}
	default:
		t.Fatal("bounded reconnect failure was not reported")
	}
}
