package daemonclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func TestEventStreamRejectsUnsupportedContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"worker.state"}`))
	}))
	defer server.Close()
	stateDir := t.TempDir()
	writeClientEndpoint(t, stateDir, server.URL, 1, 1, "token")
	client, err := New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Events(context.Background()); !hasClientErrorKind(err, ErrorProtocol) {
		t.Fatalf("error=%v, want protocol", err)
	}
}

type eventStreamRoundTripper func(*http.Request) (*http.Response, error)

func (roundTripper eventStreamRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTripper(request)
}

func newEventStreamTestClient(t *testing.T, frames ...string) *Client {
	t.Helper()
	responseIndex := 0
	return &Client{
		endpoint: daemon.Endpoint{URL: "http://orc.invalid", EndpointKey: "test-key"},
		http: &http.Client{Transport: eventStreamRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path != "/test-key/v1/events" {
				t.Errorf("event request path=%q", request.URL.Path)
			}
			if responseIndex >= len(frames) {
				return nil, fmt.Errorf("unexpected extra event connection")
			}
			body := frames[responseIndex]
			responseIndex++
			header := make(http.Header)
			header.Set("Content-Type", "text/event-stream")
			return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		})},
	}
}

func TestEventStreamConsumesSyncBaselineAndReturnsFollowingEvents(t *testing.T) {
	client := newEventStreamTestClient(t, "id: 10\nevent: stream.sync\ndata: {\"seq\":10,\"type\":\"stream.sync\"}\n\nid: 11\nevent: worker.state\ndata: {\"seq\":11,\"type\":\"worker.state\",\"worker\":\"one\"}\n\nid: 12\nevent: worker.state\ndata: {\"seq\":12,\"type\":\"worker.state\",\"worker\":\"two\"}\n\n")
	stream, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for _, want := range []daemon.Event{{Seq: 11, Type: "worker.state", Worker: "one"}, {Seq: 12, Type: "worker.state", Worker: "two"}} {
		got, err := stream.Next()
		if err != nil || got != want {
			t.Fatalf("event=%#v err=%v, want %#v", got, err, want)
		}
	}
}

func TestEventStreamDoesNotLimitCumulativeSubscriptionBytes(t *testing.T) {
	const baseline = "id: 0\nevent: stream.sync\ndata: {\"seq\":0,\"type\":\"stream.sync\"}\n\n"
	eventCount := maxResponseBytes/64 + 1
	var frames strings.Builder
	frames.WriteString(baseline)
	for sequence := 1; sequence <= eventCount; sequence++ {
		fmt.Fprintf(&frames, "id: %d\ndata: {\"seq\":%d,\"type\":\"worker.state\",\"worker\":\"w\"}\n\n", sequence, sequence)
	}
	if frames.Len() <= maxResponseBytes {
		t.Fatalf("stream fixture size=%d, want more than %d bytes", frames.Len(), maxResponseBytes)
	}

	var subscriptions int
	client := &Client{
		endpoint: daemon.Endpoint{URL: "http://orc.invalid", EndpointKey: "test-key"},
		http: &http.Client{Transport: eventStreamRoundTripper(func(request *http.Request) (*http.Response, error) {
			subscriptions++
			header := make(http.Header)
			header.Set("Content-Type", "text/event-stream")
			return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(frames.String())), Request: request}, nil
		})},
	}
	stream, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for sequence := 1; sequence <= eventCount; sequence++ {
		event, err := stream.Next()
		if err != nil || event.Seq != uint64(sequence) || event.Worker != "w" {
			t.Fatalf("event %d=%#v err=%v", sequence, event, err)
		}
	}
	if subscriptions != 1 {
		t.Fatalf("event subscriptions=%d, want one connection for %d events", subscriptions, eventCount)
	}
}

func TestEventStreamBoundsIndividualLineSize(t *testing.T) {
	client := newEventStreamTestClient(t, strings.Repeat("x", maxResponseBytes+1)+"\n")
	stream, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := stream.Next(); err == nil || !hasClientErrorKind(err, ErrorProtocol) || !strings.Contains(err.Error(), "event line is too large") {
		t.Fatalf("oversized event line error=%v, want bounded protocol error", err)
	}
}

func TestEventStreamGapReturnsDecodedEvent(t *testing.T) {
	client := newEventStreamTestClient(t, "id: 10\nevent: stream.sync\ndata: {\"seq\":10,\"type\":\"stream.sync\"}\n\nid: 12\nevent: worker.state\ndata: {\"seq\":12,\"type\":\"worker.state\",\"worker\":\"inspectable\"}\n\n")
	stream, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	event, err := stream.Next()
	if err != ErrEventGap || event.Seq != 12 || event.Worker != "inspectable" {
		t.Fatalf("gap event=%#v err=%v, want decoded sequence 12 and ErrEventGap", event, err)
	}
}

func TestEventStreamDetectsGapAfterZeroBaseline(t *testing.T) {
	client := newEventStreamTestClient(t, "id: 0\nevent: stream.sync\ndata: {\"seq\":0,\"type\":\"stream.sync\"}\n\nid: 2\nevent: worker.state\ndata: {\"seq\":2,\"type\":\"worker.state\"}\n\n")
	stream, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if event, err := stream.Next(); err != ErrEventGap || event.Seq != 2 {
		t.Fatalf("zero-baseline event=%#v err=%v, want gap at sequence 2", event, err)
	}
}

func TestEventStreamReconnectStartsFreshSyncBaseline(t *testing.T) {
	client := newEventStreamTestClient(t,
		"id: 10\nevent: stream.sync\ndata: {\"seq\":10,\"type\":\"stream.sync\"}\n\nid: 11\nevent: worker.state\ndata: {\"seq\":11,\"type\":\"worker.state\"}\n\n",
		"id: 100\nevent: stream.sync\ndata: {\"seq\":100,\"type\":\"stream.sync\"}\n\nid: 101\nevent: worker.state\ndata: {\"seq\":101,\"type\":\"worker.state\"}\n\n",
	)
	first, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event, err := first.Next(); err != nil || event.Seq != 11 {
		t.Fatalf("first connection event=%#v err=%v", event, err)
	}
	_ = first.Close()
	second, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if event, err := second.Next(); err != nil || event.Seq != 101 {
		t.Fatalf("reconnected event=%#v err=%v, want fresh baseline 100", event, err)
	}
}
