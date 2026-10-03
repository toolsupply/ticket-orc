package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestEventStreamStartsWithLiteralSyncBaseline(t *testing.T) {
	for _, baseline := range []uint64{0, 42} {
		t.Run(fmt.Sprint(baseline), func(t *testing.T) {
			server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			server.lifecycle = context.Background()
			for seq := uint64(1); seq <= baseline; seq++ {
				server.PublishEvent(Event{Type: "worker.state"})
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			request := httptest.NewRequest(http.MethodGet, "/v1/events", nil).WithContext(ctx)
			response := httptest.NewRecorder()
			server.handleEvents(response, request)
			want := fmt.Sprintf("id: %d\nevent: stream.sync\ndata: {\"seq\":%d,\"type\":\"stream.sync\"}\n\n", baseline, baseline)
			if response.Code != http.StatusOK || response.Body.String() != want {
				t.Fatalf("sync response status=%d body=%q, want %q", response.Code, response.Body.String(), want)
			}
		})
	}
}

func TestEventSubscriptionAndConcurrentPublishAreOrdered(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		broker := newEventBroker()
		start := make(chan struct{})
		type subscriptionResult struct {
			id       uint64
			baseline uint64
			events   <-chan Event
			ok       bool
		}
		subscribed := make(chan subscriptionResult, 1)
		var publishers sync.WaitGroup
		publishers.Add(2)
		go func() {
			defer publishers.Done()
			<-start
			id, baseline, events, ok := broker.subscribe()
			subscribed <- subscriptionResult{id: id, baseline: baseline, events: events, ok: ok}
		}()
		go func() {
			defer publishers.Done()
			<-start
			broker.publish(Event{Type: "worker.state"})
		}()
		close(start)
		publishers.Wait()
		result := <-subscribed
		if !result.ok {
			t.Fatal("concurrent subscription failed")
		}
		switch result.baseline {
		case 0:
			select {
			case event := <-result.events:
				if event.Seq != 1 {
					t.Fatalf("event after baseline 0 has sequence %d, want 1", event.Seq)
				}
			default:
				t.Fatal("publish after subscription was not delivered")
			}
		case 1:
			if len(result.events) != 0 {
				t.Fatalf("event published before subscription was replayed: queue length %d", len(result.events))
			}
		default:
			t.Fatalf("concurrent subscription baseline=%d, want 0 or 1", result.baseline)
		}
		broker.unsubscribe(result.id)
	}
}

func TestEventAfterSyncBaselineUsesNextSequence(t *testing.T) {
	broker := newEventBroker()
	for seq := 0; seq < 42; seq++ {
		broker.publish(Event{Type: "worker.state"})
	}
	id, baseline, events, ok := broker.subscribe()
	if !ok {
		t.Fatal("subscription failed")
	}
	defer broker.unsubscribe(id)
	syncFrame, err := encodeSSEEvent(Event{Seq: baseline, Type: "stream.sync"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "id: 42\nevent: stream.sync\ndata: {\"seq\":42,\"type\":\"stream.sync\"}\n\n"; string(syncFrame) != want {
		t.Fatalf("sync frame = %q, want %q", syncFrame, want)
	}
	broker.publish(Event{Type: "worker.state", Worker: "coder"})
	domainEvent := <-events
	domainFrame, err := encodeSSEEvent(domainEvent)
	if err != nil {
		t.Fatal(err)
	}
	if want := "id: 43\nevent: worker.state\ndata: {\"seq\":43,\"type\":\"worker.state\",\"worker\":\"coder\"}\n\n"; string(domainFrame) != want {
		t.Fatalf("first post-sync domain frame = %q, want %q", domainFrame, want)
	}
}

func TestPublishedRepositoryChangeSSEAndStatusUseExplicitIdentity(t *testing.T) {
	const repositoryID = "5dc15231-0b71-4bb8-bb22-9dbf655e29ee"
	const repositoryKey = "project"
	const ticketPath = "/ticket/reported/path"
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Status: func() Status {
			return Status{Repositories: []RepositoryStatus{{ID: repositoryID, Key: repositoryKey, Path: ticketPath, State: "healthy"}}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	subscriber, baseline, events, ok := server.events.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
	}
	if baseline != 0 {
		t.Fatalf("subscription baseline=%d, want 0", baseline)
	}
	defer server.events.unsubscribe(subscriber)
	server.PublishEvent(RuntimeEventDTO(supervisor.RuntimeEvent{
		Type: "ticket.repository_changed", RepositoryID: repositoryID, RepositoryKey: repositoryKey, Code: "submitted",
	}))
	event := <-events
	data, err := encodeSSEEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"event: ticket.repository_changed", `"repository_id":"` + repositoryID + `"`, `"repository_key":"project"`, `"code":"submitted"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("SSE event missing %q: %q", want, text)
		}
	}
	if strings.Contains(text, ticketPath) || strings.Contains(text, `"repository":`) || strings.Contains(text, `"session_target":`) || strings.Contains(text, `"capability`) {
		t.Fatalf("SSE event contains an ambiguous repository or private field: %q", text)
	}
	statusData, err := json.Marshal(server.currentStatus().Repositories[0])
	if err != nil {
		t.Fatal(err)
	}
	var status RepositoryStatus
	if err := json.Unmarshal(statusData, &status); err != nil {
		t.Fatal(err)
	}
	if status.ID != repositoryID || status.Key != repositoryKey || status.Path != ticketPath || status.Path == repositoryID {
		t.Fatalf("repository status=%#v, want Ticket-reported path and stable ID", status)
	}
}

func TestEventBrokerSlowSubscriberCreatesSequenceGapForStatusResync(t *testing.T) {
	broker := newEventBroker()
	id, baseline, events, ok := broker.subscribe()
	if !ok || id == 0 {
		t.Fatal("subscribe failed")
	}
	if baseline != 0 {
		t.Fatalf("initial baseline=%d, want 0", baseline)
	}
	for i := 0; i < cap(events); i++ {
		broker.publish(Event{Type: "worker.state", Worker: "coder"})
	}
	// The channel is full, so this notification is deliberately dropped for
	// the slow subscriber. A later notification makes the gap observable.
	broker.publish(Event{Type: "steer.delivery", Worker: "coder", Phase: "accepted"})
	for i := 0; i < cap(events); i++ {
		if got := (<-events).Seq; got != uint64(i+1) {
			t.Fatalf("buffered event sequence = %d, want %d", got, i+1)
		}
	}
	broker.publish(Event{Type: "daemon.started"})
	if got := (<-events).Seq; got != uint64(cap(events)+2) {
		t.Fatalf("post-gap sequence = %d, want %d", got, cap(events)+2)
	}
	broker.unsubscribe(id)
	broker.mu.Lock()
	if len(broker.subscribers) != 0 {
		t.Fatalf("subscribers after disconnect = %d, want 0", len(broker.subscribers))
	}
	broker.mu.Unlock()
}

func TestEventBrokerCloseUnblocksSubscribersAndRejectsNewOnes(t *testing.T) {
	broker := newEventBroker()
	_, _, events, ok := broker.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
	}
	broker.close()
	if _, open := <-events; open {
		t.Fatal("closed broker left subscriber open")
	}
	if _, _, _, ok := broker.subscribe(); ok {
		t.Fatal("closed broker accepted a new subscriber")
	}
	broker.publish(Event{Type: "daemon.stopping"})
}

func TestSlowSubscriberCanRecoverWithAuthoritativeStatusSnapshot(t *testing.T) {
	server, _ := startTestServer(t)
	id, baseline, events, ok := server.events.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
	}
	if baseline != 0 {
		t.Fatalf("initial baseline=%d, want 0", baseline)
	}
	defer server.events.unsubscribe(id)
	for i := 0; i < cap(events); i++ {
		server.PublishEvent(Event{Type: "worker.state", Worker: "coder"})
	}
	server.PublishEvent(Event{Type: "worker.state", Worker: "dropped"})
	for i := 0; i < cap(events); i++ {
		<-events
	}
	server.PublishEvent(Event{Type: "daemon.started"})
	if got := (<-events).Seq; got != uint64(cap(events)+2) {
		t.Fatalf("observable event gap = %d, want %d", got, cap(events)+2)
	}
	request, err := http.NewRequest(http.MethodGet, server.Endpoint().CapabilityURL()+"/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status resynchronization response = %d, want %d", response.StatusCode, http.StatusOK)
	}
}
