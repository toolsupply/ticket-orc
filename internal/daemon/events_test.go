package daemon

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

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
	subscriber, events, ok := server.events.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
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
	if strings.Contains(text, `"repository":`) || strings.Contains(text, `"session_target":`) || strings.Contains(text, `"capability`) {
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
	id, events, ok := broker.subscribe()
	if !ok || id == 0 {
		t.Fatal("subscribe failed")
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
	_, events, ok := broker.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
	}
	broker.close()
	if _, open := <-events; open {
		t.Fatal("closed broker left subscriber open")
	}
	if _, _, ok := broker.subscribe(); ok {
		t.Fatal("closed broker accepted a new subscriber")
	}
	broker.publish(Event{Type: "daemon.stopping"})
}

func TestSlowSubscriberCanRecoverWithAuthoritativeStatusSnapshot(t *testing.T) {
	server, _ := startTestServer(t)
	id, events, ok := server.events.subscribe()
	if !ok {
		t.Fatal("subscribe failed")
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
