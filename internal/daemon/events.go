package daemon

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/toolsupply/ticket-orc/internal/state"
)

// Event is the bounded public notification envelope delivered by the daemon.
// Fleet events contain classifications and identifiers only; prompts, full session
// targets, and raw errors never belong in fleet events. Join requests may include
// a short session hint and Ticket repository ID for operator correlation. The
// explicit conversation payload carries only committed semantic transcript records.
type Event struct {
	Conversation   *state.ConversationRecord `json:"conversation,omitempty"`
	Seq            uint64                    `json:"seq"`
	Type           string                    `json:"type"`
	Worker         string                    `json:"worker,omitempty"`
	Role           string                    `json:"role,omitempty"`
	State          string                    `json:"state,omitempty"`
	Phase          string                    `json:"phase,omitempty"`
	Code           string                    `json:"code,omitempty"`
	Ticket         string                    `json:"ticket,omitempty"`
	RepositoryID   string                    `json:"repository_id,omitempty"`
	RepositoryKey  string                    `json:"repository_key,omitempty"`
	RepositoryName string                    `json:"repository_name,omitempty"`
	Actor          string                    `json:"actor,omitempty"`
	Harness        string                    `json:"harness,omitempty"`
	Session        string                    `json:"session,omitempty"`
	Applied        bool                      `json:"mutation_applied,omitempty"`
	Failure        *WorkerFailure            `json:"failure,omitempty"`
}

type eventBroker struct {
	mu          sync.Mutex
	nextID      uint64
	sequence    uint64
	subscribers map[uint64]chan Event
	closed      bool
}

func newEventBroker() *eventBroker {
	return &eventBroker{subscribers: make(map[uint64]chan Event)}
}

func (b *eventBroker) publish(event Event) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.sequence++
	event.Seq = b.sequence
	for _, subscriber := range b.subscribers {
		outgoing := event
		if event.Conversation != nil && len(subscriber) >= 4 {
			// Reserve most of the existing buffer for fleet/control notifications.
			// One bounded hint replaces a burst; further loss is visible in the
			// independent position watermark even if this is the last publication.
			if len(subscriber) != 4 {
				continue
			}
			outgoing = Event{Seq: event.Seq, Type: "conversation.resync"}
		}
		select {
		case subscriber <- outgoing:
		default:
			// Events are notifications. A slow subscriber resynchronizes from
			// GET /v1/status after observing a sequence gap.
		}
	}
}

func (b *eventBroker) subscribe() (uint64, uint64, <-chan Event, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, 0, nil, false
	}
	b.nextID++
	id := b.nextID
	channel := make(chan Event, 32)
	b.subscribers[id] = channel
	return id, b.sequence, channel, true
}

func (b *eventBroker) unsubscribe(id uint64) {
	if b == nil || id == 0 {
		return
	}
	b.mu.Lock()
	if channel, ok := b.subscribers[id]; ok {
		delete(b.subscribers, id)
		close(channel)
	}
	b.mu.Unlock()
}

func (b *eventBroker) close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	for id, channel := range b.subscribers {
		delete(b.subscribers, id)
		close(channel)
	}
	b.mu.Unlock()
}

func encodeSSEEvent(event Event) ([]byte, error) {
	if strings.TrimSpace(event.Type) == "" {
		return nil, fmt.Errorf("event type must not be empty")
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", event.Seq, event.Type, data)), nil
}

// checkpoint drains the bounded queue and captures its watermark atomically so
// the watermark never overtakes a previously queued content notification.
func (b *eventBroker) checkpoint(id uint64) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	frames := make([]Event, 0, 33)
	if subscriber, ok := b.subscribers[id]; ok {
		for len(subscriber) > 0 {
			frames = append(frames, <-subscriber)
		}
	}
	return append(frames, Event{Seq: b.sequence, Type: "stream.position"})
}
