package state

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Conversations use a separate, instance-scoped file. They are deliberately
// absent from repository Store snapshots and ticket-wide session cleanup.
const conversationFileName = "conversations.json"

// ConversationStateBytes is the hard instance storage bound, including receipts.
const ConversationStateBytes = maxStateBytes

type Conversation struct {
	ID                 string                `json:"id"`
	State              string                `json:"state"`
	Profile            string                `json:"profile"`
	RepositoryIDs      []string              `json:"repository_ids"`
	Operator           string                `json:"operator"`
	Execution          ConversationExecution `json:"execution"`
	SessionID          string                `json:"session_id,omitempty"`
	SessionUnavailable bool                  `json:"session_unavailable,omitempty"`
	CreatedAt          time.Time             `json:"created_at"`
	ClosedAt           *time.Time            `json:"closed_at,omitempty"`
	ClosedBy           string                `json:"closed_by,omitempty"`
	Turns              []ConversationTurn    `json:"turns"`
	Records            []ConversationRecord  `json:"records"`
}

// ConversationExecution is a captured server-side configuration, never an HTTP
// request. Profile validation precedes creation; these trusted managed sessions
// use the existing harness permission configuration.
type ConversationExecution struct {
	Harness          string                   `json:"harness"`
	Model            string                   `json:"model"`
	Reasoning        string                   `json:"reasoning,omitempty"`
	WorkingDir       string                   `json:"working_dir"`
	Workspace        string                   `json:"workspace"`
	Actor            string                   `json:"actor"`
	CodexSandbox     string                   `json:"codex_sandbox,omitempty"`
	CodexServiceTier string                   `json:"codex_service_tier,omitempty"`
	Repositories     []ConversationRepository `json:"repositories,omitempty"`
	TranscriptBytes  int                      `json:"transcript_bytes,omitempty"`
	MaxDuration      time.Duration            `json:"max_duration"`
}

// ConversationRepository captures Ticket-authoritative identity and routing.
// Paths are private execution data, never public profile metadata.
type ConversationRepository struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

type ConversationTurn struct {
	ID              string `json:"id"`
	State           string `json:"state"`
	Operator        string `json:"operator"`
	CancelOperator  string `json:"cancel_operator,omitempty"`
	CancelRequested bool   `json:"cancel_requested,omitempty"`
	Code            string `json:"code,omitempty"`
}

func (t ConversationTurn) Active() bool { return t.State == "accepted" || t.State == "running" }

// ConversationRecord is the durable semantic payload shared by transcript reads
// and future live events. Provider JSON and private process data do not belong here.
type ConversationRecord struct {
	ConversationID string                `json:"conversation_id"`
	TurnID         string                `json:"turn_id"`
	Revision       uint64                `json:"revision"`
	Kind           string                `json:"kind"`
	Message        *ConversationMessage  `json:"message,omitempty"`
	Turn           *ConversationTurn     `json:"turn,omitempty"`
	Activity       *ConversationActivity `json:"activity,omitempty"`
}

type ConversationMessage struct {
	ID   string `json:"message_id"`
	Role string `json:"role"`
	Text string `json:"text"`
}

type ConversationActivity struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type ConversationReceipt struct {
	Fingerprint    string    `json:"fingerprint"`
	ConversationID string    `json:"conversation_id"`
	TurnID         string    `json:"turn_id,omitempty"`
	Revision       uint64    `json:"revision"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type ConversationSnapshot struct {
	Version       int                            `json:"version"`
	Conversations map[string]Conversation        `json:"conversations"`
	Receipts      map[string]ConversationReceipt `json:"receipts"`
	Expired       map[string]uint64              `json:"expired"`
}

type ConversationStore struct{ store *Store }

func NewConversationStore(dir string) *ConversationStore {
	return &ConversationStore{store: NewAdministrative(dir)}
}

func (s *ConversationStore) Read(ctx context.Context) (ConversationSnapshot, error) {
	var snapshot ConversationSnapshot
	err := s.store.withLock(ctx, func() error {
		var err error
		snapshot, err = s.load()
		return err
	})
	return snapshot, err
}

// Update holds the existing state-directory lock only for a bounded local
// transaction. Callers must never execute a harness or publish events inside fn.
func (s *ConversationStore) Update(ctx context.Context, fn func(*ConversationSnapshot) error) error {
	return s.store.withLock(ctx, func() error {
		snapshot, err := s.load()
		if err != nil {
			return err
		}
		if err := fn(&snapshot); err != nil {
			return err
		}
		if err := validateConversations(snapshot); err != nil {
			return err
		}
		data, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		if len(data) > maxStateBytes {
			return ErrTooLarge
		}
		return writeStateAtomic(filepath.Join(s.store.dir, conversationFileName), data)
	})
}

func (s *ConversationStore) load() (ConversationSnapshot, error) {
	data, err := readBoundedStateFile(filepath.Join(s.store.dir, conversationFileName))
	if os.IsNotExist(err) {
		return ConversationSnapshot{Version: 1, Conversations: map[string]Conversation{}, Receipts: map[string]ConversationReceipt{}, Expired: map[string]uint64{}}, nil
	}
	if err != nil {
		return ConversationSnapshot{}, err
	}
	var snapshot ConversationSnapshot
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return snapshot, fmt.Errorf("%w: conversations: %v", ErrMalformed, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return snapshot, err
	}
	return snapshot, validateConversations(snapshot)
}

func validateConversations(s ConversationSnapshot) error {
	bad := func(reason string) error { return fmt.Errorf("%w: conversations: %s", ErrMalformed, reason) }
	if s.Version != 1 {
		return bad(fmt.Sprintf("unsupported version %d; expected 1", s.Version))
	}
	if s.Conversations == nil || s.Receipts == nil || s.Expired == nil {
		return bad("collections must be objects")
	}
	for id, c := range s.Conversations {
		if id == "" || c.ID != id || c.CreatedAt.IsZero() || c.Profile == "" || c.Operator == "" {
			return bad("invalid conversation metadata")
		}
		if c.State != "open" && c.State != "closed" {
			return bad("unsupported conversation state")
		}
		if (c.State == "closed") != (c.ClosedAt != nil) {
			return bad("closed state disagrees with closed_at")
		}
		if c.Execution.Harness != "codex" || c.Execution.Actor == "" || c.Execution.MaxDuration <= 0 {
			return bad("invalid captured execution settings")
		}
		turns := map[string]bool{}
		latest := map[string]ConversationTurn{}
		messages := map[string]bool{}
		active := 0
		for i, t := range c.Turns {
			if t.ID == "" || t.Operator == "" || turns[t.ID] || !validConversationTurn(t) {
				return bad("invalid or duplicate turn")
			}
			turns[t.ID] = true
			switch t.State {
			case "accepted", "running":
				active++
				if i != len(c.Turns)-1 {
					return bad("active turn must be latest")
				}
			case "completed", "failed", "cancelled", "interrupted":
			default:
				return bad("unsupported turn state")
			}
		}
		if active > 1 || (c.State == "closed" && active != 0) {
			return bad("conversation has conflicting active turns")
		}
		for i, r := range c.Records {
			if r.ConversationID != id || !turns[r.TurnID] || r.Revision != uint64(i+1) {
				return bad("record identity or revision mismatch")
			}
			switch r.Kind {
			case "message":
				if r.Message == nil || r.Turn != nil || r.Activity != nil || r.Message.ID == "" || messages[r.Message.ID] || (r.Message.Role != "operator" && r.Message.Role != "assistant") {
					return bad("invalid message record")
				}
				messages[r.Message.ID] = true
			case "turn":
				if r.Turn == nil || r.Message != nil || r.Activity != nil || r.Turn.ID != r.TurnID || !validConversationTurn(*r.Turn) {
					return bad("invalid turn record")
				}
				latest[r.TurnID] = *r.Turn
			case "activity":
				if r.Activity == nil || r.Message != nil || r.Turn != nil || r.Activity.Name == "" {
					return bad("invalid activity record")
				}
			default:
				return bad("unsupported record kind")
			}
		}
		for _, t := range c.Turns {
			if latest[t.ID] != t {
				return bad("latest lifecycle record disagrees with turn")
			}
		}
	}
	for id, earliest := range s.Expired {
		if id == "" || earliest == 0 {
			return bad("invalid history expiry boundary")
		}
		if _, live := s.Conversations[id]; live {
			return bad("expired conversation is still live")
		}
	}
	for key, receipt := range s.Receipts {
		if key == "" || receipt.Fingerprint == "" || receipt.ConversationID == "" || receipt.ExpiresAt.IsZero() {
			return bad("invalid request receipt")
		}
	}
	return nil
}

func validConversationTurn(t ConversationTurn) bool {
	if t.ID == "" || t.Operator == "" || len(t.Operator) > 256 || len(t.CancelOperator) > 256 || len(t.Code) > 128 {
		return false
	}
	if t.CancelRequested != (t.CancelOperator != "") {
		return false
	}
	switch t.State {
	case "accepted", "running", "completed", "failed", "cancelled", "interrupted":
		return true
	}
	return false
}
