package orc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
)

// ConversationError contains a safe classification, never transcript text or
// provider diagnostics. Accepted mutations are recovered using their request ID.
type ConversationError struct {
	Code             string
	EarliestRevision uint64
}

func (e *ConversationError) Error() string { return e.Code }
func conversationError(code string) error  { return &ConversationError{Code: code} }

const ConversationRequestWindow = 24 * time.Hour

const (
	maxConversationAttributionBytes = 256 // encoded JSON string content
	maxConversationCodeBytes        = 128 // encoded JSON string content
	maxConversationSessionBytes     = 256 // provider ID input bytes
)

// ConversationLimits bound the entire instance. The file additionally has the
// state's hard size bound; reaching either bound rejects without launching.
type ConversationLimits struct {
	Count, ConcurrentTurns, TextBytes, RecordBytes, TranscriptBytes, Receipts int
	ClosedAge                                                                 time.Duration
}

func DefaultConversationLimits() ConversationLimits {
	return ConversationLimits{Count: 16, ConcurrentTurns: 2, TextBytes: 16 << 10, RecordBytes: 20 << 10, TranscriptBytes: 48 << 10, Receipts: 512, ClosedAge: 7 * 24 * time.Hour}
}

type Conversations struct {
	Store       *state.ConversationStore
	Limits      ConversationLimits
	publication sync.Mutex
	publish     func(state.ConversationRecord)
}

// SetPublisher installs the nonblocking committed-record sink before API admission.
func (s *Conversations) SetPublisher(publish func(state.ConversationRecord)) {
	s.publication.Lock()
	defer s.publication.Unlock()
	s.publish = publish
}

// Serialize commit and notification order without holding the durable store lock
// during publication. The sink must never wait for subscribers or external work.
func (s *Conversations) update(ctx context.Context, fn func(*state.ConversationSnapshot) error) error {
	s.publication.Lock()
	defer s.publication.Unlock()
	var records []state.ConversationRecord
	err := s.Store.Update(ctx, func(snapshot *state.ConversationSnapshot) error {
		before := make(map[string]int, len(snapshot.Conversations))
		for id, c := range snapshot.Conversations {
			before[id] = len(c.Records)
		}
		if err := fn(snapshot); err != nil {
			return err
		}
		for id, c := range snapshot.Conversations {
			records = append(records, c.Records[before[id]:]...)
		}
		return nil
	})
	if err == nil && s.publish != nil {
		for _, r := range records {
			s.publish(r)
		}
	}
	return err
}

func NewConversations(store *state.ConversationStore, limits ConversationLimits) (*Conversations, error) {
	if store == nil || limits.Count <= 0 || limits.ConcurrentTurns <= 0 || limits.TextBytes <= 0 || limits.RecordBytes < 1024 || limits.TranscriptBytes < limits.RecordBytes || limits.Receipts <= 0 || limits.ClosedAge <= 0 {
		return nil, conversationError("invalid_conversation_limits")
	}
	return &Conversations{Store: store, Limits: limits}, nil
}

// NewConversationRequestID returns UTC YYYYMMDDTHHMMSSZ-<32 hex digits>.
// New requests are valid for 24 hours with at most five minutes of clock skew.
// Retained receipts deduplicate beyond that window; pruned IDs cannot execute.
func NewConversationRequestID(now time.Time) string {
	return now.UTC().Format("20060102T150405Z") + "-" + conversationID()
}
func conversationID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(id[:])
}
func requestExpiry(id string, now time.Time) (time.Time, error) {
	if len(id) != 49 || id[16] != '-' {
		return time.Time{}, conversationError("invalid_request_id")
	}
	stamp, err := time.Parse("20060102T150405Z", id[:16])
	if err != nil {
		return time.Time{}, conversationError("invalid_request_id")
	}
	if _, err := hex.DecodeString(id[17:]); err != nil {
		return time.Time{}, conversationError("invalid_request_id")
	}
	expiry := stamp.Add(ConversationRequestWindow)
	if stamp.After(now.Add(5*time.Minute)) || !now.Before(expiry) {
		return time.Time{}, conversationError("request_id_expired")
	}
	return expiry, nil
}
func fingerprint(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// reserve measures a conservative complete lifecycle using the same JSON and
// canonical records as persistence. Each accepted turn reserves running, cancel,
// terminal, retained-session, and close metadata before launch or more output.
// No lifecycle transition spends space promised to another conversation.
func (s *Conversations) reserve(snapshot *state.ConversationSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	size := len(data)
	for _, c := range snapshot.Conversations {
		projected := c
		projected.Turns = append([]state.ConversationTurn(nil), c.Turns...)
		projected.Records = append([]state.ConversationRecord(nil), c.Records...)
		if i, active := activeTurn(projected); active {
			t := projected.Turns[i]
			if t.State == "accepted" && !t.CancelRequested {
				t.State = "running"
				appendTurn(&projected, t)
			}
			if !t.CancelRequested {
				t.CancelRequested = true
				t.CancelOperator = strings.Repeat("x", maxConversationAttributionBytes) // maximum encoded attribution
				appendTurn(&projected, t)
			}
			t.State = "interrupted"                                // longest terminal state
			t.Code = strings.Repeat("x", maxConversationCodeBytes) // maximum encoded outcome code
			projected.Turns[i] = t
			appendTurn(&projected, t)
			// Provider IDs are bounded by input bytes; every byte can escape to six
			// JSON bytes. SessionUnavailable adds a field when true (omitempty).
			projected.SessionID = strings.Repeat("\x01", maxConversationSessionBytes)
			projected.SessionUnavailable = true
		}
		if projected.State == "open" {
			closed := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
			projected.State = "closed"
			projected.ClosedAt = &closed
			projected.ClosedBy = strings.Repeat("x", maxConversationAttributionBytes)
		}
		// This also checks lifecycle variants before accepting a turn. Valid stop
		// intent and terminal records must always fit the advertised record limit.
		for _, record := range projected.Records {
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if len(encoded) > s.Limits.RecordBytes {
				return conversationError("capacity_exceeded")
			}
		}
		projectedSize := conversationSize(projected)
		if projectedSize > s.transcriptLimit(c) {
			return conversationError("capacity_exceeded")
		}
		size += projectedSize - conversationSize(c)
	}
	if size > state.ConversationStateBytes {
		return conversationError("capacity_exceeded")
	}
	return nil
}

func conversationSize(c state.Conversation) int { data, _ := json.Marshal(c); return len(data) }
func activeTurn(c state.Conversation) (int, bool) {
	for i := range c.Turns {
		if c.Turns[i].Active() {
			return i, true
		}
	}
	return 0, false
}
func appendRecord(c *state.Conversation, r state.ConversationRecord) {
	r.ConversationID = c.ID
	r.Revision = uint64(len(c.Records) + 1)
	c.Records = append(c.Records, r)
}
func appendTurn(c *state.Conversation, t state.ConversationTurn) {
	appendRecord(c, state.ConversationRecord{TurnID: t.ID, Kind: "turn", Turn: &t})
}

// Creation is an internal primitive. Only server-resolved execution settings
// may reach it; public callers use ConversationProfileCreate.
type ConversationCreate struct {
	RequestID, Profile, Operator string
	RepositoryIDs                []string
	Execution                    state.ConversationExecution
}

func (s *Conversations) Create(ctx context.Context, req ConversationCreate, now time.Time) (state.ConversationReceipt, error) {
	return s.create(ctx, req, now, fingerprint(req))
}

func (s *Conversations) create(ctx context.Context, req ConversationCreate, now time.Time, fp string) (state.ConversationReceipt, error) {
	var receipt state.ConversationReceipt
	err := s.update(ctx, func(snapshot *state.ConversationSnapshot) error {
		key := "create\x00" + req.RequestID
		if old, ok := snapshot.Receipts[key]; ok {
			if old.Fingerprint != fp {
				return conversationError("request_id_conflict")
			}
			receipt = old
			return nil
		}
		expiry, err := requestExpiry(req.RequestID, now)
		if err != nil {
			return err
		}
		if req.Profile == "" || !validAttribution(req.Operator) || req.Execution.Harness != "codex" || req.Execution.Actor == "" || req.Execution.MaxDuration <= 0 {
			return conversationError("profile_unavailable")
		}
		s.prune(snapshot, now)
		if len(snapshot.Conversations) >= s.Limits.Count || len(snapshot.Receipts) >= s.Limits.Receipts {
			return conversationError("capacity_exceeded")
		}
		c := state.Conversation{ID: conversationID(), State: "open", Profile: req.Profile, RepositoryIDs: req.RepositoryIDs, Operator: req.Operator, Execution: req.Execution, CreatedAt: now}
		snapshot.Conversations[c.ID] = c
		receipt = state.ConversationReceipt{Fingerprint: fp, ConversationID: c.ID, ExpiresAt: expiry}
		snapshot.Receipts[key] = receipt
		return s.reserve(snapshot)
	})
	return receipt, err
}

// Audit attribution is bounded in encoded JSON bytes as well as input bytes.
// This keeps the same maximum for ordinary 256-byte names and escaping-heavy
// values, and guarantees cancellation metadata fits the reserved record budget.
func validAttribution(value string) bool {
	return strings.TrimSpace(value) != "" && len(value) <= maxConversationAttributionBytes && encodedStringBytes(value) <= maxConversationAttributionBytes
}
func encodedStringBytes(value string) int { data, _ := json.Marshal(value); return len(data) - 2 }

type ConversationSubmit struct {
	RequestID string `json:"request_id"`
	Text      string `json:"text"`
	Operator  string `json:"operator"`
}

// Accept atomically persists the request receipt, operator message, and active
// turn. The bool reports a new acceptance; duplicate receipts must never launch.
func (s *Conversations) Accept(ctx context.Context, id string, req ConversationSubmit, now time.Time) (receipt state.ConversationReceipt, fresh bool, err error) {
	err = s.update(ctx, func(snapshot *state.ConversationSnapshot) error {
		key := id + "\x00" + req.RequestID
		fp := fingerprint(req)
		if old, ok := snapshot.Receipts[key]; ok {
			if old.Fingerprint != fp {
				return conversationError("request_id_conflict")
			}
			receipt = old
			return nil
		}
		expiry, err := requestExpiry(req.RequestID, now)
		if err != nil {
			return err
		}
		c, ok := snapshot.Conversations[id]
		if !ok {
			return conversationError("conversation_not_found")
		}
		if c.State == "closed" {
			return conversationError("conversation_closed")
		}
		if _, active := activeTurn(c); active {
			return conversationError("turn_active")
		}
		if c.SessionUnavailable {
			return conversationError("session_unavailable")
		}
		if !validAttribution(req.Operator) || strings.TrimSpace(req.Text) == "" || len(req.Text) > s.Limits.TextBytes {
			return conversationError("invalid_turn")
		}
		active := 0
		for _, other := range snapshot.Conversations {
			if _, ok := activeTurn(other); ok {
				active++
			}
		}
		if active >= s.Limits.ConcurrentTurns || len(snapshot.Receipts) >= s.Limits.Receipts {
			return conversationError("capacity_exceeded")
		}
		turn := state.ConversationTurn{ID: conversationID(), State: "accepted", Operator: req.Operator}
		c.Turns = append(c.Turns, turn)
		appendTurn(&c, turn)
		record := state.ConversationRecord{TurnID: turn.ID, Kind: "message", Message: &state.ConversationMessage{ID: conversationID(), Role: "operator", Text: req.Text}}
		appendRecord(&c, record)
		snapshot.Conversations[id] = c
		receipt = state.ConversationReceipt{Fingerprint: fp, ConversationID: id, TurnID: turn.ID, Revision: uint64(len(c.Records)), ExpiresAt: expiry}
		snapshot.Receipts[key] = receipt
		fresh = true
		return s.reserve(snapshot)
	})
	if err != nil {
		fresh = false
	}
	return
}

func (s *Conversations) Get(ctx context.Context, id string) (state.Conversation, error) {
	snapshot, err := s.Store.Read(ctx)
	if err != nil {
		return state.Conversation{}, err
	}
	c, ok := snapshot.Conversations[id]
	if !ok {
		if earliest, expired := snapshot.Expired[id]; expired {
			return c, &ConversationError{Code: "history_expired", EarliestRevision: earliest}
		}
		return c, conversationError("conversation_not_found")
	}
	return c, nil
}

func (s *Conversations) updateTurn(ctx context.Context, id, turn string, reserveOutput bool, fn func(*state.Conversation, *state.ConversationTurn) error) error {
	return s.update(ctx, func(snapshot *state.ConversationSnapshot) error {
		c, ok := snapshot.Conversations[id]
		if !ok {
			return conversationError("conversation_not_found")
		}
		for i := range c.Turns {
			if c.Turns[i].ID != turn {
				continue
			}
			if err := fn(&c, &c.Turns[i]); err != nil {
				return err
			}
			snapshot.Conversations[id] = c
			if err := s.reserve(snapshot); err != nil {
				if reserveOutput && IsConversationError(err, "capacity_exceeded") {
					return conversationError("transcript_limit")
				}
				return err
			}
			return nil
		}
		return conversationError("turn_not_found")
	})
}

func (s *Conversations) Running(ctx context.Context, id, turn string) error {
	return s.updateTurn(ctx, id, turn, false, func(c *state.Conversation, t *state.ConversationTurn) error {
		if t.State != "accepted" || t.CancelRequested {
			return conversationError("turn_not_launchable")
		}
		t.State = "running"
		appendTurn(c, *t)
		return nil
	})
}

func (s *Conversations) RequestCancel(ctx context.Context, id, turn, operator string) error {
	if !validAttribution(operator) {
		return conversationError("invalid_operator")
	}
	return s.updateTurn(ctx, id, turn, false, func(c *state.Conversation, t *state.ConversationTurn) error {
		if !t.Active() || t.CancelRequested {
			return nil
		}
		t.CancelRequested = true
		t.CancelOperator = operator
		// Repeated cancellation does not consume transcript space. Space for this
		// record and the terminal record was reserved at acceptance/output time.
		appendTurn(c, *t)
		return nil
	})
}

// FinishStopped is called only after supervisor confirms that execution has
// stopped, including recovery. Session IDs are private durable data.
func (s *Conversations) FinishStopped(ctx context.Context, id, turn, outcome, code, session string, reusable bool) error {
	switch outcome {
	case "completed", "failed", "cancelled", "interrupted":
	default:
		return conversationError("invalid_turn_outcome")
	}
	if len(code) > maxConversationCodeBytes || encodedStringBytes(code) > maxConversationCodeBytes || len(session) > maxConversationSessionBytes {
		return conversationError("invalid_turn_outcome")
	}
	return s.updateTurn(ctx, id, turn, false, func(c *state.Conversation, t *state.ConversationTurn) error {
		if !t.Active() {
			return nil
		}
		if t.CancelRequested && outcome != "interrupted" {
			outcome = "cancelled"
		}
		t.State = outcome
		t.Code = code
		c.SessionUnavailable = !reusable
		if session != "" {
			c.SessionID = session
		}
		appendTurn(c, *t)
		return nil
	})
}

// ConversationRunOutcome contains process facts. Orc owns their classification;
// supervisor supplies them only after the child-stop barrier.
type ConversationRunOutcome struct {
	Result                              harness.RunResult
	RunError, OutputError, ContextError error
	Interrupted                         bool
}

func (s *Conversations) FinishExecutionStopped(ctx context.Context, id, turn string, run ConversationRunOutcome) error {
	c, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	outcome, code := "completed", ""
	reusable := run.RunError == nil && run.Result.SessionOutcome != harness.SessionInvalidated && run.Result.SessionID != ""
	if run.RunError != nil || run.Result.ExitCode != 0 {
		outcome, code, reusable = "failed", "execution_failed", false
	}
	if run.Result.SessionOutcome == harness.SessionInvalidated || (run.RunError == nil && run.Result.SessionID == "") || (c.SessionID != "" && run.Result.SessionID != c.SessionID) || len(run.Result.SessionID) > maxConversationSessionBytes {
		outcome, code, reusable = "failed", "session_unavailable", false
	}
	if run.OutputError != nil {
		code = "output_persistence_failed"
		if IsConversationError(run.OutputError, "transcript_limit") {
			code = "transcript_limit"
		}
		outcome, reusable = "failed", false
	} else if run.ContextError != nil {
		outcome, code, reusable = "failed", "duration_exceeded", false
	}
	if run.Interrupted {
		outcome, code, reusable = "interrupted", "daemon_stopped", false
	}
	session := run.Result.SessionID
	if len(session) > maxConversationSessionBytes {
		session = ""
	}
	return s.FinishStopped(ctx, id, turn, outcome, code, session, reusable)
}

// Append persists a complete semantic record. Live publishers read committed
// revisions after this returns; they must never run under the state lock.
func (s *Conversations) Append(ctx context.Context, id, turn string, record state.ConversationRecord) error {
	return s.updateTurn(ctx, id, turn, true, func(c *state.Conversation, t *state.ConversationTurn) error {
		if t.State != "running" {
			return conversationError("turn_not_running")
		}
		record.TurnID = turn
		record.ConversationID = c.ID
		record.Revision = uint64(len(c.Records) + 1)
		if record.Kind == "message" && record.Message != nil && record.Message.Role == "assistant" {
			if record.Message.ID == "" {
				record.Message.ID = conversationID()
			}
		} else if record.Kind != "activity" || record.Activity == nil {
			return conversationError("invalid_record")
		}
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if len(data) > s.Limits.RecordBytes {
			return conversationError("transcript_limit")
		}
		appendRecord(c, record)
		return nil
	})
}

func (s *Conversations) Close(ctx context.Context, id, operator string, now time.Time) error {
	if !validAttribution(operator) {
		return conversationError("invalid_operator")
	}
	return s.update(ctx, func(snapshot *state.ConversationSnapshot) error {
		c, ok := snapshot.Conversations[id]
		if !ok {
			return conversationError("conversation_not_found")
		}
		if _, active := activeTurn(c); active {
			return conversationError("turn_active")
		}
		if c.State == "closed" {
			return nil
		}
		now = now.UTC()
		c.State = "closed"
		c.ClosedAt = &now
		c.ClosedBy = operator
		snapshot.Conversations[id] = c
		return s.reserve(snapshot)
	})
}

func (s *Conversations) Prune(ctx context.Context, now time.Time) error {
	return s.update(ctx, func(snapshot *state.ConversationSnapshot) error { s.prune(snapshot, now); return nil })
}
func (s *Conversations) prune(snapshot *state.ConversationSnapshot, now time.Time) {
	for id, c := range snapshot.Conversations {
		if c.ClosedAt != nil && !now.Before(c.ClosedAt.Add(s.Limits.ClosedAge)) {
			snapshot.Expired[id] = uint64(len(c.Records) + 1)
			delete(snapshot.Conversations, id)
		}
	}
	for key, r := range snapshot.Receipts {
		if _, exists := snapshot.Conversations[r.ConversationID]; !exists && !now.Before(r.ExpiresAt) {
			delete(snapshot.Receipts, key)
		}
	}
	retained := map[string]bool{}
	for _, r := range snapshot.Receipts {
		retained[r.ConversationID] = true
	}
	for id := range snapshot.Expired {
		if !retained[id] {
			delete(snapshot.Expired, id)
		}
	}
}

type ConversationTranscript struct {
	Records                                      []state.ConversationRecord
	NextAfter, ThroughRevision, EarliestRevision uint64
	HasMore                                      bool
	Turn                                         *state.ConversationTurn
}

func (s *Conversations) Transcript(ctx context.Context, id string, after uint64, limit int) (ConversationTranscript, error) {
	c, err := s.Get(ctx, id)
	if err != nil {
		return ConversationTranscript{}, err
	}
	if limit <= 0 || limit > 128 {
		return ConversationTranscript{}, conversationError("invalid_page_limit")
	}
	p := ConversationTranscript{Records: []state.ConversationRecord{}, NextAfter: after, ThroughRevision: uint64(len(c.Records)), EarliestRevision: 1}
	if after > p.ThroughRevision {
		return p, &ConversationError{Code: "history_expired", EarliestRevision: p.EarliestRevision}
	}
	for _, r := range c.Records {
		if r.Revision > after && len(p.Records) < limit {
			p.Records = append(p.Records, r)
			p.NextAfter = r.Revision
		}
	}
	p.HasMore = p.NextAfter < p.ThroughRevision
	if len(c.Turns) > 0 {
		t := c.Turns[len(c.Turns)-1]
		p.Turn = &t
	}
	return p, nil
}

func IsConversationError(err error, code string) bool {
	var e *ConversationError
	return errors.As(err, &e) && e.Code == code
}

func (s *Conversations) transcriptLimit(c state.Conversation) int {
	if c.Execution.TranscriptBytes > 0 {
		return min(s.Limits.TranscriptBytes, c.Execution.TranscriptBytes)
	}
	return s.Limits.TranscriptBytes
}
