package daemon

import (
	"encoding/base64"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

const conversationPageSize = 128

// ConversationAPI binds the already configured policy and process lifecycle.
// No execution settings are accepted from HTTP callers.
type ConversationAPI struct {
	Policy   *orc.Conversations
	Profiles *orc.ConversationProfiles
	Manager  *supervisor.ConversationManager
}

type ConversationCapabilities struct {
	SchemaVersion              int                               `json:"schema_version"`
	Enabled                    bool                              `json:"enabled"`
	Stream                     bool                              `json:"stream"`
	Profiles                   []orc.ConversationProfileMetadata `json:"profiles"`
	OutputGranularity          string                            `json:"output_granularity"`
	TextBytes                  int                               `json:"request_text_bytes"`
	RecordBytes                int                               `json:"record_bytes"`
	PageSize                   int                               `json:"transcript_page_size"`
	Count                      int                               `json:"conversation_count"`
	TranscriptBytes            int                               `json:"transcript_bytes"`
	ConcurrentTurns            int                               `json:"concurrent_turns"`
	RequestIDFormat            string                            `json:"request_id_format"`
	RequestIDWindowSeconds     int                               `json:"request_id_window_seconds"`
	RequestIDFutureSkewSeconds int                               `json:"request_id_future_skew_seconds"`
	WatermarkSeconds           float64                           `json:"watermark_seconds"`
}

func (s *Server) conversationCapabilities() *ConversationCapabilities {
	api := s.config.Conversations
	if api == nil {
		return nil
	}
	l := api.Policy.Limits
	profiles := api.Profiles.Metadata()
	return &ConversationCapabilities{SchemaVersion: 1, Enabled: len(profiles) > 0, Stream: true, Profiles: profiles, OutputGranularity: "complete_messages", TextBytes: l.TextBytes, RecordBytes: l.RecordBytes, PageSize: conversationPageSize, Count: l.Count, TranscriptBytes: l.TranscriptBytes, ConcurrentTurns: l.ConcurrentTurns, RequestIDFormat: "UTC YYYYMMDDTHHMMSSZ-<32 hex digits>", RequestIDWindowSeconds: int(orc.ConversationRequestWindow / time.Second), RequestIDFutureSkewSeconds: 300, WatermarkSeconds: s.config.EventHeartbeat.Seconds()}
}

// SetConversations is called before Start, once lifecycle and output are wired.
func (s *Server) SetConversations(api *ConversationAPI) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopped {
		return errors.New("daemon already started")
	}
	if api == nil || api.Policy == nil || api.Profiles == nil || api.Manager == nil {
		return errors.New("incomplete conversation API")
	}
	s.config.Conversations = api
	api.Policy.SetPublisher(func(r state.ConversationRecord) {
		s.events.publish(Event{Type: "conversation." + r.Kind, Conversation: &r})
	})
	return nil
}

type ConversationMetadata struct {
	ID                 string                  `json:"id"`
	State              string                  `json:"state"`
	Profile            string                  `json:"profile"`
	RepositoryIDs      []string                `json:"repository_ids"`
	Operator           string                  `json:"operator"`
	CreatedAt          time.Time               `json:"created_at"`
	ClosedAt           *time.Time              `json:"closed_at,omitempty"`
	Revision           uint64                  `json:"revision"`
	EarliestRevision   uint64                  `json:"earliest_retained_revision"`
	Turn               *state.ConversationTurn `json:"turn,omitempty"`
	SessionUnavailable bool                    `json:"session_unavailable,omitempty"`
}

func conversationMetadata(c state.Conversation) ConversationMetadata {
	m := ConversationMetadata{ID: c.ID, State: c.State, Profile: c.Profile, RepositoryIDs: c.RepositoryIDs, Operator: c.Operator, CreatedAt: c.CreatedAt, ClosedAt: c.ClosedAt, Revision: uint64(len(c.Records)), EarliestRevision: 1, SessionUnavailable: c.SessionUnavailable}
	if len(c.Turns) > 0 {
		t := c.Turns[len(c.Turns)-1]
		m.Turn = &t
	}
	return m
}

type conversationReceipt struct {
	ConversationID string `json:"conversation_id"`
	TurnID         string `json:"turn_id,omitempty"`
	State          string `json:"state"`
	Revision       uint64 `json:"revision"`
}

func writeConversationError(w http.ResponseWriter, err error, mutation bool) {
	var lifecycle *supervisor.LifecycleError
	applied := errors.As(err, &lifecycle) && lifecycle.Applied
	message := "conversation operation rejected"
	if applied {
		message = "mutation was applied; reconcile the original resource"
	}
	var ce *orc.ConversationError
	if errors.As(err, &ce) {
		status := http.StatusConflict
		switch ce.Code {
		case "conversation_not_found", "turn_not_found":
			status = http.StatusNotFound
		case "history_expired":
			status = http.StatusGone
		case "capability_unavailable", "profile_unavailable":
			status = http.StatusUnprocessableEntity
		case "invalid_request_id", "request_id_expired", "invalid_turn", "invalid_operator", "invalid_page_limit", "repository_out_of_scope":
			status = http.StatusBadRequest
		case "capacity_exceeded":
			status = http.StatusTooManyRequests
		}
		writeJSON(w, status, map[string]any{"error": map[string]any{"code": ce.Code, "message": message, "mutation_applied": applied, "earliest_retained_revision": ce.EarliestRevision}})
		return
	}
	if applied {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"code": "conversation_unavailable", "message": message, "mutation_applied": true}})
		return
	}
	// A storage/control failure may follow a committed mutation. Clients reconcile
	// using the same request ID; no provider or filesystem diagnostics are public.
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"code": "conversation_unavailable", "message": "reconcile using the original request ID or resource", "outcome_unknown": mutation}})
}

func (s *Server) handleConversations(w http.ResponseWriter, r *http.Request) {
	api := s.config.Conversations
	if api == nil {
		writeError(w, 422, "capability_unavailable", "conversations unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	method := r.Method
	if path == "conversation-profiles" {
		if method != http.MethodGet {
			writeError(w, 405, "method_not_allowed", "method not allowed")
			return
		}
		writeJSON(w, 200, map[string]any{"profiles": api.Profiles.Metadata()})
		return
	}
	parts := strings.Split(path, "/")
	valid := len(parts) == 1 || (len(parts) == 2 && parts[1] != "") || (len(parts) == 3 && (parts[2] == "transcript" || parts[2] == "turns" || parts[2] == "close")) || (len(parts) == 5 && parts[2] == "turns" && parts[3] != "" && parts[4] == "cancel")
	if !valid {
		writeError(w, 404, "not_found", "endpoint not found")
		return
	}
	read := len(parts) <= 2 || (len(parts) == 3 && parts[2] == "transcript")
	if method != http.MethodPost && !(method == http.MethodGet && read) || (method == http.MethodPost && (len(parts) == 2 || (len(parts) == 3 && parts[2] == "transcript"))) {
		writeError(w, 405, "method_not_allowed", "method not allowed")
		return
	}
	if method == http.MethodGet {
		if len(parts) == 1 {
			s.listConversations(w, r)
			return
		}
		if len(parts) == 2 {
			c, err := api.Policy.Get(r.Context(), parts[1])
			if err != nil {
				writeConversationError(w, err, false)
				return
			}
			writeJSON(w, 200, conversationMetadata(c))
			return
		}
		limit, err := conversationLimit(r)
		if err != nil {
			writeConversationError(w, err, false)
			return
		}
		after := uint64(0)
		if raw := r.URL.Query().Get("after"); raw != "" {
			after, err = strconv.ParseUint(raw, 10, 64)
			if err != nil {
				writeError(w, 400, "invalid_request", "invalid revision")
				return
			}
		}
		p, err := api.Policy.Transcript(r.Context(), parts[1], after, limit)
		if err != nil {
			writeConversationError(w, err, false)
			return
		}
		writeJSON(w, 200, map[string]any{"records": p.Records, "next_after": p.NextAfter, "has_more": p.HasMore, "through_revision": p.ThroughRevision, "earliest_retained_revision": p.EarliestRevision, "turn": p.Turn})
		return
	}
	// Accepted work has a daemon-owned lifetime, independent of the HTTP request.
	ctx, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, 409, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	if len(parts) == 1 {
		var req orc.ConversationProfileCreate
		if err := DecodeJSON(w, r, &req); err != nil {
			writeRequestDecodeError(w, err)
			return
		}
		receipt, err := api.Profiles.Create(ctx, api.Policy, req, time.Now())
		if err != nil {
			writeConversationError(w, err, true)
			return
		}
		c, err := api.Policy.Get(ctx, receipt.ConversationID)
		if err != nil {
			status, code := http.StatusServiceUnavailable, "accepted_read_unavailable"
			if orc.IsConversationError(err, "history_expired") || orc.IsConversationError(err, "conversation_not_found") {
				status, code = http.StatusGone, "history_expired"
			}
			writeJSON(w, status, map[string]any{"error": map[string]any{"code": code, "message": "conversation was accepted; reconcile the original resource", "mutation_applied": true}, "receipt": conversationReceipt{ConversationID: receipt.ConversationID, State: "accepted"}})
			return
		}
		writeJSON(w, 201, conversationMetadata(c))
		return
	}
	if len(parts) == 3 && parts[2] == "turns" {
		var req orc.ConversationSubmit
		if err := DecodeJSON(w, r, &req); err != nil {
			writeRequestDecodeError(w, err)
			return
		}
		receipt, err := api.Manager.Submit(ctx, parts[1], req, time.Now())
		if err != nil {
			writeConversationError(w, err, true)
			return
		}
		writeJSON(w, 202, conversationReceipt{ConversationID: receipt.ConversationID, TurnID: receipt.TurnID, State: "accepted", Revision: receipt.Revision})
		return
	}
	var req struct {
		Operator string `json:"operator"`
	}
	if err := DecodeJSON(w, r, &req); err != nil {
		writeRequestDecodeError(w, err)
		return
	}
	if len(parts) == 3 && parts[2] == "close" {
		if err := api.Manager.Close(ctx, parts[1], req.Operator, time.Now()); err != nil {
			writeConversationError(w, err, true)
			return
		}
		c, err := api.Policy.Get(ctx, parts[1])
		if err != nil {
			writeConversationError(w, &supervisor.LifecycleError{Applied: true, Cause: err}, true)
			return
		}
		writeJSON(w, 200, conversationMetadata(c))
		return
	}
	stopped, err := api.Manager.Cancel(ctx, parts[1], parts[3], req.Operator)
	if err != nil {
		writeConversationError(w, err, true)
		return
	}
	c, err := api.Policy.Get(ctx, parts[1])
	if err != nil {
		writeConversationError(w, &supervisor.LifecycleError{Applied: true, Cause: err}, true)
		return
	}
	for _, t := range c.Turns {
		if t.ID == parts[3] {
			writeJSON(w, 200, map[string]any{"conversation_id": c.ID, "turn": t, "stop_confirmed": stopped, "revision": len(c.Records)})
			return
		}
	}
}

func conversationLimit(r *http.Request) (int, error) {
	if r.URL.Query().Get("limit") == "" {
		return 32, nil
	}
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 || n > conversationPageSize {
		return 0, &orc.ConversationError{Code: "invalid_page_limit"}
	}
	return n, nil
}
func (s *Server) listConversations(w http.ResponseWriter, r *http.Request) {
	limit, err := conversationLimit(r)
	if err != nil {
		writeConversationError(w, err, false)
		return
	}
	cursor := ""
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		if len(raw) != 43 {
			writeError(w, 400, "invalid_request", "invalid cursor")
			return
		}
		decoded, e := base64.RawURLEncoding.DecodeString(raw)
		if e != nil || len(decoded) != 32 {
			writeError(w, 400, "invalid_request", "invalid cursor")
			return
		}
		cursor = string(decoded)
	}
	snapshot, err := s.config.Conversations.Policy.Store.Read(r.Context())
	if err != nil {
		writeConversationError(w, err, false)
		return
	}
	ids := make([]string, 0, len(snapshot.Conversations))
	for id := range snapshot.Conversations {
		if id > cursor {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	items := make([]ConversationMetadata, 0, min(limit, len(ids)))
	for _, id := range ids[:min(limit, len(ids))] {
		items = append(items, conversationMetadata(snapshot.Conversations[id]))
	}
	next := ""
	if len(ids) > limit {
		next = base64.RawURLEncoding.EncodeToString([]byte(ids[limit-1]))
	}
	writeJSON(w, 200, map[string]any{"conversations": items, "next_cursor": next, "has_more": next != ""})
}
