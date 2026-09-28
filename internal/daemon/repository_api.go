package daemon

import (
	"context"
	"fmt"
)

// RepositoryTicketQuery is the bounded read-only query accepted by the
// repository gateway. Each filter class combines with AND; repeated states
// select their union.
type RepositoryTicketQuery struct {
	Search   string
	States   []string
	Priority *int
	Assignee string
	Tags     []string
	Limit    int
	Offset   int
}

// RepositoryTicket is the stable ticket projection exposed to UI clients.
// It intentionally contains no raw TASK.md sections or private metadata.
type RepositoryTicket struct {
	ID            string                             `json:"id"`
	Archived      bool                               `json:"archived,omitempty"`
	Title         string                             `json:"title,omitempty"`
	State         string                             `json:"state"`
	Priority      int                                `json:"priority,omitempty"`
	Assignee      string                             `json:"assignee,omitempty"`
	BlockedReason string                             `json:"blocked_reason,omitempty"`
	Parent        string                             `json:"parent,omitempty"`
	Tags          []string                           `json:"tags,omitempty"`
	DependsOn     []string                           `json:"depends_on,omitempty"`
	Sections      map[string]RepositoryTicketSection `json:"sections,omitempty"`
	Available     []string                           `json:"available_sections,omitempty"`
	Readiness     *RepositoryTicketReadiness         `json:"readiness,omitempty"`
	Created       string                             `json:"created,omitempty"`
	Modified      string                             `json:"modified,omitempty"`
	Body          *string                            `json:"body,omitempty"`
	BodyTruncated *bool                              `json:"body_truncated,omitempty"`
	Truncated     bool                               `json:"truncated,omitempty"`
}

// RepositoryTicketDetail is a flat ticket detail projection with the
// repository identity that scopes the ticket ID.
type RepositoryTicketDetail struct {
	RepositoryID  string `json:"repository_id"`
	RepositoryKey string `json:"repository_key,omitempty"`
	RepositoryTicket
}

// RepositoryTicketReadiness is Ticket's authoritative readiness result.
type RepositoryTicketReadiness struct {
	Ready    bool                               `json:"ready"`
	Blockers []RepositoryTicketReadinessBlocker `json:"blockers,omitempty"`
}

// RepositoryTicketReadinessBlocker describes one Ticket-computed blocker.
type RepositoryTicketReadinessBlocker struct {
	Code    string `json:"code"`
	ID      string `json:"id,omitempty"`
	Message string `json:"message,omitempty"`
}

// RepositoryTicketSection is a bounded section of Ticket detail text.
type RepositoryTicketSection struct {
	Text      string `json:"text"`
	Truncated bool   `json:"truncated,omitempty"`
}

// RepositoryTicketList is the bounded list/search response keyed to one Orc
// repository resource.
type RepositoryTicketList struct {
	RepositoryID  string             `json:"repository_id"`
	RepositoryKey string             `json:"repository_key,omitempty"`
	Items         []RepositoryTicket `json:"items"`
	More          bool               `json:"more"`
}

// RepositoryTicketMutationRequest is the bounded input shared by workflow
// operations. Actor is supplied by the UI and is bound to the Ticket child
// process; Orc never infers a human actor from a worker.
type RepositoryTicketMutationRequest struct {
	Actor   string `json:"actor"`
	Handoff string `json:"handoff,omitempty"`
	Message string `json:"message,omitempty"`
	Outcome string `json:"outcome,omitempty"`
}

// RepositoryTicketCreateRequest is the typed create surface used by the UI.
type RepositoryTicketCreateRequest struct {
	Actor     string            `json:"actor"`
	Title     string            `json:"title"`
	Priority  *int              `json:"priority,omitempty"`
	Tags      []string          `json:"tags,omitempty"`
	Parent    string            `json:"parent,omitempty"`
	DependsOn []string          `json:"depends_on,omitempty"`
	Sections  map[string]string `json:"sections,omitempty"`
}

// RepositoryTicketUpdateRequest is the typed metadata/section update surface
// used by the UI. Set contains only Ticket's documented scalar fields.
type RepositoryTicketUpdateRequest struct {
	Actor    string            `json:"actor"`
	Set      map[string]any    `json:"set,omitempty"`
	Sections map[string]string `json:"sections,omitempty"`
}

// RepositoryTicketMutation is the authoritative post-mutation projection.
// The ticket is reread from Ticket after the command completes.
type RepositoryTicketMutation struct {
	RepositoryID  string           `json:"repository_id"`
	RepositoryKey string           `json:"repository_key,omitempty"`
	Operation     string           `json:"operation"`
	Changed       bool             `json:"changed"`
	Ticket        RepositoryTicket `json:"ticket"`
}

// RepositoryGateway supplies bounded Ticket projections and typed mutations to
// the daemon control plane. Implementations must resolve repository IDs
// themselves and must never accept caller filesystem paths or arbitrary argv.
type RepositoryGateway struct {
	ListTickets  func(context.Context, string, RepositoryTicketQuery) (RepositoryTicketList, error)
	GetTicket    func(context.Context, string, string) (RepositoryTicketDetail, error)
	CreateTicket func(context.Context, string, RepositoryTicketCreateRequest) (RepositoryTicketMutation, error)
	UpdateTicket func(context.Context, string, string, RepositoryTicketUpdateRequest) (RepositoryTicketMutation, error)
	MutateTicket func(context.Context, string, string, string, RepositoryTicketMutationRequest) (RepositoryTicketMutation, error)
}

// RepositoryReadError lets a gateway return a stable HTTP error without
// exposing command arguments, paths, or raw Ticket output.
type RepositoryReadError struct {
	Code    string
	Status  int
	Message string
	Cause   error
}

// RepositoryMutationError is a safe, typed failure for UI Ticket mutations.
// Applied remains authoritative when Ticket reports an uncertain mutation;
// callers must reread instead of retrying blindly.
type RepositoryMutationError struct {
	Code    string
	Status  int
	Message string
	Applied bool
	Cause   error
}

func (e *RepositoryMutationError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return "repository mutation failed"
	}
	return e.Message
}

func (e *RepositoryMutationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *RepositoryReadError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return fmt.Sprintf("%s: repository read failed", e.Code)
	}
	return e.Message
}

func (e *RepositoryReadError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
