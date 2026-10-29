package daemon

import (
	"context"
	"fmt"
	"io"

	"github.com/toolsupply/ticket-orc/internal/attachments"
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

// RepositoryTicketDeleteRequest accepts only the actor performing deletion.
type RepositoryTicketDeleteRequest struct {
	Actor string `json:"actor"`
}

// RepositoryTicketDeletion is a receipt, not a surviving ticket projection.
type RepositoryTicketDeletion struct {
	RepositoryID  string `json:"repository_id"`
	RepositoryKey string `json:"repository_key,omitempty"`
	ID            string `json:"id"`
	Operation     string `json:"operation"`
	Changed       bool   `json:"changed"`
}

// RepositoryTicketCreateRequest is the typed create surface used by the UI.
// State and Assignee pointers preserve explicit empty values for Ticket validation.
type RepositoryTicketCreateRequest struct {
	Actor     string            `json:"actor"`
	Title     string            `json:"title"`
	State     *string           `json:"state,omitempty"`
	Assignee  *string           `json:"assignee,omitempty"`
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

// Attachment requests contain only relative entries and explicit caller attribution.
// Actor is selected by the authorized caller, never inferred from a worker.
type RepositoryAttachmentQuery struct {
	Path   string
	Limit  int
	Cursor string
}
type RepositoryAttachmentRequest struct {
	Actor     string
	Path      string
	Condition attachments.Condition
}
type RepositoryAttachmentMoveRequest struct {
	Actor, From, To, Revision string
	Condition                 attachments.Condition
}
type RepositoryAttachmentContent struct {
	Entry   attachments.Entry
	Content io.ReadCloser
}

// RepositoryGateway supplies bounded Ticket projections and typed mutations to
// the daemon control plane. Implementations must resolve repository IDs
// themselves and must never accept caller filesystem paths or arbitrary argv.
type RepositoryGateway struct {
	ListAttachments func(context.Context, string, string, RepositoryAttachmentQuery) (attachments.Listing, error)
	ReadAttachment  func(context.Context, string, string, string) (RepositoryAttachmentContent, error)
	// WriteAttachment streams body under the storage transfer deadline. Callers must
	// make blocking reader IO obey context cancellation and the total transfer deadline,
	// including shutdown during staging. The final callback runs only after a complete
	// body is accepted and its cancellation detached. It must obtain the existing
	// server-owned lifecycle via Server.MutationContext; shutdown remains effective.
	// The callback's cancel function is always called; publication is never retried.
	WriteAttachment           func(context.Context, string, string, RepositoryAttachmentRequest, io.Reader, int64, attachments.CommitContext) (attachments.Receipt, error)
	CreateAttachmentDirectory func(context.Context, string, string, RepositoryAttachmentRequest) (attachments.Receipt, error)
	MoveAttachment            func(context.Context, string, string, RepositoryAttachmentMoveRequest) (attachments.Receipt, error)
	DeleteAttachment          func(context.Context, string, string, RepositoryAttachmentRequest) (attachments.Receipt, error)
	DeleteTicket              func(context.Context, string, string, RepositoryTicketDeleteRequest) (RepositoryTicketDeletion, error)
	ListTickets               func(context.Context, string, RepositoryTicketQuery) (RepositoryTicketList, error)
	GetTicket                 func(context.Context, string, string) (RepositoryTicketDetail, error)
	CreateTicket              func(context.Context, string, RepositoryTicketCreateRequest) (RepositoryTicketMutation, error)
	UpdateTicket              func(context.Context, string, string, RepositoryTicketUpdateRequest) (RepositoryTicketMutation, error)
	MutateTicket              func(context.Context, string, string, string, RepositoryTicketMutationRequest) (RepositoryTicketMutation, error)
	// TicketBodyBudgetBytes is the maximum body size returned by GetTicket.
	TicketBodyBudgetBytes int
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
// AppliedKnown distinguishes known true/false from an unknown outcome. When
// certainty is unknown, the HTTP error omits mutation_applied so clients do
// not mistake transport failure for proof that the mutation was rejected.
type RepositoryMutationError struct {
	Code         string
	Status       int
	Message      string
	Applied      bool
	AppliedKnown bool
	Cause        error
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
