package ticketclient

const (
	// APIVersion is the Ticket JSON API version understood by this client.
	APIVersion = 2

	// StateClosed is Ticket's canonical successful terminal lifecycle state.
	StateClosed = "closed"
	// StateLegacyCompleted is accepted only at compatibility boundaries for
	// older Ticket output and is normalized to StateClosed immediately.
	StateLegacyCompleted = "completed"
)

// NormalizeLifecycleState maps legacy Ticket state output to the canonical
// spelling used by Orc. Other values are preserved for queue and diagnostic
// handling.
func NormalizeLifecycleState(state string) string {
	if state == StateLegacyCompleted {
		return StateClosed
	}
	return state
}

// Ticket is the subset of ticket's JSON representation needed by the
// orchestrator.
type Ticket struct {
	ID        string   `json:"id"`
	Title     string   `json:"title,omitempty"`
	State     string   `json:"state"`
	Priority  int      `json:"priority,omitempty"`
	Assignee  string   `json:"assignee,omitempty"`
	Parent    string   `json:"parent,omitempty"`
	Tags      []string `json:"tags,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// TicketSection is the bounded public Markdown section returned by Ticket's
// default show projection.
type TicketSection struct {
	Text      string `json:"text"`
	Truncated bool   `json:"truncated,omitempty"`
}

// TicketDetail is the typed read-only detail projection used by repository
// gateways. Filesystem paths and raw ticket documents are intentionally not
// retained.
type TicketDetail struct {
	ID            string                   `json:"id"`
	Archived      bool                     `json:"archived"`
	Title         string                   `json:"title"`
	State         string                   `json:"state"`
	Priority      int                      `json:"priority"`
	Assignee      string                   `json:"assignee,omitempty"`
	BlockedReason string                   `json:"blocked_reason,omitempty"`
	Parent        string                   `json:"parent,omitempty"`
	Tags          []string                 `json:"tags"`
	DependsOn     []string                 `json:"depends_on"`
	Sections      map[string]TicketSection `json:"sections,omitempty"`
	Available     []string                 `json:"available_sections,omitempty"`
	Readiness     *TicketReadiness         `json:"readiness,omitempty"`
	Created       string                   `json:"created,omitempty"`
	Modified      string                   `json:"modified,omitempty"`
	Body          string                   `json:"body,omitempty"`
	BodyTruncated bool                     `json:"body_truncated,omitempty"`
	Truncated     bool                     `json:"truncated,omitempty"`
}

// TicketReadiness is Ticket's authoritative readiness projection.
type TicketReadiness struct {
	Ready    bool                     `json:"ready"`
	Blockers []TicketReadinessBlocker `json:"blockers,omitempty"`
}

// TicketReadinessBlocker is one structured Ticket readiness blocker.
type TicketReadinessBlocker struct {
	Code    string `json:"code"`
	ID      string `json:"id,omitempty"`
	Message string `json:"message,omitempty"`
}

// ListQuery describes the typed bounded read-only projection used by Orc's
// repository gateway. Ticket owns filter, ordering, and pagination semantics.
type ListQuery struct {
	IDs      []string
	States   []string
	Priority *int
	Assignee string
	Tags     []string
	Limit    int
	Offset   int
}

// ListResult is the public Ticket list projection returned to Orc callers.
type ListResult struct {
	Items []Ticket `json:"items"`
	More  bool     `json:"more"`
}

// MutationResult is the bounded result shared by Ticket's typed mutation
// commands. Commands that return a narrower shape are normalized into this
// projection by Client.
type MutationResult struct {
	ID            string   `json:"id"`
	Changed       bool     `json:"changed"`
	FromState     string   `json:"from_state,omitempty"`
	State         string   `json:"state,omitempty"`
	Assignee      string   `json:"assignee,omitempty"`
	ChangedFields []string `json:"changed_fields,omitempty"`
}

// MutationOptions contains the optional Ticket workflow context accepted by
// UI mutations. The client binds actor identity through its Ticket process.
type MutationOptions struct {
	Handoff string
	Message string
	Outcome string
}

// CreateInput is the typed public input accepted by Ticket create --input -.
type CreateInput struct {
	Title     string            `json:"title"`
	Priority  *int              `json:"priority,omitempty"`
	Tags      []string          `json:"tags,omitempty"`
	Parent    string            `json:"parent,omitempty"`
	DependsOn []string          `json:"depends_on,omitempty"`
	Sections  map[string]string `json:"sections,omitempty"`
}

// UpdateInput is the typed public input accepted by Ticket update --input -.
type UpdateInput struct {
	Set      map[string]any    `json:"set,omitempty"`
	Sections map[string]string `json:"sections,omitempty"`
}

// Transition is the subset returned by release and other lifecycle commands.
type Transition struct {
	ID        string `json:"id"`
	Changed   bool   `json:"changed"`
	FromState string `json:"from_state,omitempty"`
	State     string `json:"state,omitempty"`
}

// WorkLogView is the bounded public Ticket view used by operational reports.
// It contains only the selected work-log section and current lifecycle state.
type WorkLogView struct {
	ID        string
	State     string
	WorkLog   string
	Truncated bool
}

func (ticket Ticket) NormalizeState() Ticket {
	ticket.State = NormalizeLifecycleState(ticket.State)
	return ticket
}

func (transition Transition) NormalizeState() Transition {
	transition.FromState = NormalizeLifecycleState(transition.FromState)
	transition.State = NormalizeLifecycleState(transition.State)
	return transition
}
