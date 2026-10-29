package ticketclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// DeleteTicket permanently deletes one explicit ticket using Ticket's public
// command. A receipt is required; a failed detail read is never deletion proof.
func (c *Client) DeleteTicket(ctx context.Context, id string) (DeleteResult, error) {
	if err := validateFullID(id); err != nil {
		return DeleteResult{}, err
	}
	var result struct {
		ID      string `json:"id"`
		Changed *bool  `json:"changed"`
	}
	if err := c.invoke(ctx, []string{"delete", id}, &result); err != nil {
		return DeleteResult{}, err
	}
	if result.ID != id || result.Changed == nil {
		return DeleteResult{}, fmt.Errorf("%w: invalid delete receipt", ErrProtocol)
	}
	return DeleteResult{ID: result.ID, Changed: *result.Changed}, nil
}

// CreateTicket creates one ticket from the typed public input. The caller's
// actor is bound to the Ticket process independently of the requested initial
// assignee. Ticket validates and applies initial state and ownership atomically.
func (c *Client) CreateTicket(ctx context.Context, input CreateInput) (MutationResult, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return MutationResult{}, fmt.Errorf("encode ticket create input: %w", err)
	}
	var result MutationResult
	if err := c.invokeWithStdin(ctx, []string{"create", "--input", "-"}, data, &result); err != nil {
		return MutationResult{}, err
	}
	if err := validateMutationID(result.ID); err != nil {
		return MutationResult{}, fmt.Errorf("%w: create result: %v", ErrProtocol, err)
	}
	return result, nil
}

// UpdateTicket applies only the typed metadata and section fields accepted by
// Ticket's update --input - command.
func (c *Client) UpdateTicket(ctx context.Context, id string, input UpdateInput) (MutationResult, error) {
	if err := validateFullID(id); err != nil {
		return MutationResult{}, err
	}
	data, err := json.Marshal(input)
	if err != nil {
		return MutationResult{}, fmt.Errorf("encode ticket update input: %w", err)
	}
	var result MutationResult
	if err := c.invokeWithStdin(ctx, []string{"update", id, "--input", "-"}, data, &result); err != nil {
		return MutationResult{}, err
	}
	if err := validateMutationResult(result, id); err != nil {
		return MutationResult{}, err
	}
	return result, nil
}

// ClaimTicket claims one explicit ticket for the client's actor.
func (c *Client) ClaimTicket(ctx context.Context, id string) (MutationResult, error) {
	return c.invokeTicketMutation(ctx, []string{"claim", id}, id)
}

// ReleaseTicket releases one ticket and optionally records handoff context.
func (c *Client) ReleaseTicket(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	return c.invokeWorkflowMutation(ctx, "release", id, options, true)
}

// ReassignTicket atomically transfers ownership of one ticket to a responsible
// user while preserving its current lifecycle state. Ticket owns the
// transition; Orc callers must never emulate this with release plus claim.
func (c *Client) ReassignTicket(ctx context.Context, id, assignee string, options MutationOptions) (MutationResult, error) {
	if err := validateFullID(id); err != nil {
		return MutationResult{}, err
	}
	if strings.TrimSpace(assignee) != assignee || !validActor(assignee) {
		return MutationResult{}, fmt.Errorf("invalid reassign assignee")
	}
	data, err := json.Marshal(struct {
		Assignee string `json:"assignee"`
		Handoff  string `json:"handoff,omitempty"`
		Message  string `json:"message,omitempty"`
	}{Assignee: assignee, Handoff: options.Handoff, Message: options.Message})
	if err != nil {
		return MutationResult{}, fmt.Errorf("encode ticket reassign input: %w", err)
	}
	var result MutationResult
	if err := c.invokeWithStdin(ctx, []string{"reassign", id, "--input", "-"}, data, &result); err != nil {
		return MutationResult{}, err
	}
	if err := validateMutationResult(result, id); err != nil {
		return MutationResult{}, err
	}
	return result, nil
}

// OpenTicket returns a ticket to open, optionally recording handoff context.
func (c *Client) OpenTicket(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	args := []string{"open", id}
	appendWorkflowFlags(&args, options, true)
	return c.invokeTicketMutation(ctx, args, id)
}

// HoldTicket moves an assigned ticket to hold.
func (c *Client) HoldTicket(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	return c.invokeWorkflowMutation(ctx, "hold", id, options, false)
}

// SubmitTicket submits an implementation for review.
func (c *Client) SubmitTicket(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	return c.invokeWorkflowMutation(ctx, "submit", id, options, false)
}

// ReviewTicket moves a ticket directly into review.
func (c *Client) ReviewTicket(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	args := []string{"review", id}
	appendMessageFlag(&args, options.Message)
	return c.invokeTicketMutation(ctx, args, id)
}

// ApproveTicket approves one reviewed ticket for signoff.
func (c *Client) ApproveTicket(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	args := []string{"approve", id}
	appendMessageFlag(&args, options.Message)
	return c.invokeTicketMutation(ctx, args, id)
}

// CloseTicketWithOptions closes one ticket with optional outcome context.
func (c *Client) CloseTicketWithOptions(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	return c.invokeWorkflowMutation(ctx, "close", id, options, true)
}

// RejectTicket rejects one ticket with an outcome.
func (c *Client) RejectTicket(ctx context.Context, id string, options MutationOptions) (MutationResult, error) {
	return c.invokeWorkflowMutation(ctx, "reject", id, options, true)
}

// BumpTicket raises one ticket's priority using Ticket's own bounded rule.
func (c *Client) BumpTicket(ctx context.Context, id string) (MutationResult, error) {
	return c.invokeTicketMutation(ctx, []string{"bump", id}, id)
}

// Release gives up ownership of one explicit full ticket ID.
func (c *Client) Release(ctx context.Context, id string) (Transition, error) {
	if err := validateFullID(id); err != nil {
		return Transition{}, err
	}
	var transition Transition
	if err := c.invoke(ctx, []string{"release", id}, &transition); err != nil {
		return Transition{}, err
	}
	if transition.ID != id {
		return Transition{}, fmt.Errorf("%w: release returned ticket %q, expected %q", ErrProtocol, transition.ID, id)
	}
	return transition.NormalizeState(), nil
}

func (c *Client) invokeWorkflowMutation(ctx context.Context, command, id string, options MutationOptions, input bool) (MutationResult, error) {
	if err := validateFullID(id); err != nil {
		return MutationResult{}, err
	}
	args := []string{command, id}
	if input {
		data, err := json.Marshal(struct {
			Handoff string `json:"handoff,omitempty"`
			Message string `json:"message,omitempty"`
			Outcome string `json:"outcome,omitempty"`
		}{Handoff: options.Handoff, Message: options.Message, Outcome: options.Outcome})
		if err != nil {
			return MutationResult{}, fmt.Errorf("encode ticket %s input: %w", command, err)
		}
		args = append(args, "--input", "-")
		var result MutationResult
		if err := c.invokeWithStdin(ctx, args, data, &result); err != nil {
			return MutationResult{}, err
		}
		if err := validateMutationResult(result, id); err != nil {
			return MutationResult{}, err
		}
		return result, nil
	}
	appendWorkflowFlags(&args, options, true)
	return c.invokeTicketMutation(ctx, args, id)
}

func (c *Client) invokeTicketMutation(ctx context.Context, args []string, id string) (MutationResult, error) {
	if err := validateFullID(id); err != nil {
		return MutationResult{}, err
	}
	var result MutationResult
	if err := c.invoke(ctx, args, &result); err != nil {
		return MutationResult{}, err
	}
	if err := validateMutationResult(result, id); err != nil {
		return MutationResult{}, err
	}
	return result, nil
}

// CloseTicket transitions one explicit full ticket ID to its terminal closed
// state. The command response remains typed so callers can preserve command
// and mutation-uncertainty errors without retrying an ambiguous mutation.
func (c *Client) CloseTicket(ctx context.Context, id string) (Transition, error) {
	if err := validateFullID(id); err != nil {
		return Transition{}, err
	}
	var transition Transition
	if err := c.invoke(ctx, []string{"close", id}, &transition); err != nil {
		return Transition{}, err
	}
	if transition.ID != id {
		return Transition{}, fmt.Errorf("%w: close returned ticket %q, expected %q", ErrProtocol, transition.ID, id)
	}
	return transition.NormalizeState(), nil
}

func appendWorkflowFlags(args *[]string, options MutationOptions, includeHandoff bool) {
	if includeHandoff && options.Handoff != "" {
		*args = append(*args, "--handoff", options.Handoff)
	}
	appendMessageFlag(args, options.Message)
}

func appendMessageFlag(args *[]string, message string) {
	if message != "" {
		*args = append(*args, "--message", message)
	}
}

// AcquireHold authorizes exactly one assessment by atomically selecting an
// unassigned hold and assigning it to this client's existing actor. A prior
// same-actor assignment is never execution authority. Errors are not retryable:
// the attached reread is diagnostic evidence only, never permission to launch.
func (c *Client) AcquireHold(ctx context.Context, id string, filters QueueFilters) (*Ticket, error) {
	if err := validateFullID(id); err != nil {
		return nil, err
	}
	filters, err := CanonicalQueueFilters(filters)
	if err != nil {
		return nil, err
	}
	args := []string{"query", "id:" + id, "state:hold", "unclaimed"}
	for _, tag := range filters.Tags {
		args = append(args, "tag:"+tag)
	}
	for _, tag := range filters.WithoutTags {
		args = append(args, "not", "tag:"+tag)
	}
	args = append(args, "::", "reassign", "--to", c.actor)
	var receipt struct {
		ID       string  `json:"id"`
		State    string  `json:"state"`
		Assignee string  `json:"assignee"`
		From     *string `json:"from_assignee"`
		Changed  *bool   `json:"changed"`
	}
	err = c.invoke(ctx, args, &receipt)
	if err != nil {
		var command *CommandError
		if errors.As(err, &command) && command.Code == "invalid_argument" && !command.MutationApplied() {
			var count *int
			if raw, ok := command.Details["count"]; ok && json.Unmarshal(raw, &count) == nil && count != nil && *count == 0 {
				return nil, nil
			}
		}
	} else if receipt.ID != id || receipt.State != "hold" || receipt.Assignee != c.actor || receipt.From == nil || *receipt.From != "" || receipt.Changed == nil || !*receipt.Changed {
		err = fmt.Errorf("%w: invalid hold acquisition receipt: %+v", ErrProtocol, receipt)
	}
	observed, readErr := c.Show(ctx, id)
	if err != nil || readErr != nil {
		return nil, fmt.Errorf("hold acquisition %s requires operator reconciliation; observed id=%s state=%s owner=%q: %w", id, observed.ID, observed.State, observed.Assignee, errors.Join(err, readErr))
	}
	if observed.ID != id || observed.State != "hold" || observed.Assignee != c.actor || !matchesHoldFilters(observed, filters) {
		return nil, fmt.Errorf("hold acquisition %s changed before dispatch; observed state=%s owner=%q; operator reconciliation required", id, observed.State, observed.Assignee)
	}
	return &observed, nil
}

func matchesHoldFilters(ticket Ticket, filters QueueFilters) bool {
	tags := make(map[string]bool, len(ticket.Tags))
	for _, tag := range ticket.Tags {
		tags[tag] = true
	}
	for _, tag := range filters.Tags {
		if !tags[tag] {
			return false
		}
	}
	for _, tag := range filters.WithoutTags {
		if tags[tag] {
			return false
		}
	}
	return true
}

// OwnedHold reports an old assessment assignment without authorizing execution.
// It is intentionally independent of queue filters and role policy.
func (c *Client) OwnedHold(ctx context.Context) (*Ticket, error) {
	var response struct {
		Items []Ticket `json:"items"`
	}
	if err := c.invoke(ctx, []string{"list", "--state", "hold", "--assignee", c.actor, "--limit", "1", "--fields", "id,state,assignee"}, &response); err != nil {
		return nil, err
	}
	if len(response.Items) == 0 {
		return nil, nil
	}
	item := response.Items[0]
	if validateFullID(item.ID) != nil || item.State != "hold" || item.Assignee != c.actor {
		return nil, fmt.Errorf("%w: invalid owned hold observation", ErrProtocol)
	}
	return &item, nil
}

// EscalateHold performs one guarded human handoff after a clean managed
// assessment. A stale guard is observation only; ambiguous mutations are never
// replayed. Existing Handoff must be complete before it can be preserved.
func (c *Client) EscalateHold(ctx context.Context, id, owner, explanation string) (Ticket, error) {
	if err := validateFullID(id); err != nil {
		return Ticket{}, err
	}
	if owner != "" && owner != c.actor {
		return Ticket{}, fmt.Errorf("hold escalation cannot transfer another actor's work")
	}
	var detail TicketDetail
	if err := c.invoke(ctx, []string{"show", id, "--section", "handoff", "--max-bytes", "1048576"}, &detail); err != nil {
		return Ticket{}, err
	}
	if detail.ID != id || detail.Truncated || detail.Sections["handoff"].Truncated {
		return Ticket{}, fmt.Errorf("%w: incomplete hold Handoff observation", ErrProtocol)
	}
	current, err := c.Show(ctx, id)
	if err != nil {
		return Ticket{}, err
	}
	if current.State != "hold" || current.Assignee != owner {
		return current, nil
	}
	handoff := detail.Sections["handoff"].Text
	if strings.TrimSpace(handoff) != "" {
		handoff += "\n\n"
	}
	handoff += explanation
	guard := "unclaimed"
	if owner != "" {
		guard = "assignee:" + owner
	}
	var receipt struct {
		ID       string  `json:"id"`
		State    string  `json:"state"`
		Assignee string  `json:"assignee"`
		From     *string `json:"from_assignee"`
		Changed  *bool   `json:"changed"`
		Handoff  string  `json:"handoff"`
	}
	err = c.invoke(ctx, []string{"query", "id:" + id, "state:hold", guard, "::", "reassign", "--to", "user", "--handoff", handoff, "--message", "Automated hold assessment unresolved; escalated to user."}, &receipt)
	if err != nil {
		var command *CommandError
		if errors.As(err, &command) && command.Code == "invalid_argument" && !command.MutationApplied() {
			var count *int
			if raw, ok := command.Details["count"]; ok && json.Unmarshal(raw, &count) == nil && count != nil && *count == 0 {
				return c.Show(ctx, id)
			}
		}
		return Ticket{}, err
	}
	if receipt.ID != id || receipt.State != "hold" || receipt.Assignee != "user" || receipt.From == nil || *receipt.From != owner || receipt.Changed == nil || !*receipt.Changed || receipt.Handoff != handoff {
		return Ticket{}, fmt.Errorf("%w: invalid hold escalation receipt", ErrProtocol)
	}
	return c.Show(ctx, id)
}
