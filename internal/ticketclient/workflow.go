package ticketclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// CreateTicket creates one ticket from the typed public input. The caller's
// actor is bound to the Ticket process even though create has no ownership
// transition of its own.
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
