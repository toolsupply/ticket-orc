package ticketclient

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// RepositoryDetailBodyBudgetBytes keeps worst-case JSON escaping within the
// ticketclient response frame limit while remaining large enough for normal
// ticket display bodies. Ticket's own show limit is 1 MiB, but JSON can expand
// control characters by up to six bytes each before the 2 MiB frame cap.
const RepositoryDetailBodyBudgetBytes = 256 << 10

// ListAllForReport returns a bounded snapshot of every ticket through Ticket's
// public list projection. It is intentionally separate from lifecycle queue
// helpers because reports must include terminal and reopened tickets.
func (c *Client) ListAllForReport(ctx context.Context, limit int) ([]Ticket, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("ticket report limit must be greater than zero")
	}
	var response struct {
		Items []Ticket `json:"items"`
		More  bool     `json:"more"`
	}
	args := []string{"list", "--state", "all", "--limit", fmt.Sprint(limit), "--fields", "id,state"}
	if err := c.invoke(ctx, args, &response); err != nil {
		return nil, err
	}
	if response.More {
		return nil, fmt.Errorf("ticket report exceeds bounded ticket limit %d", limit)
	}
	for i := range response.Items {
		response.Items[i] = response.Items[i].NormalizeState()
		if err := validateFullID(response.Items[i].ID); err != nil {
			return nil, fmt.Errorf("%w: report item: %v", ErrProtocol, err)
		}
		if strings.TrimSpace(response.Items[i].State) == "" {
			return nil, fmt.Errorf("%w: report item %q has no state", ErrProtocol, response.Items[i].ID)
		}
	}
	return response.Items, nil
}

// ListTickets returns a bounded public ticket projection. Query filtering and
// ordering are delegated to Ticket's JSON command; Orc never reads Ticket
// files or reconstructs repository semantics locally.
func (c *Client) ListTickets(ctx context.Context, query ListQuery) (ListResult, error) {
	if query.Limit <= 0 || query.Limit > 256 {
		return ListResult{}, fmt.Errorf("ticket list limit must be between 1 and 256")
	}
	if query.Offset < 0 {
		return ListResult{}, fmt.Errorf("ticket list offset must not be negative")
	}
	if query.Priority != nil && (*query.Priority < 0 || *query.Priority > 4) {
		return ListResult{}, fmt.Errorf("ticket list priority must be between 0 and 4")
	}
	var response ListResult
	args := []string{"list"}
	for _, id := range query.IDs {
		if err := validateFullID(id); err != nil {
			return ListResult{}, fmt.Errorf("ticket list candidate ID is invalid")
		}
		args = append(args, id)
	}
	states := append([]string(nil), query.States...)
	if len(states) == 0 && len(query.IDs) == 0 {
		states = []string{"all"}
	}
	for _, state := range states {
		state = NormalizeLifecycleState(strings.TrimSpace(state))
		if state == "" {
			return ListResult{}, fmt.Errorf("ticket list state must not be empty")
		}
		args = append(args, "--state", state)
	}
	if query.Priority != nil {
		args = append(args, "--priority", fmt.Sprint(*query.Priority))
	}
	if query.Assignee != "" {
		args = append(args, "--assignee", query.Assignee)
	}
	for _, tag := range query.Tags {
		if strings.TrimSpace(tag) == "" {
			return ListResult{}, fmt.Errorf("ticket list tag must not be empty")
		}
		args = append(args, "--tag", tag)
	}
	args = append(args, "--limit", fmt.Sprint(query.Limit), "--offset", fmt.Sprint(query.Offset), "--fields", "id,title,state,priority,assignee,tags,parent,depends_on")
	if err := c.invoke(ctx, args, &response); err != nil {
		return ListResult{}, err
	}
	if err := normalizeListResult(&response); err != nil {
		return ListResult{}, err
	}
	return response, nil
}

// SearchTickets delegates repository search to Ticket's public grep command.
// Ticket returns a bounded summary projection and remains the authority for
// matching ticket content.
func (c *Client) SearchTickets(ctx context.Context, expression string, limit, offset int) (ListResult, error) {
	if strings.TrimSpace(expression) == "" {
		return ListResult{}, fmt.Errorf("ticket search expression must not be empty")
	}
	if len(expression) > 256 {
		return ListResult{}, fmt.Errorf("ticket search expression is too long")
	}
	if limit <= 0 || limit > 256 {
		return ListResult{}, fmt.Errorf("ticket search limit must be between 1 and 256")
	}
	if offset < 0 {
		return ListResult{}, fmt.Errorf("ticket search offset must not be negative")
	}
	response, err := c.SearchTicketCandidates(ctx, expression)
	if err != nil {
		return ListResult{}, err
	}
	if offset >= len(response.Items) {
		response.Items = nil
		response.More = false
	} else {
		response.Items = response.Items[offset:]
		if len(response.Items) > limit {
			response.Items = response.Items[:limit]
			response.More = true
		}
	}
	return response, nil
}

// SearchTicketCandidates returns the complete bounded candidate projection
// from Ticket's public grep command. Callers can hydrate/filter candidates with
// Ticket's typed list operation before applying pagination.
func (c *Client) SearchTicketCandidates(ctx context.Context, expression string) (ListResult, error) {
	if strings.TrimSpace(expression) == "" {
		return ListResult{}, fmt.Errorf("ticket search expression must not be empty")
	}
	if len(expression) > 256 {
		return ListResult{}, fmt.Errorf("ticket search expression is too long")
	}
	var response ListResult
	if err := c.invoke(ctx, []string{"grep", "--", regexp.QuoteMeta(expression)}, &response); err != nil {
		return ListResult{}, err
	}
	if response.More || len(response.Items) > maxSearchResultItems {
		return ListResult{}, fmt.Errorf("ticket search result exceeds bounded limit of %d items", maxSearchResultItems)
	}
	if err := normalizeListResult(&response); err != nil {
		return ListResult{}, err
	}
	return response, nil
}

// ShowWorkLog returns one bounded, public work-log projection for a full
// ticket ID. Prompts, credentials, and other ticket sections are never
// requested or retained.
func (c *Client) ShowWorkLog(ctx context.Context, id string) (WorkLogView, error) {
	if err := validateFullID(id); err != nil {
		return WorkLogView{}, err
	}
	var response struct {
		ID       string `json:"id"`
		State    string `json:"state"`
		Sections map[string]struct {
			Text      string `json:"text"`
			Truncated bool   `json:"truncated,omitempty"`
		} `json:"sections"`
	}
	if err := c.invoke(ctx, []string{"show", id, "--section", "work_log", "--max-bytes", fmt.Sprint(1 << 20)}, &response); err != nil {
		return WorkLogView{}, err
	}
	if response.ID != id || strings.TrimSpace(response.State) == "" {
		return WorkLogView{}, fmt.Errorf("%w: work-log response does not identify ticket %q", ErrProtocol, id)
	}
	section := response.Sections["work_log"]
	return WorkLogView{ID: response.ID, State: NormalizeLifecycleState(response.State), WorkLog: section.Text, Truncated: section.Truncated}, nil
}

// Show reads authoritative ticket state for one explicit full ID.
func (c *Client) Show(ctx context.Context, id string) (Ticket, error) {
	if err := validateFullID(id); err != nil {
		return Ticket{}, err
	}
	var ticket Ticket
	if err := c.invoke(ctx, []string{"show", id}, &ticket); err != nil {
		return Ticket{}, err
	}
	if ticket.ID != id {
		return Ticket{}, fmt.Errorf("%w: show returned ticket %q, expected %q", ErrProtocol, ticket.ID, id)
	}
	ticket = ticket.NormalizeState()
	if strings.TrimSpace(ticket.State) == "" {
		return Ticket{}, fmt.Errorf("%w: show response has no state", ErrProtocol)
	}
	return ticket, nil
}

// ShowDetail reads Ticket's bounded metadata, sections, and readiness
// projection for one explicit full ID. Status timestamps and the bounded full
// display body are read through their separate typed public operations.
func (c *Client) ShowDetail(ctx context.Context, id string) (TicketDetail, error) {
	detail, err := c.ShowReadiness(ctx, id)
	if err != nil {
		return TicketDetail{}, err
	}
	status, err := c.ShowStatus(ctx, id)
	if err != nil {
		return TicketDetail{}, err
	}
	body, err := c.ShowFullBody(ctx, id)
	if err != nil {
		return TicketDetail{}, err
	}
	detail.Created = status.Created
	detail.Modified = status.Modified
	detail.Body = body.Body
	detail.BodyTruncated = body.Truncated
	detail.Truncated = detail.Truncated || body.Truncated
	return detail, nil
}

// ShowReadiness reads the authoritative lifecycle, ownership, and readiness
// projection needed for dispatch safety without fetching display-only data.
func (c *Client) ShowReadiness(ctx context.Context, id string) (TicketDetail, error) {
	if err := validateFullID(id); err != nil {
		return TicketDetail{}, err
	}
	var detail TicketDetail
	if err := c.invoke(ctx, []string{"show", id, "--readiness"}, &detail); err != nil {
		return TicketDetail{}, err
	}
	if detail.ID != id {
		return TicketDetail{}, fmt.Errorf("%w: show returned ticket %q, expected %q", ErrProtocol, detail.ID, id)
	}
	detail.State = NormalizeLifecycleState(detail.State)
	if strings.TrimSpace(detail.State) == "" {
		return TicketDetail{}, fmt.Errorf("%w: show response has no state", ErrProtocol)
	}
	if detail.Readiness == nil {
		return TicketDetail{}, fmt.Errorf("%w: show response has no readiness", ErrProtocol)
	}
	return detail, nil
}

// TicketStatus is the concise public status projection used for ticket
// creation and modification timestamps.
type TicketStatus struct {
	ID       string `json:"id"`
	Created  string `json:"created"`
	Modified string `json:"modified"`
}

// ShowStatus returns Ticket's typed status projection for one full ID.
func (c *Client) ShowStatus(ctx context.Context, id string) (TicketStatus, error) {
	if err := validateFullID(id); err != nil {
		return TicketStatus{}, err
	}
	var status TicketStatus
	if err := c.invoke(ctx, []string{"status", id}, &status); err != nil {
		return TicketStatus{}, err
	}
	if status.ID != id {
		return TicketStatus{}, fmt.Errorf("%w: status returned ticket %q, expected %q", ErrProtocol, status.ID, id)
	}
	if _, err := time.Parse("2006-01-02", status.Created); err != nil {
		return TicketStatus{}, fmt.Errorf("%w: status response has invalid created date", ErrProtocol)
	}
	if _, err := time.Parse(time.RFC3339, status.Modified); err != nil {
		return TicketStatus{}, fmt.Errorf("%w: status response has invalid modified timestamp", ErrProtocol)
	}
	return status, nil
}

// ShowFullBody reads Ticket's full display body with a conservative response
// budget that accounts for JSON escaping before the client frame-size limit.
func (c *Client) ShowFullBody(ctx context.Context, id string) (TicketDetail, error) {
	if err := validateFullID(id); err != nil {
		return TicketDetail{}, err
	}
	var detail TicketDetail
	if err := c.invoke(ctx, []string{"show", id, "--full", "--max-bytes", fmt.Sprint(RepositoryDetailBodyBudgetBytes)}, &detail); err != nil {
		return TicketDetail{}, err
	}
	if detail.ID != id {
		return TicketDetail{}, fmt.Errorf("%w: show returned ticket %q, expected %q", ErrProtocol, detail.ID, id)
	}
	if len([]byte(detail.Body)) > RepositoryDetailBodyBudgetBytes {
		return TicketDetail{}, fmt.Errorf("%w: show body exceeds bounded display limit", ErrProtocol)
	}
	return detail, nil
}

const maxSearchResultItems = 1024

func normalizeListResult(response *ListResult) error {
	if response == nil {
		return fmt.Errorf("%w: ticket list response is nil", ErrProtocol)
	}
	for i := range response.Items {
		response.Items[i] = response.Items[i].NormalizeState()
		if err := validateFullID(response.Items[i].ID); err != nil {
			return fmt.Errorf("%w: list item: %v", ErrProtocol, err)
		}
		if strings.TrimSpace(response.Items[i].State) == "" {
			return fmt.Errorf("%w: list item %q has no state", ErrProtocol, response.Items[i].ID)
		}
	}
	return nil
}
