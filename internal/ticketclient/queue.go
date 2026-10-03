package ticketclient

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// QueueFilters contains conjunctive required tags and tags that must be absent.
// Ownership observations intentionally do not accept this selector.
type QueueFilters struct {
	Tags        []string
	WithoutTags []string
}

// CanonicalQueueFilters validates and canonicalizes a copy. Tag order and
// nil-versus-empty slices have no semantic effect.
func CanonicalQueueFilters(filters QueueFilters) (QueueFilters, error) {
	canonicalize := func(tags []string) ([]string, error) {
		if len(tags) == 0 {
			return nil, nil
		}
		if len(tags) > 64 {
			return nil, fmt.Errorf("ticket tag filter must contain at most 64 tags")
		}
		seen := make(map[string]struct{}, len(tags))
		result := make([]string, 0, len(tags))
		for _, tag := range tags {
			if !validTagToken(tag) {
				return nil, fmt.Errorf("ticket tag filter is invalid")
			}
			if _, ok := seen[tag]; !ok {
				seen[tag] = struct{}{}
				result = append(result, tag)
			}
		}
		sort.Strings(result)
		return result, nil
	}
	tags, err := canonicalize(filters.Tags)
	if err != nil {
		return QueueFilters{}, err
	}
	withoutTags, err := canonicalize(filters.WithoutTags)
	if err != nil {
		return QueueFilters{}, err
	}
	excluded := make(map[string]struct{}, len(withoutTags))
	for _, tag := range withoutTags {
		excluded[tag] = struct{}{}
	}
	for _, tag := range tags {
		if _, ok := excluded[tag]; ok {
			return QueueFilters{}, fmt.Errorf("ticket tag %q is both required and excluded", tag)
		}
	}
	return QueueFilters{Tags: tags, WithoutTags: withoutTags}, nil
}

// WaitAndClaimImplementation atomically claims eligible implementation work.
func (c *Client) WaitAndClaimImplementation(ctx context.Context, filters QueueFilters) (Ticket, error) {
	return c.waitAndClaimQueue(ctx, "open", filters)
}

// WaitAndClaimReview atomically claims eligible review work.
func (c *Client) WaitAndClaimReview(ctx context.Context, filters QueueFilters) (Ticket, error) {
	return c.waitAndClaimQueue(ctx, "review", filters)
}

func (c *Client) waitAndClaimQueue(ctx context.Context, queue string, filters QueueFilters) (Ticket, error) {
	if queue != "open" && queue != "review" {
		return Ticket{}, fmt.Errorf("ticket queue must be open or review")
	}
	filters, err := CanonicalQueueFilters(filters)
	if err != nil {
		return Ticket{}, err
	}
	if len(filters.WithoutTags) == 0 {
		args := []string{"wait"}
		if queue == "review" {
			args = append(args, "review")
		}
		args = appendTagArgs(args, filters.Tags)
		args = append(args, "--claim")
		return c.waitAndClaim(ctx, queue, args)
	}
	// wait supports positive tags but not exclusions. Use Ticket's atomic
	// next-and-claim operation and poll when an exclusion is required.
	args := []string{"next"}
	if queue == "review" {
		args = append(args, "review")
	}
	args = appendTagArgs(args, filters.Tags)
	args = appendWithoutTagArgs(args, filters.WithoutTags)
	args = append(args, "--claim")
	for {
		var response struct {
			Item *Ticket `json:"item"`
		}
		if err := c.invoke(ctx, args, &response); err != nil {
			conflict, verificationErr := c.verifyActorQueueConflict(ctx, err, queue)
			if verificationErr != nil {
				return Ticket{}, verificationErr
			}
			if conflict != nil {
				return Ticket{}, conflict
			}
			return Ticket{}, err
		}
		if response.Item != nil {
			item := response.Item.NormalizeState()
			if err := validateFullID(item.ID); err != nil {
				return Ticket{}, fmt.Errorf("%w: claimed %s ticket: %v", ErrProtocol, queue, err)
			}
			if item.State != queue {
				return Ticket{}, fmt.Errorf("%w: claimed %s ticket has state %q", ErrProtocol, queue, item.State)
			}
			if item.Assignee != c.actor {
				return Ticket{}, fmt.Errorf("%w: claimed item belongs to actor %q, expected %q", ErrProtocol, item.Assignee, c.actor)
			}
			return item, nil
		}
		timer := time.NewTimer(filteredQueuePoll)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return Ticket{}, ctx.Err()
		}
	}
}

// HasReady reports whether Ticket's authoritative ready frontier contains
// work for one queue without claiming it. The ready command owns the queue
// semantics; Orc only consumes its boolean result for role backpressure.
func (c *Client) HasReady(ctx context.Context, queue string, filters QueueFilters) (bool, error) {
	return c.hasReady(ctx, queue, filters)
}

func (c *Client) hasReady(ctx context.Context, queue string, filters QueueFilters) (bool, error) {
	frontier, err := c.ReadyFrontier(ctx, queue, filters, 1)
	if err != nil {
		return false, err
	}
	return len(frontier.Items) > 0, nil
}

// HasActiveClaim reports whether this actor owns active work in one queue.
// Ownership is observed through Ticket's public JSON list interface; queue
// selection itself remains delegated to HasReady/WaitAndClaim.
func (c *Client) HasActiveClaim(ctx context.Context, queue string) (bool, error) {
	if queue != "open" && queue != "review" {
		return false, fmt.Errorf("ticket queue must be open or review")
	}
	var response struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	args := []string{"list", "--state", queue, "--assignee", c.actor, "--limit", "1", "--fields", "id"}
	if err := c.invoke(ctx, args, &response); err != nil {
		return false, err
	}
	return len(response.Items) > 0, nil
}

// ReadyFrontier returns Ticket's authoritative, bounded ready ordering without
// claiming any item. Ticket owns eligibility, filtering, and rank semantics.
func (c *Client) ReadyFrontier(ctx context.Context, queue string, filters QueueFilters, limit int) (ListResult, error) {
	if queue != "open" && queue != "review" {
		return ListResult{}, fmt.Errorf("ticket queue must be open or review")
	}
	if limit < 1 || limit > 20 {
		return ListResult{}, fmt.Errorf("ticket ready frontier limit must be between 1 and 20")
	}
	filters, err := CanonicalQueueFilters(filters)
	if err != nil {
		return ListResult{}, err
	}
	args := appendTagArgs([]string{"ready", queue}, filters.Tags)
	args = appendWithoutTagArgs(args, filters.WithoutTags)
	args = append(args, "--limit", fmt.Sprint(limit), "--fields", "id,title,state,assignee,priority")
	var response ListResult
	if err := c.invoke(ctx, args, &response); err != nil {
		return ListResult{}, err
	}
	if len(response.Items) > limit {
		return ListResult{}, fmt.Errorf("%w: ready frontier exceeds requested limit", ErrProtocol)
	}
	for i := range response.Items {
		response.Items[i] = response.Items[i].NormalizeState()
		if err := validateFullID(response.Items[i].ID); err != nil {
			return ListResult{}, fmt.Errorf("%w: ready %s item: %v", ErrProtocol, queue, err)
		}
		if response.Items[i].State != queue {
			return ListResult{}, fmt.Errorf("%w: ready %s item %q has state %q", ErrProtocol, queue, response.Items[i].ID, response.Items[i].State)
		}
	}
	return response, nil
}

// ActiveClaims returns a bounded list of this actor's active claims in one
// queue. It deliberately has no selector because ownership is actor-wide.
func (c *Client) ActiveClaims(ctx context.Context, queue string, limit int) (ListResult, error) {
	if queue != "open" && queue != "review" {
		return ListResult{}, fmt.Errorf("ticket queue must be open or review")
	}
	if limit < 1 || limit > 20 {
		return ListResult{}, fmt.Errorf("ticket active claim limit must be between 1 and 20")
	}
	args := []string{"list", "--state", queue, "--assignee", c.actor}
	args = append(args, "--limit", fmt.Sprint(limit), "--fields", "id,title,state,assignee,priority")
	var response ListResult
	if err := c.invoke(ctx, args, &response); err != nil {
		return ListResult{}, err
	}
	if len(response.Items) > limit {
		return ListResult{}, fmt.Errorf("%w: active claim list exceeds requested limit", ErrProtocol)
	}
	for i := range response.Items {
		response.Items[i] = response.Items[i].NormalizeState()
		if err := validateFullID(response.Items[i].ID); err != nil {
			return ListResult{}, fmt.Errorf("%w: active %s item: %v", ErrProtocol, queue, err)
		}
		if response.Items[i].State != queue || response.Items[i].Assignee != c.actor {
			return ListResult{}, fmt.Errorf("%w: active %s item %q has inconsistent state or assignee", ErrProtocol, queue, response.Items[i].ID)
		}
	}
	return response, nil
}

// ListOwned returns the actor's currently assigned tickets in one lifecycle
// queue. It is an observation primitive for role-scoped safety policy; queue
// selection and claiming remain owned by Ticket's ready/wait commands.
func (c *Client) ListOwned(ctx context.Context, queue string) ([]Ticket, error) {
	if queue != "open" && queue != "review" && queue != "signoff" {
		return nil, fmt.Errorf("ticket queue must be open, review, or signoff")
	}
	const limit = 256
	var response struct {
		Items []Ticket `json:"items"`
		More  bool     `json:"more"`
	}
	args := []string{"list", "--state", queue, "--assignee", c.actor, "--limit", fmt.Sprint(limit), "--fields", "id,state,assignee"}
	if err := c.invoke(ctx, args, &response); err != nil {
		return nil, err
	}
	if response.More {
		return nil, fmt.Errorf("ticket %s ownership observation exceeds bounded limit %d", queue, limit)
	}
	for i := range response.Items {
		response.Items[i] = response.Items[i].NormalizeState()
		if err := validateFullID(response.Items[i].ID); err != nil {
			return nil, fmt.Errorf("%w: owned %s item: %v", ErrProtocol, queue, err)
		}
		if response.Items[i].State != queue || response.Items[i].Assignee != c.actor {
			return nil, fmt.Errorf("%w: owned %s item %q has inconsistent state or assignee", ErrProtocol, queue, response.Items[i].ID)
		}
	}
	return response.Items, nil
}

// ListUnassigned observes unassigned tickets in a lifecycle queue. Orc uses
// this only for signoff tickets whose prior reviewer ownership was recorded
// before Ticket's approve transition cleared the assignee.
func (c *Client) ListUnassigned(ctx context.Context, queue string) ([]Ticket, error) {
	if queue != "signoff" {
		return nil, fmt.Errorf("unassigned observation supports only signoff")
	}
	const limit = 256
	var response struct {
		Items []Ticket `json:"items"`
		More  bool     `json:"more"`
	}
	args := []string{"list", "--state", queue, "--unassigned", "--limit", fmt.Sprint(limit), "--fields", "id,state,assignee"}
	if err := c.invoke(ctx, args, &response); err != nil {
		return nil, err
	}
	if response.More {
		return nil, fmt.Errorf("ticket %s unassigned observation exceeds bounded limit %d", queue, limit)
	}
	for i := range response.Items {
		response.Items[i] = response.Items[i].NormalizeState()
		if err := validateFullID(response.Items[i].ID); err != nil {
			return nil, fmt.Errorf("%w: unassigned %s item: %v", ErrProtocol, queue, err)
		}
		if response.Items[i].State != queue || response.Items[i].Assignee != "" {
			return nil, fmt.Errorf("%w: unassigned %s item %q has inconsistent state or assignee", ErrProtocol, queue, response.Items[i].ID)
		}
	}
	return response.Items, nil
}

func (c *Client) waitAndClaim(ctx context.Context, expectedState string, args []string) (Ticket, error) {
	var response struct {
		Item *Ticket `json:"item"`
	}
	if err := c.invoke(ctx, args, &response); err != nil {
		conflict, verificationErr := c.verifyActorQueueConflict(ctx, err, expectedState)
		if verificationErr != nil {
			return Ticket{}, verificationErr
		}
		if conflict != nil {
			return Ticket{}, conflict
		}
		return Ticket{}, err
	}
	if response.Item == nil {
		return Ticket{}, fmt.Errorf("%w: wait returned no claimed item", ErrProtocol)
	}
	item := *response.Item
	item = item.NormalizeState()
	if err := validateFullID(item.ID); err != nil {
		return Ticket{}, fmt.Errorf("%w: claimed item: %v", ErrProtocol, err)
	}
	if item.State != expectedState {
		return Ticket{}, fmt.Errorf("%w: claimed %s ticket has state %q", ErrProtocol, expectedState, item.State)
	}
	if item.Assignee != c.actor {
		return Ticket{}, fmt.Errorf("%w: claimed item belongs to actor %q, expected %q", ErrProtocol, item.Assignee, c.actor)
	}
	return item, nil
}

func (c *Client) verifyActorQueueConflict(ctx context.Context, err error, requestedQueue string) (*ActorQueueConflictError, error) {
	candidate := actorQueueConflictCandidate(err, requestedQueue)
	if candidate == nil {
		return nil, nil
	}
	observed, verifyErr := c.Show(ctx, candidate.TicketID)
	if verifyErr != nil {
		return nil, fmt.Errorf("verify Ticket queue conflict state: %w", errors.Join(err, verifyErr))
	}
	if observed.State != candidate.ConflictingQueue {
		return nil, nil
	}
	candidate.Cause = err
	return candidate, nil
}

const filteredQueuePoll = 100 * time.Millisecond

func appendTagArgs(args, tags []string) []string {
	result := append([]string(nil), args...)
	for _, tag := range tags {
		result = append(result, "--tag", tag)
	}
	return result
}

func appendWithoutTagArgs(args, tags []string) []string {
	result := append([]string(nil), args...)
	for _, tag := range tags {
		result = append(result, "--without-tag", tag)
	}
	return result
}

func validTagToken(tag string) bool {
	if tag == "" || len(tag) > 64 || ((tag[0] < 'a' || tag[0] > 'z') && (tag[0] < '0' || tag[0] > '9')) {
		return false
	}
	for _, char := range tag {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}
