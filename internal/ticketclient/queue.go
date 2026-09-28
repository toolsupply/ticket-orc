package ticketclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// WaitAndClaimImplementation blocks until ticket atomically claims eligible
// implementation work for the client's actor.
func (c *Client) WaitAndClaimImplementation(ctx context.Context) (Ticket, error) {
	return c.waitAndClaim(ctx, "open", []string{"wait", "--claim"})
}

// WaitAndClaimReview blocks until ticket atomically claims eligible review
// work for the client's actor.
func (c *Client) WaitAndClaimReview(ctx context.Context) (Ticket, error) {
	return c.waitAndClaim(ctx, "review", []string{"wait", "review", "--claim"})
}

// WaitAndClaimReviewWithoutTags keeps configured Orc review-skip tags out of
// the automatic reviewer queue. Ticket still owns queue readiness and claims.
func (c *Client) WaitAndClaimReviewWithoutTags(ctx context.Context, tags []string) (Ticket, error) {
	args, err := appendWithoutTagArgs([]string{"next", "review"}, tags)
	if err != nil {
		return Ticket{}, err
	}
	// Ticket v0.2 exposes --without-tag on next. Keep selection and claim in
	// Ticket's one atomic queue operation so a concurrent tag or ownership
	// change cannot leave Orc holding an excluded review ticket.
	args = append(args, "--claim")
	for {
		var response struct {
			Item *Ticket `json:"item"`
		}
		if err := c.invoke(ctx, args, &response); err != nil {
			conflict, verificationErr := c.verifyActorQueueConflict(ctx, err, "review")
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
				return Ticket{}, fmt.Errorf("%w: claimed review ticket: %v", ErrProtocol, err)
			}
			if item.State != "review" {
				return Ticket{}, fmt.Errorf("%w: claimed review ticket has state %q", ErrProtocol, item.State)
			}
			if item.Assignee != c.actor {
				return Ticket{}, fmt.Errorf("%w: claimed item belongs to actor %q, expected %q", ErrProtocol, item.Assignee, c.actor)
			}
			return item, nil
		}
		timer := time.NewTimer(filteredReviewPoll)
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
func (c *Client) HasReady(ctx context.Context, queue string) (bool, error) {
	return c.hasReady(ctx, queue, nil)
}

// HasReadyWithoutTags observes Ticket readiness while excluding configured
// review-policy tags from the automatic reviewer queue.
func (c *Client) HasReadyWithoutTags(ctx context.Context, queue string, tags []string) (bool, error) {
	return c.hasReady(ctx, queue, tags)
}

func (c *Client) hasReady(ctx context.Context, queue string, tags []string) (bool, error) {
	frontier, err := c.ReadyFrontier(ctx, queue, tags, 1)
	if err != nil {
		return false, err
	}
	return len(frontier.Items) > 0, nil
}

// HasActiveClaim reports whether this actor owns active work in one queue.
// Ownership is observed through Ticket's public JSON list interface; queue
// selection itself remains delegated to HasReady/WaitAndClaim.
func (c *Client) HasActiveClaim(ctx context.Context, queue string) (bool, error) {
	return c.hasActiveClaim(ctx, queue, nil)
}

// HasActiveClaimWithoutTags observes this actor's active queue work while
// excluding configured review-policy tags.
func (c *Client) HasActiveClaimWithoutTags(ctx context.Context, queue string, tags []string) (bool, error) {
	return c.hasActiveClaim(ctx, queue, tags)
}

// ReadyFrontier returns Ticket's authoritative, bounded ready ordering without
// claiming any item. Ticket owns eligibility, filtering, and rank semantics.
func (c *Client) ReadyFrontier(ctx context.Context, queue string, tags []string, limit int) (ListResult, error) {
	if queue != "open" && queue != "review" {
		return ListResult{}, fmt.Errorf("ticket queue must be open or review")
	}
	if limit < 1 || limit > 20 {
		return ListResult{}, fmt.Errorf("ticket ready frontier limit must be between 1 and 20")
	}
	args, err := appendWithoutTagArgs([]string{"ready", queue}, tags)
	if err != nil {
		return ListResult{}, err
	}
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
// queue. Review policy tags are filtered the same way as the ready frontier.
func (c *Client) ActiveClaims(ctx context.Context, queue string, tags []string, limit int) (ListResult, error) {
	if queue != "open" && queue != "review" {
		return ListResult{}, fmt.Errorf("ticket queue must be open or review")
	}
	if limit < 1 || limit > 20 {
		return ListResult{}, fmt.Errorf("ticket active claim limit must be between 1 and 20")
	}
	args, err := appendWithoutTagArgs([]string{"list", "--state", queue, "--assignee", c.actor}, tags)
	if err != nil {
		return ListResult{}, err
	}
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

func (c *Client) hasActiveClaim(ctx context.Context, queue string, tags []string) (bool, error) {
	if queue != "open" && queue != "review" {
		return false, fmt.Errorf("ticket queue must be open or review")
	}
	var response struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	args, err := appendWithoutTagArgs([]string{"list", "--state", queue, "--assignee", c.actor}, tags)
	if err != nil {
		return false, err
	}
	args = append(args, "--limit", "1", "--fields", "id")
	if err := c.invoke(ctx, args, &response); err != nil {
		return false, err
	}
	return len(response.Items) > 0, nil
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

const filteredReviewPoll = 100 * time.Millisecond

func appendWithoutTagArgs(args, tags []string) ([]string, error) {
	result := append([]string(nil), args...)
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if strings.TrimSpace(tag) == "" || strings.TrimSpace(tag) != tag || !validTagToken(tag) {
			return nil, fmt.Errorf("ticket tag filter is invalid")
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		result = append(result, "--without-tag", tag)
	}
	return result, nil
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
