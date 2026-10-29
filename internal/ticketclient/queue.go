package ticketclient

import (
	"context"
	"encoding/json"
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

// NextAndClaimReview performs Ticket's nonblocking atomic review selection.
// A nil item means the queue is empty; callers may then wait on a change watch.
func (c *Client) NextAndClaimReview(ctx context.Context, filters QueueFilters) (*Ticket, error) {
	return c.nextAndClaimQueue(ctx, "review", filters)
}

func (c *Client) nextAndClaimQueue(ctx context.Context, queue string, filters QueueFilters) (*Ticket, error) {
	if queue != "open" && queue != "review" {
		return nil, fmt.Errorf("ticket queue must be open or review")
	}
	filters, err := CanonicalQueueFilters(filters)
	if err != nil {
		return nil, err
	}
	args := []string{"next"}
	if queue == "review" {
		args = append(args, "review")
	}
	args = appendTagArgs(args, filters.Tags)
	args = appendWithoutTagArgs(args, filters.WithoutTags)
	args = append(args, "--claim")
	var response struct {
		Item *Ticket `json:"item"`
	}
	if err := c.invoke(ctx, args, &response); err != nil {
		conflict, verificationErr := c.verifyActorQueueConflict(ctx, err, queue)
		if verificationErr != nil {
			return nil, verificationErr
		}
		if conflict != nil {
			return nil, conflict
		}
		return nil, err
	}
	if response.Item == nil {
		return nil, nil
	}
	item := response.Item.NormalizeState()
	if err := validateFullID(item.ID); err != nil {
		return nil, fmt.Errorf("%w: claimed %s ticket: %v", ErrProtocol, queue, err)
	}
	if item.State != queue || item.Assignee != c.actor {
		return nil, fmt.Errorf("%w: claimed %s item has state %q and actor %q, expected %s and %q", ErrProtocol, queue, item.State, item.Assignee, queue, c.actor)
	}
	return &item, nil
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
	for {
		item, err := c.nextAndClaimQueue(ctx, queue, filters)
		if err != nil {
			return Ticket{}, err
		}
		if item != nil {
			return *item, nil
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

const (
	maxSignoffFrontierLimit = 20
)

// SignoffFrontier returns a bounded authoritative observation of unassigned
// signoff tickets matching the required and excluded tags. Ticket applies all
// filters before limit and offset. Unlike ListUnassigned's recovery completeness
// check, More is valid and reports
// further matches. Empty assignees may be omitted by Ticket's public list
// projection; --unassigned owns that predicate and any returned assignee must
// be empty.
func (c *Client) SignoffFrontier(ctx context.Context, filters QueueFilters, limit, offset int) (ListResult, error) {
	return c.unassignedFrontier(ctx, "signoff", filters, limit, offset)
}

// HoldFrontier observes a bounded page of unassigned holds. Ticket applies state,
// ownership, and tag predicates before pagination. Recheck each exact ID through
// Show; entries that have left hold or acquired an owner are omitted. More still
// describes the original list page. This observation never claims work; callers
// must reread state and ownership immediately before dispatch or mutation.
func (c *Client) HoldFrontier(ctx context.Context, filters QueueFilters, limit, offset int) (ListResult, error) {
	frontier, err := c.unassignedFrontier(ctx, "hold", filters, limit, offset)
	if err != nil {
		return ListResult{}, err
	}
	items := make([]Ticket, 0, len(frontier.Items))
	for _, candidate := range frontier.Items {
		current, err := c.Show(ctx, candidate.ID)
		if err != nil {
			return ListResult{}, fmt.Errorf("recheck hold ticket %s: %w", candidate.ID, err)
		}
		if current.State == "hold" && current.Assignee == "" {
			items = append(items, current)
		}
	}
	frontier.Items = items
	return frontier, nil
}

// unassignedFrontier shares the finite hold/signoff observation protocol, not
// Ticket's ready selection or claiming policy.
func (c *Client) unassignedFrontier(ctx context.Context, queue string, filters QueueFilters, limit, offset int) (ListResult, error) {
	if limit < 1 || limit > maxSignoffFrontierLimit {
		return ListResult{}, fmt.Errorf("ticket %s frontier limit must be between 1 and %d", queue, maxSignoffFrontierLimit)
	}
	if offset < 0 {
		return ListResult{}, fmt.Errorf("ticket %s frontier offset must not be negative", queue)
	}
	filters, err := CanonicalQueueFilters(filters)
	if err != nil {
		return ListResult{}, err
	}
	args := appendTagArgs([]string{"list", "--state", queue, "--unassigned"}, filters.Tags)
	args = appendWithoutTagArgs(args, filters.WithoutTags)
	args = append(args, "--limit", fmt.Sprint(limit), "--offset", fmt.Sprint(offset), "--fields", "id,state,assignee")
	var response struct {
		Items json.RawMessage `json:"items"`
		More  *bool           `json:"more"`
	}
	if err := c.invoke(ctx, args, &response); err != nil {
		return ListResult{}, err
	}
	if len(response.Items) == 0 || response.More == nil {
		return ListResult{}, fmt.Errorf("%w: %s frontier response is missing items or more", ErrProtocol, queue)
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(response.Items, &rawItems); err != nil || rawItems == nil {
		return ListResult{}, fmt.Errorf("%w: %s frontier items must be an array", ErrProtocol, queue)
	}
	if len(rawItems) > limit {
		return ListResult{}, fmt.Errorf("%w: %s frontier exceeds requested limit", ErrProtocol, queue)
	}
	result := ListResult{Items: make([]Ticket, 0, len(rawItems)), More: *response.More}
	seen := make(map[string]struct{}, len(rawItems))
	for _, raw := range rawItems {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return ListResult{}, fmt.Errorf("%w: %s frontier item must be an object", ErrProtocol, queue)
		}
		for _, name := range []string{"id", "state"} {
			value, ok := fields[name]
			if !ok || len(value) == 0 {
				return ListResult{}, fmt.Errorf("%w: %s frontier item is missing %q", ErrProtocol, queue, name)
			}
			var text *string
			if err := json.Unmarshal(value, &text); err != nil || text == nil {
				return ListResult{}, fmt.Errorf("%w: %s frontier item field %q must be a string", ErrProtocol, queue, name)
			}
		}
		if value, ok := fields["assignee"]; ok {
			var assignee *string
			if err := json.Unmarshal(value, &assignee); err != nil || assignee == nil {
				return ListResult{}, fmt.Errorf("%w: %s frontier item field %q must be a string", ErrProtocol, queue, "assignee")
			}
			if *assignee != "" {
				return ListResult{}, fmt.Errorf("%w: %s frontier item is assigned to %q", ErrProtocol, queue, *assignee)
			}
		}
		var item Ticket
		if err := json.Unmarshal(raw, &item); err != nil {
			return ListResult{}, fmt.Errorf("%w: decode %s frontier item: %v", ErrProtocol, queue, err)
		}
		item = item.NormalizeState()
		if err := validateFullID(item.ID); err != nil {
			return ListResult{}, fmt.Errorf("%w: %s frontier item: %v", ErrProtocol, queue, err)
		}
		if _, ok := seen[item.ID]; ok {
			return ListResult{}, fmt.Errorf("%w: duplicate %s frontier item %q", ErrProtocol, queue, item.ID)
		}
		seen[item.ID] = struct{}{}
		if item.State != queue || item.Assignee != "" {
			return ListResult{}, fmt.Errorf("%w: %s frontier item %q is not unassigned %s work", ErrProtocol, queue, item.ID, queue)
		}
		result.Items = append(result.Items, item)
	}
	return result, nil
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
