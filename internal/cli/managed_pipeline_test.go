package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const managedPipelineTicketID = "20261006-50698"
const managedPipelineRepositoryID = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"

type managedPipelineTickets struct {
	mu                sync.Mutex
	ticket            ticketclient.Ticket
	otherTickets      map[string]ticketclient.Ticket
	implementation    int
	reviews           int
	acceptanceQueries int
	approvals         int
	claimedActors     []string
	selectorByQueue   map[string]ticketclient.QueueFilters
}

func (tickets *managedPipelineTickets) WaitAndClaimImplementation(_ context.Context, filters ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	tickets.selectorByQueue["open"] = filters
	if tickets.implementation != 0 {
		return ticketclient.Ticket{}, context.Canceled
	}
	for _, item := range tickets.itemsLocked() {
		if item.State != "open" || item.Assignee != "" || !pipelineMatches(item, filters) {
			continue
		}
		item.Assignee = "api-implementer"
		tickets.setItemLocked(item)
		tickets.implementation++
		tickets.claimedActors = append(tickets.claimedActors, item.Assignee)
		return item, nil
	}
	return ticketclient.Ticket{}, context.Canceled
}

func (tickets *managedPipelineTickets) WaitAndClaimReview(_ context.Context, filters ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	tickets.selectorByQueue["review"] = filters
	if tickets.reviews != 0 {
		return ticketclient.Ticket{}, context.Canceled
	}
	for _, item := range tickets.itemsLocked() {
		if item.State != "review" || item.Assignee != "" || !pipelineMatches(item, filters) {
			continue
		}
		item.Assignee = "api-reviewer"
		tickets.setItemLocked(item)
		tickets.reviews++
		tickets.claimedActors = append(tickets.claimedActors, item.Assignee)
		return item, nil
	}
	return ticketclient.Ticket{}, context.Canceled
}

func (tickets *managedPipelineTickets) Show(_ context.Context, id string) (ticketclient.Ticket, error) {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	if id == tickets.ticket.ID {
		return tickets.ticket, nil
	}
	if item, ok := tickets.otherTickets[id]; ok {
		return item, nil
	}
	return ticketclient.Ticket{}, errors.New("unexpected ticket ID")
}

func (tickets *managedPipelineTickets) Release(context.Context, string) (ticketclient.Transition, error) {
	return ticketclient.Transition{}, nil
}

func (tickets *managedPipelineTickets) CloseTicket(_ context.Context, id string) (ticketclient.Transition, error) {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	if id != tickets.ticket.ID || tickets.ticket.State != "signoff" {
		return ticketclient.Transition{}, errors.New("close requires the signoff ticket")
	}
	tickets.ticket.State = "closed"
	tickets.ticket.Assignee = ""
	return ticketclient.Transition{ID: id, Changed: true, FromState: "signoff", State: "closed"}, nil
}

func (tickets *managedPipelineTickets) ApproveTicket(_ context.Context, id string) (ticketclient.MutationResult, error) {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	if id != tickets.ticket.ID || tickets.ticket.State != "review" {
		return ticketclient.MutationResult{}, errors.New("approval requires the review ticket")
	}
	tickets.approvals++
	tickets.ticket.State = "signoff"
	tickets.ticket.Assignee = ""
	return ticketclient.MutationResult{ID: id, Changed: true, FromState: "review", State: "signoff"}, nil
}

func (tickets *managedPipelineTickets) SignoffFrontier(_ context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	tickets.selectorByQueue["signoff"] = filters
	tickets.acceptanceQueries++
	if offset != 0 || limit < 1 {
		return ticketclient.ListResult{}, nil
	}
	var eligible []ticketclient.Ticket
	for _, item := range tickets.itemsLocked() {
		if item.State == "signoff" && item.Assignee == "" && pipelineMatches(item, filters) {
			eligible = append(eligible, item)
		}
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].ID < eligible[j].ID })
	return ticketclient.ListResult{Items: eligible}, nil
}

func (*managedPipelineTickets) StartChangeWatch(_ context.Context, repositoryID string) (ticketclient.ChangeWatcher, error) {
	if repositoryID != managedPipelineRepositoryID {
		return nil, errors.New("unexpected repository identity")
	}
	return managedPipelineWatch{}, nil
}

func (tickets *managedPipelineTickets) transition(from, to string) error {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	if tickets.ticket.State != from {
		return errors.New("unexpected lifecycle state")
	}
	tickets.ticket.State = to
	tickets.ticket.Assignee = ""
	return nil
}

func (tickets *managedPipelineTickets) transitionTo(from, to, assignee string) error {
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	if tickets.ticket.State != from {
		return errors.New("unexpected lifecycle state")
	}
	tickets.ticket.State = to
	tickets.ticket.Assignee = assignee
	return nil
}

func (tickets *managedPipelineTickets) itemsLocked() []ticketclient.Ticket {
	items := make([]ticketclient.Ticket, 0, len(tickets.otherTickets)+1)
	items = append(items, tickets.ticket)
	for _, item := range tickets.otherTickets {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items
}

func (tickets *managedPipelineTickets) setItemLocked(item ticketclient.Ticket) {
	if item.ID == tickets.ticket.ID {
		tickets.ticket = item
		return
	}
	tickets.otherTickets[item.ID] = item
}

type managedPipelineWatch struct{}

func (managedPipelineWatch) Check() error { return nil }
func (managedPipelineWatch) WaitForChange(ctx context.Context) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}
func (managedPipelineWatch) Close() error { return nil }

type managedPipelineCleanup struct{ cleaned []string }

func (cleanup *managedPipelineCleanup) CleanupConfirmed(_ context.Context, ticket ticketclient.Ticket) (orc.CleanupResult, error) {
	cleanup.cleaned = append(cleanup.cleaned, ticket.ID)
	return orc.CleanupResult{}, nil
}

func TestManagedThreeStagePipelineAcrossResolvedConfigAndOrc(t *testing.T) {
	resolved := loadManagedThreeStageConfig(t, nil)
	workers := make(map[string]supervisor.RoleConfig, len(resolved.Workers))
	for _, worker := range resolved.Workers {
		workers[worker.Config.TicketQueue] = worker.Config
	}
	openRole, reviewRole, signoffRole := workers["open"], workers["review"], workers["signoff"]
	if openRole.RoleName != "coder-api" || reviewRole.RoleName != "reviewer-api" || signoffRole.RoleName != "architect-api" {
		t.Fatalf("resolved roles = %q, %q, %q", openRole.RoleName, reviewRole.RoleName, signoffRole.RoleName)
	}
	filters := func(config supervisor.RoleConfig) ticketclient.QueueFilters {
		return ticketclient.QueueFilters{Tags: splitTicketTags(config.TicketTags), WithoutTags: splitTicketTags(config.TicketWithoutTags)}
	}
	tickets := &managedPipelineTickets{
		ticket: ticketclient.Ticket{ID: managedPipelineTicketID, State: "open", Tags: []string{"api"}},
		otherTickets: map[string]ticketclient.Ticket{
			"20261006-50690": {ID: "20261006-50690", State: "open", Tags: []string{"frontend"}},
			"20261006-50691": {ID: "20261006-50691", State: "open", Tags: []string{"api", "blocked"}},
			"20261006-50692": {ID: "20261006-50692", State: "review", Tags: []string{"frontend"}},
			"20261006-50693": {ID: "20261006-50693", State: "review", Tags: []string{"api", "blocked"}},
			"20261006-50694": {ID: "20261006-50694", State: "review", Tags: []string{"api", "no-review"}},
			"20261006-50695": {ID: "20261006-50695", State: "signoff", Tags: []string{"frontend"}},
			"20261006-50696": {ID: "20261006-50696", State: "signoff", Tags: []string{"api", "blocked"}},
		},
		selectorByQueue: make(map[string]ticketclient.QueueFilters),
	}
	stateStore := state.NewForRepository(t.TempDir(), managedPipelineRepositoryID)
	cleanup := &managedPipelineCleanup{}
	var runs []harness.RunRequest
	runStage := func(config supervisor.RoleConfig, transitionFrom, transitionTo string) (*fakeManagedPipelineHarness, context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		agent := &fakeManagedPipelineHarness{onRun: func(request harness.RunRequest) {
			runs = append(runs, request)
			if request.TicketID != managedPipelineTicketID {
				t.Errorf("worker received ineligible ticket %s for role %s", request.TicketID, request.Role)
				cancel()
				return
			}
			if err := tickets.transition(transitionFrom, transitionTo); err != nil {
				t.Errorf("transition %s -> %s: %v", transitionFrom, transitionTo, err)
			}
			cancel()
		}}
		return agent, ctx, cancel
	}
	implementation, implementationCtx, cancelImplementation := runStage(openRole, "open", "review")
	implementationErr := orc.RunCoder(implementationCtx, orc.CoderConfig{
		WorkerName: openRole.WorkerName, RoleName: openRole.RoleName, Actor: openRole.Actor, Harness: openRole.Harness,
		MaxBounces: openRole.MaxBounces, SessionPolicy: string(supervisor.SessionPolicyFresh), MinimumReuseContextPercent: 20, QueueFilters: filters(openRole),
		ReviewCompletion: openRole.ReviewFinalGate, WorkingDir: openRole.WorkingDir,
	}, tickets, implementation, stateStore, cleanup)
	cancelImplementation()
	if !errors.Is(implementationErr, context.Canceled) {
		t.Fatalf("coder stage error = %v, want cancellation after one completed turn", implementationErr)
	}
	review, reviewCtx, cancelReview := runStage(reviewRole, "review", "signoff")
	reviewErr := orc.RunReviewer(reviewCtx, orc.ReviewerConfig{
		WorkerName: reviewRole.WorkerName, RoleName: reviewRole.RoleName, TicketQueue: reviewRole.TicketQueue,
		Actor: reviewRole.Actor, Harness: reviewRole.Harness, MaxBounces: reviewRole.MaxBounces, SessionPolicy: string(supervisor.SessionPolicyFresh),
		MinimumReuseContextPercent: 20, ReviewCompletion: reviewRole.ReviewCompletion, QueueFilters: filters(reviewRole),
	}, tickets, review, stateStore, cleanup)
	cancelReview()
	if !errors.Is(reviewErr, context.Canceled) {
		t.Fatalf("review stage error = %v, want cancellation after signoff", reviewErr)
	}
	acceptance, acceptanceCtx, cancelAcceptance := runStage(signoffRole, "signoff", "closed")
	acceptanceErr := orc.RunAcceptance(acceptanceCtx, orc.AcceptanceConfig{
		WorkerName: signoffRole.WorkerName, RoleName: signoffRole.RoleName, Actor: signoffRole.Actor,
		Harness: signoffRole.Harness, SessionPolicy: string(supervisor.SessionPolicyFresh),
		MinimumReuseContextPercent: 20, QueueFilters: filters(signoffRole), RepositoryID: managedPipelineRepositoryID,
	}, tickets, acceptance, stateStore, cleanup)
	cancelAcceptance()
	if !errors.Is(acceptanceErr, context.Canceled) {
		t.Fatalf("acceptance stage error = %v, want cancellation after close", acceptanceErr)
	}
	tickets.mu.Lock()
	final := tickets.ticket
	claimedActors := append([]string(nil), tickets.claimedActors...)
	queries := tickets.acceptanceQueries
	selectors := tickets.selectorByQueue
	otherTickets := make(map[string]ticketclient.Ticket, len(tickets.otherTickets))
	for id, item := range tickets.otherTickets {
		otherTickets[id] = item
	}
	tickets.mu.Unlock()
	if final.State != "closed" || final.Assignee != "" || !reflect.DeepEqual(claimedActors, []string{"api-implementer", "api-reviewer"}) {
		t.Fatalf("final ticket=%#v claimed actors=%v; want closed, with no architect claim", final, claimedActors)
	}
	wantSelectors := map[string]ticketclient.QueueFilters{
		"open":    {Tags: []string{"api"}, WithoutTags: []string{"blocked"}},
		"review":  {Tags: []string{"api"}, WithoutTags: []string{"blocked", "no-review"}},
		"signoff": {Tags: []string{"api"}, WithoutTags: []string{"blocked"}},
	}
	for queue, want := range wantSelectors {
		if !reflect.DeepEqual(selectors[queue], want) {
			t.Errorf("%s selector = %#v, want %#v", queue, selectors[queue], want)
		}
	}
	if queries != 1 || !reflect.DeepEqual(cleanup.cleaned, []string{managedPipelineTicketID}) {
		t.Fatalf("acceptance frontier queries=%d cleanup=%v", queries, cleanup.cleaned)
	}
	wantUnchanged := map[string]string{
		"20261006-50690": "open",
		"20261006-50691": "open",
		"20261006-50692": "review",
		"20261006-50693": "review",
		"20261006-50694": "review",
		"20261006-50695": "signoff",
		"20261006-50696": "signoff",
	}
	for id, state := range wantUnchanged {
		item, ok := otherTickets[id]
		if !ok || item.State != state || item.Assignee != "" {
			t.Errorf("ineligible item %s changed: present=%v item=%#v; want state %s and no assignee", id, ok, item, state)
		}
	}
	if len(runs) != 3 {
		t.Fatalf("worker dispatches=%#v; want implementation, review, and signoff for %s", runs, managedPipelineTicketID)
	}
	for _, request := range runs {
		if request.TicketID != managedPipelineTicketID {
			t.Errorf("worker %s received unexpected ticket %s", request.WorkerName, request.TicketID)
		}
	}
	wantPresentation := []string{"coder-api/api-implementer", "reviewer-api/api-reviewer", "architect-api/api-architect"}
	gotPresentation := make([]string, 0, len(runs))
	for _, request := range runs {
		gotPresentation = append(gotPresentation, request.Role+"/"+request.Actor)
	}
	if !reflect.DeepEqual(gotPresentation, wantPresentation) {
		t.Fatalf("role/actor presentation=%v, want %v", gotPresentation, wantPresentation)
	}
	for _, want := range []string{"Review only ticket " + managedPipelineTicketID, "review_completion=signoff", "independently"} {
		if !strings.Contains(runs[1].Prompt, want) {
			t.Errorf("reviewer prompt %q missing independent review instruction %q", runs[1].Prompt, want)
		}
	}
}

func TestManagedCompatibilityPoliciesReachResolvedOrcExecutors(t *testing.T) {
	t.Run("explicit direct review close", func(t *testing.T) {
		resolved := loadManagedThreeStageConfig(t, func(config *FileConfig) {
			role := config.Roles["reviewer-api"]
			role.ReviewCompletion = ReviewCompletionClose
			config.Roles["reviewer-api"] = role
		})
		var role supervisor.RoleConfig
		for _, worker := range resolved.Workers {
			if worker.Config.TicketQueue == "review" {
				role = worker.Config
			}
		}
		if role.ReviewCompletion != ReviewCompletionClose {
			t.Fatalf("resolved review completion=%q, want close", role.ReviewCompletion)
		}
		tickets := &managedPipelineTickets{
			ticket:          ticketclient.Ticket{ID: managedPipelineTicketID, State: "review", Tags: []string{"api"}},
			selectorByQueue: make(map[string]ticketclient.QueueFilters),
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		agent := &fakeManagedPipelineHarness{onRun: func(harness.RunRequest) {
			if err := tickets.transition("review", "signoff"); err != nil {
				t.Errorf("review approval transition: %v", err)
			}
			cancel()
		}}
		cleanup := &managedPipelineCleanup{}
		err := orc.RunReviewer(ctx, orc.ReviewerConfig{
			WorkerName: role.WorkerName, RoleName: role.RoleName, TicketQueue: role.TicketQueue,
			Actor: role.Actor, Harness: role.Harness, MaxBounces: role.MaxBounces,
			SessionPolicy: string(supervisor.SessionPolicyFresh), MinimumReuseContextPercent: 20,
			ReviewCompletion: role.ReviewCompletion,
			QueueFilters:     ticketclient.QueueFilters{Tags: splitTicketTags(role.TicketTags), WithoutTags: splitTicketTags(role.TicketWithoutTags)},
		}, tickets, agent, state.NewForRepository(t.TempDir(), managedPipelineRepositoryID), cleanup)
		if !errors.Is(err, context.Canceled) || tickets.ticket.State != "closed" || !reflect.DeepEqual(cleanup.cleaned, []string{managedPipelineTicketID}) {
			t.Fatalf("direct-close execution error=%v ticket=%#v cleanup=%v", err, tickets.ticket, cleanup.cleaned)
		}
	})

	t.Run("legacy review skip tag is propagated to coder bypass", func(t *testing.T) {
		resolved := loadManagedThreeStageConfig(t, func(config *FileConfig) {
			config.Review.SkipTags = []string{"no-review"}
		})
		var role supervisor.RoleConfig
		for _, worker := range resolved.Workers {
			if worker.Config.TicketQueue == "open" {
				role = worker.Config
			}
		}
		if !reflect.DeepEqual(splitReviewSkipTags(role.ReviewSkipTags), []string{"no-review"}) {
			t.Fatalf("resolved coder ReviewSkipTags=%q", role.ReviewSkipTags)
		}
		tickets := &managedPipelineTickets{
			ticket:          ticketclient.Ticket{ID: managedPipelineTicketID, State: "open", Tags: []string{"api", "no-review"}},
			selectorByQueue: make(map[string]ticketclient.QueueFilters),
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		agent := &fakeManagedPipelineHarness{onRun: func(harness.RunRequest) {
			if err := tickets.transitionTo("open", "review", "api-implementer"); err != nil {
				t.Errorf("legacy review transition: %v", err)
			}
			cancel()
		}}
		err := orc.RunCoder(ctx, orc.CoderConfig{
			WorkerName: role.WorkerName, RoleName: role.RoleName, Actor: role.Actor, Harness: role.Harness,
			MaxBounces: role.MaxBounces, SessionPolicy: string(supervisor.SessionPolicyFresh), MinimumReuseContextPercent: 20,
			ReviewCompletion: role.ReviewFinalGate, ReviewSkipTags: splitReviewSkipTags(role.ReviewSkipTags),
			QueueFilters: ticketclient.QueueFilters{Tags: splitTicketTags(role.TicketTags), WithoutTags: splitTicketTags(role.TicketWithoutTags)},
		}, tickets, agent, state.NewForRepository(t.TempDir(), managedPipelineRepositoryID), nil)
		if !errors.Is(err, context.Canceled) || tickets.ticket.State != "signoff" || tickets.approvals != 1 || tickets.reviews != 0 {
			t.Fatalf("legacy bypass execution error=%v ticket=%#v approvals=%d review claims=%d", err, tickets.ticket, tickets.approvals, tickets.reviews)
		}
	})
}

func loadManagedThreeStageConfig(t *testing.T, mutate func(*FileConfig)) RunConfig {
	t.Helper()
	root := t.TempDir()
	fileConfig := FileConfig{
		Version:     1,
		ID:          "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa",
		DefaultRole: "coder-api",
		Defaults: DefaultsFileConfig{
			Harness: "codex", Output: "quiet", WorkingDir: root,
		},
		Roles: map[string]RoleFileConfig{
			"coder-api": {
				TicketQueue: "open", TicketTags: []string{"api"}, TicketWithoutTags: []string{"blocked"},
				NudgePrompt: "API implementation work is available.",
			},
			"reviewer-api": {
				TicketQueue: "review", TicketTags: []string{"api"}, TicketWithoutTags: []string{"blocked", "no-review"},
				ReviewCompletion: ReviewCompletionSignoff, NudgePrompt: "API review work is available.",
			},
			"architect-api": {
				TicketQueue: "signoff", TicketTags: []string{"api"}, TicketWithoutTags: []string{"blocked"},
				NudgePrompt: "API acceptance work is available.",
			},
		},
		Workers: map[string]WorkerFileConfig{
			"api-coder":     {Role: "coder-api", Actor: "api-implementer", Groups: []string{"three-stage"}},
			"api-reviewer":  {Role: "reviewer-api", Actor: "api-reviewer", Groups: []string{"three-stage"}},
			"api-architect": {Role: "architect-api", Actor: "api-architect", Groups: []string{"three-stage"}},
		},
	}
	if mutate != nil {
		mutate(&fileConfig)
	}
	data, err := json.Marshal(fileConfig)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, help, err := parseRunConfig([]string{"--config", configPath, "--group", "three-stage"}, emptyEnv)
	if help || err != nil {
		t.Fatalf("parseRunConfig(three-stage fixture): help=%v err=%v", help, err)
	}
	return resolved
}

type fakeManagedPipelineHarness struct{ onRun func(harness.RunRequest) }

func (agent *fakeManagedPipelineHarness) Run(_ context.Context, request harness.RunRequest) (harness.RunResult, error) {
	agent.onRun(request)
	return harness.RunResult{}, nil
}
func (*fakeManagedPipelineHarness) Resume(context.Context, string, harness.RunRequest) (harness.RunResult, error) {
	return harness.RunResult{}, errors.New("unexpected resume")
}
func (*fakeManagedPipelineHarness) Cleanup(context.Context, string, harness.CleanupPolicy) error {
	return nil
}

func pipelineMatches(ticket ticketclient.Ticket, filters ticketclient.QueueFilters) bool {
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
