package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const repositoryGatewayActor = "ticket-orc"

// repositoryGateway uses the supervisor's validated registry for every read.
// Callers provide only a Ticket repository ID; the resolved Ticket target never comes from
// an HTTP path, query, or current working directory.
func (m *workerManager) repositoryGateway() daemon.RepositoryGateway {
	if m == nil {
		return daemon.RepositoryGateway{}
	}
	return daemon.RepositoryGateway{
		ListTickets:           m.listRepositoryTickets,
		GetTicket:             m.getRepositoryTicket,
		CreateTicket:          m.createRepositoryTicket,
		UpdateTicket:          m.updateRepositoryTicket,
		MutateTicket:          m.mutateRepositoryTicket,
		TicketBodyBudgetBytes: ticketclient.RepositoryDetailBodyBudgetBytes,
	}
}

func (m *workerManager) repositoryTarget(repositoryID string) (supervisor.ConfiguredRepository, error) {
	if m == nil {
		return supervisor.ConfiguredRepository{}, &daemon.RepositoryReadError{Code: "repository_unavailable", Status: 503, Message: "repository service is unavailable"}
	}
	m.configMu.Lock()
	repository, ok := m.repositoriesByID[repositoryID]
	m.configMu.Unlock()
	if !ok {
		return supervisor.ConfiguredRepository{}, &daemon.RepositoryReadError{Code: "repository_not_found", Status: 404, Message: "unknown repository"}
	}
	return repository, nil
}

func (m *workerManager) listRepositoryTickets(ctx context.Context, repositoryID string, query daemon.RepositoryTicketQuery) (daemon.RepositoryTicketList, error) {
	repository, err := m.repositoryTarget(repositoryID)
	if err != nil {
		return daemon.RepositoryTicketList{}, err
	}
	limit := query.Limit
	if limit == 0 {
		limit = 50
	}
	client, err := ticketclient.NewWithTarget(repositoryGatewayActor, ticketTargetFromConfiguredRepository(repository))
	if err != nil {
		return daemon.RepositoryTicketList{}, repositoryReadFailure(err)
	}
	defer client.Close()
	result, err := listRepositoryTicketsFromClient(ctx, repositoryID, repository.Key, query, client, limit)
	if err != nil {
		return daemon.RepositoryTicketList{}, err
	}
	return result, nil
}

type repositoryTicketReader interface {
	ListTickets(context.Context, ticketclient.ListQuery) (ticketclient.ListResult, error)
	SearchTicketCandidates(context.Context, string) (ticketclient.ListResult, error)
}

func listRepositoryTicketsFromClient(ctx context.Context, repositoryID, key string, query daemon.RepositoryTicketQuery, client repositoryTicketReader, limit int) (daemon.RepositoryTicketList, error) {
	var candidateIDs []string
	if strings.TrimSpace(query.Search) != "" {
		candidates, err := client.SearchTicketCandidates(ctx, query.Search)
		if err != nil {
			return daemon.RepositoryTicketList{}, repositoryReadFailure(err)
		}
		candidateIDs = make([]string, 0, len(candidates.Items))
		for _, candidate := range candidates.Items {
			candidateIDs = append(candidateIDs, candidate.ID)
		}
		if len(candidateIDs) == 0 {
			return daemon.RepositoryTicketList{RepositoryID: repositoryID, RepositoryKey: key, Items: []daemon.RepositoryTicket{}, More: false}, nil
		}
	}
	result, err := client.ListTickets(ctx, ticketclient.ListQuery{
		IDs: candidateIDs, States: query.States, Priority: query.Priority, Assignee: query.Assignee,
		Tags: query.Tags, Limit: limit, Offset: query.Offset,
	})
	if err != nil {
		return daemon.RepositoryTicketList{}, repositoryReadFailure(err)
	}
	items := make([]daemon.RepositoryTicket, 0, len(result.Items))
	for _, ticket := range result.Items {
		items = append(items, repositoryTicketView(ticket))
	}
	return daemon.RepositoryTicketList{RepositoryID: repositoryID, RepositoryKey: key, Items: items, More: result.More}, nil
}

func (m *workerManager) getRepositoryTicket(ctx context.Context, repositoryID, id string) (daemon.RepositoryTicketDetail, error) {
	repository, err := m.repositoryTarget(repositoryID)
	if err != nil {
		return daemon.RepositoryTicketDetail{}, err
	}
	if err := ticketclient.ValidateFullID(id); err != nil {
		return daemon.RepositoryTicketDetail{}, &daemon.RepositoryReadError{Code: "invalid_ticket", Status: 400, Message: "ticket ID is invalid"}
	}
	client, err := ticketclient.NewWithTarget(repositoryGatewayActor, ticketTargetFromConfiguredRepository(repository))
	if err != nil {
		return daemon.RepositoryTicketDetail{}, repositoryReadFailure(err)
	}
	defer client.Close()
	result, err := repositoryTicketDetailFromClient(ctx, id, client)
	if err != nil {
		return daemon.RepositoryTicketDetail{}, err
	}
	return daemon.RepositoryTicketDetail{RepositoryID: repository.ID, RepositoryKey: repository.Key, RepositoryTicket: result}, nil
}

func repositoryTicketDetailFromClient(ctx context.Context, id string, client repositoryTicketDetailReader) (daemon.RepositoryTicket, error) {
	detail, err := client.ShowDetail(ctx, id)
	if err != nil {
		return daemon.RepositoryTicket{}, repositoryReadFailure(err)
	}
	return repositoryTicketDetailView(detail), nil
}

func (m *workerManager) createRepositoryTicket(ctx context.Context, repositoryID string, request daemon.RepositoryTicketCreateRequest) (daemon.RepositoryTicketMutation, error) {
	if err := ticketclient.ValidateActor(request.Actor); err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_actor", 400, "ticket actor is invalid", false, err)
	}
	repository, err := m.repositoryTarget(repositoryID)
	if err != nil {
		return daemon.RepositoryTicketMutation{}, err
	}
	client, err := ticketclient.NewWithTarget(request.Actor, ticketTargetFromConfiguredRepository(repository))
	if err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationNotAppliedFailure(err)
	}
	defer client.Close()
	input := ticketclient.CreateInput{Title: request.Title, Priority: request.Priority, Tags: request.Tags, Parent: request.Parent, DependsOn: request.DependsOn, Sections: request.Sections}
	mutation, err := client.CreateTicket(ctx, input)
	if err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationFailure(err)
	}
	return rereadRepositoryMutation(ctx, client, repositoryID, repository.Key, "create", mutation.ID, mutation.Changed)
}

func (m *workerManager) updateRepositoryTicket(ctx context.Context, repositoryID, id string, request daemon.RepositoryTicketUpdateRequest) (daemon.RepositoryTicketMutation, error) {
	if err := ticketclient.ValidateActor(request.Actor); err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_actor", 400, "ticket actor is invalid", false, err)
	}
	if err := ticketclient.ValidateFullID(id); err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_ticket", 400, "ticket ID is invalid", false, err)
	}
	repository, err := m.repositoryTarget(repositoryID)
	if err != nil {
		return daemon.RepositoryTicketMutation{}, err
	}
	client, err := ticketclient.NewWithTarget(request.Actor, ticketTargetFromConfiguredRepository(repository))
	if err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationNotAppliedFailure(err)
	}
	defer client.Close()
	mutation, err := client.UpdateTicket(ctx, id, ticketclient.UpdateInput{Set: request.Set, Sections: request.Sections})
	if err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationFailure(err)
	}
	return rereadRepositoryMutation(ctx, client, repositoryID, repository.Key, "update", id, mutation.Changed)
}

func (m *workerManager) mutateRepositoryTicket(ctx context.Context, repositoryID, id, operation string, request daemon.RepositoryTicketMutationRequest) (daemon.RepositoryTicketMutation, error) {
	if err := ticketclient.ValidateActor(request.Actor); err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_actor", 400, "ticket actor is invalid", false, err)
	}
	if err := ticketclient.ValidateFullID(id); err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_ticket", 400, "ticket ID is invalid", false, err)
	}
	if !validRepositoryMutation(operation) {
		return daemon.RepositoryTicketMutation{}, repositoryMutationError("unsupported_operation", 404, "ticket operation is unavailable", false, nil)
	}
	repository, err := m.repositoryTarget(repositoryID)
	if err != nil {
		return daemon.RepositoryTicketMutation{}, err
	}
	client, err := ticketclient.NewWithTarget(request.Actor, ticketTargetFromConfiguredRepository(repository))
	if err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationNotAppliedFailure(err)
	}
	defer client.Close()
	options := ticketclient.MutationOptions{Handoff: request.Handoff, Message: request.Message, Outcome: request.Outcome}
	var mutation ticketclient.MutationResult
	switch operation {
	case "claim":
		if options != (ticketclient.MutationOptions{}) {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "claim does not accept workflow context", false, nil)
		}
		mutation, err = client.ClaimTicket(ctx, id)
	case "release":
		if request.Outcome != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "release does not accept outcome", false, nil)
		}
		mutation, err = client.ReleaseTicket(ctx, id, options)
	case "open":
		if request.Outcome != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "open does not accept outcome", false, nil)
		}
		mutation, err = client.OpenTicket(ctx, id, options)
	case "hold":
		if request.Outcome != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "hold does not accept outcome", false, nil)
		}
		mutation, err = client.HoldTicket(ctx, id, options)
	case "submit":
		if request.Outcome != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "submit does not accept outcome", false, nil)
		}
		mutation, err = client.SubmitTicket(ctx, id, options)
	case "review":
		if request.Handoff != "" || request.Outcome != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "review accepts message only", false, nil)
		}
		mutation, err = client.ReviewTicket(ctx, id, options)
	case "approve":
		if request.Handoff != "" || request.Outcome != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "approve accepts message only", false, nil)
		}
		mutation, err = client.ApproveTicket(ctx, id, options)
	case "close":
		if request.Handoff != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "close does not accept handoff", false, nil)
		}
		mutation, err = client.CloseTicketWithOptions(ctx, id, options)
	case "reject":
		if request.Handoff != "" {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "reject does not accept handoff", false, nil)
		}
		mutation, err = client.RejectTicket(ctx, id, options)
	case "bump":
		if options != (ticketclient.MutationOptions{}) {
			return daemon.RepositoryTicketMutation{}, repositoryMutationError("invalid_request", 400, "bump does not accept workflow context", false, nil)
		}
		mutation, err = client.BumpTicket(ctx, id)
	}
	if err != nil {
		return daemon.RepositoryTicketMutation{}, repositoryMutationFailure(err)
	}
	return rereadRepositoryMutation(ctx, client, repositoryID, repository.Key, operation, id, mutation.Changed)
}

type repositoryTicketDetailReader interface {
	ShowDetail(context.Context, string) (ticketclient.TicketDetail, error)
}

func rereadRepositoryMutation(ctx context.Context, client repositoryTicketDetailReader, repositoryID, key, operation, id string, changed bool) (daemon.RepositoryTicketMutation, error) {
	detail, err := client.ShowDetail(ctx, id)
	if err != nil {
		// Ticket has already reported a changed mutation. A failed
		// authoritative reread leaves the final state unknown, so preserve
		// that applied signal and never invite an unsafe retry.
		return daemon.RepositoryTicketMutation{}, repositoryMutationFailureApplied(err, changed)
	}
	return daemon.RepositoryTicketMutation{RepositoryID: repositoryID, RepositoryKey: key, Operation: operation, Changed: changed, Ticket: repositoryTicketDetailView(detail)}, nil
}

func validRepositoryMutation(operation string) bool {
	switch operation {
	case "claim", "release", "open", "hold", "submit", "review", "approve", "close", "reject", "bump":
		return true
	default:
		return false
	}
}

func repositoryTicketView(ticket ticketclient.Ticket) daemon.RepositoryTicket {
	return daemon.RepositoryTicket{ID: ticket.ID, Title: ticket.Title, State: ticket.State, Priority: ticket.Priority, Assignee: ticket.Assignee, Parent: ticket.Parent, Tags: append([]string(nil), ticket.Tags...), DependsOn: append([]string(nil), ticket.DependsOn...)}
}

func repositoryTicketDetailView(ticket ticketclient.TicketDetail) daemon.RepositoryTicket {
	sections := make(map[string]daemon.RepositoryTicketSection, len(ticket.Sections))
	for name, section := range ticket.Sections {
		sections[name] = daemon.RepositoryTicketSection{Text: section.Text, Truncated: section.Truncated}
	}
	var readiness *daemon.RepositoryTicketReadiness
	if ticket.Readiness != nil {
		blockers := make([]daemon.RepositoryTicketReadinessBlocker, 0, len(ticket.Readiness.Blockers))
		for _, blocker := range ticket.Readiness.Blockers {
			blockers = append(blockers, daemon.RepositoryTicketReadinessBlocker{Code: blocker.Code, ID: blocker.ID, Message: blocker.Message})
		}
		readiness = &daemon.RepositoryTicketReadiness{Ready: ticket.Readiness.Ready, Blockers: blockers}
	}
	body := ticket.Body
	bodyTruncated := ticket.BodyTruncated
	return daemon.RepositoryTicket{
		ID: ticket.ID, Archived: ticket.Archived, Title: ticket.Title, State: ticket.State,
		Priority: ticket.Priority, Assignee: ticket.Assignee, BlockedReason: ticket.BlockedReason,
		Parent: ticket.Parent, Tags: append([]string(nil), ticket.Tags...), DependsOn: append([]string(nil), ticket.DependsOn...),
		Sections: sections, Available: append([]string(nil), ticket.Available...), Readiness: readiness,
		Created: ticket.Created, Modified: ticket.Modified, Body: &body,
		BodyTruncated: &bodyTruncated, Truncated: ticket.Truncated,
	}
}

func repositoryReadFailure(err error) error {
	if err == nil {
		return nil
	}
	return &daemon.RepositoryReadError{Code: "repository_read_failed", Status: 502, Message: "Ticket repository read failed", Cause: err}
}

func repositoryMutationFailure(err error) error {
	if err == nil {
		return nil
	}
	// A mutation command may have reached Ticket before an error is returned.
	// A missing or malformed response does not prove that it was rejected.
	// Preserve an explicit positive applied signal when Ticket provides one;
	// otherwise leave certainty unknown instead of inviting an unsafe retry.
	knownApplied := ticketclient.MutationApplied(err)
	return &daemon.RepositoryMutationError{
		Code: "repository_mutation_failed", Status: 502,
		Message: "Ticket repository mutation failed", Applied: knownApplied,
		AppliedKnown: knownApplied, Cause: err,
	}
}

func repositoryMutationNotAppliedFailure(err error) error {
	return repositoryMutationError("repository_mutation_failed", 502, "Ticket repository mutation failed", false, err)
}

func repositoryMutationFailureApplied(err error, applied bool) error {
	if err == nil {
		return nil
	}
	return repositoryMutationError("repository_mutation_failed", 502, "Ticket repository mutation failed", applied || ticketclient.MutationApplied(err), err)
}

func repositoryMutationError(code string, status int, message string, applied bool, cause error) error {
	return &daemon.RepositoryMutationError{Code: code, Status: status, Message: message, Applied: applied, AppliedKnown: true, Cause: cause}
}

func (m *workerManager) repositoryServiceSummary(repositoryID string) (daemon.RepositoryStatus, error) {
	if m == nil || m.runtime == nil {
		return daemon.RepositoryStatus{}, fmt.Errorf("repository service is unavailable")
	}
	for _, status := range m.runtime.RepositoryStatuses() {
		if status.ID == repositoryID {
			return daemon.RepositoryStatusDTO(status), nil
		}
	}
	return daemon.RepositoryStatus{}, fmt.Errorf("unknown repository")
}
