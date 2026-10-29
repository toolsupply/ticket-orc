package cli

import (
	"context"
	"errors"
	"io"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Resolve on every operation through the configured registry and public Ticket
// protocol. The owned client is closed before any attachment lock or body IO.
func (m *workerManager) attachmentLocation(ctx context.Context, repositoryID, id, actor string) (attachments.Location, error) {
	if ticketclient.ValidateFullID(id) != nil {
		return attachments.Location{}, &daemon.RepositoryReadError{Code: "invalid_ticket", Status: 400, Message: "ticket ID is invalid"}
	}
	repository, err := m.repositoryTarget(repositoryID)
	if err != nil {
		return attachments.Location{}, err
	}
	client, err := ticketclient.NewWithTarget(actor, ticketTargetFromConfiguredRepository(repository))
	if err != nil {
		return attachments.Location{}, attachmentResolutionFailure(err)
	}
	defer client.Close()
	loc, err := client.ResolveAttachmentLocation(ctx, repository.ID, id)
	if err != nil {
		return attachments.Location{}, attachmentResolutionFailure(err)
	}
	return attachments.Location{RepositoryID: loc.RepositoryID, TicketID: loc.TicketID, RepositoryRoot: loc.RepositoryRoot, TicketDirectory: loc.TicketDirectory}, nil
}
func attachmentResolutionFailure(err error) error {
	code, status := "repository_unavailable", 503
	var command *ticketclient.CommandError
	if errors.As(err, &command) && command.Code == "not_found" {
		code, status = "ticket_not_found", 404
	}
	if errors.Is(err, ticketclient.ErrProtocol) {
		code, status = "ticket_protocol_error", 502
	}
	return &daemon.RepositoryReadError{Code: code, Status: status, Message: "Ticket attachment location unavailable", Cause: err}
}
func (m *workerManager) listRepositoryAttachments(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentQuery) (attachments.Listing, error) {
	loc, err := m.attachmentLocation(ctx, repo, id, repositoryGatewayActor)
	if err != nil {
		return attachments.Listing{}, err
	}
	result, err := attachments.List(ctx, loc, req.Path, req.Limit, req.Cursor)
	return result, attachmentFailure(err, false, true)
}
func (m *workerManager) readRepositoryAttachment(ctx context.Context, store *attachments.Store, repo, id, path string) (daemon.RepositoryAttachmentContent, error) {
	d, err := store.BeginDownload(ctx)
	if err != nil {
		return daemon.RepositoryAttachmentContent{}, attachmentFailure(err, false, true)
	}
	loc, err := m.attachmentLocation(d.Context(), repo, id, repositoryGatewayActor)
	if err == nil {
		err = d.Open(loc, path)
	}
	if err != nil {
		d.Close()
		return daemon.RepositoryAttachmentContent{}, attachmentFailure(err, false, true)
	}
	return daemon.RepositoryAttachmentContent{Entry: d.Entry, Content: d}, nil
}
func validateAttachmentActor(actor string) error {
	if err := ticketclient.ValidateActor(actor); err != nil {
		return repositoryMutationError("invalid_actor", 400, "ticket actor is invalid", false, err)
	}
	return nil
}
func (m *workerManager) writeRepositoryAttachment(ctx context.Context, store *attachments.Store, repo, id string, req daemon.RepositoryAttachmentRequest, body io.Reader, length int64, beginCommit attachments.CommitContext) (attachments.Receipt, error) {
	if err := validateAttachmentActor(req.Actor); err != nil {
		return attachments.Receipt{}, err
	}
	if beginCommit == nil {
		return attachments.Receipt{}, repositoryMutationError("invalid_request", 400, "upload commit lifecycle is required", false, nil)
	}
	u, err := store.BeginUpload(ctx)
	if err != nil {
		return attachments.Receipt{}, attachmentFailure(err, true, true)
	}
	defer u.Close()
	loc, err := m.attachmentLocation(u.Context(), repo, id, req.Actor)
	if err != nil {
		return attachments.Receipt{}, attachmentFailure(err, true, true)
	}
	if err = u.Stage(loc, req.Actor, req.Path, req.Condition, body, length); err != nil {
		return attachments.Receipt{}, attachmentFailure(err, true, true)
	}
	// Storage accepts the completed body before obtaining the daemon-owned
	// commit lifetime. Neither shutdown cancellation nor deadlines are stripped.
	receipt, err := u.Commit(beginCommit)
	return m.attachmentMutationResult(repo, id, req.Actor, receipt, err)
}
func (m *workerManager) createRepositoryAttachmentDirectory(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentRequest) (attachments.Receipt, error) {
	if err := validateAttachmentActor(req.Actor); err != nil {
		return attachments.Receipt{}, err
	}
	loc, err := m.attachmentLocation(ctx, repo, id, req.Actor)
	if err != nil {
		return attachments.Receipt{}, attachmentFailure(err, true, true)
	}
	receipt, err := attachments.Mkdir(ctx, loc, req.Actor, req.Path, req.Condition)
	return m.attachmentMutationResult(repo, id, req.Actor, receipt, err)
}
func (m *workerManager) moveRepositoryAttachment(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentMoveRequest) (attachments.Receipt, error) {
	if err := validateAttachmentActor(req.Actor); err != nil {
		return attachments.Receipt{}, err
	}
	loc, err := m.attachmentLocation(ctx, repo, id, req.Actor)
	if err != nil {
		return attachments.Receipt{}, attachmentFailure(err, true, true)
	}
	receipt, err := attachments.Move(ctx, loc, req.Actor, req.From, req.To, req.Revision, req.Condition)
	return m.attachmentMutationResult(repo, id, req.Actor, receipt, err)
}
func (m *workerManager) deleteRepositoryAttachment(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentRequest) (attachments.Receipt, error) {
	if err := validateAttachmentActor(req.Actor); err != nil {
		return attachments.Receipt{}, err
	}
	loc, err := m.attachmentLocation(ctx, repo, id, req.Actor)
	if err != nil {
		return attachments.Receipt{}, attachmentFailure(err, true, true)
	}
	receipt, err := attachments.Delete(ctx, loc, req.Actor, req.Path, req.Condition, false)
	return m.attachmentMutationResult(repo, id, req.Actor, receipt, err)
}
func (m *workerManager) attachmentMutationResult(repo, id, actor string, receipt attachments.Receipt, err error) (attachments.Receipt, error) {
	var domain *attachments.Error
	applied := receipt.MutationApplied || errors.As(err, &domain) && domain.MutationApplied != nil && *domain.MutationApplied
	if applied && m.runtimeEventSink != nil {
		// Invalidation only: clients reconcile via list/read after any stream gap.
		// No lifecycle/Work-log mutation, private path, or storage details are emitted.
		m.runtimeEventSink(supervisor.RuntimeEvent{Type: "ticket.repository_changed", RepositoryID: repo, Ticket: id, Actor: actor, Code: "attachments_changed", Applied: true})
	}
	failure := attachmentFailure(err, true, !applied)
	if applied && failure != nil {
		mapped := failure.(*daemon.RepositoryMutationError)
		mapped.Applied, mapped.AppliedKnown = true, true
	}
	return receipt, failure
}

// beforeCommit is positive evidence for resolver/admission failures. Domain
// mutation errors carry their own certainty, including nil for an unknown result.
func attachmentFailure(err error, mutation, beforeCommit bool) error {
	if err == nil {
		return nil
	}
	code, status := "attachment_storage_failed", 502
	applied, known := false, beforeCommit
	var read *daemon.RepositoryReadError
	var domain *attachments.Error
	if errors.As(err, &read) {
		code, status = read.Code, read.Status
	} else if errors.As(err, &domain) {
		code = domain.Code
		known = domain.MutationApplied != nil
		if known {
			applied = *domain.MutationApplied
		}
		switch code {
		case "invalid_ticket", "invalid_path", "invalid_actor", "invalid_precondition", "invalid_cursor", "invalid_request":
			status = 400
		case "entry_not_found":
			status = 404
		case "entry_type_conflict", "directory_not_empty", "cursor_stale", "unsafe_attachment":
			status = 409
		case "revision_conflict":
			status = 412
		case "limit_exceeded", "capability_unavailable":
			status = 422
			if domain.FileSizeLimit {
				status = 413
			}
		case "precondition_required":
			status = 428
		case "attachment_busy", "mutation_outcome_unknown":
			status = 503
		}
	}
	message := "Attachment operation failed"
	if mutation {
		return &daemon.RepositoryMutationError{Code: code, Status: status, Message: message, Applied: applied, AppliedKnown: known, Cause: err}
	}
	return &daemon.RepositoryReadError{Code: code, Status: status, Message: message, Cause: err}
}
