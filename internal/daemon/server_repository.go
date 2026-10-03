package daemon

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	repositoryTicketDefaultPageSize = 50
	repositoryTicketMaxPageSize     = 256
	repositoryTicketMaxSearchBytes  = 256
)

func (s *Server) handleRepositories(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/repositories")
	if path == "" || path == "/" {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		status := s.currentStatus()
		repositories := append([]RepositoryStatus(nil), status.Repositories...)
		if repositories == nil {
			repositories = []RepositoryStatus{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"repositories": repositories})
		return
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) < 1 || len(parts) > 4 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "repository_not_found", "repository endpoint not found")
		return
	}
	repositoryID, err := url.PathUnescape(parts[0])
	if err != nil || repositoryID == "" || strings.Contains(repositoryID, "/") {
		writeError(w, http.StatusNotFound, "repository_not_found", "repository endpoint not found")
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		for _, repository := range s.currentStatus().Repositories {
			if repository.ID == repositoryID {
				writeJSON(w, http.StatusOK, repository)
				return
			}
		}
		writeError(w, http.StatusNotFound, "repository_not_found", "unknown repository")
		return
	}
	if parts[1] != "tickets" {
		writeError(w, http.StatusNotFound, "repository_endpoint_not_found", "repository endpoint not found")
		return
	}
	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			s.repositoryTickets(w, r, repositoryID)
			return
		}
		if r.Method == http.MethodPost {
			s.repositoryCreateTicket(w, r, repositoryID)
			return
		}
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if len(parts) == 3 {
		id, decodeErr := url.PathUnescape(parts[2])
		if decodeErr != nil || id == "" || strings.Contains(id, "/") {
			writeError(w, http.StatusBadRequest, "invalid_ticket", "ticket ID is invalid")
			return
		}
		if r.Method == http.MethodPost && id != "" {
			s.repositoryUpdateOrMutation(w, r, repositoryID, id, "")
			return
		}
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		s.repositoryTicket(w, r, repositoryID, id)
		return
	}
	if len(parts) == 4 {
		id, decodeErr := url.PathUnescape(parts[2])
		operation, operationErr := url.PathUnescape(parts[3])
		if decodeErr != nil || operationErr != nil || id == "" || operation == "" || strings.Contains(id, "/") || strings.Contains(operation, "/") {
			writeError(w, http.StatusBadRequest, "invalid_request", "ticket operation is invalid")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		s.repositoryUpdateOrMutation(w, r, repositoryID, id, operation)
		return
	}
	writeError(w, http.StatusNotFound, "repository_endpoint_not_found", "repository endpoint not found")
}

func (s *Server) repositoryTickets(w http.ResponseWriter, r *http.Request, repositoryID string) {
	if s.config.RepositoryGateway.ListTickets == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "repository ticket listing is unavailable")
		return
	}
	query, err := parseRepositoryTicketQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	ctx, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := s.config.RepositoryGateway.ListTickets(ctx, repositoryID, query)
	if callErr != nil {
		writeRepositoryReadError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) repositoryTicket(w http.ResponseWriter, r *http.Request, repositoryID, id string) {
	if s.config.RepositoryGateway.GetTicket == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "repository ticket detail is unavailable")
		return
	}
	ctx, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := s.config.RepositoryGateway.GetTicket(ctx, repositoryID, id)
	if callErr != nil {
		writeRepositoryReadError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) repositoryCreateTicket(w http.ResponseWriter, r *http.Request, repositoryID string) {
	if s.config.RepositoryGateway.CreateTicket == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "repository ticket creation is unavailable")
		return
	}
	var request RepositoryTicketCreateRequest
	if err := DecodeJSON(w, r, &request); err != nil {
		writeRequestDecodeError(w, err)
		return
	}
	if strings.TrimSpace(request.Actor) == "" {
		writeError(w, http.StatusBadRequest, "invalid_actor", "ticket actor is required")
		return
	}
	ctx, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := s.config.RepositoryGateway.CreateTicket(ctx, repositoryID, request)
	if callErr != nil {
		writeRepositoryMutationError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) repositoryUpdateOrMutation(w http.ResponseWriter, r *http.Request, repositoryID, id, operation string) {
	// The explicit /update suffix is kept alongside the collection's POST
	// form so typed daemon clients can use one stable mutation URL shape.
	if operation == "" || operation == "update" {
		if s.config.RepositoryGateway.UpdateTicket == nil {
			writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "repository ticket update is unavailable")
			return
		}
		var request RepositoryTicketUpdateRequest
		if err := DecodeJSON(w, r, &request); err != nil {
			writeRequestDecodeError(w, err)
			return
		}
		if strings.TrimSpace(request.Actor) == "" {
			writeError(w, http.StatusBadRequest, "invalid_actor", "ticket actor is required")
			return
		}
		ctx, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
		if err != nil {
			writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
			return
		}
		defer cancel()
		result, callErr := s.config.RepositoryGateway.UpdateTicket(ctx, repositoryID, id, request)
		if callErr != nil {
			writeRepositoryMutationError(w, callErr)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if !validRepositoryMutationOperation(operation) {
		writeError(w, http.StatusNotFound, "unsupported_operation", "ticket operation is unavailable")
		return
	}
	if s.config.RepositoryGateway.MutateTicket == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "repository ticket mutation is unavailable")
		return
	}
	var request RepositoryTicketMutationRequest
	if err := DecodeJSON(w, r, &request); err != nil {
		writeRequestDecodeError(w, err)
		return
	}
	if strings.TrimSpace(request.Actor) == "" {
		writeError(w, http.StatusBadRequest, "invalid_actor", "ticket actor is required")
		return
	}
	ctx, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := s.config.RepositoryGateway.MutateTicket(ctx, repositoryID, id, operation, request)
	if callErr != nil {
		writeRepositoryMutationError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func validRepositoryMutationOperation(operation string) bool {
	for _, supported := range repositoryMutationOperations() {
		if operation == supported {
			return true
		}
	}
	return false
}

func repositoryMutationOperations() []string {
	return []string{"claim", "release", "open", "hold", "submit", "review", "approve", "close", "reject", "bump"}
}

func parseRepositoryTicketQuery(r *http.Request) (RepositoryTicketQuery, error) {
	query := r.URL.Query()
	result := RepositoryTicketQuery{Search: strings.TrimSpace(query.Get("q")), Limit: repositoryTicketDefaultPageSize}
	if result.Search == "" {
		result.Search = strings.TrimSpace(query.Get("search"))
	}
	if len(result.Search) > repositoryTicketMaxSearchBytes {
		return RepositoryTicketQuery{}, fmt.Errorf("search expression is too long")
	}
	for _, state := range query["state"] {
		for _, value := range strings.Split(state, ",") {
			value = strings.TrimSpace(value)
			if value != "" {
				if !validRepositoryTicketState(value) {
					return RepositoryTicketQuery{}, fmt.Errorf("unsupported ticket state %q", value)
				}
				result.States = append(result.States, value)
			}
		}
	}
	if len(result.States) > 1 {
		for _, state := range result.States {
			if state == "all" {
				return RepositoryTicketQuery{}, fmt.Errorf("all cannot be combined with other state filters")
			}
		}
	}
	if priorities := query["priority"]; len(priorities) > 1 {
		return RepositoryTicketQuery{}, fmt.Errorf("priority may only be specified once")
	} else if len(priorities) == 1 {
		value, parseErr := strconv.Atoi(strings.TrimSpace(priorities[0]))
		if parseErr != nil || value < 0 || value > 4 {
			return RepositoryTicketQuery{}, fmt.Errorf("priority must be between 0 and 4")
		}
		result.Priority = &value
	}
	if assignees := query["assignee"]; len(assignees) > 1 {
		return RepositoryTicketQuery{}, fmt.Errorf("assignee may only be specified once")
	} else if len(assignees) == 1 {
		result.Assignee = strings.TrimSpace(assignees[0])
	}
	for _, tag := range query["tag"] {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			result.Tags = append(result.Tags, tag)
		}
	}
	if raw := query.Get("limit"); raw != "" {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 1 || value > repositoryTicketMaxPageSize {
			return RepositoryTicketQuery{}, fmt.Errorf("limit must be between 1 and %d", repositoryTicketMaxPageSize)
		}
		result.Limit = value
	}
	if raw := query.Get("offset"); raw != "" {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 0 {
			return RepositoryTicketQuery{}, fmt.Errorf("offset must not be negative")
		}
		result.Offset = value
	}
	return result, nil
}

func validRepositoryTicketState(state string) bool {
	switch state {
	case "open", "hold", "review", "signoff", "closed", "completed", "rejected", "all":
		return true
	default:
		return false
	}
}

func writeRepositoryReadError(w http.ResponseWriter, err error) {
	var readErr *RepositoryReadError
	if errors.As(err, &readErr) && readErr != nil {
		status := readErr.Status
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		code := readErr.Code
		if code == "" {
			code = "repository_read_failed"
		}
		message := readErr.Message
		if message == "" {
			message = "repository read failed"
		}
		writeError(w, status, code, message)
		return
	}
	writeError(w, http.StatusBadGateway, "repository_read_failed", "repository read failed")
}

func writeRepositoryMutationError(w http.ResponseWriter, err error) {
	var mutationErr *RepositoryMutationError
	if errors.As(err, &mutationErr) && mutationErr != nil {
		status := mutationErr.Status
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		code := mutationErr.Code
		if code == "" {
			code = "repository_mutation_failed"
		}
		message := mutationErr.Message
		if message == "" {
			message = "repository mutation failed"
		}
		message = boundedErrorMessage(message)
		errorBody := map[string]any{"code": code, "message": message}
		if mutationErr.AppliedKnown {
			errorBody["mutation_applied"] = mutationErr.Applied
		}
		writeJSON(w, status, map[string]any{"error": errorBody})
		return
	}
	var readErr *RepositoryReadError
	if errors.As(err, &readErr) && readErr != nil {
		status := readErr.Status
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		code := readErr.Code
		if code == "" {
			code = "repository_mutation_failed"
		}
		message := readErr.Message
		if message == "" {
			message = "repository mutation failed"
		}
		// A read error does not establish whether a mutation was applied. Keep
		// the certainty field absent unless the gateway returned the typed
		// RepositoryMutationError that carries that information.
		writeError(w, status, code, message)
		return
	}
	writeError(w, http.StatusBadGateway, "repository_mutation_failed", "repository mutation failed")
}

// PublishEvent publishes one classified runtime notification to connected
// event subscribers. Publication is non-blocking; clients recover from gaps
// by fetching the authoritative status snapshot.
