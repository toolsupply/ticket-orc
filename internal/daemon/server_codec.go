package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func writeControlError(w http.ResponseWriter, err error) {
	if controlErr := controlErrorDTO(err); controlErr != nil {
		status := controlErr.Status
		if status == 0 {
			status = http.StatusConflict
		}
		message := controlErr.Message
		if message == "" {
			message = "worker operation failed"
		}
		errorBody := map[string]any{"code": controlErr.Code, "message": message, "mutation_applied": controlErr.Applied}
		if controlErr.Result != nil {
			if controlErr.Result.State != "" {
				errorBody["state"] = controlErr.Result.State
			}
		}
		if controlErr.Daemon != nil {
			errorBody["daemon_control"] = controlErr.Daemon
		}
		writeJSON(w, status, map[string]any{"error": errorBody})
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusServiceUnavailable, "mutation_timeout", "mutation timed out")
		return
	}
	if errors.Is(err, context.Canceled) {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	writeError(w, http.StatusInternalServerError, "internal_error", "worker operation failed")
}

func writeGroupControlError(w http.ResponseWriter, result supervisor.GroupResult, err error) {
	controlErr := controlErrorDTO(err)
	if controlErr == nil {
		writeControlError(w, err)
		return
	}
	status := controlErr.Status
	if status == 0 {
		status = http.StatusConflict
	}
	message := controlErr.Message
	if message == "" {
		message = "group operation failed"
	}
	// Keep the typed error metadata and every attempted worker result in one
	// bounded response so clients can reconcile partial progress exactly.
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":             controlErr.Code,
			"message":          message,
			"mutation_applied": controlErr.Applied,
		},
		"group": GroupResultDTO(result),
	})
}

func controlErrorDTO(err error) *ControlError {
	var controlErr *ControlError
	if errors.As(err, &controlErr) && controlErr != nil {
		return controlErr
	}
	var lifecycleErr *supervisor.LifecycleError
	if !errors.As(err, &lifecycleErr) || lifecycleErr == nil {
		return nil
	}
	return &ControlError{
		Code: lifecycleErr.Code, Status: lifecycleHTTPStatus(lifecycleErr),
		Message: lifecycleErr.Message, Applied: lifecycleErr.Applied,
		Cause:   lifecycleErr.Cause,
		Result:  lifecycleMutationResultDTO(lifecycleErr.Result),
		Failure: WorkerFailureDTO(lifecycleErr.Failure),
	}
}

func lifecycleMutationResultDTO(result *supervisor.MutationResult) *MutationResult {
	if result == nil {
		return nil
	}
	dto := MutationResultDTO(*result)
	return &dto
}

func lifecycleHTTPStatus(err *supervisor.LifecycleError) int {
	switch err.Code {
	case "invalid_request":
		return http.StatusBadRequest
	case "invalid_config":
		if err.Conflict {
			return http.StatusConflict
		}
		return http.StatusBadRequest
	case "capability_unavailable":
		return http.StatusUnprocessableEntity
	case "worker_not_found", "group_not_found":
		return http.StatusNotFound
	case "daemon_stopping", "worker_paused", "invalid_worker_state", "worker_identity_conflict":
		return http.StatusConflict
	case "ticket_target_unavailable", "worker_unavailable", "worker_startup_failed", "worker_startup_timeout", "group_partial_failure", "mutation_timeout", "repository_observer_failed":
		return http.StatusServiceUnavailable
	default:
		return http.StatusConflict
	}
}

func writeRequestDecodeError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrRequestTooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON request")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	if len(message) > 256 {
		message = message[:256]
	}
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

// DecodeJSON strictly decodes one bounded JSON document and rejects unknown
// fields, trailing documents, and oversized bodies for future mutation routes.

func DecodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	if r == nil || r.Body == nil {
		return fmt.Errorf("request body is required")
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return fmt.Errorf("content type must be application/json")
	}
	limited := http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return classifyJSONDecodeError("decode JSON request", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request must contain one JSON document")
		}
		return classifyJSONDecodeError("decode trailing JSON", err)
	}
	return nil
}

func classifyJSONDecodeError(prefix string, err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return fmt.Errorf("%w: limit %d bytes", ErrRequestTooLarge, maxRequestBody)
	}
	return fmt.Errorf("%s: %w", prefix, err)
}
