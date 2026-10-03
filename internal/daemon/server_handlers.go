package daemon

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	status := s.currentStatus()
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	status := s.currentStatus()
	writeJSON(w, http.StatusOK, map[string]any{"workers": status.Workers})
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	// A missing callback intentionally reports an empty inventory. The
	// callback is the only source of group names; worker metadata is not a
	// second registry for this endpoint.
	var names []string
	if s.config.Control != nil && s.config.Control.Groups != nil {
		names = s.config.Control.Groups()
	}
	if len(names) > maxGroupInventoryEntries {
		writeError(w, http.StatusServiceUnavailable, "group_inventory_unavailable", "group inventory exceeds the response bound")
		return
	}
	groups := make([]GroupStatus, 0, len(names))
	for _, name := range names {
		if !validPublicGroupName(name) {
			writeError(w, http.StatusServiceUnavailable, "group_inventory_unavailable", "group inventory contains an invalid name")
			return
		}
		groups = append(groups, GroupStatus{Name: name})
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
}

func validPublicGroupName(name string) bool {
	if name == "" || len(name) > maxPublicGroupNameBytes || name == "." || name == ".." || strings.TrimSpace(name) != name {
		return false
	}
	for _, char := range name {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func (s *Server) handleDaemonPause(w http.ResponseWriter, r *http.Request) {
	var callback func(context.Context) (DaemonControlResult, error)
	if s.config.Control != nil {
		callback = s.config.Control.PauseDaemon
	}
	s.handleDaemonModeMutation(w, r, "pause", callback)
}

func (s *Server) handleDaemonResume(w http.ResponseWriter, r *http.Request) {
	var callback func(context.Context) (DaemonControlResult, error)
	if s.config.Control != nil {
		callback = s.config.Control.ResumeDaemon
	}
	s.handleDaemonModeMutation(w, r, "resume", callback)
}

func (s *Server) handleDaemonAbort(w http.ResponseWriter, r *http.Request) {
	var callback func(context.Context) (DaemonControlResult, error)
	if s.config.Control != nil {
		callback = s.config.Control.AbortDaemon
	}
	s.handleDaemonModeMutation(w, r, "abort", callback)
}

func (s *Server) handleDaemonModeMutation(w http.ResponseWriter, r *http.Request, operation string, callback func(context.Context) (DaemonControlResult, error)) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.config.Control == nil || callback == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "daemon "+operation+" is unavailable")
		return
	}
	if r.ContentLength != 0 {
		if err := DecodeJSON(w, r, &struct{}{}); err != nil {
			writeRequestDecodeError(w, err)
			return
		}
	}
	mutation, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := callback(mutation)
	if callErr != nil {
		writeControlError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) PublishEvent(event Event) {
	if s == nil {
		return
	}
	s.events.publish(event)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "stream_unavailable", "event stream is unavailable")
		return
	}
	id, baseline, events, ok := s.events.subscribe()
	if !ok {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer s.events.unsubscribe(id)
	syncEvent, err := encodeSSEEvent(Event{Seq: baseline, Type: "stream.sync"})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "stream_unavailable", "event stream is unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	if _, err := w.Write(syncEvent); err != nil {
		return
	}
	flusher.Flush()
	heartbeat := time.NewTicker(s.config.EventHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case event, open := <-events:
			if !open {
				return
			}
			data, err := encodeSSEEvent(event)
			if err != nil {
				return
			}
			if _, err := w.Write(data); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		case <-s.lifecycle.Done():
			return
		}
	}
}

func (s *Server) currentStatus() Status {
	status := Status{Version: s.config.Version, InstanceID: s.config.InstanceID, Protocol: s.config.Protocol, PID: s.config.PID, URL: s.endpoint.URL, StartedAt: s.startedAt}
	if s.config.Status != nil {
		status = s.config.Status()
		status.Version = s.config.Version
		status.InstanceID = s.config.InstanceID
		status.Protocol = s.config.Protocol
		status.PID = s.config.PID
		status.URL = s.endpoint.URL
		status.StartedAt = s.startedAt
	}
	status.Capabilities = s.capabilities()
	if s.config.Control != nil && s.config.Control.DaemonMode != nil {
		status.Mode = s.config.Control.DaemonMode()
	}
	if status.Mode == "" {
		status.Mode = "running"
	}
	return status
}

func (s *Server) handleWorkerMutation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/workers/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	name, err := url.PathUnescape(parts[0])
	if err != nil || !s.hasWorker(name) {
		writeError(w, http.StatusNotFound, "worker_not_found", "unknown worker")
		return
	}
	if s.config.Control == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "worker control is unavailable")
		return
	}
	var operation func(context.Context, string) (supervisor.MutationResult, error)
	switch parts[1] {
	case "start":
		operation = s.config.Control.StartWorker
	case "stop":
		operation = s.config.Control.StopWorker
	case "pause":
		operation = s.config.Control.PauseWorker
	case "resume":
		operation = s.config.Control.ResumeWorker
	case "restart":
		operation = s.config.Control.RestartWorker
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	if operation == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "worker operation is unavailable")
		return
	}
	mutation, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	if r.ContentLength != 0 {
		if err := DecodeJSON(w, r, &struct{}{}); err != nil {
			writeRequestDecodeError(w, err)
			return
		}
	}
	result, callErr := operation(mutation, name)
	if callErr != nil {
		writeControlError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, MutationResultDTO(result))
}

func (s *Server) handleGroupMutation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/groups/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	name, err := url.PathUnescape(parts[0])
	if err != nil || !s.hasGroup(name) {
		writeError(w, http.StatusNotFound, "group_not_found", "unknown group")
		return
	}
	if s.config.Control == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "group control is unavailable")
		return
	}
	var operation func(context.Context, string) (supervisor.GroupResult, error)
	switch parts[1] {
	case "start":
		operation = s.config.Control.StartGroup
	case "stop":
		operation = s.config.Control.StopGroup
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	if operation == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "group operation is unavailable")
		return
	}
	mutation, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := operation(mutation, name)
	if callErr != nil {
		writeGroupControlError(w, result, callErr)
		return
	}
	writeJSON(w, http.StatusOK, GroupResultDTO(result))
}

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.config.Control == nil || s.config.Control.Reload == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "config reload is unavailable")
		return
	}
	if r.ContentLength != 0 {
		if err := DecodeJSON(w, r, &struct{}{}); err != nil {
			writeRequestDecodeError(w, err)
			return
		}
	}
	mutation, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := s.config.Control.Reload(mutation)
	if callErr != nil {
		writeControlError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, ReloadResultDTO(result))
}

func (s *Server) handleDoctor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.config.Control == nil || s.config.Control.Doctor == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "doctor recovery is unavailable")
		return
	}
	if r.ContentLength != 0 {
		if err := DecodeJSON(w, r, &struct{}{}); err != nil {
			writeRequestDecodeError(w, err)
			return
		}
	}
	mutation, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	result, callErr := s.config.Control.Doctor(mutation)
	if callErr != nil {
		writeControlError(w, callErr)
		return
	}
	writeJSON(w, http.StatusOK, DoctorResultDTO(result))
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	mutation, cancel, err := s.MutationContext(r.Context(), defaultShutdownTimeout)
	if err != nil {
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	defer cancel()
	if s.config.Control != nil && s.config.Control.Shutdown != nil {
		if err := s.config.Control.Shutdown(mutation); err != nil {
			writeControlError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"mutation_applied": true})
	go func() {
		shutdown, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer shutdownCancel()
		_ = s.Shutdown(shutdown)
	}()
}

func (s *Server) hasWorker(name string) bool {
	for _, worker := range s.currentStatus().Workers {
		if worker.Name == name {
			return true
		}
	}
	return false
}

func (s *Server) hasGroup(name string) bool {
	// Groups are exposed through the worker snapshot's role-independent names.
	// The control callback remains authoritative for membership and may return a
	// partial result when one member fails.
	if s.config.Control == nil {
		return false
	}
	if s.config.Control.Groups != nil {
		for _, group := range s.config.Control.Groups() {
			if group == name {
				return true
			}
		}
	}
	for _, worker := range s.currentStatus().Workers {
		for _, group := range worker.Groups {
			if group == name {
				return true
			}
		}
	}
	return false
}
