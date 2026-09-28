package daemon

import (
	"net/http"
	"net/url"
	"strings"
)

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/v1/workers", s.handleWorkers)
	mux.HandleFunc("/v1/repositories", s.handleRepositories)
	mux.HandleFunc("/v1/repositories/", s.handleRepositories)
	mux.HandleFunc("/v1/events", s.handleEvents)
	mux.HandleFunc("/v1/pause", s.handleDaemonPause)
	mux.HandleFunc("/v1/resume", s.handleDaemonResume)
	mux.HandleFunc("/v1/abort", s.handleDaemonAbort)
	mux.HandleFunc("/v1/workers/", s.handleWorkerMutation)
	mux.HandleFunc("/v1/groups/", s.handleGroupMutation)
	mux.HandleFunc("/v1/config/reload", s.handleReload)
	mux.HandleFunc("/v1/doctor", s.handleDoctor)
	mux.HandleFunc("/v1/shutdown", s.handleShutdown)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		capabilityPrefix := "/" + s.config.EndpointKey
		request := r.Clone(r.Context())
		request.URL = new(url.URL)
		*request.URL = *r.URL
		if strings.HasPrefix(r.URL.Path, capabilityPrefix+"/") {
			request.URL.Path = strings.TrimPrefix(r.URL.Path, capabilityPrefix)
			if r.URL.RawPath != "" {
				request.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, capabilityPrefix)
			}
		} else {
			writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
			return
		}
		if !strings.HasPrefix(request.URL.Path, "/v1/") {
			writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
			return
		}
		mux.ServeHTTP(w, request)
	})
}
