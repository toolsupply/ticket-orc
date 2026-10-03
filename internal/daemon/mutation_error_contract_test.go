package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestRepositoryMutationErrorCertaintyUsesNestedEnvelope(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "applied",
			err:  &RepositoryMutationError{Code: "repository_mutation_failed", Status: http.StatusBadGateway, Message: "mutation may have applied", Applied: true, AppliedKnown: true},
			want: `{"error":{"code":"repository_mutation_failed","message":"mutation may have applied","mutation_applied":true}}` + "\n",
		},
		{
			name: "not applied",
			err:  &RepositoryMutationError{Code: "repository_mutation_failed", Status: http.StatusBadGateway, Message: "mutation did not apply", Applied: false, AppliedKnown: true},
			want: `{"error":{"code":"repository_mutation_failed","message":"mutation did not apply","mutation_applied":false}}` + "\n",
		},
		{
			name: "uncertain after request",
			err:  &RepositoryMutationError{Code: "repository_mutation_failed", Status: http.StatusBadGateway, Message: "mutation result is unknown"},
			want: `{"error":{"code":"repository_mutation_failed","message":"mutation result is unknown"}}` + "\n",
		},
		{
			name: "read error has no certainty",
			err:  &RepositoryReadError{Code: "repository_not_found", Status: http.StatusNotFound, Message: "unknown repository"},
			want: `{"error":{"code":"repository_not_found","message":"unknown repository"}}` + "\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewServer(Config{
				EndpointKey: testEndpointKey,
				StateDir:    t.TempDir(),
				RepositoryGateway: RepositoryGateway{CreateTicket: func(context.Context, string, RepositoryTicketCreateRequest) (RepositoryTicketMutation, error) {
					return RepositoryTicketMutation{}, test.err
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			server.lifecycle = context.Background()
			server.started = true
			request := httptest.NewRequest(http.MethodPost, "/"+testEndpointKey+"/v1/repositories/"+testRepositoryID+"/tickets", strings.NewReader(`{"actor":"ui","title":"example"}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			server.handler().ServeHTTP(response, request)
			if response.Body.String() != test.want {
				t.Fatalf("response = %s, want %s", response.Body.String(), test.want)
			}
		})
	}
}

func TestControlMutationErrorsUseNestedCertaintyEnvelope(t *testing.T) {
	for _, test := range []struct {
		name    string
		path    string
		applied bool
		want    string
	}{
		{
			name:    "worker applied",
			path:    "/v1/workers/coder/start",
			applied: true,
			want:    `{"error":{"code":"control_failed","message":"operation failed","mutation_applied":true}}` + "\n",
		},
		{
			name:    "worker not applied",
			path:    "/v1/workers/coder/start",
			applied: false,
			want:    `{"error":{"code":"control_failed","message":"operation failed","mutation_applied":false}}` + "\n",
		},
		{
			name:    "group applied",
			path:    "/v1/groups/default/start",
			applied: true,
			want:    `{"error":{"code":"control_failed","message":"operation failed","mutation_applied":true},"group":{"group":"default","results":[]}}` + "\n",
		},
		{
			name:    "daemon not applied",
			path:    "/v1/pause",
			applied: false,
			want:    `{"error":{"code":"control_failed","message":"operation failed","mutation_applied":false}}` + "\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := func() error {
				return &ControlError{Code: "control_failed", Status: http.StatusServiceUnavailable, Message: "operation failed", Applied: test.applied}
			}
			control := &Control{}
			status := Status{Workers: []WorkerStatus{{Name: "coder"}}}
			switch test.path {
			case "/v1/workers/coder/start":
				control.StartWorker = func(context.Context, string) (supervisor.MutationResult, error) {
					return supervisor.MutationResult{}, failure()
				}
			case "/v1/groups/default/start":
				control.Groups = func() []string { return []string{"default"} }
				control.StartGroup = func(context.Context, string) (supervisor.GroupResult, error) {
					return supervisor.GroupResult{Group: "default", Results: []supervisor.MutationResult{}}, failure()
				}
			case "/v1/pause":
				control.PauseDaemon = func(context.Context) (DaemonControlResult, error) {
					return DaemonControlResult{}, failure()
				}
			}
			server, err := NewServer(Config{
				EndpointKey: testEndpointKey,
				StateDir:    t.TempDir(),
				Status:      func() Status { return status },
				Control:     control,
			})
			if err != nil {
				t.Fatal(err)
			}
			server.lifecycle = context.Background()
			server.started = true
			request := httptest.NewRequest(http.MethodPost, "/"+testEndpointKey+test.path, nil)
			response := httptest.NewRecorder()
			server.handler().ServeHTTP(response, request)
			if response.Body.String() != test.want {
				t.Fatalf("response = %s, want %s", response.Body.String(), test.want)
			}
		})
	}
}

func TestPreMutationAuthorizationErrorOmitsMutationCertainty(t *testing.T) {
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	request := httptest.NewRequest(http.MethodPost, "/wrong/v1/pause", nil)
	response := httptest.NewRecorder()
	server.handler().ServeHTTP(response, request)
	want := `{"error":{"code":"not_found","message":"endpoint not found"}}` + "\n"
	if response.Body.String() != want {
		t.Fatalf("response = %s, want %s", response.Body.String(), want)
	}
}

func TestTypedMutationErrorMessagesAreBounded(t *testing.T) {
	longMessage := strings.Repeat("x", 300)
	for _, test := range []struct {
		name  string
		write func(http.ResponseWriter)
	}{
		{
			name: "repository",
			write: func(w http.ResponseWriter) {
				writeRepositoryMutationError(w, &RepositoryMutationError{Code: "repository_mutation_failed", Status: http.StatusBadGateway, Message: longMessage})
			},
		},
		{
			name: "worker control",
			write: func(w http.ResponseWriter) {
				writeControlError(w, &ControlError{Code: "control_failed", Status: http.StatusServiceUnavailable, Message: longMessage})
			},
		},
		{
			name: "group control",
			write: func(w http.ResponseWriter) {
				writeGroupControlError(w, supervisor.GroupResult{Group: "default"}, &ControlError{Code: "control_failed", Status: http.StatusServiceUnavailable, Message: longMessage})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			test.write(response)
			var envelope struct {
				Error struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if got, want := envelope.Error.Message, longMessage[:256]; got != want {
				t.Fatalf("message length=%d value=%q, want first %d bytes", len(got), got, len(want))
			}
		})
	}
}
