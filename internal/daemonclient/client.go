// Package daemonclient implements the capability-routed client for Orc daemon
// commands.
package daemonclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

const (
	protocolVersion = 1
	// Repository detail includes a bounded 256 KiB body plus selected
	// sections. encoding/json can expand HTML-sensitive characters sixfold,
	// so the prior 1 MiB response cap rejected valid bounded detail responses.
	maxResponseBytes = 2 << 20
)

type ErrorKind string

const (
	ErrorDiscovery   ErrorKind = "discovery"
	ErrorAuth        ErrorKind = "auth"
	ErrorProtocol    ErrorKind = "protocol"
	ErrorTransport   ErrorKind = "transport"
	ErrorApplication ErrorKind = "application"
)

// Error is a bounded client failure classified for operator diagnostics.
type Error struct {
	Kind          ErrorKind
	Code          string
	Message       string
	StatusCode    int
	Applied       bool
	Result        *daemon.MutationResult
	Group         *daemon.GroupResult
	DaemonControl *daemon.DaemonControlResult
	Cause         error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	message := e.Message
	if message == "" {
		message = string(e.Kind) + " error"
	}
	if e.Code != "" {
		return e.Code + ": " + message
	}
	return message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Client routes requests through the capability path from the private daemon
// discovery document. The key is never exposed through status or renderers.
type Client struct {
	endpoint     daemon.Endpoint
	http         *http.Client
	instanceDir  string
	endpointPath string
}

func New(stateDir string) (*Client, error) {
	return NewWithEndpoint(stateDir, "", os.Getenv("TICKET_ORC_ENDPOINT"))
}

// NewWithEndpoint resolves and validates one client destination while keeping
// command, environment, and instance metadata precedence in one place.
func NewWithEndpoint(stateDir, explicitEndpoint, environmentEndpoint string) (*Client, error) {
	endpoint, err := daemon.ResolveClientEndpoint(explicitEndpoint, environmentEndpoint, stateDir)
	if err != nil {
		message := "daemon endpoint is unavailable or invalid"
		if strings.TrimSpace(explicitEndpoint) == "" && strings.TrimSpace(environmentEndpoint) == "" && strings.TrimSpace(stateDir) != "" {
			message = err.Error()
		}
		return nil, &Error{Kind: ErrorDiscovery, Message: message, Cause: err}
	}
	client := &Client{endpoint: endpoint, http: http.DefaultClient}
	if strings.TrimSpace(explicitEndpoint) == "" && strings.TrimSpace(environmentEndpoint) == "" {
		client.instanceDir = stateDir
		client.endpointPath = daemon.EndpointPath(stateDir)
	}
	if endpoint.Version != protocolVersion || endpoint.Protocol != protocolVersion {
		return nil, &Error{Kind: ErrorProtocol, Message: client.discoveryMessage("daemon protocol is unsupported")}
	}
	return client, nil
}

func (c *Client) discoveryMessage(message string) string {
	return message + c.discoveryContext()
}

func (c *Client) discoveryContext() string {
	if c != nil && c.endpointPath != "" {
		return fmt.Sprintf(" for selected instance %s (endpoint metadata %s)", c.instanceDir, c.endpointPath)
	}
	return ""
}

func NewWithHTTPClient(stateDir string, client *http.Client) (*Client, error) {
	result, err := New(stateDir)
	if err != nil {
		return nil, err
	}
	if client != nil {
		result.http = client
	}
	return result, nil
}

func (c *Client) Status(ctx context.Context) (daemon.Status, error) {
	var status daemon.Status
	err := c.do(ctx, http.MethodGet, "/v1/status", nil, &status)
	return status, err
}

func (c *Client) Workers(ctx context.Context) ([]daemon.WorkerStatus, error) {
	var response struct {
		Workers []daemon.WorkerStatus `json:"workers"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/workers", nil, &response)
	return response.Workers, err
}

// Repositories returns the daemon's configured repository resources and their
// current observer health snapshots.
func (c *Client) Repositories(ctx context.Context) ([]daemon.RepositoryStatus, error) {
	var response struct {
		Repositories []daemon.RepositoryStatus `json:"repositories"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/repositories", nil, &response)
	return response.Repositories, err
}

// Repository returns one configured repository by its authoritative
// repository ID from the inventory.
func (c *Client) Repository(ctx context.Context, repositoryID string) (daemon.RepositoryStatus, error) {
	var result daemon.RepositoryStatus
	err := c.do(ctx, http.MethodGet, "/v1/repositories/"+url.PathEscape(repositoryID), nil, &result)
	return result, err
}

// RepositoryTickets reads a bounded list or search projection through the
// daemon's typed repository gateway. repositoryID comes from Repositories;
// callers never supply a filesystem path.
func (c *Client) RepositoryTickets(ctx context.Context, repositoryID string, query daemon.RepositoryTicketQuery) (daemon.RepositoryTicketList, error) {
	var result daemon.RepositoryTicketList
	values := url.Values{}
	if query.Search != "" {
		values.Set("q", query.Search)
	}
	for _, state := range query.States {
		values.Add("state", state)
	}
	if query.Priority != nil {
		values.Set("priority", fmt.Sprint(*query.Priority))
	}
	if query.Assignee != "" {
		values.Set("assignee", query.Assignee)
	}
	for _, tag := range query.Tags {
		values.Add("tag", tag)
	}
	if query.Limit != 0 {
		values.Set("limit", fmt.Sprint(query.Limit))
	}
	if query.Offset != 0 {
		values.Set("offset", fmt.Sprint(query.Offset))
	}
	path := "/v1/repositories/" + url.PathEscape(repositoryID) + "/tickets"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	err := c.do(ctx, http.MethodGet, path, nil, &result)
	return result, err
}

// RepositoryTicket reads one explicit full ticket ID from an Orc repository
// selected by its inventory ID.
func (c *Client) RepositoryTicket(ctx context.Context, repositoryID, id string) (daemon.RepositoryTicketDetail, error) {
	var result daemon.RepositoryTicketDetail
	path := "/v1/repositories/" + url.PathEscape(repositoryID) + "/tickets/" + url.PathEscape(id)
	err := c.do(ctx, http.MethodGet, path, nil, &result)
	return result, err
}

// CreateRepositoryTicket invokes the typed UI create mutation for one Orc
// repository ID. The actor is carried in the typed request and never in the
// daemon bearer token or a generic argv payload.
func (c *Client) CreateRepositoryTicket(ctx context.Context, repositoryID string, request daemon.RepositoryTicketCreateRequest) (daemon.RepositoryTicketMutation, error) {
	var result daemon.RepositoryTicketMutation
	data, err := json.Marshal(request)
	if err != nil {
		return result, &Error{Kind: ErrorProtocol, Code: "encode_request", Message: "encode repository ticket create request"}
	}
	err = c.do(ctx, http.MethodPost, "/v1/repositories/"+url.PathEscape(repositoryID)+"/tickets", bytes.NewReader(data), &result)
	return result, err
}

// UpdateRepositoryTicket invokes the typed UI update mutation for one full
// ticket ID.
func (c *Client) UpdateRepositoryTicket(ctx context.Context, repositoryID, id string, request daemon.RepositoryTicketUpdateRequest) (daemon.RepositoryTicketMutation, error) {
	var result daemon.RepositoryTicketMutation
	data, err := json.Marshal(request)
	if err != nil {
		return result, &Error{Kind: ErrorProtocol, Code: "encode_request", Message: "encode repository ticket update request"}
	}
	path := "/v1/repositories/" + url.PathEscape(repositoryID) + "/tickets/" + url.PathEscape(id) + "/update"
	err = c.do(ctx, http.MethodPost, path, bytes.NewReader(data), &result)
	return result, err
}

// MutateRepositoryTicket invokes one allowlisted typed workflow operation.
func (c *Client) MutateRepositoryTicket(ctx context.Context, repositoryID, id, operation string, request daemon.RepositoryTicketMutationRequest) (daemon.RepositoryTicketMutation, error) {
	if !validRepositoryMutationOperation(operation) {
		return daemon.RepositoryTicketMutation{}, &Error{Kind: ErrorApplication, Code: "unsupported_operation", Message: "unsupported repository ticket operation"}
	}
	var result daemon.RepositoryTicketMutation
	data, err := json.Marshal(request)
	if err != nil {
		return result, &Error{Kind: ErrorProtocol, Code: "encode_request", Message: "encode repository ticket mutation request"}
	}
	path := "/v1/repositories/" + url.PathEscape(repositoryID) + "/tickets/" + url.PathEscape(id) + "/" + url.PathEscape(operation)
	err = c.do(ctx, http.MethodPost, path, bytes.NewReader(data), &result)
	return result, err
}

func validRepositoryMutationOperation(operation string) bool {
	switch operation {
	case "claim", "release", "open", "hold", "submit", "review", "approve", "close", "reject", "bump":
		return true
	default:
		return false
	}
}

func (c *Client) Worker(ctx context.Context, name, operation string) (daemon.MutationResult, error) {
	var result daemon.MutationResult
	path := "/v1/workers/" + url.PathEscape(name) + "/" + url.PathEscape(operation)
	err := c.do(ctx, http.MethodPost, path, nil, &result)
	if err != nil {
		var clientErr *Error
		if errors.As(err, &clientErr) && clientErr.Result != nil {
			result = *clientErr.Result
			result.Worker = name
		}
	}
	return result, err
}

func (c *Client) Group(ctx context.Context, name, operation string) (daemon.GroupResult, error) {
	var result daemon.GroupResult
	err := c.do(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(name)+"/"+url.PathEscape(operation), nil, &result)
	if err != nil {
		var clientErr *Error
		if errors.As(err, &clientErr) && clientErr.Group != nil {
			result = *clientErr.Group
		}
	}
	return result, err
}

func (c *Client) Reload(ctx context.Context) (daemon.ReloadResult, error) {
	var result daemon.ReloadResult
	err := c.do(ctx, http.MethodPost, "/v1/config/reload", nil, &result)
	return result, err
}

func (c *Client) Pause(ctx context.Context) (daemon.DaemonControlResult, error) {
	return c.daemonControl(ctx, "/v1/pause")
}

func (c *Client) Resume(ctx context.Context) (daemon.DaemonControlResult, error) {
	return c.daemonControl(ctx, "/v1/resume")
}

func (c *Client) Abort(ctx context.Context) (daemon.DaemonControlResult, error) {
	return c.daemonControl(ctx, "/v1/abort")
}

func (c *Client) daemonControl(ctx context.Context, path string) (daemon.DaemonControlResult, error) {
	var result daemon.DaemonControlResult
	err := c.do(ctx, http.MethodPost, path, nil, &result)
	if err != nil {
		var clientErr *Error
		if errors.As(err, &clientErr) && clientErr.DaemonControl != nil {
			result = *clientErr.DaemonControl
		}
	}
	return result, err
}

func (c *Client) Doctor(ctx context.Context) (daemon.DoctorResult, error) {
	var result daemon.DoctorResult
	err := c.do(ctx, http.MethodPost, "/v1/doctor", nil, &result)
	return result, err
}

func (c *Client) Shutdown(ctx context.Context) error {
	var result struct {
		Applied bool `json:"mutation_applied"`
	}
	return c.do(ctx, http.MethodPost, "/v1/shutdown", nil, &result)
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, output any) error {
	if c == nil || c.http == nil {
		return &Error{Kind: ErrorTransport, Message: "daemon client is unavailable"}
	}
	if ctx == nil {
		return &Error{Kind: ErrorTransport, Message: "daemon request context is unavailable"}
	}
	request, err := http.NewRequestWithContext(ctx, method, c.endpoint.CapabilityURL()+path, body)
	if err != nil {
		return &Error{Kind: ErrorProtocol, Message: "build daemon request", Cause: err}
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return &Error{Kind: ErrorTransport, Message: c.discoveryMessage("daemon is unreachable"), Cause: err}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return &Error{Kind: ErrorTransport, Message: c.discoveryMessage("read daemon response"), Cause: err}
	}
	if len(data) > maxResponseBytes {
		return &Error{Kind: ErrorProtocol, Message: "daemon response is too large"}
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusNotFound {
		return &Error{Kind: ErrorAuth, Code: "unauthorized", Message: c.discoveryMessage("daemon authentication failed"), StatusCode: response.StatusCode}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return c.applicationError(response.StatusCode, data)
	}
	if err := decodeOne(data, output); err != nil {
		return &Error{Kind: ErrorProtocol, Message: "decode daemon response", StatusCode: response.StatusCode, Cause: err}
	}
	return nil
}

func (c *Client) applicationError(status int, data []byte) error {
	var envelope struct {
		Error struct {
			Code          string                      `json:"code"`
			Message       string                      `json:"message"`
			Applied       bool                        `json:"mutation_applied"`
			State         string                      `json:"state"`
			DaemonControl *daemon.DaemonControlResult `json:"daemon_control"`
		} `json:"error"`
		Group daemon.GroupResult `json:"group"`
	}
	if err := decodeOne(data, &envelope); err != nil {
		return &Error{Kind: ErrorProtocol, Message: "decode daemon error", StatusCode: status, Cause: err}
	}
	if status == http.StatusUnauthorized {
		return &Error{Kind: ErrorAuth, Code: "unauthorized", Message: "daemon authentication failed", StatusCode: status}
	}
	result := daemon.MutationResult{State: envelope.Error.State, Applied: envelope.Error.Applied}
	group := envelope.Group
	var resultPtr *daemon.MutationResult
	if result.Applied || result.State != "" {
		resultPtr = &result
	}
	var groupPtr *daemon.GroupResult
	if group.Group != "" || len(group.Results) > 0 {
		groupPtr = &group
	}
	return &Error{Kind: ErrorApplication, Code: envelope.Error.Code, Message: envelope.Error.Message, StatusCode: status, Applied: result.Applied, Result: resultPtr, Group: groupPtr, DaemonControl: envelope.Error.DaemonControl}
}

func decodeOne(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON documents")
		}
		return err
	}
	return nil
}
