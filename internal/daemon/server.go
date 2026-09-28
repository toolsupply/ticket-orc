package daemon

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultShutdownTimeout   = 5 * time.Second
	defaultEventHeartbeat    = 15 * time.Second
	maxRequestBody           = 1 << 20
)

// ErrRequestTooLarge identifies a request body rejected by the bounded JSON
// decoder.
var ErrRequestTooLarge = errors.New("request body too large")

// WorkerStatus is the public, deliberately small worker representation used
// by the status endpoint. It contains no prompts, credentials, or environment.
type WorkerStatus struct {
	Name                 string         `json:"name"`
	Role                 string         `json:"role,omitempty"`
	Harness              string         `json:"harness,omitempty"`
	TicketActor          string         `json:"ticket_actor,omitempty"`
	TicketActivityAt     time.Time      `json:"ticket_activity_at,omitempty"`
	TicketActivity       string         `json:"ticket_activity,omitempty"`
	TicketActivityTicket string         `json:"ticket_activity_ticket,omitempty"`
	TicketActivitySource string         `json:"ticket_activity_source,omitempty"`
	State                string         `json:"state"`
	Reason               string         `json:"reason,omitempty"`
	Failure              *WorkerFailure `json:"failure,omitempty"`
	TicketFrontierState  string         `json:"ticket_frontier_state,omitempty"`
	RepositoryID         string         `json:"repository_id,omitempty"`
	RepositoryName       string         `json:"repository_name,omitempty"`
	RepositoryKey        string         `json:"repository_key,omitempty"`
	RepositoryPath       string         `json:"repository_path,omitempty"`
	TicketConfig         string         `json:"ticket_config,omitempty"`
	TicketScope          string         `json:"ticket_scope,omitempty"`
	Groups               []string       `json:"groups,omitempty"`
}

// Status is the authoritative daemon snapshot exposed to local clients.
type Status struct {
	Version        string             `json:"version"`
	Protocol       int                `json:"protocol"`
	PID            int                `json:"pid"`
	URL            string             `json:"url"`
	StartedAt      time.Time          `json:"started_at"`
	Mode           string             `json:"mode"`
	ConfigRevision string             `json:"config_revision,omitempty"`
	Workers        []WorkerStatus     `json:"workers"`
	Repositories   []RepositoryStatus `json:"repositories,omitempty"`
	Steer          []SteerStatus      `json:"steer,omitempty"`
}

// SteerStatus reports one dynamic external session without exposing its home
// directory or prompt.
type SteerStatus struct {
	RepositoryID   string `json:"repository_id"`
	RepositoryName string `json:"repository_name,omitempty"`
	Role           string `json:"role"`
	Actor          string `json:"actor"`
	Session        string `json:"session,omitempty"`
	State          string `json:"state"`
	// Ticket identifies the active claim when one was observed.
	Ticket          string `json:"ticket,omitempty"`
	Code            string `json:"code,omitempty"`
	PersistenceCode string `json:"persistence_code,omitempty"`
	ManagedOwner    string `json:"managed_owner,omitempty"`
}

// RepositoryStatus reports bounded health for one configured Ticket repository
// observer. Repository changes are notifications that state may be stale; the
// status snapshot is the recovery source when a notification is missed.
type RepositoryStatus struct {
	ID            string    `json:"id"`
	Key           string    `json:"key"`
	Name          string    `json:"name,omitempty"`
	Path          string    `json:"path,omitempty"`
	State         string    `json:"state"`
	LastEventAt   time.Time `json:"last_event_at,omitempty"`
	LastRestartAt time.Time `json:"last_restart_at,omitempty"`
	RestartCount  int       `json:"restart_count,omitempty"`
	Failure       string    `json:"failure,omitempty"`
}

// Config controls a daemon server. Status is called for every status
// request and should return a snapshot without blocking on external clients.
type Config struct {
	StateDir    string
	EndpointKey string
	// ListenAddress selects the IP literal to bind. Empty uses the default address.
	ListenAddress string
	// Port selects the TCP port. Zero requests an OS-assigned ephemeral port.
	Port              int
	Protocol          int
	PID               int
	Version           string
	Status            func() Status
	RepositoryGateway RepositoryGateway
	Control           *Control
	ReadHeaderTimeout time.Duration
	EventHeartbeat    time.Duration
}

// Server owns one listener, HTTP server, and private endpoint document.
// It does not own the supervisor lock; callers acquire that lock before Start.
type Server struct {
	config          Config
	listener        net.Listener
	http            *http.Server
	endpoint        Endpoint
	endpointPath    string
	startedAt       time.Time
	done            chan error
	shutdownDone    chan struct{}
	events          *eventBroker
	lifecycle       context.Context
	lifecycleCancel context.CancelFunc
	startOnce       sync.Once
	startErr        error
	mu              sync.Mutex
	started         bool
	stopped         bool
}
