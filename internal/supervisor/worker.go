package supervisor

import (
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type Role string

const (
	RoleCoder    Role = "coder"
	RoleReviewer Role = "reviewer"
)

type SessionPolicy string

const (
	SessionPolicyTicket SessionPolicy = "ticket"
	SessionPolicyFresh  SessionPolicy = "fresh"
)

type CleanupPolicy string

const (
	CleanupDelete  CleanupPolicy = "delete"
	CleanupArchive CleanupPolicy = "archive"
	CleanupKeep    CleanupPolicy = "keep"
)

type OutputMode string

const (
	OutputCompact OutputMode = "compact"
	OutputQuiet   OutputMode = "quiet"
	OutputJSON    OutputMode = "json"
)

type CodexFileConfig struct {
	Sandbox string `json:"sandbox"`
}
type PiFileConfig struct {
	Provider string `json:"provider"`
}
type ClaudeFileConfig struct {
	PermissionMode string `json:"permission_mode"`
}

// RoleConfig is the resolved process configuration consumed by the runtime.
type RoleConfig struct {
	WorkerName                 string
	Role                       Role
	RoleName                   string
	TicketQueue                string
	Harness                    string
	Actor                      string
	Model                      string
	Reasoning                  string
	MaxBounces                 int
	SessionPolicy              SessionPolicy
	SessionCleanup             CleanupPolicy
	MinimumReuseContextPercent int
	StateDir                   string
	InstanceID                 string
	LocalDirConfigured         bool
	WorkingDir                 string
	Repository                 string
	RepositoryKey              string `json:"repository_key,omitempty"`
	// RepositoryIdentity is Ticket's stable repository ID after target preflight.
	RepositoryIdentity string `json:"repository_identity,omitempty"`
	Ticket             TicketTarget
	Output             OutputMode
	ReviewCompletion   string
	ReviewFinalGate    string
	TicketTags         string
	ReviewSkipTags     string
	TicketPrompt       string
	Codex              CodexFileConfig
	Pi                 PiFileConfig
	Claude             ClaudeFileConfig
}

// RunWorker is one fully resolved worker selected for a supervisor run.
type RunWorker struct {
	Name           string
	Groups         []string
	RequiredSkills []string
	Config         RoleConfig
	TicketInfo     *ticketclient.RepositoryInfo
	EventSink      orc.EventSink
}

func (worker RunWorker) SupervisorWorkerName() string { return worker.Name }
