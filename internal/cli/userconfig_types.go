package cli

import "github.com/toolsupply/ticket-orc/internal/supervisor"

const maxIdentifierBytes = 64

const defaultInstanceDirectoryName = ".ticket-orc"
const instanceConfigFileName = "config.json"

const coderNudgePrompt = "[ticket-orc] Coding work is available. Process all actionable Ticket work for your current actor until none remains. Before submitting each ticket, review your changes against the objective and acceptance criteria and run the relevant checks. Fix obvious issues before handing it to review. Set a goal before starting."
const reviewerNudgePrompt = "[ticket-orc] Review work is available. Perform a substantive review of all actionable Ticket work for your current actor until none remains. Assume the coder has already run the routine/basic test suite; do not rerun the same tests by default. Focus on specification compliance, correctness, edge cases, integration behavior, error handling, and code quality. Run targeted tests only when they help investigate a specific concern. Set a goal before starting."

// FileConfig is the versioned on-disk configuration. It remains separate
// from resolved worker/runtime configuration used by execution.
type FileConfig struct {
	Version      int                             `json:"version"`
	ID           string                          `json:"id,omitempty"`
	LocalDir     string                          `json:"local_dir,omitempty"`
	DefaultRole  string                          `json:"default_role,omitempty"`
	Defaults     DefaultsFileConfig              `json:"defaults"`
	Roles        map[string]RoleFileConfig       `json:"roles"`
	Review       ReviewFileConfig                `json:"review"`
	Repositories map[string]RepositoryFileConfig `json:"repositories"`
	Workers      map[string]WorkerFileConfig     `json:"workers"`
	Supervisor   SupervisorFileConfig            `json:"supervisor"`
}

// ReviewFileConfig contains Orc-owned review-routing policy. Ticket tags stay
// ordinary metadata; these values only affect Orc's worker routing.
type ReviewFileConfig struct {
	SkipTags []string `json:"skip_tags"`
}

// RepositoryFileConfig declares one Ticket repository owned by Orc. The
// direct repository form and the config/scope form are mutually exclusive.
// Workers refer to this entry by its map key rather than repeating the
// Ticket target.
type RepositoryFileConfig struct {
	Repository string           `json:"repository"`
	Ticket     TicketFileConfig `json:"ticket"`
}

type DefaultsFileConfig struct {
	RequiredSkills             []string                    `json:"required_skills"`
	Harness                    string                      `json:"harness"`
	Model                      string                      `json:"model"`
	Reasoning                  string                      `json:"reasoning"`
	SessionPolicy              string                      `json:"session_policy"`
	SessionCleanup             string                      `json:"session_cleanup"`
	MinimumReuseContextPercent *int                        `json:"minimum_reuse_context_percent"`
	ReviewCompletion           string                      `json:"review_completion"`
	TicketPrompt               string                      `json:"ticket_prompt"`
	WorkingDir                 string                      `json:"working_dir"`
	Repository                 string                      `json:"repository"`
	Ticket                     TicketFileConfig            `json:"ticket"`
	Output                     string                      `json:"output"`
	Codex                      supervisor.CodexFileConfig  `json:"codex"`
	Pi                         supervisor.PiFileConfig     `json:"pi"`
	Claude                     supervisor.ClaudeFileConfig `json:"claude"`
}

type RoleFileConfig struct {
	TicketQueue                string                      `json:"ticket_queue"`
	TicketTags                 []string                    `json:"ticket_tags"`
	NudgePrompt                string                      `json:"nudge_prompt"`
	Groups                     []string                    `json:"groups"`
	RequiredSkills             []string                    `json:"required_skills"`
	Harness                    string                      `json:"harness"`
	Actor                      string                      `json:"actor"`
	Model                      string                      `json:"model"`
	Reasoning                  string                      `json:"reasoning"`
	MaxBounces                 *int                        `json:"max_bounces"`
	SessionPolicy              string                      `json:"session_policy"`
	SessionCleanup             string                      `json:"session_cleanup"`
	MinimumReuseContextPercent *int                        `json:"minimum_reuse_context_percent"`
	WorkingDir                 string                      `json:"working_dir"`
	Repository                 string                      `json:"repository"`
	Ticket                     TicketFileConfig            `json:"ticket"`
	Output                     string                      `json:"output"`
	ReviewCompletion           string                      `json:"review_completion"`
	TicketPrompt               string                      `json:"ticket_prompt"`
	Codex                      supervisor.CodexFileConfig  `json:"codex"`
	Pi                         supervisor.PiFileConfig     `json:"pi"`
	Claude                     supervisor.ClaudeFileConfig `json:"claude"`
}

type WorkerFileConfig struct {
	Role                       string                      `json:"role"`
	Groups                     []string                    `json:"groups"`
	Harness                    string                      `json:"harness"`
	Actor                      string                      `json:"actor"`
	Model                      string                      `json:"model"`
	Reasoning                  string                      `json:"reasoning"`
	MaxBounces                 *int                        `json:"max_bounces"`
	SessionPolicy              string                      `json:"session_policy"`
	SessionCleanup             string                      `json:"session_cleanup"`
	MinimumReuseContextPercent *int                        `json:"minimum_reuse_context_percent"`
	WorkingDir                 string                      `json:"working_dir"`
	Repository                 string                      `json:"repository"`
	Ticket                     TicketFileConfig            `json:"ticket"`
	Output                     string                      `json:"output"`
	ReviewCompletion           string                      `json:"review_completion"`
	TicketPrompt               string                      `json:"ticket_prompt"`
	RequiredSkills             []string                    `json:"required_skills"`
	Codex                      supervisor.CodexFileConfig  `json:"codex"`
	Pi                         supervisor.PiFileConfig     `json:"pi"`
	Claude                     supervisor.ClaudeFileConfig `json:"claude"`
}

// TicketFileConfig selects an optional Ticket user configuration and named
// scope. The two fields inherit independently across Orc config levels.
type TicketFileConfig struct {
	Config string `json:"config"`
	Scope  string `json:"scope"`
}

type SupervisorFileConfig struct {
	StartupGroups []string `json:"startup_groups"`
	ListenAddress string   `json:"listen"`
	Port          *int     `json:"port"`
	EndpointKey   string   `json:"endpoint_key"`
}

// InstanceContext carries every resolved filesystem path for one Orc instance.
type InstanceContext struct {
	ConfigPath         string
	InstanceDir        string
	LocalDir           string
	LocalDirConfigured bool
	Explicit           bool
}

// LoadedFileConfig pairs validated configuration with the instance that owns it.
type LoadedFileConfig struct {
	Config   FileConfig
	Instance InstanceContext
}
