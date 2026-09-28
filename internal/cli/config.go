package cli

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

const (
	RoleCoder           = supervisor.RoleCoder
	RoleReviewer        = supervisor.RoleReviewer
	SessionPolicyTicket = supervisor.SessionPolicyTicket
	SessionPolicyFresh  = supervisor.SessionPolicyFresh
	CleanupDelete       = supervisor.CleanupDelete
	CleanupArchive      = supervisor.CleanupArchive
	CleanupKeep         = supervisor.CleanupKeep
	OutputCompact       = supervisor.OutputCompact
	OutputQuiet         = supervisor.OutputQuiet
	OutputJSON          = supervisor.OutputJSON
)

const (
	ReviewCompletionSignoff           = "signoff"
	ReviewCompletionClose             = "close"
	defaultMinimumReuseContextPercent = 20
)

const (
	TicketTargetImplicit   = supervisor.TicketTargetImplicit
	TicketTargetRepository = supervisor.TicketTargetRepository
	TicketTargetScoped     = supervisor.TicketTargetScoped
)

func resolveTicketTarget(repository string, ticket TicketFileConfig) (supervisor.TicketTarget, error) {
	target := supervisor.TicketTarget{Repository: repository, Config: ticket.Config, Scope: ticket.Scope}
	hasRepository := strings.TrimSpace(repository) != ""
	hasConfig := strings.TrimSpace(ticket.Config) != ""
	hasScope := strings.TrimSpace(ticket.Scope) != ""
	switch {
	case !hasConfig && !hasScope:
		if hasRepository {
			target.Mode = TicketTargetRepository
		} else {
			target.Mode = TicketTargetImplicit
		}
		return target, nil
	case hasConfig != hasScope:
		return supervisor.TicketTarget{}, configValidation("ticket.target_incomplete", "ticket", "ticket config and scope must be configured together", "set both ticket.config and ticket.scope")
	case hasRepository:
		return supervisor.TicketTarget{}, configValidation("ticket.target_mixed", "ticket", "repository and ticket config/scope cannot be configured together", "configure exactly one Ticket target form")
	default:
		target.Mode = TicketTargetScoped
		return target, nil
	}
}

func splitReviewSkipTags(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, "\x1f")
}

func effectiveWorkerTicketTarget(config FileConfig, name string) (supervisor.TicketTarget, error) {
	worker, ok := config.Workers[name]
	if !ok {
		return supervisor.TicketTarget{}, configValidation("ticket.worker_missing", "workers."+name, "worker is not configured", "select a configured worker")
	}
	if len(config.Repositories) > 0 {
		repositoryKey := worker.Repository
		if repositoryKey == "" {
			role := workerPolicy(config, worker)
			repositoryKey = firstNonEmpty(role.Repository, config.Defaults.Repository)
		}
		if repositoryKey == "" {
			return supervisor.TicketTarget{}, configValidation("repository.required", "repository", "worker must reference a configured repository", "set workers."+name+".repository to a repository key")
		}
		entry, ok := config.Repositories[repositoryKey]
		if !ok {
			return supervisor.TicketTarget{}, configValidation("repository.unknown", "repository", "worker references an unknown configured repository", "choose a key from repositories")
		}
		return resolveTicketTarget(entry.Repository, entry.Ticket)
	}
	repository := config.Defaults.Repository
	role := workerPolicy(config, worker)
	if role.Repository != "" {
		repository = role.Repository
	}
	if worker.Repository != "" {
		repository = worker.Repository
	}
	target, err := resolveTicketTarget(repository, effectiveWorkerTicketFileConfig(config, name))
	if err != nil {
		if validation, ok := err.(*ConfigValidationError); ok {
			validation.Path = "workers." + name + ".ticket"
		}
		return supervisor.TicketTarget{}, err
	}
	return target, nil
}

// ConfigValidationError is a stable, safe semantic validation finding. It
// deliberately omits rejected values such as prompts, actors, and targets.
type ConfigValidationError struct {
	Code        string
	Path        string
	Message     string
	Remediation string
	Detail      string
}

func (e *ConfigValidationError) Error() string {
	if e == nil {
		return "configuration validation failed"
	}
	if e.Detail != "" {
		return e.Detail
	}
	switch e.Code {
	case "harness.unsupported":
		return "unsupported harness"
	case "reasoning.invalid":
		return "Codex reasoning value is not supported"
	case "session_policy.invalid":
		return "session-policy must be ticket or fresh"
	case "session_cleanup.invalid":
		return "session-cleanup must be delete, archive, or keep"
	case "minimum_reuse_context_percent.invalid":
		return "minimum-reuse-context-percent must be an integer from 0 to 100"
	case "output.invalid":
		return "output must be compact, quiet, or json"
	}
	return e.Code + ": " + e.Message
}

func configValidation(code, path, message, remediation string) error {
	return &ConfigValidationError{Code: code, Path: path, Message: message, Remediation: remediation}
}

func configValidationDetail(code, path, message, remediation, detail string) error {
	return &ConfigValidationError{Code: code, Path: path, Message: message, Remediation: remediation, Detail: detail}
}

type envLookup func(string) (string, bool)

const (
	defaultHarness        = "codex"
	defaultMaxBounces     = "6"
	defaultSessionPolicy  = "ticket"
	defaultSessionCleanup = "delete"
	defaultOutput         = "compact"
	launchSnapshotVersion = 1
	launchSnapshotEnv     = "TICKET_ORC_LAUNCH_SNAPSHOT"
)

// launchSnapshot is the supervisor-approved worker policy sent to a child.
// Children decode this value before considering the config path, so a config
// edit after supervisor validation cannot change an already-approved launch.
type launchSnapshot struct {
	Version        int                   `json:"version"`
	Worker         string                `json:"worker"`
	Role           supervisor.Role       `json:"role"`
	RequiredSkills []string              `json:"required_skills,omitempty"`
	Config         supervisor.RoleConfig `json:"config"`
}

func encodeLaunchSnapshot(worker supervisor.RunWorker) (string, error) {
	payload, err := json.Marshal(launchSnapshot{
		Version:        launchSnapshotVersion,
		Worker:         worker.Name,
		Role:           worker.Config.Role,
		RequiredSkills: append([]string(nil), worker.RequiredSkills...),
		Config:         worker.Config,
	})
	if err != nil {
		return "", fmt.Errorf("encode worker launch snapshot: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// equalLaunchSnapshots compares the resolved policy captured by a worker
// child. Group membership remains supervisor metadata and is intentionally
// live-reloadable; every field in the launch snapshot affects a new child.
func equalLaunchSnapshots(left, right supervisor.RunWorker) bool {
	return reflect.DeepEqual(
		launchSnapshot{Worker: left.Name, Role: left.Config.Role, RequiredSkills: left.RequiredSkills, Config: left.Config},
		launchSnapshot{Worker: right.Name, Role: right.Config.Role, RequiredSkills: right.RequiredSkills, Config: right.Config},
	)
}

func decodeLaunchSnapshot(role supervisor.Role, args []string, encoded string) (supervisor.RoleConfig, bool, error) {
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return supervisor.RoleConfig{}, false, fmt.Errorf("decode worker launch snapshot: %w", err)
	}
	var snapshot launchSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return supervisor.RoleConfig{}, false, fmt.Errorf("decode worker launch snapshot: %w", err)
	}
	if snapshot.Version != launchSnapshotVersion || snapshot.Role != role || snapshot.Config.Role != role || snapshot.Worker == "" || snapshot.Config.WorkerName != snapshot.Worker {
		return supervisor.RoleConfig{}, false, errors.New("worker launch snapshot is incompatible")
	}
	values, help, err := parseRoleFlags(args)
	if err != nil || help {
		return supervisor.RoleConfig{}, help, err
	}
	if worker, ok := values["worker"]; ok && worker != snapshot.Worker {
		return supervisor.RoleConfig{}, false, errors.New("worker launch snapshot does not match selected worker")
	}
	return snapshot.Config, false, nil
}

func parseRoleConfig(role supervisor.Role, args []string, lookupEnv envLookup) (supervisor.RoleConfig, bool, error) {
	if encoded, ok := lookupEnv(launchSnapshotEnv); ok && strings.TrimSpace(encoded) != "" {
		return decodeLaunchSnapshot(role, args, encoded)
	}
	values, help, err := parseRoleFlags(args)
	if err != nil || help {
		return supervisor.RoleConfig{}, help, err
	}
	loaded, err := loadInvocationConfig(values, lookupEnv)
	if err != nil {
		return supervisor.RoleConfig{}, false, err
	}
	return resolveRoleConfig(role, values, loaded, lookupEnv)
}

// resolveInheritedConfigValue applies the declarative defaults -> role ->
// worker chain. Empty values mean that the layer does not override the next
// layer, matching the configuration file's optional-field semantics.
func resolveInheritedConfigValue(workerValue, roleValue, defaultValue, builtin string) string {
	if workerValue != "" {
		return workerValue
	}
	if roleValue != "" {
		return roleValue
	}
	if defaultValue != "" {
		return defaultValue
	}
	return builtin
}

// applyConfigOverrides applies the supported invocation-level override chain
// on top of an already resolved declarative value.
func applyConfigOverrides(flag, env string, values map[string]string, lookupEnv envLookup, inherited string) string {
	if value, ok := values[flag]; ok {
		return value
	}
	if value, ok := lookupEnv(env); ok {
		return value
	}
	return inherited
}

func resolveRoleConfig(role supervisor.Role, values map[string]string, loaded LoadedFileConfig, lookupEnv envLookup) (supervisor.RoleConfig, bool, error) {
	workerName, worker, err := selectWorker(role, values, lookupEnv, loaded.Config)
	if err != nil {
		return supervisor.RoleConfig{}, false, err
	}
	var roleDefaults RoleFileConfig
	if worker != nil {
		roleDefaults = loaded.Config.Roles[worker.Role]
	} else {
		roleDefaults = loaded.Config.Roles[loaded.Config.DefaultRole]
	}
	if worker == nil && workflowRole(roleDefaults) != role {
		roleNames := make([]string, 0, len(loaded.Config.Roles))
		for name := range loaded.Config.Roles {
			roleNames = append(roleNames, name)
		}
		sort.Strings(roleNames)
		for _, name := range roleNames {
			candidate := loaded.Config.Roles[name]
			if workflowRole(candidate) == role {
				roleDefaults = candidate
				break
			}
		}
	}
	if role == RoleCoder {
		if _, ok := values["review-completion"]; ok {
			return supervisor.RoleConfig{}, false, configValidationDetail("review_completion.invalid", "review_completion", "review completion is only valid for reviewers", "remove it from coder settings", "review-completion is only valid for reviewer")
		}
		if _, ok := lookupEnv("TICKET_ORC_REVIEW_COMPLETION"); ok {
			return supervisor.RoleConfig{}, false, configValidationDetail("review_completion.invalid", "review_completion", "review completion is only valid for reviewers", "remove it from coder settings", "TICKET_ORC_REVIEW_COMPLETION is only valid for reviewer")
		}
		if worker != nil && worker.ReviewCompletion != "" {
			return supervisor.RoleConfig{}, false, configValidationDetail("review_completion.invalid", "review_completion", "review completion is only valid for reviewers", "remove it from coder settings", "selected coder worker cannot set review-completion")
		}
		if roleDefaults.ReviewCompletion != "" {
			return supervisor.RoleConfig{}, false, configValidationDetail("review_completion.invalid", "review_completion", "review completion is only valid for reviewers", "remove it from coder settings", "coder role defaults cannot set review-completion")
		}
	}
	builtinPrompt := orc.CoderTicketPromptTemplate
	if role == RoleReviewer {
		builtinPrompt = orc.ReviewerTicketPromptTemplate
	}

	value := func(flag, env, workerValue, roleValue, defaultValue, builtin string) string {
		return applyConfigOverrides(flag, env, values, lookupEnv, resolveInheritedConfigValue(workerValue, roleValue, defaultValue, builtin))
	}

	cleanupExplicit := hasExplicitValue(values, lookupEnv, "session-cleanup", "TICKET_ORC_SESSION_CLEANUP") ||
		(worker != nil && worker.SessionCleanup != "") || roleDefaults.SessionCleanup != "" || loaded.Config.Defaults.SessionCleanup != ""
	cleanup := value("session-cleanup", "TICKET_ORC_SESSION_CLEANUP", workerString(worker, func(w WorkerFileConfig) string { return w.SessionCleanup }), roleDefaults.SessionCleanup, loaded.Config.Defaults.SessionCleanup, "")
	minimumReuseContextPercent := value("minimum-reuse-context-percent", "TICKET_ORC_MINIMUM_REUSE_CONTEXT_PERCENT", workerInt(worker, func(w WorkerFileConfig) *int { return w.MinimumReuseContextPercent }), intPointerString(roleDefaults.MinimumReuseContextPercent), intPointerString(loaded.Config.Defaults.MinimumReuseContextPercent), strconv.Itoa(defaultMinimumReuseContextPercent))
	minimumReuseContextPercentValue, err := strconv.Atoi(minimumReuseContextPercent)
	if err != nil {
		return supervisor.RoleConfig{}, false, configValidation("minimum_reuse_context_percent.invalid", "minimum_reuse_context_percent", "minimum reuse context percent must be an integer", "set a percentage from 0 to 100")
	}
	if cleanup == "" && !cleanupExplicit {
		cleanup = defaultSessionCleanup
		if harnessName := value("harness", "TICKET_ORC_HARNESS", workerString(worker, func(w WorkerFileConfig) string { return w.Harness }), roleDefaults.Harness, loaded.Config.Defaults.Harness, defaultHarness); harnessName == "pi" || harnessName == "claude" {
			cleanup = string(CleanupKeep)
		}
	}
	workingDir := loaded.Config.Defaults.WorkingDir
	if roleDefaults.WorkingDir != "" {
		workingDir = roleDefaults.WorkingDir
	}
	if worker != nil && worker.WorkingDir != "" {
		workingDir = worker.WorkingDir
	}
	repository := loaded.Config.Defaults.Repository
	repositoryKey := ""
	if roleDefaults.Repository != "" {
		repository = roleDefaults.Repository
	}
	ticketFile := TicketFileConfig{
		Config: firstNonEmpty(roleDefaults.Ticket.Config, loaded.Config.Defaults.Ticket.Config),
		Scope:  firstNonEmpty(roleDefaults.Ticket.Scope, loaded.Config.Defaults.Ticket.Scope),
	}
	if worker != nil && worker.Repository != "" {
		repository = worker.Repository
	}
	if worker != nil {
		ticketFile.Config = firstNonEmpty(worker.Ticket.Config, ticketFile.Config)
		ticketFile.Scope = firstNonEmpty(worker.Ticket.Scope, ticketFile.Scope)
	}
	if len(loaded.Config.Repositories) > 0 {
		repositoryKey = repository
		if worker != nil && worker.Repository != "" {
			repositoryKey = worker.Repository
		}
		if repositoryKey == "" {
			return supervisor.RoleConfig{}, false, configValidation("repository.required", "repository", "worker must reference a configured repository", "set the worker repository key")
		}
		entry, ok := loaded.Config.Repositories[repositoryKey]
		if !ok {
			return supervisor.RoleConfig{}, false, configValidation("repository.unknown", "repository", "worker references an unknown configured repository", "choose a key from repositories")
		}
		repository = entry.Repository
		ticketFile = entry.Ticket
	}
	ticketTarget, err := resolveTicketTarget(repository, ticketFile)
	if err != nil {
		if validation, ok := err.(*ConfigValidationError); ok && workerName != "" {
			validation.Path = "workers." + workerName + ".ticket"
		}
		return supervisor.RoleConfig{}, false, err
	}
	stateDir := loaded.Instance.LocalDir
	config := supervisor.RoleConfig{
		WorkerName:                 workerName,
		Role:                       role,
		Harness:                    value("harness", "TICKET_ORC_HARNESS", workerString(worker, func(w WorkerFileConfig) string { return w.Harness }), roleDefaults.Harness, loaded.Config.Defaults.Harness, defaultHarness),
		Actor:                      value("actor", "TICKET_ORC_ACTOR", workerString(worker, func(w WorkerFileConfig) string { return w.Actor }), roleDefaults.Actor, "", string(role)),
		Model:                      value("model", "TICKET_ORC_MODEL", workerString(worker, func(w WorkerFileConfig) string { return w.Model }), roleDefaults.Model, loaded.Config.Defaults.Model, ""),
		Reasoning:                  value("reasoning", "TICKET_ORC_REASONING", workerString(worker, func(w WorkerFileConfig) string { return w.Reasoning }), roleDefaults.Reasoning, loaded.Config.Defaults.Reasoning, ""),
		SessionPolicy:              supervisor.SessionPolicy(value("session-policy", "TICKET_ORC_SESSION_POLICY", workerString(worker, func(w WorkerFileConfig) string { return w.SessionPolicy }), roleDefaults.SessionPolicy, loaded.Config.Defaults.SessionPolicy, defaultSessionPolicy)),
		SessionCleanup:             supervisor.CleanupPolicy(cleanup),
		MinimumReuseContextPercent: minimumReuseContextPercentValue,
		StateDir:                   stateDir,
		WorkingDir:                 workingDir,
		Repository:                 repository,
		RepositoryKey:              repositoryKey,
		Ticket:                     ticketTarget,
		Output:                     supervisor.OutputMode(value("output", "TICKET_ORC_OUTPUT", workerString(worker, func(w WorkerFileConfig) string { return w.Output }), roleDefaults.Output, loaded.Config.Defaults.Output, defaultOutput)),
		TicketPrompt:               value("ticket-prompt", "TICKET_ORC_TICKET_PROMPT", workerString(worker, func(w WorkerFileConfig) string { return w.TicketPrompt }), roleDefaults.TicketPrompt, loaded.Config.Defaults.TicketPrompt, builtinPrompt),
		ReviewSkipTags:             strings.Join(loaded.Config.Review.SkipTags, "\x1f"),
		Codex:                      supervisor.CodexFileConfig{Sandbox: value("codex-sandbox", "TICKET_ORC_CODEX_SANDBOX", workerString(worker, func(w WorkerFileConfig) string { return w.Codex.Sandbox }), roleDefaults.Codex.Sandbox, loaded.Config.Defaults.Codex.Sandbox, "")},
		Pi:                         supervisor.PiFileConfig{Provider: value("pi-provider", "TICKET_ORC_PI_PROVIDER", workerString(worker, func(w WorkerFileConfig) string { return w.Pi.Provider }), roleDefaults.Pi.Provider, loaded.Config.Defaults.Pi.Provider, "")},
		Claude:                     supervisor.ClaudeFileConfig{PermissionMode: value("claude-permission-mode", "TICKET_ORC_CLAUDE_PERMISSION_MODE", workerString(worker, func(w WorkerFileConfig) string { return w.Claude.PermissionMode }), roleDefaults.Claude.PermissionMode, loaded.Config.Defaults.Claude.PermissionMode, "")},
	}
	if role == RoleReviewer {
		config.ReviewCompletion = value("review-completion", "TICKET_ORC_REVIEW_COMPLETION", workerString(worker, func(w WorkerFileConfig) string { return w.ReviewCompletion }), roleDefaults.ReviewCompletion, loaded.Config.Defaults.ReviewCompletion, ReviewCompletionSignoff)
	} else {
		config.ReviewFinalGate = firstNonEmpty(reviewCompletionForQueue(loaded.Config.Roles), loaded.Config.Defaults.ReviewCompletion)
	}
	maxBounces := value("max-bounces", "TICKET_ORC_MAX_BOUNCES", workerInt(worker, func(w WorkerFileConfig) *int { return w.MaxBounces }), intPointerString(roleDefaults.MaxBounces), "", defaultMaxBounces)
	config.MaxBounces, err = strconv.Atoi(maxBounces)
	if err != nil || config.MaxBounces < 1 {
		return supervisor.RoleConfig{}, false, configValidation("max_bounces.invalid", "max_bounces", "max bounces must be a positive integer", "set a positive integer")
	}
	if err := validateRoleConfig(config, values, lookupEnv); err != nil {
		return supervisor.RoleConfig{}, false, err
	}
	return config, false, nil
}

func reviewCompletionForQueue(roles map[string]RoleFileConfig) string {
	names := make([]string, 0, len(roles))
	for name := range roles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		role := roles[name]
		if role.TicketQueue == "review" && role.ReviewCompletion != "" {
			return role.ReviewCompletion
		}
	}
	return ""
}

func workerString(worker *WorkerFileConfig, get func(WorkerFileConfig) string) string {
	if worker == nil {
		return ""
	}
	return get(*worker)
}

func workerInt(worker *WorkerFileConfig, get func(WorkerFileConfig) *int) string {
	if worker == nil {
		return ""
	}
	return intPointerString(get(*worker))
}

func intPointerString(value *int) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(*value)
}

func selectWorker(role supervisor.Role, values map[string]string, lookupEnv envLookup, config FileConfig) (string, *WorkerFileConfig, error) {
	workers := config.Workers
	policyFor := func(worker WorkerFileConfig) RoleFileConfig { return workerPolicy(config, worker) }
	selected, explicit := values["worker"]
	if !explicit {
		selected, explicit = lookupEnv("TICKET_ORC_WORKER")
	}
	if explicit {
		if strings.TrimSpace(selected) == "" {
			return "", nil, fmt.Errorf("worker must not be empty")
		}
		worker, ok := workers[selected]
		if !ok {
			return "", nil, fmt.Errorf("worker %q is not configured", selected)
		}
		if workflowRole(policyFor(worker)) != role {
			return "", nil, fmt.Errorf("worker %q has role %q, cannot run %s", selected, worker.Role, role)
		}
		copy := worker
		return selected, &copy, nil
	}
	if len(workers) == 0 {
		return "", nil, nil
	}
	matches := make([]string, 0)
	for name, worker := range workers {
		if workflowRole(policyFor(worker)) == role {
			matches = append(matches, name)
		}
	}
	sort.Strings(matches)
	if len(matches) == 1 {
		worker := workers[matches[0]]
		return matches[0], &worker, nil
	}
	for _, name := range matches {
		if name == string(role) {
			worker := workers[name]
			return name, &worker, nil
		}
	}
	if len(matches) == 0 {
		return "", nil, fmt.Errorf("no configured %s worker; use --worker or TICKET_ORC_WORKER", role)
	}
	return "", nil, fmt.Errorf("multiple configured %s workers (%s); select one with --worker", role, strings.Join(matches, ", "))
}

func validateRoleConfig(config supervisor.RoleConfig, values map[string]string, lookupEnv envLookup) error {
	if config.Role != RoleCoder && config.Role != RoleReviewer {
		return configValidation("role.invalid", "role", "worker role is invalid", "choose coder or reviewer")
	}
	if config.Harness != "codex" {
		if config.Harness != "pi" && config.Harness != "claude" {
			return configValidation("harness.unsupported", "harness", "worker harness is unsupported", "choose codex, pi, or claude")
		}
	}
	if strings.TrimSpace(config.Actor) == "" {
		return configValidation("actor.required", "actor", "worker actor is required", "set an actor identity")
	}
	if strings.IndexFunc(config.Actor, unicode.IsSpace) >= 0 {
		return configValidation("actor.invalid", "actor", "worker actor must not contain whitespace", "set a valid actor identity")
	}
	if !oneOf(string(config.SessionPolicy), string(SessionPolicyTicket), string(SessionPolicyFresh)) {
		return configValidation("session_policy.invalid", "session_policy", "session policy is invalid", "choose ticket or fresh")
	}
	if !oneOf(string(config.SessionCleanup), string(CleanupDelete), string(CleanupArchive), string(CleanupKeep)) {
		return configValidation("session_cleanup.invalid", "session_cleanup", "session cleanup is invalid", "choose delete, archive, or keep")
	}
	if config.MinimumReuseContextPercent < 0 || config.MinimumReuseContextPercent > 100 {
		return configValidation("minimum_reuse_context_percent.invalid", "minimum_reuse_context_percent", "minimum reuse context percent must be from 0 to 100", "set a percentage from 0 to 100")
	}
	if strings.TrimSpace(config.StateDir) == "" {
		return configValidation("local_dir.required", "local_dir", "local runtime directory is required", "check the selected Orc config")
	}
	if !oneOf(string(config.Output), string(OutputCompact), string(OutputQuiet), string(OutputJSON)) {
		return configValidation("output.invalid", "output", "output mode is invalid", "choose compact, quiet, or json")
	}
	if config.Role == RoleCoder {
		if config.ReviewCompletion != "" {
			return configValidation("review_completion.invalid", "review_completion", "review completion is only valid for reviewers", "remove it from coder settings")
		}
	} else if !oneOf(config.ReviewCompletion, ReviewCompletionSignoff, ReviewCompletionClose) {
		return configValidation("review_completion.invalid", "review_completion", "review completion is invalid", "choose signoff or close")
	}
	if strings.ContainsRune(config.Model, '\x00') {
		return configValidation("model.invalid", "model", "model contains a control character", "set a valid model name")
	}
	if strings.ContainsRune(config.Reasoning, '\x00') {
		return configValidation("reasoning.invalid", "reasoning", "reasoning contains a control character", "set a supported reasoning level")
	}
	switch config.Harness {
	case "codex":
		if config.Reasoning != "" && !oneOf(config.Reasoning, "low", "medium", "high", "xhigh", "max", "ultra") {
			return configValidation("reasoning.invalid", "reasoning", "reasoning is unsupported by the selected harness", "choose a supported reasoning level")
		}
		if config.Codex.Sandbox != "" && !oneOf(config.Codex.Sandbox, "read-only", "workspace-write", "danger-full-access") {
			return configValidation("codex_sandbox.invalid", "codex.sandbox", "Codex sandbox mode is invalid", "choose a supported sandbox mode")
		}
	case "pi":
		if config.Reasoning != "" && !oneOf(config.Reasoning, "off", "minimal", "low", "medium", "high", "xhigh", "max") {
			return configValidation("reasoning.invalid", "reasoning", "reasoning is unsupported by the selected harness", "choose a supported reasoning level")
		}
		if strings.ContainsRune(config.Pi.Provider, '\x00') {
			return configValidation("pi_provider.invalid", "pi.provider", "Pi provider contains a control character", "set a valid provider")
		}
	case "claude":
		if config.Reasoning != "" && !oneOf(config.Reasoning, "low", "medium", "high", "xhigh", "max", "ultracode") {
			return configValidation("reasoning.invalid", "reasoning", "reasoning is unsupported by the selected harness", "choose a supported reasoning level")
		}
		if config.Claude.PermissionMode != "" && !oneOf(config.Claude.PermissionMode, "default", "acceptEdits", "plan", "auto", "dontAsk", "bypassPermissions", "manual") {
			return configValidation("claude_permission_mode.invalid", "claude.permission_mode", "Claude permission mode is invalid", "choose a supported permission mode")
		}
	}
	if err := orc.ValidateTicketPrompt(config.TicketPrompt, string(config.Role)); err != nil {
		return configValidationDetail("ticket_prompt.invalid", "ticket_prompt", "ticket prompt is invalid", "use the role ticket prompt template", err.Error())
	}
	if err := validateHarnessNamespace(config, values, lookupEnv); err != nil {
		detail := "selected " + string(config.Harness) + " harness cannot use incompatible settings"
		switch config.Harness {
		case "codex":
			detail = "selected codex harness cannot use pi or claude settings"
		case "pi":
			detail = "selected pi harness cannot use codex or claude settings"
		case "claude":
			detail = "selected claude harness cannot use codex or pi settings"
		}
		return configValidationDetail("harness_settings.invalid", "harness", "harness-specific settings are invalid", "remove settings for other harnesses", detail)
	}
	return nil
}

func defaultTicketPrompt(role supervisor.Role) string {
	if role == RoleReviewer {
		return orc.ReviewerTicketPromptTemplate
	}
	return orc.CoderTicketPromptTemplate
}

func validateHarnessNamespace(config supervisor.RoleConfig, values map[string]string, lookupEnv envLookup) error {
	if strings.IndexFunc(config.Codex.Sandbox, unicode.IsControl) >= 0 || strings.ContainsRune(config.Codex.Sandbox, '\x00') {
		return fmt.Errorf("codex-sandbox must not contain control characters")
	}
	if strings.IndexFunc(config.Pi.Provider, unicode.IsControl) >= 0 || strings.ContainsRune(config.Pi.Provider, '\x00') {
		return fmt.Errorf("pi-provider must not contain control characters")
	}
	if strings.IndexFunc(config.Claude.PermissionMode, unicode.IsControl) >= 0 || strings.ContainsRune(config.Claude.PermissionMode, '\x00') {
		return fmt.Errorf("claude-permission-mode must not contain control characters")
	}
	switch config.Harness {
	case "codex":
		if config.Pi.Provider != "" || config.Claude.PermissionMode != "" || hasExplicitValue(values, lookupEnv, "pi-provider", "TICKET_ORC_PI_PROVIDER") || hasExplicitValue(values, lookupEnv, "claude-permission-mode", "TICKET_ORC_CLAUDE_PERMISSION_MODE") {
			return fmt.Errorf("selected codex harness cannot use pi or claude settings")
		}
	case "pi":
		if config.Codex.Sandbox != "" || config.Claude.PermissionMode != "" || hasExplicitValue(values, lookupEnv, "codex-sandbox", "TICKET_ORC_CODEX_SANDBOX") || hasExplicitValue(values, lookupEnv, "claude-permission-mode", "TICKET_ORC_CLAUDE_PERMISSION_MODE") {
			return fmt.Errorf("selected pi harness cannot use codex or claude settings")
		}
	case "claude":
		if config.Codex.Sandbox != "" || config.Pi.Provider != "" || hasExplicitValue(values, lookupEnv, "codex-sandbox", "TICKET_ORC_CODEX_SANDBOX") || hasExplicitValue(values, lookupEnv, "pi-provider", "TICKET_ORC_PI_PROVIDER") {
			return fmt.Errorf("selected claude harness cannot use codex or pi settings")
		}
	}
	return nil
}

func hasExplicitValue(values map[string]string, lookupEnv envLookup, flag, env string) bool {
	if _, ok := values[flag]; ok {
		return true
	}
	_, ok := lookupEnv(env)
	return ok
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
