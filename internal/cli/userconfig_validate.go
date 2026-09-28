package cli

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func validateAndNormalizeFileConfig(config *FileConfig, configDir string) error {
	if !isUUIDv4(config.ID) {
		return errors.New("id must be a UUIDv4")
	}
	if strings.TrimSpace(config.LocalDir) == "" && config.LocalDir != "" {
		return errors.New("local_dir must not contain only whitespace")
	}
	if err := validateReviewCompletionPolicy("defaults.review_completion", config.Defaults.ReviewCompletion); err != nil {
		return err
	}
	if config.Roles == nil {
		config.Roles = map[string]RoleFileConfig{}
	}
	if len(config.Roles) > 0 || config.DefaultRole != "" || len(config.Workers) > 0 {
		if err := validateRolePolicies(config); err != nil {
			return err
		}
	}
	if config.Workers == nil {
		config.Workers = map[string]WorkerFileConfig{}
	}
	if config.Repositories == nil {
		config.Repositories = map[string]RepositoryFileConfig{}
	}
	if err := validateReviewSkipTags("review.skip_tags", config.Review.SkipTags); err != nil {
		return err
	}
	if err := validateMinimumReuseContextPercent("defaults.minimum_reuse_context_percent", config.Defaults.MinimumReuseContextPercent); err != nil {
		return err
	}
	for name, repository := range config.Repositories {
		if err := validateRepositoryName(name); err != nil {
			return fmt.Errorf("repository %q: %w", name, err)
		}
		if err := normalizePath(&repository.Repository, configDir, "repositories."+name+".repository"); err != nil {
			return err
		}
		if err := normalizePath(&repository.Ticket.Config, configDir, "repositories."+name+".ticket.config"); err != nil {
			return err
		}
		if strings.TrimSpace(repository.Repository) == "" && (strings.TrimSpace(repository.Ticket.Config) == "" || strings.TrimSpace(repository.Ticket.Scope) == "") {
			return fmt.Errorf("repositories.%s must configure a direct repository or ticket config and scope", name)
		}
		if _, err := resolveTicketTarget(repository.Repository, repository.Ticket); err != nil {
			return fmt.Errorf("repositories.%s: %w", name, err)
		}
		config.Repositories[name] = repository
	}
	if len(config.Repositories) > 0 {
		if config.Defaults.Repository != "" {
			if _, ok := config.Repositories[config.Defaults.Repository]; !ok {
				return fmt.Errorf("defaults.repository references unknown repository %q", config.Defaults.Repository)
			}
		}
		if config.Defaults.Ticket.Config != "" || config.Defaults.Ticket.Scope != "" {
			return errors.New("defaults cannot configure a Ticket target when repositories are configured")
		}
		for name, role := range config.Roles {
			if role.Repository != "" || role.Ticket.Config != "" || role.Ticket.Scope != "" {
				return fmt.Errorf("roles.%s cannot configure a Ticket target when repositories are configured", name)
			}
		}
	}
	if err := validateRequiredSkills("defaults required_skills", config.Defaults.RequiredSkills); err != nil {
		return err
	}
	if err := normalizeWorkingDir(&config.Defaults.WorkingDir, configDir, "defaults.working_dir"); err != nil {
		return err
	}
	if len(config.Repositories) == 0 {
		if err := normalizePath(&config.Defaults.Repository, configDir, "defaults.repository"); err != nil {
			return err
		}
	}
	if err := normalizePath(&config.Defaults.Ticket.Config, configDir, "defaults.ticket.config"); err != nil {
		return err
	}
	for name, roleValue := range config.Roles {
		role := &roleValue
		if err := validateMinimumReuseContextPercent("roles."+name+".minimum_reuse_context_percent", role.MinimumReuseContextPercent); err != nil {
			return err
		}
		if err := validateGroups("role "+name+" groups", role.Groups); err != nil {
			return err
		}
		if err := validateRequiredSkills("role "+name+" required_skills", role.RequiredSkills); err != nil {
			return err
		}
		if err := normalizeWorkingDir(&role.WorkingDir, configDir, "roles."+name+".working_dir"); err != nil {
			return err
		}
		if err := normalizePath(&role.Repository, configDir, "roles."+name+".repository"); err != nil {
			return err
		}
		if err := normalizePath(&role.Ticket.Config, configDir, "roles."+name+".ticket.config"); err != nil {
			return err
		}
		config.Roles[name] = *role
	}
	for name, worker := range config.Workers {
		if err := validateMinimumReuseContextPercent("workers."+name+".minimum_reuse_context_percent", worker.MinimumReuseContextPercent); err != nil {
			return err
		}
		if err := validateWorkerName(name); err != nil {
			return fmt.Errorf("worker %q: %w", name, err)
		}
		if err := validateWorker(name, worker); err != nil {
			return err
		}
		if _, ok := config.Roles[worker.Role]; !ok {
			return fmt.Errorf("worker %q references unknown role %q; configure it in roles", name, worker.Role)
		}
		if err := normalizeWorkingDir(&worker.WorkingDir, configDir, "workers."+name+".working_dir"); err != nil {
			return err
		}
		if len(config.Repositories) == 0 {
			if err := normalizePath(&worker.Repository, configDir, "workers."+name+".repository"); err != nil {
				return err
			}
		} else if worker.Repository != "" {
			if worker.Ticket.Config != "" || worker.Ticket.Scope != "" {
				return fmt.Errorf("worker %q cannot configure ticket target when repositories are configured", name)
			}
			if err := validateRepositoryName(worker.Repository); err != nil {
				return fmt.Errorf("worker %q repository: %w", name, err)
			}
			if _, ok := config.Repositories[worker.Repository]; !ok {
				return fmt.Errorf("worker %q references unknown repository %q", name, worker.Repository)
			}
		}
		if err := normalizePath(&worker.Ticket.Config, configDir, "workers."+name+".ticket.config"); err != nil {
			return err
		}
		config.Workers[name] = worker
	}
	for name := range config.Workers {
		if err := validateRequiredSkills("worker "+name+" effective required_skills", effectiveWorkerRequiredSkills(*config, name)); err != nil {
			return err
		}
		if _, err := effectiveWorkerTicketTarget(*config, name); err != nil {
			return err
		}
	}
	if err := validateGroups("supervisor.startup_groups", config.Supervisor.StartupGroups); err != nil {
		return err
	}
	if _, err := daemon.NormalizeListenAddress(config.Supervisor.ListenAddress); err != nil {
		return fmt.Errorf("supervisor.listen: %w", err)
	}
	if config.Supervisor.Port != nil {
		if err := daemon.ValidateListenPort(*config.Supervisor.Port); err != nil {
			return fmt.Errorf("supervisor.port: %w", err)
		}
	}
	if config.Supervisor.EndpointKey != "" {
		if err := daemon.ValidateEndpointKey(config.Supervisor.EndpointKey); err != nil {
			return err
		}
	}
	return nil
}

func isUUIDv4(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || strings.ToLower(value) != value {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16 && decoded[6]>>4 == 4 && decoded[8]>>6 == 2
}

func validateRolePolicies(config *FileConfig) error {
	if config.DefaultRole == "" || strings.TrimSpace(config.DefaultRole) != config.DefaultRole {
		return errors.New("default_role must name a configured role")
	}
	if _, ok := config.Roles[config.DefaultRole]; !ok {
		return fmt.Errorf("default_role %q is not configured in roles", config.DefaultRole)
	}
	for name, role := range config.Roles {
		if err := validateWorkerName(name); err != nil {
			return fmt.Errorf("role %q: %w", name, err)
		}
		if role.TicketQueue != "open" && role.TicketQueue != "review" {
			return fmt.Errorf("roles.%s.ticket_queue must be open or review", name)
		}
		if role.ReviewCompletion != "" && role.TicketQueue != "review" {
			return fmt.Errorf("roles.%s.review_completion is only valid for review roles", name)
		}
		if err := validateReviewCompletionPolicy("roles."+name+".review_completion", role.ReviewCompletion); err != nil {
			return err
		}
		if strings.TrimSpace(role.NudgePrompt) == "" {
			return fmt.Errorf("roles.%s.nudge_prompt must not be empty", name)
		}
	}
	return nil
}

func validateReviewCompletionPolicy(path, value string) error {
	if value != "" && !oneOf(value, ReviewCompletionSignoff, ReviewCompletionClose) {
		return fmt.Errorf("%s must be signoff or close", path)
	}
	return nil
}

func workflowRole(policy RoleFileConfig) supervisor.Role {
	if policy.TicketQueue == "review" {
		return RoleReviewer
	}
	return RoleCoder
}

func workerPolicy(config FileConfig, worker WorkerFileConfig) RoleFileConfig {
	return config.Roles[worker.Role]
}

func validateMinimumReuseContextPercent(path string, value *int) error {
	if value == nil || (*value >= 0 && *value <= 100) {
		return nil
	}
	return fmt.Errorf("%s must be from 0 to 100", path)
}

func validateReviewSkipTags(label string, tags []string) error {
	if len(tags) > 64 {
		return fmt.Errorf("%s must contain at most 64 tags", label)
	}
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if strings.TrimSpace(tag) == "" || strings.TrimSpace(tag) != tag || strings.IndexFunc(tag, unicode.IsControl) >= 0 || len(tag) > maxIdentifierBytes || !isSafeIdentifier(tag) {
			return fmt.Errorf("%s contains an invalid Ticket tag", label)
		}
		if tag != strings.ToLower(tag) {
			return fmt.Errorf("%s contains a non-canonical Ticket tag", label)
		}
		if !isCanonicalTagToken(tag) {
			return fmt.Errorf("%s contains an invalid Ticket tag", label)
		}
		if _, ok := seen[tag]; ok {
			return fmt.Errorf("%s contains duplicate tag %q", label, tag)
		}
		seen[tag] = struct{}{}
	}
	return nil
}

func isCanonicalTagToken(value string) bool {
	if value == "" || ((value[0] < 'a' || value[0] > 'z') && (value[0] < '0' || value[0] > '9')) {
		return false
	}
	return true
}

func validateRepositoryName(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("repository key must not be empty")
	}
	if len(name) > maxIdentifierBytes {
		return fmt.Errorf("repository key exceeds %d bytes", maxIdentifierBytes)
	}
	for i, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			if i == 0 && r == '.' {
				return errors.New("repository key must start with a letter or digit")
			}
			continue
		}
		return errors.New("repository key must contain only letters, digits, '.', '_' or '-'")
	}
	if name == "." || name == ".." {
		return errors.New("repository key must start with a letter or digit")
	}
	first := name[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || (first >= '0' && first <= '9')) {
		return errors.New("repository key must start with a letter or digit")
	}
	return nil
}

func effectiveWorkerTicketFileConfig(config FileConfig, name string) TicketFileConfig {
	worker, ok := config.Workers[name]
	if !ok {
		return TicketFileConfig{}
	}
	role := workerPolicy(config, worker)
	return TicketFileConfig{
		Config: firstNonEmpty(worker.Ticket.Config, role.Ticket.Config, config.Defaults.Ticket.Config),
		Scope:  firstNonEmpty(worker.Ticket.Scope, role.Ticket.Scope, config.Defaults.Ticket.Scope),
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// effectiveWorkerGroups returns role memberships followed by worker-specific
// memberships, preserving declaration order while removing duplicates.
func effectiveWorkerGroups(config FileConfig, name string) []string {
	worker, ok := config.Workers[name]
	if !ok {
		return nil
	}
	roleGroups := workerPolicy(config, worker).Groups
	groups := make([]string, 0, len(roleGroups)+len(worker.Groups))
	seen := make(map[string]struct{}, len(roleGroups)+len(worker.Groups))
	for _, group := range append(append([]string(nil), roleGroups...), worker.Groups...) {
		if _, exists := seen[group]; exists {
			continue
		}
		seen[group] = struct{}{}
		groups = append(groups, group)
	}
	return groups
}

// effectiveWorkerRequiredSkills returns configured Skills in declaration
// order, with duplicates removed.
func effectiveWorkerRequiredSkills(config FileConfig, name string) []string {
	worker, ok := config.Workers[name]
	if !ok {
		return nil
	}
	roleSkills := workerPolicy(config, worker).RequiredSkills
	values := make([]string, 0, len(config.Defaults.RequiredSkills)+len(roleSkills)+len(worker.RequiredSkills))
	seen := make(map[string]struct{}, cap(values))
	for _, skill := range append(append(append([]string(nil), config.Defaults.RequiredSkills...), roleSkills...), worker.RequiredSkills...) {
		if _, exists := seen[skill]; exists {
			continue
		}
		seen[skill] = struct{}{}
		values = append(values, skill)
	}
	return values
}

func validateWorker(name string, worker WorkerFileConfig) error {
	if worker.Role == "" {
		return fmt.Errorf("worker %q must reference a configured role", name)
	}
	if err := validateGroups("worker "+name+" groups", worker.Groups); err != nil {
		return err
	}
	if err := validateRequiredSkills("worker "+name+" required_skills", worker.RequiredSkills); err != nil {
		return err
	}
	if worker.Harness != "" {
		if worker.Harness != "codex" && worker.Codex.Sandbox != "" {
			return fmt.Errorf("worker %q configures codex settings for harness %q", name, worker.Harness)
		}
		if worker.Harness != "pi" && worker.Pi.Provider != "" {
			return fmt.Errorf("worker %q configures pi settings for harness %q", name, worker.Harness)
		}
		if worker.Harness != "claude" && worker.Claude.PermissionMode != "" {
			return fmt.Errorf("worker %q configures claude settings for harness %q", name, worker.Harness)
		}
	}
	return nil
}

const maxRequiredSkills = 16

func validateRequiredSkills(label string, skills []string) error {
	if len(skills) > maxRequiredSkills {
		return fmt.Errorf("%s must contain at most %d skills", label, maxRequiredSkills)
	}
	seen := make(map[string]struct{}, len(skills))
	for _, skill := range skills {
		if strings.TrimSpace(skill) == "" || strings.TrimSpace(skill) != skill || strings.IndexFunc(skill, unicode.IsControl) >= 0 || len(skill) > maxIdentifierBytes || skill == "." || skill == ".." || !isSafeIdentifier(skill) {
			return fmt.Errorf("%s contains an invalid skill name", label)
		}
		if _, ok := seen[skill]; ok {
			return fmt.Errorf("%s contains duplicate skill %q", label, skill)
		}
		seen[skill] = struct{}{}
	}
	return nil
}

func validateWorkerName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("worker name must not be empty")
	}
	if strings.TrimSpace(name) != name || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return fmt.Errorf("worker name has surrounding whitespace or control characters")
	}
	if len(name) > maxIdentifierBytes {
		return fmt.Errorf("worker name must be at most %d bytes", maxIdentifierBytes)
	}
	if name == "." || name == ".." || !isSafeIdentifier(name) {
		return fmt.Errorf("worker name must use only ASCII letters, digits, dot, underscore, and hyphen")
	}
	return nil
}

func validateGroups(label string, groups []string) error {
	seen := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if strings.TrimSpace(group) == "" || strings.TrimSpace(group) != group || strings.IndexFunc(group, unicode.IsControl) >= 0 || len(group) > maxIdentifierBytes || group == "." || group == ".." || !isSafeIdentifier(group) {
			return fmt.Errorf("%s contains an invalid group name", label)
		}
		if _, ok := seen[group]; ok {
			return fmt.Errorf("%s contains duplicate group %q", label, group)
		}
		seen[group] = struct{}{}
	}
	return nil
}

func isSafeIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func normalizePath(value *string, base, label string) error {
	if strings.TrimSpace(*value) == "" {
		return nil
	}
	path := *value
	if !filepath.IsAbs(path) {
		path = filepath.Join(base, path)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", label, err)
	}
	*value = filepath.Clean(absolute)
	return nil
}

func normalizeWorkingDir(value *string, base, label string) error {
	if err := normalizePath(value, base, label); err != nil {
		return err
	}
	if strings.TrimSpace(*value) == "" {
		return nil
	}
	info, err := os.Stat(*value)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s does not exist", label)
		}
		return fmt.Errorf("stat %s: %w", label, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", label)
	}
	return nil
}
