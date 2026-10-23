package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type ConversationsFileConfig struct {
	Profiles map[string]ConversationProfileFileConfig `json:"profiles"`
	Limits   ConversationLimitsFileConfig             `json:"limits"`
}

type ConversationProfileFileConfig struct {
	Harness         string                     `json:"harness"`
	Model           string                     `json:"model"`
	Reasoning       string                     `json:"reasoning"`
	WorkingDir      string                     `json:"working_dir"`
	Workspace       string                     `json:"workspace"`
	Actor           string                     `json:"actor"`
	RepositoryIDs   []string                   `json:"repository_ids"`
	Codex           supervisor.CodexFileConfig `json:"codex"`
	MaxDuration     string                     `json:"max_duration"`
	TranscriptBytes *int                       `json:"transcript_bytes"`
}

type ConversationLimitsFileConfig struct {
	Count           *int   `json:"count"`
	ConcurrentTurns *int   `json:"concurrent_turns"`
	TextBytes       *int   `json:"text_bytes"`
	RecordBytes     *int   `json:"record_bytes"`
	TranscriptBytes *int   `json:"transcript_bytes"`
	Receipts        *int   `json:"receipts"`
	ClosedAge       string `json:"closed_age"`
}

func validateConversationConfigFields(raw json.RawMessage) error {
	var section map[string]json.RawMessage
	if err := json.Unmarshal(raw, &section); err != nil {
		return err
	}
	if err := validateObjectFields(section, "conversations", configFieldSet("profiles", "limits")); err != nil {
		return err
	}
	if raw := section["limits"]; raw != nil {
		if err := validateNestedConfigObject(raw, "conversations.limits", configFieldSet("count", "concurrent_turns", "text_bytes", "record_bytes", "transcript_bytes", "receipts", "closed_age")); err != nil {
			return err
		}
	}
	var profiles map[string]json.RawMessage
	if raw := section["profiles"]; raw != nil {
		if err := json.Unmarshal(raw, &profiles); err != nil {
			return err
		}
	}
	for name, raw := range profiles {
		if err := validateNestedConfigObject(raw, "conversations.profiles."+name, configFieldSet("harness", "model", "reasoning", "working_dir", "workspace", "actor", "repository_ids", "codex", "max_duration", "transcript_bytes")); err != nil {
			return err
		}
	}
	return nil
}

func conversationLimits(config ConversationsFileConfig) (orc.ConversationLimits, error) {
	limits := orc.DefaultConversationLimits()
	f := config.Limits
	for _, v := range []struct {
		src *int
		dst *int
	}{{f.Count, &limits.Count}, {f.ConcurrentTurns, &limits.ConcurrentTurns}, {f.TextBytes, &limits.TextBytes}, {f.RecordBytes, &limits.RecordBytes}, {f.TranscriptBytes, &limits.TranscriptBytes}, {f.Receipts, &limits.Receipts}} {
		if v.src != nil {
			*v.dst = *v.src
		}
	}
	if f.ClosedAge != "" {
		d, err := time.ParseDuration(f.ClosedAge)
		if err != nil {
			return limits, fmt.Errorf("conversations.limits.closed_age must be a duration")
		}
		limits.ClosedAge = d
	}
	if _, err := orc.NewConversations(state.NewConversationStore(""), limits); err != nil {
		return limits, fmt.Errorf("conversations.limits: %w", err)
	}
	if limits.TranscriptBytes > state.ConversationStateBytes || limits.RecordBytes > state.ConversationStateBytes || limits.TextBytes > limits.RecordBytes || limits.ConcurrentTurns > limits.Count {
		return limits, fmt.Errorf("conversations.limits exceed storage or admission bounds")
	}
	return limits, nil
}

func validateConversationFileConfig(config *FileConfig, base string) error {
	limits, err := conversationLimits(config.Conversations)
	if err != nil {
		return err
	}
	for name, p := range config.Conversations.Profiles {
		if err := validateWorkerName(name); err != nil {
			return fmt.Errorf("conversations profile name: %w", err)
		}
		p.WorkingDir = firstNonEmpty(p.WorkingDir, config.Defaults.WorkingDir, base)
		p.Workspace = firstNonEmpty(p.Workspace, p.WorkingDir)
		for _, v := range []struct {
			value *string
			label string
		}{{&p.WorkingDir, "working_dir"}, {&p.Workspace, "workspace"}} {
			if err := normalizeWorkingDir(v.value, base, "conversations.profiles."+name+"."+v.label); err != nil {
				return err
			}
			canonical, err := filepath.EvalSymlinks(*v.value)
			if err != nil {
				return fmt.Errorf("conversation profile directory is unavailable")
			}
			*v.value = canonical
		}
		config.Conversations.Profiles[name] = p
		if _, err := resolveConversationExecution(p, config.Defaults, limits); err != nil {
			return fmt.Errorf("conversations.profiles.%s: %w", name, err)
		}
	}
	return nil
}

func resolveConversationExecution(p ConversationProfileFileConfig, d DefaultsFileConfig, limits orc.ConversationLimits) (state.ConversationExecution, error) {
	e := state.ConversationExecution{Harness: firstNonEmpty(p.Harness, d.Harness, defaultHarness), Model: firstNonEmpty(p.Model, d.Model), Reasoning: firstNonEmpty(p.Reasoning, d.Reasoning), WorkingDir: p.WorkingDir, Workspace: p.Workspace, Actor: p.Actor, CodexSandbox: firstNonEmpty(p.Codex.Sandbox, d.Codex.Sandbox), CodexServiceTier: firstNonEmpty(p.Codex.ServiceTier, d.Codex.ServiceTier), MaxDuration: 30 * time.Minute, TranscriptBytes: limits.TranscriptBytes}
	if e.Harness != "codex" {
		return e, fmt.Errorf("conversation harness must be codex")
	}
	if err := ticketclient.ValidateActor(e.Actor); err != nil {
		return e, fmt.Errorf("explicit conversation agent actor is required and must be valid")
	}
	if p.MaxDuration != "" {
		duration, err := time.ParseDuration(p.MaxDuration)
		if err != nil {
			return e, fmt.Errorf("max_duration must be a duration")
		}
		e.MaxDuration = duration
	}
	if e.MaxDuration <= 0 || e.MaxDuration > 24*time.Hour {
		return e, fmt.Errorf("max_duration must be positive and at most 24h")
	}
	if p.TranscriptBytes != nil {
		e.TranscriptBytes = *p.TranscriptBytes
	}
	if e.TranscriptBytes < limits.RecordBytes || e.TranscriptBytes > limits.TranscriptBytes {
		return e, fmt.Errorf("transcript_bytes must fit instance record and transcript limits")
	}
	seen := map[string]bool{}
	for _, id := range p.RepositoryIDs {
		if !ticketclient.ValidRepositoryID(id) || seen[id] {
			return e, fmt.Errorf("repository_ids must contain unique Ticket repository IDs")
		}
		seen[id] = true
	}
	if err := codex.New().Validate(harness.PreflightConfig{Model: e.Model, Reasoning: e.Reasoning, OutputMode: defaultOutput, CodexSandbox: e.CodexSandbox, CodexServiceTier: e.CodexServiceTier}); err != nil {
		return e, fmt.Errorf("invalid conversation harness settings")
	}
	return e, nil
}

// Resolve using Ticket's public info boundary. A profile cannot invent a path
// for a repository ID, and scoped configuration is resolved before child launch.
func resolveConversationProfiles(ctx context.Context, loaded LoadedFileConfig, registry supervisor.RepositoryRegistry, probe repositoryProbe) ([]orc.ConversationProfile, error) {
	limits, err := conversationLimits(loaded.Config.Conversations)
	if err != nil {
		return nil, err
	}
	if len(loaded.Config.Conversations.Profiles) == 0 {
		return nil, nil
	}
	byID := map[string]state.ConversationRepository{}
	for _, key := range repositoryKeys(registry) {
		r := registry[key]
		info, err := probe(ctx, r)
		if err != nil {
			return nil, fmt.Errorf("conversation repository is unavailable")
		}
		if repositoryIdentity(info) == "" || info.Path == "" || !filepath.IsAbs(info.Path) {
			return nil, fmt.Errorf("conversation repository returned invalid identity or routing")
		}
		if _, ok := byID[info.ID]; ok {
			return nil, fmt.Errorf("conversation repositories have duplicate identities")
		}
		byID[info.ID] = state.ConversationRepository{ID: info.ID, Path: info.Path}
	}
	names := make([]string, 0, len(loaded.Config.Conversations.Profiles))
	for name := range loaded.Config.Conversations.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	profiles := make([]orc.ConversationProfile, 0, len(names))
	for _, name := range names {
		p := loaded.Config.Conversations.Profiles[name]
		execution, err := resolveConversationExecution(p, loaded.Config.Defaults, limits)
		if err != nil {
			return nil, err
		}
		for _, id := range p.RepositoryIDs {
			r, ok := byID[id]
			if !ok {
				return nil, fmt.Errorf("conversation profile references unavailable repository ID")
			}
			execution.Repositories = append(execution.Repositories, r)
		}
		profiles = append(profiles, orc.ConversationProfile{Name: name, Execution: execution})
	}
	return profiles, nil
}
