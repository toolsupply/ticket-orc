package orc

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
)

// ConversationProfile is resolved server configuration. It is never decoded
// from a creation request. Repositories provide task context, not confinement.
type ConversationProfile struct {
	Name      string
	Execution state.ConversationExecution
}

type ConversationProfileMetadata struct {
	Name              string                    `json:"name"`
	RepositoryIDs     []string                  `json:"repository_ids"`
	OutputGranularity harness.OutputGranularity `json:"output_granularity"`
}

// ConversationProfiles atomically replaces the available creation profiles.
// Existing conversations execute exclusively from their durable captured settings.
type ConversationProfiles struct {
	mu       sync.RWMutex
	profiles map[string]ConversationProfile
}

func NewConversationProfiles(profiles []ConversationProfile) *ConversationProfiles {
	p := &ConversationProfiles{}
	p.Replace(profiles)
	return p
}

func (p *ConversationProfiles) Replace(profiles []ConversationProfile) {
	next := make(map[string]ConversationProfile, len(profiles))
	for _, profile := range profiles {
		profile.Execution.Repositories = slices.Clone(profile.Execution.Repositories)
		next[profile.Name] = profile
	}
	p.mu.Lock()
	p.profiles = next
	p.mu.Unlock()
}

func (p *ConversationProfiles) Metadata() []ConversationProfileMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]ConversationProfileMetadata, 0, len(p.profiles))
	for _, profile := range p.profiles {
		ids := make([]string, 0, len(profile.Execution.Repositories))
		for _, r := range profile.Execution.Repositories {
			ids = append(ids, r.ID)
		}
		result = append(result, ConversationProfileMetadata{Name: profile.Name, RepositoryIDs: ids, OutputGranularity: harness.OutputCompleteMessages})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// ConversationProfileCreate is the entire client-controlled creation payload.
// No actor, paths, execution settings or environment can be supplied here.
type ConversationProfileCreate struct {
	RequestID     string   `json:"request_id"`
	Profile       string   `json:"profile"`
	RepositoryIDs []string `json:"repository_ids"`
	Operator      string   `json:"operator"`
}

func (p *ConversationProfiles) Create(ctx context.Context, policy *Conversations, req ConversationProfileCreate, now time.Time) (state.ConversationReceipt, error) {
	// Deduplicate the client payload before consulting reloaded configuration.
	// Captured execution settings are deliberately excluded from its fingerprint.
	fp := fingerprint(req)
	snapshot, err := policy.Store.Read(ctx)
	if err != nil {
		return state.ConversationReceipt{}, err
	}
	if receipt, ok := snapshot.Receipts["create\x00"+req.RequestID]; ok {
		if receipt.Fingerprint != fp {
			return state.ConversationReceipt{}, conversationError("request_id_conflict")
		}
		return receipt, nil
	}
	p.mu.RLock()
	profile, ok := p.profiles[req.Profile]
	profile.Execution.Repositories = slices.Clone(profile.Execution.Repositories)
	p.mu.RUnlock()
	if !ok {
		return state.ConversationReceipt{}, conversationError("profile_unavailable")
	}
	available := map[string]state.ConversationRepository{}
	for _, r := range profile.Execution.Repositories {
		available[r.ID] = r
	}
	selected := make([]state.ConversationRepository, 0, len(req.RepositoryIDs))
	seen := map[string]bool{}
	for _, id := range req.RepositoryIDs {
		r, ok := available[id]
		if !ok || seen[id] {
			return state.ConversationReceipt{}, conversationError("repository_out_of_scope")
		}
		selected = append(selected, r)
		seen[id] = true
	}
	profile.Execution.Repositories = selected
	return policy.create(ctx, ConversationCreate{RequestID: req.RequestID, Profile: req.Profile, Operator: req.Operator, RepositoryIDs: slices.Clone(req.RepositoryIDs), Execution: profile.Execution}, now, fp)
}

// ConversationPrompt contains only human-directed task context. It establishes
// no scheduling role and never asks the agent to consume a Ticket queue.
func ConversationPrompt(c state.Conversation, text string) string {
	routes, _ := json.Marshal(c.Execution.Repositories)
	return "This is a human-directed operator conversation. Use the Ticket Skill for Ticket operations and the supported ticket-orc CLI/API for Orc management. Do not automatically select or claim queue work. Selected Ticket repositories (task context, not access controls): " + string(routes) + "\n\n" + text
}
