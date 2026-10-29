package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/orc"
)

func TestSteerPromptDefaultsFollowQueue(t *testing.T) {
	for _, test := range []struct{ name, queue, want string }{
		{"quality", "open", coderNudgePrompt},
		{"architect", "review", reviewerNudgePrompt},
		{"coder", "signoff", architectNudgePrompt},
	} {
		for _, override := range []string{"", `,"nudge_prompt":""`, `,"nudge_prompt":" custom\nsteering "`, `,"nudge_prompt":"Make the final acceptance decision for work approved by the reviewer."`} {
			t.Run(test.name+override, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "config.json")
				data := fmt.Sprintf(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":%q,"roles":{%q:{"ticket_queue":%q%s}}}`, test.name, test.name, test.queue, override)
				if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}
				loaded, err := LoadFileConfig(filepath.Dir(path), path, true)
				if err != nil {
					t.Fatal(err)
				}
				policies, err := resolveSteerRolePolicies(loaded.Config, emptyEnv)
				if err != nil {
					t.Fatal(err)
				}
				want := test.want
				if strings.Contains(override, "custom") {
					want = " custom\nsteering "
				}
				if strings.Contains(override, "Make the final acceptance") {
					want = "Make the final acceptance decision for work approved by the reviewer."
				}
				policy := policies[test.name]
				if policy.NudgePrompt != want {
					t.Fatalf("prompt = %q, want %q", policy.NudgePrompt, want)
				}
				if test.queue == "signoff" {
					if override == "" || override == `,"nudge_prompt":""` {
						assertSeriousHoldGuidance(t, policy.NudgePrompt)
					}
					return // Signoff delivery uses its separate exact-target path.
				}
				plan := planSteerNotification(policy, string(orc.SteerIdle), "", "20261020-12345", false, false)
				if !plan.workBearing || !strings.Contains(plan.message, want) {
					t.Fatalf("operational wake omitted resolved prompt: %#v", plan)
				}
				if !strings.Contains(plan.message, steerOneItemTurnPrompt) {
					t.Fatalf("operational wake omitted one-ticket boundary: %q", plan.message)
				}
			})
		}
	}
}

func TestMinimalManagedConfigWithoutPromptOverrides(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	// Write the complete fixture directly: helpers must not supply role prompts.
	data := `{
  "version": 1,
  "id": "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa",
  "default_role": "backend",
  "defaults": {"working_dir": ".", "repository": "."},
  "roles": {
    "backend": {"ticket_queue": "open"},
    "quality": {"ticket_queue": "review"}
  },
  "workers": {
    "coder": {"role": "backend", "actor": "coder"},
    "reviewer": {"role": "quality", "actor": "reviewer"}
  }
}`
	write := func(data string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(data)
	if _, err := LoadFileConfig(root, path, true); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputJSON}, &stdout, &stderr, emptyEnv); err != nil {
		t.Fatalf("config check: %v stdout=%s stderr=%s", err, &stdout, &stderr)
	}
	config, help, err := parseRunConfig([]string{"--config", path}, emptyEnv)
	if err != nil || help || len(config.Workers) != 2 {
		t.Fatalf("managed startup resolution: config=%#v help=%t err=%v", config, help, err)
	}
	for _, worker := range config.Workers {
		want := orc.CoderTicketPromptTemplate
		if worker.Name == "reviewer" {
			want = orc.ReviewerTicketPromptTemplate
		}
		if worker.Config.TicketPrompt != want {
			t.Fatalf("managed %s prompt = %q, want %q", worker.Name, worker.Config.TicketPrompt, want)
		}
	}
	// The same configured roles are also available to dynamic steer sessions.
	policies := config.steerPolicies
	if policies.Snapshot().roles["backend"].NudgePrompt != coderNudgePrompt || policies.Snapshot().roles["quality"].NudgePrompt != reviewerNudgePrompt {
		t.Fatalf("mixed role defaults = %#v", policies.Snapshot())
	}
	manager := newWorkerManager(context.Background(), config, os.Args[0], io.Discard, io.Discard, nil)
	t.Cleanup(manager.repositoryWatch.StopObservers)
	for _, prompt := range []string{"custom steering", ""} {
		write(strings.Replace(data, `"ticket_queue": "open"`, fmt.Sprintf(`"ticket_queue": "open", "nudge_prompt": %q`, prompt), 1))
		result, err := manager.reload(context.Background())
		if err != nil || !result.Applied {
			t.Fatalf("reload with prompt %q: result=%#v err=%v", prompt, result, err)
		}
		want := prompt
		if want == "" {
			want = coderNudgePrompt
		}
		if got := policies.Snapshot().roles["backend"].NudgePrompt; got != want {
			t.Fatalf("reloaded steering prompt=%q, want %q", got, want)
		}
		for _, worker := range config.Workers {
			if got := manager.lifecycle.Workers()[worker.Name].Config.TicketPrompt; got != worker.Config.TicketPrompt {
				t.Fatalf("steering prompt changed managed %s prompt: %q", worker.Name, got)
			}
		}
	}
}

func assertSeriousHoldGuidance(t *testing.T, prompt string) {
	t.Helper()
	for _, required := range []string{
		"architectural acceptance authority, not as another code reviewer",
		"entry point into an assessment of the relevant project work",
		"parent specification, related and dependent tickets, outstanding holds",
		"Trust the reviewer for routine code-level correctness",
		"architectural drift, unnecessary complexity, duplicated mechanisms, circular dependencies",
		"actual Ticket relationships, not narrative similarity",
		"Prefer simplification and existing project conventions",
		"Avoid speculative work and duplicate tickets",
		"amend unowned related tickets",
		"record findings for owned work without taking it over",
		"Preserve genuine dependencies",
		"Planning maintenance does not authorize implementing code",
		"Honor existing human authorization",
		"requires authority not already granted",
		"unresolved material defects",
		"require whole-project completion only for final integration",
		"Do not withhold acceptance solely for unrelated improvements",
		"Process only the dispatched signoff ticket's lifecycle",
		"do not claim or process additional signoff tickets",
		"Record the acceptance decision, related planning changes, and outstanding integration work",
	} {
		if !strings.Contains(prompt, required) {
			t.Errorf("acceptance guidance missing %q in %q", required, prompt)
		}
	}
}
