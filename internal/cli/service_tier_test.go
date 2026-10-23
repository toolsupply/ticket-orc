package cli

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexServiceTierPrecedence(t *testing.T) {
	for _, tier := range []string{"default", "fast"} {
		for _, source := range []string{"omitted", "defaults", "role", "worker", "environment", "flag", "empty environment", "empty flag"} {
			t.Run(tier+"/"+source, func(t *testing.T) {
				other := "fast"
				if tier == "fast" {
					other = "default"
				}
				defaults, role, worker := "", "", ""
				env := map[string]string{}
				flags := []string{}
				want := tier
				switch source {
				case "omitted":
					want = ""
				case "defaults":
					defaults = tier
				case "role":
					defaults, role = other, tier
				case "worker":
					defaults, role, worker = tier, other, tier
				case "environment":
					defaults, role, worker = tier, tier, other
					env["TICKET_ORC_CODEX_SERVICE_TIER"] = tier
				case "flag":
					worker = other
					env["TICKET_ORC_CODEX_SERVICE_TIER"] = other
					flags = []string{"--codex-service-tier", tier}
				case "empty environment":
					worker = tier
					env["TICKET_ORC_CODEX_SERVICE_TIER"] = ""
					want = ""
				case "empty flag":
					worker = tier
					env["TICKET_ORC_CODEX_SERVICE_TIER"] = tier
					flags = []string{"--codex-service-tier", ""}
					want = ""
				}
				path := filepath.Join(t.TempDir(), "config.json")
				writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"defaults":{"codex":{"service_tier":%q}},"roles":{"coder":{"codex":{"service_tier":%q}}},"workers":{"one":{"role":"coder","actor":"one","codex":{"service_tier":%q}}}}`, defaults, role, worker))
				config, _, err := parseRoleConfig(RoleCoder, append([]string{"--config", path, "--worker", "one"}, flags...), mapEnv(env))
				if err != nil || config.Codex.ServiceTier != want {
					t.Fatalf("tier=%q, want %q; err=%v", config.Codex.ServiceTier, want, err)
				}
			})
		}
	}
}

func TestCodexServiceTierValidation(t *testing.T) {
	for _, tier := range []string{"priority", "invalid", " fast", "fast ", " ", "FAST", "fast\n", "fast\x00", "fast\t", "fa\u0085st"} {
		t.Run(fmt.Sprintf("%q", tier), func(t *testing.T) {
			_, _, err := parseRoleConfig(RoleCoder, []string{"--actor", "one", "--codex-service-tier", tier}, testInstanceEnv(t))
			if err == nil || !strings.Contains(err.Error(), "service tier") {
				t.Fatalf("invalid tier error=%v", err)
			}
		})
	}
	for _, harness := range []string{"pi", "claude"} {
		for _, tier := range []string{"", "default", "fast"} {
			for _, source := range []string{"flag", "environment", "file"} {
				t.Run(harness+"/"+source+"/"+tier, func(t *testing.T) {
					args := []string{"--actor", "one", "--harness", harness}
					env := map[string]string{}
					switch source {
					case "flag":
						args = append(args, "--codex-service-tier", tier)
					case "environment":
						env["TICKET_ORC_CODEX_SERVICE_TIER"] = tier
					case "file":
						if tier == "" {
							return
						} // Empty file values follow existing inheritance semantics.
						path := filepath.Join(t.TempDir(), "config.json")
						writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"workers":{"one":{"role":"coder","harness":%q,"codex":{"service_tier":%q}}}}`, harness, tier))
						args = append(args, "--config", path, "--worker", "one")
					}
					_, _, err := parseRoleConfig(RoleCoder, args, testInstanceEnvWith(t, env))
					if err == nil || !strings.Contains(err.Error(), "codex") {
						t.Fatalf("non-Codex tier error=%v", err)
					}
				})
			}
		}
	}
}

func TestCodexServiceTierManagedChildSnapshot(t *testing.T) {
	for _, tier := range []string{"", "default", "fast"} {
		t.Run(tier, func(t *testing.T) {
			config := loadManagedThreeStageConfig(t, nil)
			for _, worker := range config.Workers {
				worker.Config.Codex.ServiceTier = tier
				encoded, err := encodeLaunchSnapshot(worker)
				if err != nil {
					t.Fatal(err)
				}
				// The validated launch snapshot wins even over later config/env changes.
				resolved, _, err := parseRoleConfig(worker.Config.Role, []string{"--worker", worker.Name}, mapEnv(map[string]string{launchSnapshotEnv: encoded, "TICKET_ORC_CODEX_SERVICE_TIER": "invalid"}))
				if err != nil || resolved.Codex.ServiceTier != tier {
					t.Fatalf("child %s tier=%q, err=%v", worker.Name, resolved.Codex.ServiceTier, err)
				}
				if resolved.WorkerName != worker.Name {
					t.Fatalf("tier changed worker identity: %q", resolved.WorkerName)
				}
			}
		})
	}
}

func TestCodexServiceTierConfigDiagnostic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"one":{"role":"coder","actor":"one","codex":{"service_tier":"invalid"}}}}`)
	config, _, err := parseRunConfig([]string{"--config", path, "--all"}, emptyEnv)
	if err == nil {
		t.Fatalf("invalid tier accepted: %#v", config)
	}
	if !strings.Contains(err.Error(), "service tier") {
		t.Fatalf("error=%v", err)
	}
}
