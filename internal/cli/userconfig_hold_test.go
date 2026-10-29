package cli

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestHoldQueueConfigPropagationAndCompatibility(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			var baseline supervisor.RunWorker
			var baselinePolicy steerRolePolicy
			for _, field := range []string{"", `,"hold_queue":false`, `,"hold_queue":true`} {
				path := filepath.Join(t.TempDir(), "config.json")
				writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"default_role":"quality","roles":{"quality":{"ticket_queue":%q%s,"ticket_tags":["api"],"ticket_without_tags":["draft"]}},"repositories":{"partition":{"repository":"./repo"}},"review":{"skip_tags":["no-review"]},"workers":{"worker":{"role":"quality","actor":"worker","repository":"partition"}}}`, queue, field))
				loaded, err := LoadFileConfig(filepath.Dir(path), path, true)
				if err != nil {
					t.Fatal(err)
				}
				cfg, help, err := parseRoleConfig(workflowRole(loaded.Config.Roles["quality"]), []string{"--config", path, "--worker", "worker"}, emptyEnv)
				if err != nil || help {
					t.Fatalf("managed config: help=%v err=%v", help, err)
				}
				wantEnabled := strings.Contains(field, "true")
				if cfg.HoldQueue != wantEnabled || cfg.TicketQueue != queue || cfg.RepositoryKey != "partition" || cfg.Ticket.Repository != filepath.Join(filepath.Dir(path), "repo") {
					t.Fatalf("managed secondary input/routing = %#v", cfg)
				}
				if cfg.TicketTags != "api" || cfg.TicketWithoutTags != "draft" {
					t.Fatalf("hold generic selector = %q/%q", cfg.TicketTags, cfg.TicketWithoutTags)
				}
				policies, err := resolveSteerRolePolicies(loaded.Config, emptyEnv)
				if err != nil {
					t.Fatal(err)
				}
				policy := policies["quality"]
				wantHoldFilters := ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"draft"}}
				if policy.HoldQueue != wantEnabled || policy.TicketQueue != queue || !reflect.DeepEqual(policy.holdQueueFilters(), wantHoldFilters) {
					t.Fatalf("steer secondary input = %#v", policy)
				}
				wantPrimaryFilters := wantHoldFilters
				if queue == "review" {
					wantPrimaryFilters.WithoutTags = []string{"draft", "no-review"}
				}
				if !reflect.DeepEqual(policy.QueueFilters, wantPrimaryFilters) {
					t.Fatalf("primary filters = %#v; want %#v", policy.QueueFilters, wantPrimaryFilters)
				}
				// Compare the execution policy independently of the fixture root.
				worker := supervisor.RunWorker{Name: "worker", Config: supervisor.RoleConfig{WorkerName: "worker", Role: cfg.Role, RoleName: cfg.RoleName, TicketQueue: cfg.TicketQueue, HoldQueue: cfg.HoldQueue, TicketTags: cfg.TicketTags, TicketWithoutTags: cfg.TicketWithoutTags}}
				if field == "" {
					baseline, baselinePolicy = worker, policy
				} else if equalLaunchSnapshots(baseline, worker) == wantEnabled || reflect.DeepEqual(baselinePolicy, policy) == wantEnabled {
					t.Fatalf("false must match omitted; true must change managed and steer policy: %q", field)
				}
				encoded, err := encodeLaunchSnapshot(worker)
				if err != nil {
					t.Fatal(err)
				}
				child, _, err := decodeLaunchSnapshot(worker.Config.Role, nil, encoded)
				if err != nil || child.HoldQueue != wantEnabled {
					t.Fatalf("child hold setting = %t, %v", child.HoldQueue, err)
				}
			}
		})
	}
}

func TestHoldQueueRejectsInvalidPoliciesAndValues(t *testing.T) {
	for _, test := range []struct{ queue, value, want string }{
		{"open", "true", "is only valid with ticket_queue review or signoff"},
		{"open", "false", "is only valid with ticket_queue review or signoff"},
		{"review", `"true"`, "must be a boolean"},
		{"signoff", "1", "must be a boolean"},
		{"review", "null", "must be a boolean"},
		{"review", "[]", "must be a boolean"},
		{"review", "{}", "must be a boolean"},
	} {
		t.Run(test.queue+"/"+test.value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"default_role":"quality","roles":{"quality":{"ticket_queue":%q,"hold_queue":%s}}}`, test.queue, test.value))
			_, err := LoadFileConfig(filepath.Dir(path), path, true)
			if err == nil || !strings.Contains(err.Error(), "roles.quality.hold_queue "+test.want) {
				t.Fatalf("invalid hold policy error = %v", err)
			}
		})
	}
}
