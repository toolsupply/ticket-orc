package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestManagedThreeStageConfigResolvesConfiguredPipeline(t *testing.T) {
	config := loadManagedThreeStageConfig(t, nil)
	if len(config.Workers) != 3 {
		t.Fatalf("resolved workers=%d, want all three-stage workers", len(config.Workers))
	}
	want := map[string]struct {
		role, actor, queue, tags, without string
	}{
		"api-coder":     {"coder-api", "api-implementer", "open", "api", "blocked"},
		"api-reviewer":  {"reviewer-api", "api-reviewer", "review", "api", "blocked,no-review"},
		"api-architect": {"architect-api", "api-architect", "signoff", "api", "blocked"},
	}
	for _, worker := range config.Workers {
		policy, ok := want[worker.Name]
		if !ok {
			t.Fatalf("unexpected worker %q", worker.Name)
		}
		cfg := worker.Config
		if cfg.RoleName != policy.role || cfg.Actor != policy.actor || cfg.TicketQueue != policy.queue || cfg.TicketTags != policy.tags || strings.Join(splitTicketTags(cfg.TicketWithoutTags), ",") != policy.without {
			t.Errorf("worker %q resolved role=%q actor=%q queue=%q required=%q excluded=%q", worker.Name, cfg.RoleName, cfg.Actor, cfg.TicketQueue, cfg.TicketTags, cfg.TicketWithoutTags)
		}
		if worker.Name == "api-reviewer" && cfg.ReviewCompletion != ReviewCompletionSignoff {
			t.Errorf("review completion=%q, want explicit signoff", cfg.ReviewCompletion)
		}
	}
}

func TestLaunchSnapshotComparisonIncludesResolvedLaunchPolicy(t *testing.T) {
	base := supervisorTestWorker("worker", t.TempDir())
	base.RequiredSkills = []string{"ticket"}
	cases := []struct {
		name   string
		change func(*supervisor.RunWorker)
	}{
		{name: "actor", change: func(worker *supervisor.RunWorker) { worker.Config.Actor = "changed" }},
		{name: "configured role", change: func(worker *supervisor.RunWorker) { worker.Config.RoleName = "changed" }},
		{name: "ticket queue", change: func(worker *supervisor.RunWorker) { worker.Config.TicketQueue = "review" }},
		{name: "ticket selectors", change: func(worker *supervisor.RunWorker) { worker.Config.TicketTags = "backend" }},
		{name: "review exclusions", change: func(worker *supervisor.RunWorker) { worker.Config.ReviewSkipTags = "no-review" }},
		{name: "harness", change: func(worker *supervisor.RunWorker) { worker.Config.Harness = "pi" }},
		{name: "model", change: func(worker *supervisor.RunWorker) { worker.Config.Model = "changed" }},
		{name: "reasoning", change: func(worker *supervisor.RunWorker) { worker.Config.Reasoning = "high" }},
		{name: "sandbox", change: func(worker *supervisor.RunWorker) { worker.Config.Codex.Sandbox = "read-only" }},
		{name: "ticket target", change: func(worker *supervisor.RunWorker) {
			worker.Config.Ticket = supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/changed/repository"}
		}},
		{name: "skills", change: func(worker *supervisor.RunWorker) { worker.RequiredSkills = []string{"ticket", "custom"} }},
		{name: "review policy", change: func(worker *supervisor.RunWorker) { worker.Config.ReviewCompletion = ReviewCompletionClose }},
		{name: "ticket prompt", change: func(worker *supervisor.RunWorker) { worker.Config.TicketPrompt = "changed ticket prompt" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.change(&changed)
			if equalLaunchSnapshots(base, changed) {
				t.Fatalf("launch snapshots considered equal after %s change", test.name)
			}
		})
	}
	live := base
	live.Groups = []string{"changed-group"}
	if !equalLaunchSnapshots(base, live) {
		t.Fatal("group-only change should remain live-reloadable")
	}
}

func TestLaunchSnapshotCarriesResolvedLocalRootToChild(t *testing.T) {
	localDir := filepath.Join(t.TempDir(), ".local", "instance-id")
	worker := supervisorTestWorker("worker-a", localDir)
	encoded, err := encodeLaunchSnapshot(worker)
	if err != nil {
		t.Fatal(err)
	}
	lookup := mapEnv(map[string]string{launchSnapshotEnv: encoded})
	resolved, help, err := parseRoleConfig(RoleCoder, []string{"--config", "/untrusted/cwd/config.json", "--worker", worker.Name}, lookup)
	if err != nil || help {
		t.Fatalf("decode worker launch config=%#v help=%t err=%v", resolved, help, err)
	}
	if resolved.StateDir != localDir || resolved.WorkerName != worker.Name {
		t.Fatalf("child config local root=%q worker=%q, want %q and %q", resolved.StateDir, resolved.WorkerName, localDir, worker.Name)
	}
}

func TestSelectRunWorkersUnionsAndDeduplicates(t *testing.T) {
	config := FileConfig{Workers: map[string]WorkerFileConfig{
		"coder":    {Role: "coder", Groups: []string{"default", "backend"}},
		"reviewer": {Role: "reviewer", Groups: []string{"default"}},
		"frontend": {Role: "coder", Groups: []string{"frontend"}},
	}}
	got, err := selectRunWorkers(config, []string{"reviewer", "coder"}, []string{"backend"}, false)
	if err != nil {
		t.Fatalf("selectRunWorkers: %v", err)
	}
	want := []string{"reviewer", "coder"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected workers = %#v, want %#v", got, want)
	}

	got, err = selectRunWorkers(config, nil, nil, true)
	if err != nil {
		t.Fatalf("--all selection: %v", err)
	}
	want = []string{"coder", "frontend", "reviewer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all workers = %#v, want %#v", got, want)
	}
}

func TestParseRunConfigAcceptsAllShortAlias(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"short-alias"}}}`)
	config, help, err := parseRunConfig([]string{"-a", "-c", path}, mapEnv(map[string]string{"TICKET_ORC_ACTOR": "short-alias"}))
	if err != nil || help || len(config.Workers) != 1 || config.Workers[0].Name != "coder" {
		t.Fatalf("run -a config=%#v help=%v err=%v", config, help, err)
	}
	if _, _, err := parseRunConfig([]string{"-a", "--all"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "duplicate flag --all") {
		t.Fatalf("duplicate -a/--all error = %v", err)
	}
}

func TestParseRunConfigResolvesStateDirectoryFromConfigPath(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "settings")
	workingDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(configDir, "orc.json")
	writeConfigFixture(t, path, `{"version":1,"defaults":{"working_dir":"../workspace"},"workers":{"coder":{"role":"coder","actor":"state-test"}}}`)
	otherCWD := t.TempDir()
	t.Chdir(otherCWD)

	config, help, err := parseRunConfig([]string{"--all", "--config", path}, emptyEnv)
	if err != nil || help {
		t.Fatalf("parseRunConfig config=%#v help=%t err=%v", config, help, err)
	}
	want := filepath.Join(configDir, ".local")
	if config.StateDir != want || config.InstanceID != "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa" || len(config.Workers) != 1 || config.Workers[0].Config.StateDir != want {
		t.Fatalf("instance/supervisor/worker state = %q/%q/%q, want ID %q and state dir %q", config.InstanceID, config.StateDir, config.Workers[0].Config.StateDir, "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa", want)
	}
	if config.Workers[0].Config.WorkingDir != workingDir {
		t.Fatalf("worker working directory = %q, want %q", config.Workers[0].Config.WorkingDir, workingDir)
	}

	writeConfigFixture(t, path, `{"version":1,"local_dir":"../state","defaults":{"working_dir":"../workspace"},"workers":{"coder":{"role":"coder","actor":"state-test"}}}`)
	config, _, err = parseRunConfig([]string{"--all", "--config", path}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	want = filepath.Join(root, "state")
	if config.StateDir != want || config.Workers[0].Config.StateDir != want {
		t.Fatalf("relative state dirs = %q/%q, want %q", config.StateDir, config.Workers[0].Config.StateDir, want)
	}
	absoluteStateDir := filepath.Join(root, "absolute-state")
	writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"local_dir":%q,"defaults":{"working_dir":"../workspace"},"workers":{"coder":{"role":"coder","actor":"state-test"}}}`, absoluteStateDir))
	config, _, err = parseRunConfig([]string{"--all", "--config", path}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	if config.StateDir != absoluteStateDir || config.Workers[0].Config.StateDir != absoluteStateDir {
		t.Fatalf("absolute state dirs = %q/%q, want %q", config.StateDir, config.Workers[0].Config.StateDir, absoluteStateDir)
	}
}

func TestParseRunConfigAcceptsInteractiveAlias(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"interactive"}}}`)
	config, help, err := parseRunConfig([]string{"-i", "--config", path}, emptyEnv)
	if err != nil || help || !config.Interactive {
		t.Fatalf("run -i config=%#v help=%v err=%v", config, help, err)
	}
	if _, _, err := parseRunConfig([]string{"-i", "--interactive", "--config", path}, emptyEnv); err == nil || !strings.Contains(err.Error(), "duplicate flag --interactive") {
		t.Fatalf("duplicate -i/--interactive error=%v", err)
	}
}

func TestParseRunConfigListenCLIOverridesConfig(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"supervisor":{"listen":"127.0.0.2","port":43123},"workers":{"coder":{"role":"coder","actor":"listen-test"}}}`)
	config, help, err := parseRunConfig([]string{"--config", path, "--listen", "127.0.0.1", "--port=43210"}, emptyEnv)
	if err != nil || help {
		t.Fatalf("parseRunConfig config=%#v help=%t err=%v", config, help, err)
	}
	if config.ListenAddress != "127.0.0.1" || config.Port != 43210 {
		t.Fatalf("listen config = %#v", config)
	}
	fromFile, _, err := parseRunConfig([]string{"--config", path}, emptyEnv)
	if err != nil || fromFile.ListenAddress != "127.0.0.2" || fromFile.Port != 43123 {
		t.Fatalf("config listen values = %#v err=%v", fromFile, err)
	}
	for _, args := range [][]string{{"--config", path, "--listen", "localhost"}, {"--config", path, "--port", "80"}, {"--config", path, "--port", "not-a-port"}} {
		if _, _, err := parseRunConfig(args, emptyEnv); err == nil {
			t.Fatalf("invalid listen arguments accepted: %v", args)
		}
	}
}

func TestParseRunConfigListenDefaultsAndEnvironmentPrecedence(t *testing.T) {
	defaults, _, err := parseRunConfig(nil, testInstanceEnv(t))
	if err != nil || defaults.ListenAddress != "127.0.0.1" || defaults.Port != 0 {
		t.Fatalf("default listener = %#v, err=%v", defaults, err)
	}

	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"supervisor":{"listen":"::1","port":0}}`)
	env := mapEnv(map[string]string{"TICKET_ORC_LISTEN": "0.0.0.0", "TICKET_ORC_PORT": "43210"})
	fromEnv, _, err := parseRunConfig([]string{"--config", path}, env)
	if err != nil || fromEnv.ListenAddress != "0.0.0.0" || fromEnv.Port != 43210 {
		t.Fatalf("environment listener = %#v, err=%v", fromEnv, err)
	}
	fromCLI, _, err := parseRunConfig([]string{"--config", path, "--listen", "::1", "--port", "0"}, env)
	if err != nil || fromCLI.ListenAddress != "::1" || fromCLI.Port != 0 {
		t.Fatalf("CLI listener = %#v, err=%v", fromCLI, err)
	}
	fromConfig, _, err := parseRunConfig([]string{"--config", path}, emptyEnv)
	if err != nil || fromConfig.ListenAddress != "::1" || fromConfig.Port != 0 {
		t.Fatalf("explicit config port zero was lost: %#v, err=%v", fromConfig, err)
	}
	fromEnvZero, _, err := parseRunConfig([]string{"--config", path}, mapEnv(map[string]string{"TICKET_ORC_PORT": "0"}))
	if err != nil || fromEnvZero.Port != 0 {
		t.Fatalf("explicit environment port zero was lost: %#v, err=%v", fromEnvZero, err)
	}
	if _, _, err := parseRunConfig(nil, mapEnv(map[string]string{"TICKET_ORC_PORT": "invalid"})); err == nil {
		t.Fatal("invalid TICKET_ORC_PORT was accepted")
	}
}

func TestSelectRunWorkersRejectsUnknownAndEmptyGroups(t *testing.T) {
	config := FileConfig{Workers: map[string]WorkerFileConfig{
		"coder": {Role: "coder", Groups: []string{"default"}},
	}}
	for _, test := range []struct {
		name string
		call func() error
		want string
	}{
		{"worker", func() error {
			_, err := selectRunWorkers(config, []string{"missing"}, nil, false)
			return err
		}, "not configured"},
		{"group", func() error {
			_, err := selectRunWorkers(config, nil, []string{"empty"}, false)
			return err
		}, "has no configured workers"},
		{"all", func() error {
			_, err := selectRunWorkers(FileConfig{}, nil, nil, true)
			return err
		}, "no workers"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSelectRunWorkersUsesConfiguredStartupGroups(t *testing.T) {
	config := FileConfig{
		Workers: map[string]WorkerFileConfig{
			"coder": {Role: "coder", Groups: []string{"default"}},
		},
		Supervisor: SupervisorFileConfig{StartupGroups: []string{"default"}},
	}
	got, err := selectRunWorkers(config, nil, nil, false)
	if err != nil {
		t.Fatalf("startup group selection: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"coder"}) {
		t.Fatalf("startup workers = %#v", got)
	}

	got, err = selectRunWorkers(FileConfig{}, nil, nil, false)
	if err != nil || len(got) != 0 {
		t.Fatalf("bare idle selection = %#v, %v", got, err)
	}
}

func TestSelectRunWorkersStartupGroupsAndExplicitPrecedence(t *testing.T) {
	config := FileConfig{
		Workers: map[string]WorkerFileConfig{
			"alpha": {Groups: []string{"coding", "shared"}},
			"beta":  {Groups: []string{"reviewing", "shared"}},
			"gamma": {Groups: []string{"other"}},
		},
		Supervisor: SupervisorFileConfig{StartupGroups: []string{"shared", "coding"}},
	}
	for _, test := range []struct {
		name            string
		workers, groups []string
		all             bool
		want            []string
	}{
		{name: "startup union", want: []string{"alpha", "beta"}},
		{name: "worker overrides", workers: []string{"gamma"}, want: []string{"gamma"}},
		{name: "group overrides", groups: []string{"other"}, want: []string{"gamma"}},
		{name: "explicit union", workers: []string{"gamma", "gamma"}, groups: []string{"coding", "other"}, want: []string{"gamma", "alpha"}},
		{name: "all overrides", all: true, want: []string{"alpha", "beta", "gamma"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectRunWorkers(config, test.workers, test.groups, test.all)
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("selected=%v err=%v, want %v", got, err, test.want)
			}
		})
	}
	for _, selectors := range []struct{ workers, groups []string }{
		{workers: []string{"missing"}}, {groups: []string{"missing"}},
	} {
		if _, err := selectRunWorkers(config, selectors.workers, selectors.groups, true); err == nil {
			t.Fatal("--all hid an unknown explicit selector")
		}
	}
}

func TestSelectRunWorkersUsesAdditiveRoleGroups(t *testing.T) {
	config := FileConfig{
		DefaultRole: "coder",
		Roles: map[string]RoleFileConfig{
			"coder":    {TicketQueue: "open", NudgePrompt: coderNudgePrompt, Groups: []string{"role", "shared"}},
			"reviewer": {TicketQueue: "review", NudgePrompt: reviewerNudgePrompt, Groups: []string{"shared"}},
		},
		Workers: map[string]WorkerFileConfig{
			"coder":    {Role: "coder", Groups: []string{"worker", "shared"}},
			"reviewer": {Role: "reviewer", Groups: []string{"reviewer"}},
		},
		Supervisor: SupervisorFileConfig{StartupGroups: []string{"role"}},
	}
	got, err := selectRunWorkers(config, nil, []string{"shared"}, false)
	if err != nil {
		t.Fatalf("role group selection: %v", err)
	}
	if want := []string{"coder", "reviewer"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected workers = %#v, want %#v", got, want)
	}
	if got := effectiveWorkerGroups(config, "coder"); !reflect.DeepEqual(got, []string{"role", "shared", "worker"}) {
		t.Fatalf("effective coder groups = %#v", got)
	}
}

func TestParseRunConfigResolvesDynamicReviewerCompletion(t *testing.T) {
	tests := []struct {
		name       string
		defaults   string
		rolePolicy string
		envValue   string
		want       string
		wantErr    bool
	}{
		{name: "default signoff", want: ReviewCompletionSignoff},
		{name: "configured default close", defaults: `,"defaults":{"review_completion":"close"}`, want: ReviewCompletionClose},
		{name: "role signoff overrides default close", defaults: `,"defaults":{"review_completion":"close"}`, rolePolicy: `,"review_completion":"signoff"`, want: ReviewCompletionSignoff},
		{name: "role close overrides default signoff", defaults: `,"defaults":{"review_completion":"signoff"}`, rolePolicy: `,"review_completion":"close"`, want: ReviewCompletionClose},
		{name: "environment close overrides role signoff", rolePolicy: `,"review_completion":"signoff"`, envValue: ReviewCompletionClose, want: ReviewCompletionClose},
		{name: "environment signoff overrides role close", rolePolicy: `,"review_completion":"close"`, envValue: ReviewCompletionSignoff, want: ReviewCompletionSignoff},
		{name: "invalid environment override", envValue: "bogus", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := `{"version":1,"default_role":"coder"` + test.defaults + `,"roles":{"coder":{"ticket_queue":"open","nudge_prompt":"code"},"quality":{"ticket_queue":"review","nudge_prompt":"review"` + test.rolePolicy + `}}}`
			writeConfigFixture(t, path, data)
			lookup := emptyEnv
			if test.envValue != "" {
				lookup = mapEnv(map[string]string{"TICKET_ORC_REVIEW_COMPLETION": test.envValue})
			}
			config, _, err := parseRunConfig([]string{"--config", path}, lookup)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "review_completion.invalid") {
					t.Fatalf("invalid environment review completion error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			policies := config.steerPolicies.Snapshot().roles
			if got := policies["quality"].ReviewCompletion; got != test.want {
				t.Fatalf("dynamic reviewer completion=%q, want %q", got, test.want)
			}
			if got := policies["coder"].ReviewCompletion; got != "" {
				t.Fatalf("non-review role acquired reviewer completion policy %q", got)
			}
		})
	}
}

func TestParseRunConfigResolvesCompleteSteerQueueFilters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"coding","ticket_tags":["backend"]},"quality":{"ticket_queue":"review","nudge_prompt":"review","ticket_tags":["urgent","backend"]}},"review":{"skip_tags":["no-review"]}}`)
	config, _, err := parseRunConfig([]string{"--config", path}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := config.steerPolicies.Snapshot()
	want := map[string]ticketclient.QueueFilters{
		"coder":   {Tags: []string{"backend"}},
		"quality": {Tags: []string{"backend", "urgent"}, WithoutTags: []string{"no-review"}},
	}
	for role, filters := range want {
		if !reflect.DeepEqual(snapshot.roles[role].QueueFilters, filters) {
			t.Fatalf("role %q steering filters snapshot=%#v, want %#v", role, snapshot.roles[role].QueueFilters, filters)
		}
	}
}
