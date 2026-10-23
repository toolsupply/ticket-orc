package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/toolsupply/ticket-orc/internal/state"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestConversationProfileConfiguration(t *testing.T) {
	root := t.TempDir()
	id := "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	configPath := filepath.Join(root, "config.json")
	data := `{"version":1,"defaults":{"model":"inherited","reasoning":"high","codex":{"sandbox":"danger-full-access","service_tier":"fast"}},"repositories":{"project":{"ticket":{"config":"ticket.json","scope":"project"}}},"conversations":{"profiles":{"planner":{"actor":"planner-agent","working_dir":".","workspace":".","repository_ids":["` + id + `"],"max_duration":"2m"}}}}`
	writeConfigFixture(t, configPath, data)
	loaded, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := resolveConfiguredRepositories(loaded.Config)
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := resolveConversationProfiles(t.Context(), loaded, registry, func(_ context.Context, r supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
		if r.Target.Scope != "project" || r.Target.Config != filepath.Join(root, "ticket.json") {
			t.Fatalf("routing: %#v", r)
		}
		return ticketclient.RepositoryInfo{ID: id, Path: filepath.Join(root, "tickets")}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].Execution.Model != "inherited" || profiles[0].Execution.CodexSandbox != "danger-full-access" || profiles[0].Execution.CodexServiceTier != "fast" || profiles[0].Execution.Actor != "planner-agent" || profiles[0].Execution.Repositories[0].ID != id {
		t.Fatalf("resolved profile: %#v", profiles)
	}
	// Resolution remains independent of worker queues and inherited worker actor.
	if len(loaded.Config.Workers) != 0 {
		t.Fatal("profile became a worker")
	}
	for _, change := range []struct{ old, new string }{
		{`"actor":"planner-agent"`, `"actor":""`},
		{`"actor":"planner-agent"`, `"actor":"bad actor"`},
		{`"max_duration":"2m"`, `"max_duration":"0s"`},
		{`"max_duration":"2m"`, `"max_duration":"25h"`},
		{`"working_dir":"."`, `"working_dir":"missing"`},
		{`"workspace":"."`, `"workspace":"missing"`},
		{`"actor":"planner-agent"`, `"actor":"planner-agent","harness":"pi"`},
		{`"reasoning":"high"`, `"reasoning":"unknown"`},
		{`"sandbox":"danger-full-access"`, `"sandbox":"invalid"`},
		{`"max_duration":"2m"`, `"max_duration":"2m","transcript_bytes":0`},
		{`"max_duration":"2m"`, `"max_duration":"2m","env":{}`},
		{`"max_duration":"2m"`, `"max_duration":"2m","codex":{"unknown":true}`},
	} {
		writeConfigFixture(t, configPath, strings.Replace(data, change.old, change.new, 1))
		if _, err := LoadFileConfig(root, configPath, true); err == nil {
			t.Errorf("accepted invalid change: %s", change.new)
		}
	}
	writeConfigFixture(t, configPath, `{"version":1,"conversations":{"limits":{"count":0}}}`)
	if _, err := LoadFileConfig(root, configPath, true); err == nil {
		t.Fatal("accepted zero count")
	}
	writeConfigFixture(t, configPath, `{"version":1}`)
	disabled, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := resolveConversationProfiles(t.Context(), disabled, nil, nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("absent section: %v", err)
	}
	metadata, _ := json.Marshal(orc.NewConversationProfiles(profiles).Metadata())
	if strings.Contains(string(metadata), root) || strings.Contains(string(metadata), "planner-agent") {
		t.Fatalf("private metadata: %s", metadata)
	}

}

func TestConversationProfilesReloadIsTransactionalAndPreservesCaptures(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	stateDir := filepath.Join(root, "runtime")
	configData := func(actor string) string {
		return fmt.Sprintf(`{"version":1,"local_dir":%q,"conversations":{"profiles":{"planning":{"actor":%q}}}}`, stateDir, actor)
	}
	writeConfigFixture(t, path, configData("original-agent"))
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := resolveConversationProfiles(t.Context(), loaded, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	profiles := orc.NewConversationProfiles(initial)
	policy, err := orc.NewConversations(state.NewConversationStore(stateDir), orc.DefaultConversationLimits())
	if err != nil {
		t.Fatal(err)
	}
	request := orc.ConversationProfileCreate{RequestID: orc.NewConversationRequestID(time.Now()), Profile: "planning", Operator: "human"}
	before, err := profiles.Create(t.Context(), policy, request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	manager := newWorkerManager(t.Context(), RunConfig{ConfigPath: path, StateDir: stateDir, InstanceID: loaded.Config.ID, conversationProfiles: profiles, conversationLimits: orc.DefaultConversationLimits()}, os.Args[0], io.Discard, io.Discard, nil)
	t.Cleanup(manager.repositoryWatch.StopObservers)
	writeConfigFixture(t, path, configData("replacement-agent"))
	result, err := manager.reload(t.Context())
	if err != nil || !result.Applied {
		t.Fatalf("reload: %#v %v", result, err)
	}
	duplicate, err := profiles.Create(t.Context(), policy, request, time.Now())
	if err != nil || duplicate != before {
		t.Fatalf("retry changed after reload: %v", err)
	}
	request.RequestID = orc.NewConversationRequestID(time.Now())
	after, err := profiles.Create(t.Context(), policy, request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	old, err := policy.Get(t.Context(), before.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := policy.Get(t.Context(), after.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	if old.Execution.Actor != "original-agent" || fresh.Execution.Actor != "replacement-agent" {
		t.Fatal("reload replaced existing captured execution")
	}
	bad := strings.Replace(configData("bad-agent"), `"conversations":{`, `"conversations":{"limits":{"count":1},`, 1)
	writeConfigFixture(t, path, bad)
	if _, err := manager.reload(t.Context()); err == nil {
		t.Fatal("accepted live instance limit change")
	}
	request.RequestID = orc.NewConversationRequestID(time.Now())
	receipt, err := profiles.Create(t.Context(), policy, request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c, err := policy.Get(t.Context(), receipt.ConversationID)
	if err != nil || c.Execution.Actor != "replacement-agent" {
		t.Fatal("failed reload changed profiles")
	}
}
