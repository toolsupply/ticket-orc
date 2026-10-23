package cli

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
)

func TestConversationEnvironmentUsesHostDiscovery(t *testing.T) {
	instance := t.TempDir()
	configPath := filepath.Join(instance, "config.json")
	localDir := filepath.Join(instance, "runtime")
	writeConfigFixture(t, configPath, `{"version":1,"local_dir":"runtime"}`)
	foreign := "http://127.0.0.1:54321/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	child := conversationEnvironment([]string{"TICKET_ORC_ENDPOINT=" + foreign, "TICKET_ORC=/foreign", "KEEP=value"}, configPath, state.Conversation{Execution: state.ConversationExecution{Actor: "agent"}})
	values := map[string]string{}
	for _, entry := range child {
		key, value, _ := strings.Cut(entry, "=")
		values[key] = value
	}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	options, _, _, err := parseDaemonOptions(nil, lookup, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if options.environmentURL != "" || options.endpoint != "" || options.localDir != localDir {
		t.Fatalf("conversation did not select its host: %#v", options)
	}
	if values["KEEP"] != "value" {
		t.Fatal("unrelated child environment changed")
	}
	// Trusted agents retain the normal explicit cross-instance command option.
	explicit, _, _, err := parseDaemonOptions([]string{"--endpoint", foreign}, lookup, true, false)
	if err != nil || explicit.endpoint != foreign {
		t.Fatalf("explicit endpoint no longer works: %#v %v", explicit, err)
	}
}
