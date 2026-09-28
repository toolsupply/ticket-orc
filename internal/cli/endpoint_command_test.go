package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func TestEndpointCommandPrintsCapabilityURLWithWarningInHelp(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	var out, errOut bytes.Buffer
	stateDir := filepath.Join(cwd, "runtime")
	configPath := filepath.Join(cwd, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"local_dir":"runtime"}`)
	key, err := daemon.LoadOrCreateEndpointKey(stateDir, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(stateDir, "run"), 0o700); err != nil {
		t.Fatal(err)
	}
	endpoint := daemon.Endpoint{Version: 1, Protocol: 1, PID: os.Getpid(), URL: "http://127.0.0.1:43123", EndpointKey: key}
	data, err := json.Marshal(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(daemon.EndpointPath(stateDir), data, 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errOut.Reset()
	if code := run([]string{"endpoint", "--config", configPath}, &out, &errOut, emptyEnv, rejectExecution); code != 0 || out.String() != endpoint.CapabilityURL()+"\n" || !strings.Contains(errOut.String(), "reveals the endpoint key") {
		t.Fatalf("endpoint code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
	out.Reset()
	if code := run([]string{"endpoint", "--help"}, &out, &errOut, emptyEnv, rejectExecution); code != 0 || !strings.Contains(out.String(), "including its endpoint key") || !strings.Contains(out.String(), "unencrypted HTTP") {
		t.Fatalf("endpoint help code=%d stdout=%q", code, out.String())
	}
}

func TestDaemonClientOptionsKeepExplicitConfigInstanceOverAmbientEndpoint(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	configPath := filepath.Join(cwd, "orc.json")
	key := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	writeConfigFixture(t, configPath, `{"version":1,"local_dir":"config-state"}`)
	environmentURL := "https://env.example.net:8443/" + key
	explicitURL := "http://explicit.example.net:43123/" + key
	lookup := func(name string) (string, bool) {
		if name == "TICKET_ORC_ENDPOINT" {
			return environmentURL, true
		}
		return "", false
	}
	options, _, help, err := parseDaemonCommandOptions([]string{"--config", configPath, "--endpoint", explicitURL}, lookup, false)
	if err != nil || help {
		t.Fatalf("parse explicit endpoint options=%#v help=%t err=%v", options, help, err)
	}
	if options.endpoint != explicitURL || options.environmentURL != environmentURL || options.localDir != filepath.Join(cwd, "config-state") {
		t.Fatalf("endpoint options = %#v", options)
	}
	options, _, _, err = parseDaemonCommandOptions([]string{"--config", configPath}, lookup, false)
	if err != nil || options.endpoint != "" || options.environmentURL != "" || options.localDir != filepath.Join(cwd, "config-state") {
		t.Fatalf("environment endpoint options = %#v err=%v", options, err)
	}
}

func TestDaemonEndpointOverrideDoesNotRequireAnInstance(t *testing.T) {
	cwd := t.TempDir()
	noInstance := func(name string) (string, bool) {
		if name == "HOME" || name == "USERPROFILE" {
			return t.TempDir(), true
		}
		return "", false
	}
	t.Chdir(cwd)
	endpoint := "http://explicit.example.net:43123/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	options, _, help, err := parseDaemonCommandOptions([]string{"--endpoint", endpoint}, noInstance, false)
	if err != nil || help || options.endpoint != endpoint || options.localDir != "" {
		t.Fatalf("explicit endpoint options=%#v help=%t err=%v", options, help, err)
	}
	lookup := func(name string) (string, bool) {
		if name == "TICKET_ORC_ENDPOINT" {
			return endpoint, true
		}
		return "", false
	}
	options, _, help, err = parseDaemonCommandOptions(nil, lookup, false)
	if err != nil || help || options.environmentURL != endpoint || options.localDir != "" {
		t.Fatalf("environment endpoint options=%#v help=%t err=%v", options, help, err)
	}

	badConfig := filepath.Join(cwd, "bad.json")
	if err := os.WriteFile(badConfig, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := parseDaemonCommandOptions([]string{"--config", badConfig, "--endpoint", endpoint}, lookup, false); err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("explicit config error was hidden by endpoint override: %v", err)
	}
}
