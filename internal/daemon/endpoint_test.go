package daemon

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestGenerateEndpointKeyCreatesCanonical256BitCapability(t *testing.T) {
	first, err := GenerateEndpointKey()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateEndpointKey()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(first)
	if err != nil || len(decoded) != 32 || ValidateEndpointKey(first) != nil || first == second {
		t.Fatalf("generated keys are not distinct canonical 256-bit capabilities: first=%q second=%q err=%v", first, second, err)
	}
}

func TestLoadOrCreateEndpointKeyIsPrivateStableAndAdoptsLegacyValue(t *testing.T) {
	stateDir := t.TempDir()
	legacy, err := GenerateEndpointKey()
	if err != nil {
		t.Fatal(err)
	}
	first, err := LoadOrCreateEndpointKey(stateDir, legacy)
	if err != nil || first != legacy {
		t.Fatalf("first key = %q, err=%v", first, err)
	}
	second, err := LoadOrCreateEndpointKey(stateDir, "")
	if err != nil || second != first {
		t.Fatalf("restarted key = %q, err=%v; want %q", second, err, first)
	}
	info, err := os.Stat(EndpointKeyPath(stateDir))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("endpoint key permissions = %04o, want owner-only", info.Mode().Perm())
	}
	generatedDir := t.TempDir()
	generated, err := LoadOrCreateEndpointKey(generatedDir, "")
	if err != nil || ValidateEndpointKey(generated) != nil {
		t.Fatalf("generated key = %q, err=%v", generated, err)
	}
}

func TestLoadOrCreateEndpointKeyIsAtomicAcrossConcurrentCreators(t *testing.T) {
	stateDir := t.TempDir()
	const creators = 12
	keys := make(chan string, creators)
	errs := make(chan error, creators)
	for i := 0; i < creators; i++ {
		go func() {
			key, err := LoadOrCreateEndpointKey(stateDir, "")
			keys <- key
			errs <- err
		}()
	}
	want := ""
	for i := 0; i < creators; i++ {
		key, err := <-keys, <-errs
		if err != nil {
			t.Fatal(err)
		}
		if want == "" {
			want = key
		} else if key != want {
			t.Fatalf("creator returned key %q, want stable key %q", key, want)
		}
	}
	data, err := os.ReadFile(EndpointKeyPath(stateDir))
	if err != nil || strings.TrimSpace(string(data)) != want {
		t.Fatalf("persisted key = %q, err=%v; want %q", data, err, want)
	}
}

func TestEndpointCapabilityRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	stateDir := t.TempDir()
	target := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(target, []byte(testEndpointKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, EndpointKeyPath(stateDir)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadEndpointKey(stateDir); err == nil || !strings.Contains(err.Error(), EndpointKeyPath(stateDir)) {
		t.Fatalf("LoadEndpointKey symlink error=%v; want rejection naming path %s", err, EndpointKeyPath(stateDir))
	}
}

func TestNewServerRequiresConfiguredEndpointKey(t *testing.T) {
	if _, err := NewServer(Config{StateDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "endpoint_key") {
		t.Fatalf("NewServer error=%v, want missing endpoint_key validation", err)
	}
}

func TestReadEndpointKeepsBindMetadataSeparateFromClientURL(t *testing.T) {
	stateDir := t.TempDir()
	runDir := filepath.Join(stateDir, "run")
	if err := os.Mkdir(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	want := Endpoint{
		Version:       endpointVersion,
		Protocol:      endpointProtocol,
		PID:           os.Getpid(),
		URL:           "http://[::1]:43123",
		ListenAddress: "::",
		Port:          43123,
		EndpointKey:   testEndpointKey,
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(EndpointPath(stateDir), data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEndpoint(stateDir)
	if err != nil || got != want {
		t.Fatalf("ReadEndpoint = %#v, %v; want %#v", got, err, want)
	}
}

func TestPrepareRuntimeDirRepairsUnixModeAndRejectsSymlink(t *testing.T) {
	stateDir := t.TempDir()
	runDir := filepath.Join(stateDir, "run")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareRuntimeDir(stateDir); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(runDir)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("runtime directory mode=%v err=%v; want 0700", info, err)
		}
	}
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	target := t.TempDir()
	if err := os.Remove(runDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, runDir); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareRuntimeDir(stateDir); err == nil || !strings.Contains(err.Error(), runDir) {
		t.Fatalf("runtime symlink error=%v; want rejection naming %s", err, runDir)
	}
}

func TestParseCapabilityURLAcceptsReachableHostnames(t *testing.T) {
	key := testEndpointKey
	got, err := ParseCapabilityURL("https://orc.example.net:8443/" + key)
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "https://orc.example.net:8443" || got.EndpointKey != key || got.CapabilityURL() != "https://orc.example.net:8443/"+key {
		t.Fatalf("parsed endpoint = %#v, capability URL = %q", got, got.CapabilityURL())
	}
	for _, invalid := range []string{
		"http://user@orc.example.net:43123/" + key,
		"http://orc.example.net:43123",
		"http://orc.example.net:43123/" + key + "/extra",
		"http://orc.example.net:43123/" + key + "?query=1",
		"http://orc.example.net:43123/short",
	} {
		if _, err := ParseCapabilityURL(invalid); err == nil {
			t.Errorf("ParseCapabilityURL(%q) succeeded", invalid)
		}
	}
}

func TestResolveClientEndpointPrecedenceAndInstanceDiscovery(t *testing.T) {
	stateDir := t.TempDir()
	key := testEndpointKey
	runtime := Endpoint{Version: endpointVersion, Protocol: endpointProtocol, PID: os.Getpid(), URL: "http://127.0.0.1:43124", ListenAddress: "127.0.0.1", Port: 43124, EndpointKey: key}
	if err := os.MkdirAll(filepath.Dir(EndpointPath(stateDir)), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(EndpointPath(stateDir), data, 0o600); err != nil {
		t.Fatal(err)
	}

	explicitURL := "http://explicit.example.net:9000/" + key
	environmentURL := "http://environment.example.net:9001/" + key
	for _, test := range []struct {
		name, explicit, environment, want string
	}{
		{name: "explicit first", explicit: explicitURL, environment: environmentURL, want: explicitURL},
		{name: "environment before runtime", environment: environmentURL, want: environmentURL},
		{name: "selected instance runtime metadata", want: runtime.CapabilityURL()},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ResolveClientEndpoint(test.explicit, test.environment, stateDir)
			if err != nil || got.CapabilityURL() != test.want {
				t.Fatalf("endpoint=%#v err=%v, want %q", got, err, test.want)
			}
		})
	}
	overrideState := filepath.Join(t.TempDir(), "missing-runtime")
	for _, test := range []struct {
		name, explicit, environment, want string
	}{
		{name: "command endpoint bypasses missing instance", explicit: explicitURL, want: explicitURL},
		{name: "environment endpoint bypasses missing instance", environment: environmentURL, want: environmentURL},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ResolveClientEndpoint(test.explicit, test.environment, overrideState)
			if err != nil || got.CapabilityURL() != test.want {
				t.Fatalf("endpoint=%#v err=%v, want override %q", got, err, test.want)
			}
		})
	}
	missingState := filepath.Join(t.TempDir(), "runtime")
	if _, err := ResolveClientEndpoint("", "", missingState); err == nil || !strings.Contains(err.Error(), EndpointPath(missingState)) || !strings.Contains(err.Error(), missingState) {
		t.Fatalf("missing endpoint metadata error=%v, want selected instance and endpoint path", err)
	}
	localState := t.TempDir()
	_, err = LoadOrCreateEndpointKey(localState, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveClientEndpoint("", "", localState); err == nil || !strings.Contains(err.Error(), EndpointPath(localState)) {
		t.Fatalf("persisted capability enabled fixed-port fallback: %v", err)
	}
	if _, err := ResolveClientEndpoint("not a URL", environmentURL, stateDir); err == nil {
		t.Fatal("malformed explicit endpoint fell through to lower-precedence endpoint")
	}
}
