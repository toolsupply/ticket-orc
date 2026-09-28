//go:build windows

package daemon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsEndpointFilesUseProfileACLAndRemainReadable(t *testing.T) {
	stateDir := t.TempDir()
	key, err := LoadOrCreateEndpointKey(stateDir, "")
	if err != nil || ValidateEndpointKey(key) != nil {
		t.Fatalf("create endpoint key under profile ACL = %q, %v", key, err)
	}
	runDir, err := prepareRuntimeDir(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	want := Endpoint{Version: endpointVersion, Protocol: endpointProtocol, PID: os.Getpid(), URL: "http://127.0.0.1:43123", ListenAddress: "127.0.0.1", Port: 43123, EndpointKey: key}
	if err := publishEndpoint(filepath.Join(runDir, "endpoint.json"), want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEndpoint(stateDir)
	if err != nil || got != want {
		t.Fatalf("read endpoint = %#v, %v; want %#v", got, err, want)
	}
}
