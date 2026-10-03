package daemon

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/state"
)

const (
	endpointVersion  = 1
	endpointProtocol = 1
)

// Endpoint is the private local discovery document published while a daemon
// owns the repository runtime. EndpointKey is never included in status.
type Endpoint struct {
	Version       int    `json:"version"`
	Protocol      int    `json:"protocol"`
	InstanceID    string `json:"instance_id"`
	PID           int    `json:"pid"`
	URL           string `json:"url"`
	ListenAddress string `json:"listen_address,omitempty"`
	Port          int    `json:"port,omitempty"`
	EndpointKey   string `json:"endpoint_key"`
}

// GenerateEndpointKey returns a 256-bit URL-safe capability for daemon routes.
func GenerateEndpointKey() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate endpoint capability: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

// ValidateEndpointKey accepts only the canonical unpadded URL-safe encoding
// of exactly 256 random bits.
func ValidateEndpointKey(key string) error {
	raw, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != key {
		return fmt.Errorf("supervisor.endpoint_key must be a 256-bit base64url capability")
	}
	return nil
}

// EndpointKeyPath returns the private persistent capability path for a state directory.
func EndpointKeyPath(stateDir string) string { return filepath.Join(stateDir, "endpoint.key") }

// LoadEndpointKey reads the private persistent capability without creating it.
func LoadEndpointKey(stateDir string) (string, error) {
	path := EndpointKeyPath(stateDir)
	data, err := readSecureEndpointFile(path)
	if err != nil {
		return "", fmt.Errorf("read endpoint capability %s: %w", path, err)
	}
	key := strings.TrimSpace(string(data))
	if err := ValidateEndpointKey(key); err != nil {
		return "", fmt.Errorf("endpoint capability file %s is invalid", path)
	}
	return key, nil
}

// LoadOrCreateEndpointKey stores a stable capability in the ignored local state
// directory. A legacy config key is adopted when present to preserve existing URLs.
func LoadOrCreateEndpointKey(stateDir, legacyKey string) (string, error) {
	if strings.TrimSpace(stateDir) == "" {
		return "", fmt.Errorf("endpoint state directory must not be empty")
	}
	if err := ensureEndpointDirectory(stateDir); err != nil {
		return "", fmt.Errorf("prepare endpoint state directory %s: %w", stateDir, err)
	}
	if key, err := LoadEndpointKey(stateDir); err == nil {
		if err := syncEndpointDirectory(stateDir); err != nil {
			return "", fmt.Errorf("sync endpoint state directory %s: %w", stateDir, err)
		}
		return key, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	key := legacyKey
	if key == "" {
		var err error
		key, err = GenerateEndpointKey()
		if err != nil {
			return "", err
		}
	} else if err := ValidateEndpointKey(key); err != nil {
		return "", err
	}
	data := []byte(key + "\n")
	path := EndpointKeyPath(stateDir)
	temporary, err := os.CreateTemp(stateDir, ".endpoint-key.tmp-")
	if err != nil {
		return "", fmt.Errorf("create temporary endpoint capability: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := secureEndpointFile(temporary, temporaryPath, 0o600); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("secure temporary endpoint capability %s: %w", temporaryPath, err)
	}
	written, err := temporary.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("write endpoint capability %s: %w", path, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("sync endpoint capability %s: %w", path, err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close endpoint capability %s: %w", path, err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			key, loadErr := LoadEndpointKey(stateDir)
			if loadErr != nil {
				return "", loadErr
			}
			if syncErr := syncEndpointDirectory(stateDir); syncErr != nil {
				return "", fmt.Errorf("sync endpoint state directory %s: %w", stateDir, syncErr)
			}
			return key, nil
		}
		return "", fmt.Errorf("publish endpoint capability %s: %w", path, err)
	}
	if err := syncEndpointDirectory(stateDir); err != nil {
		return "", fmt.Errorf("sync endpoint state directory %s: %w", stateDir, err)
	}
	return key, nil
}

// CapabilityURL returns the locally usable base URL, including the capability
// path when this endpoint uses key-protected routing.
func (e Endpoint) CapabilityURL() string {
	base := strings.TrimRight(e.URL, "/")
	if e.EndpointKey == "" {
		return base
	}
	return base + "/" + e.EndpointKey
}

// ParseCapabilityURL validates and separates an endpoint URL from its path
// capability. The host may be a custom DNS name or IP address.
func ParseCapabilityURL(raw string) (Endpoint, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || strings.TrimSpace(raw) != raw {
		return Endpoint{}, fmt.Errorf("Orc endpoint must be a complete capability URL")
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return Endpoint{}, fmt.Errorf("Orc endpoint must be a complete capability URL")
	}
	if port := parsed.Port(); port != "" {
		value, parseErr := strconv.Atoi(port)
		if parseErr != nil || value < 1 || value > 65535 {
			return Endpoint{}, fmt.Errorf("Orc endpoint must be a complete capability URL")
		}
	}
	key := strings.TrimPrefix(parsed.Path, "/")
	if key == "" || strings.Contains(key, "/") || parsed.Path != "/"+key || (parsed.RawPath != "" && parsed.EscapedPath() != "/"+key) || ValidateEndpointKey(key) != nil {
		return Endpoint{}, fmt.Errorf("Orc endpoint must include a valid endpoint capability path")
	}
	base := *parsed
	base.Path, base.RawPath, base.RawQuery, base.Fragment = "", "", "", ""
	return Endpoint{Version: endpointVersion, Protocol: endpointProtocol, URL: base.String(), EndpointKey: key}, nil
}

// ResolveClientEndpoint applies the shared client destination precedence:
// command override, environment override, then runtime metadata for the
// selected instance. Runtime metadata is required when no endpoint override
// is supplied; there is no fixed-port fallback.
func ResolveClientEndpoint(explicit, environment, stateDir string) (Endpoint, error) {
	for _, candidate := range []string{explicit, environment} {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		endpoint, err := ParseCapabilityURL(candidate)
		if err != nil {
			return Endpoint{}, err
		}
		return endpoint, nil
	}
	if strings.TrimSpace(stateDir) == "" {
		return Endpoint{}, fmt.Errorf("no daemon endpoint override or instance state directory was selected")
	}
	endpoint, err := ReadEndpoint(stateDir)
	if err != nil {
		return Endpoint{}, fmt.Errorf("daemon for selected instance %s is unavailable: %w", stateDir, err)
	}
	return endpoint, nil
}

func endpointPath(stateDir string) string {
	return filepath.Join(stateDir, "run", "endpoint.json")
}

// EndpointPath returns the runtime discovery file path for a state directory.
func EndpointPath(stateDir string) string { return endpointPath(stateDir) }

// ReadEndpoint reads a discovery document without trusting it for ownership.
// Callers must still connect to URL and handle stale metadata as expected.
func ReadEndpoint(stateDir string) (Endpoint, error) {
	path := endpointPath(stateDir)
	data, err := readSecureEndpointFile(path)
	if err != nil {
		return Endpoint{}, fmt.Errorf("read daemon endpoint %s: %w", path, err)
	}
	var endpoint Endpoint
	if err := json.Unmarshal(data, &endpoint); err != nil {
		return Endpoint{}, fmt.Errorf("decode daemon endpoint %s: %w", path, err)
	}
	parsed, parseErr := url.Parse(endpoint.URL)
	if parseErr != nil || parsed == nil {
		return Endpoint{}, fmt.Errorf("daemon endpoint %s is incomplete", path)
	}
	ip := net.ParseIP(parsed.Hostname())
	port, portErr := strconv.Atoi(parsed.Port())
	if parsed.Scheme != "http" || ip == nil || portErr != nil || port < 1 || port > 65535 || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil || endpoint.Version < 1 || endpoint.Protocol < 1 || endpoint.PID < 1 {
		return Endpoint{}, fmt.Errorf("daemon endpoint %s is incomplete", path)
	}
	if err := ValidateEndpointKey(endpoint.EndpointKey); err != nil {
		return Endpoint{}, fmt.Errorf("daemon endpoint %s is incomplete: %w", path, err)
	}
	if endpoint.ListenAddress != "" {
		listenIP := net.ParseIP(endpoint.ListenAddress)
		clientIP := net.ParseIP(clientAddressForListener(endpoint.ListenAddress))
		if listenIP == nil || clientIP == nil || endpoint.Port < 1 || endpoint.Port > 65535 || port != endpoint.Port || !ip.Equal(clientIP) {
			return Endpoint{}, fmt.Errorf("daemon endpoint listen address or port is invalid: %s", path)
		}
	} else if endpoint.Port != 0 || !ip.IsLoopback() {
		return Endpoint{}, fmt.Errorf("daemon endpoint listen metadata is invalid: %s", path)
	}
	return endpoint, nil
}

func prepareRuntimeDir(stateDir string) (string, error) {
	if strings.TrimSpace(stateDir) == "" {
		return "", fmt.Errorf("daemon state directory must not be empty")
	}
	runDir := filepath.Join(stateDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return "", fmt.Errorf("create daemon runtime directory %s: %w", runDir, err)
	}
	info, err := os.Lstat(runDir)
	if err != nil {
		return "", fmt.Errorf("inspect daemon runtime directory %s: %w", runDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("daemon runtime path must be a directory: %s", runDir)
	}
	if err := secureEndpointDirectory(runDir); err != nil {
		return "", fmt.Errorf("secure daemon runtime directory %s: %w", runDir, err)
	}
	info, err = os.Lstat(runDir)
	if err != nil {
		return "", fmt.Errorf("verify daemon runtime directory %s: %w", runDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !endpointDirectoryModeMatches(info) {
		return "", fmt.Errorf("daemon runtime path is not a private directory: %s", runDir)
	}
	return runDir, nil
}

func ensureEndpointDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("path is not a directory")
	}
	if err := secureEndpointDirectory(path); err != nil {
		return err
	}
	info, err = os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !endpointDirectoryModeMatches(info) {
		return fmt.Errorf("final path is not a private directory")
	}
	return nil
}

func readSecureEndpointFile(path string) ([]byte, error) {
	directory := filepath.Dir(path)
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() {
		return nil, fmt.Errorf("parent path is not a directory")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("file changed while opening")
	}
	if err := secureEndpointFile(file, path, 0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return data, nil
}

func publishEndpoint(path string, endpoint Endpoint) error {
	data, err := json.Marshal(endpoint)
	if err != nil {
		return fmt.Errorf("encode daemon endpoint %s: %w", path, err)
	}
	if err := state.WriteAtomic(path, data); err != nil {
		return fmt.Errorf("write daemon endpoint %s: %w", path, err)
	}
	return nil
}
