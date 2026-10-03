package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/state"
)

func executeInit(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	force := false
	global := false
	for _, arg := range args {
		switch arg {
		case "--force":
			if force {
				return usageError(stderr, "duplicate flag --force")
			}
			force = true
		case "--global":
			if global {
				return usageError(stderr, "duplicate flag --global")
			}
			global = true
		case "-h", "--help":
			fmt.Fprint(stdout, initHelp)
			return 0
		default:
			return usageError(stderr, "unknown init argument %q", arg)
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(stderr, "error: resolve init working directory: %v\n", err)
		return 1
	}
	dir, err := resolveInitInstanceDir(cwd, global, lookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if err := ensureSecureInstanceDir(dir); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	id, err := newInstanceID()
	if err != nil {
		fmt.Fprintf(stderr, "error: generate Orc instance ID: %v\n", err)
		return 1
	}
	config := struct {
		Version     int                     `json:"version"`
		ID          string                  `json:"id"`
		DefaultRole string                  `json:"default_role"`
		Roles       map[string]initRoleFile `json:"roles"`
	}{
		Version: 1, ID: id, DefaultRole: "coder",
		Roles: map[string]initRoleFile{
			"coder":    {TicketQueue: "open", NudgePrompt: coderNudgePrompt},
			"reviewer": {TicketQueue: "review", NudgePrompt: reviewerNudgePrompt, ReviewCompletion: ReviewCompletionClose},
		},
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		fmt.Fprintf(stderr, "error: encode Orc config: %v\n", err)
		return 1
	}
	data = append(data, '\n')
	configPath := filepath.Join(dir, instanceConfigFileName)
	var runtimeGuard state.Lock
	if !force {
		if _, err := os.Lstat(configPath); err == nil {
			fmt.Fprintf(stderr, "error: config already exists: %s (use --force to overwrite)\n", configPath)
			return 1
		} else if !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "error: inspect config path %s: %v\n", configPath, err)
			return 1
		}
	} else {
		runtimeGuard, err = acquireRuntimeGuard(context.Background(), initRuntimeResetGuardRoot(dir, configPath))
		if err != nil {
			fmt.Fprintf(stderr, "error: acquire Orc instance reset guard: %v\n", err)
			return 1
		}
		defer runtimeGuard.Release()
		if err := resetInitRuntime(dir, configPath); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
	}
	if err := writeInitGitignore(dir); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if err := writeInitConfig(configPath, data, force); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "\nCreated Orc configuration:\n\n  %s\n\nStart it with:\n\n  ticket-orc run -i\n\n", configPath)
	return 0
}

func resolveInitInstanceDir(cwd string, global bool, lookupEnv envLookup) (string, error) {
	if !global && lookupEnv != nil {
		if value, ok := lookupEnv("TICKET_ORC"); ok && strings.TrimSpace(value) != "" {
			path := value
			if !filepath.IsAbs(path) {
				path = filepath.Join(cwd, path)
			}
			resolved, err := filepath.Abs(path)
			if err != nil {
				return "", fmt.Errorf("resolve Orc init instance directory: %w", err)
			}
			return filepath.Clean(resolved), nil
		}
	}
	if !global {
		resolved, err := filepath.Abs(filepath.Join(cwd, defaultInstanceDirectoryName))
		if err != nil {
			return "", fmt.Errorf("resolve Orc init instance directory: %w", err)
		}
		return filepath.Clean(resolved), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("resolve Orc init instance directory: home directory is unavailable")
	}
	resolved, err := filepath.Abs(filepath.Join(home, defaultInstanceDirectoryName))
	if err != nil {
		return "", fmt.Errorf("resolve Orc init instance directory: %w", err)
	}
	return filepath.Clean(resolved), nil
}

const initGitignore = `/.local/
/.local.guard/
`

const previousInitGitignore = `/.local/
`

func writeInitGitignore(dir string) error {
	path := filepath.Join(dir, ".gitignore")
	if _, err := os.Lstat(path); err == nil {
		return upgradeInitGitignore(dir)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect Orc ignore file %s: %w", path, err)
	}
	tmp, err := os.CreateTemp(dir, ".gitignore-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary Orc ignore file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := secureInstanceFile(tmp, tmpName, 0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("secure temporary Orc ignore file %s: %w", tmpName, err)
	}
	data := []byte(initGitignore)
	written, err := tmp.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		tmp.Close()
		return fmt.Errorf("write Orc ignore file %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync Orc ignore file %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close Orc ignore file %s: %w", path, err)
	}
	if err := os.Link(tmpName, path); err != nil {
		if os.IsExist(err) {
			return nil
		}
		return fmt.Errorf("install Orc ignore file %s: %w", path, err)
	}
	if err := verifyInstanceFile(path, 0o600); err != nil {
		return fmt.Errorf("verify Orc ignore file %s: %w", path, err)
	}
	return nil
}

func upgradeInitGitignore(dir string) error {
	path := filepath.Join(dir, ".gitignore")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) || err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect Orc ignore file %s: %w", path, err)
	}
	data, err := os.ReadFile(path)
	if err == nil && string(data) != previousInitGitignore {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Orc ignore file %s: %w", path, err)
	}
	if err := state.WriteAtomicMode(path, []byte(initGitignore), 0o600); err != nil {
		return fmt.Errorf("update Orc ignore file %s: %w", path, err)
	}
	return nil
}

func ensureSecureInstanceDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create Orc instance directory %s: %w", dir, err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("inspect Orc instance directory %s: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("Orc instance path is not a directory: %s", dir)
	}
	if err := secureInstanceDirectory(dir); err != nil {
		return fmt.Errorf("secure Orc instance directory %s: %w", dir, err)
	}
	info, err = os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("verify Orc instance directory %s: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !instanceDirectoryModeMatches(info) {
		return fmt.Errorf("Orc instance directory is not a private directory: %s", dir)
	}
	return nil
}

type initRoleFile struct {
	TicketQueue      string `json:"ticket_queue"`
	NudgePrompt      string `json:"nudge_prompt"`
	ReviewCompletion string `json:"review_completion,omitempty"`
}

func newInstanceID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(id[:])
	return strings.Join([]string{encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]}, "-"), nil
}

func writeInitConfig(path string, data []byte, force bool) error {
	if !force {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("config already exists: %s (use --force to overwrite)", path)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect config path %s: %w", path, err)
		}
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary config for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := secureInstanceFile(tmp, tmpName, 0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("secure temporary config %s: %w", tmpName, err)
	}
	written, err := tmp.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		tmp.Close()
		return fmt.Errorf("write config file %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync config file %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close config file %s: %w", path, err)
	}
	if force {
		if err := replaceInitConfig(tmpName, path); err != nil {
			return fmt.Errorf("install config file %s: %w", path, err)
		}
	} else if err := os.Link(tmpName, path); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("config already exists: %s (use --force to overwrite)", path)
		}
		return fmt.Errorf("install config file %s: %w", path, err)
	}
	if err := verifyInstanceFile(path, 0o600); err != nil {
		return fmt.Errorf("verify config file %s: %w", path, err)
	}
	return nil
}
