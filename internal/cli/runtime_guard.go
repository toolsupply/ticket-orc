package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
)

const runtimeGuardDirectoryName = ".local.guard"

const runtimeGuardTimeout = 30 * time.Second

// acquireRuntimeGuard serializes reset and startup for a runtime root. Its
// sibling guard remains stable while the runtime root is moved or removed.
func acquireRuntimeGuard(ctx context.Context, runtimeRoot string) (state.Lock, error) {
	guardPath, err := runtimeGuardPath(runtimeRoot)
	if err != nil {
		return nil, err
	}
	return state.AcquireLock(ctx, guardPath, runtimeGuardTimeout)
}

func runtimeGuardPath(runtimeRoot string) (string, error) {
	if strings.TrimSpace(runtimeRoot) == "" {
		return "", fmt.Errorf("Orc runtime root is empty")
	}
	runtimeRoot, err := filepath.Abs(runtimeRoot)
	if err != nil {
		return "", fmt.Errorf("resolve Orc runtime root: %w", err)
	}
	canonicalRoot, err := canonicalRuntimeRoot(runtimeRoot)
	if err != nil {
		return "", err
	}
	base := filepath.Base(canonicalRoot)
	if base == string(filepath.Separator) || base == "." {
		return "", fmt.Errorf("Orc runtime root must not be a filesystem root: %s", canonicalRoot)
	}
	guardName := runtimeGuardDirectoryName
	if base != defaultRuntimeRootName {
		guardName = base
		if !strings.HasPrefix(guardName, ".") {
			guardName = "." + guardName
		}
		guardName += ".guard"
	}
	return filepath.Join(filepath.Dir(canonicalRoot), guardName), nil
}

func canonicalRuntimeRoot(runtimeRoot string) (string, error) {
	candidate := filepath.Clean(runtimeRoot)
	var missing []string
	for {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err == nil {
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("resolve Orc runtime root %s: %w", runtimeRoot, err)
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return "", fmt.Errorf("resolve Orc runtime root %s: %w", runtimeRoot, err)
		}
		missing = append(missing, filepath.Base(candidate))
		candidate = parent
	}
}

func validateRunConfigIdentity(config RunConfig) error {
	if config.ConfigPath == "" || config.InstanceID == "" {
		return fmt.Errorf("run config identity is unavailable")
	}
	loaded, err := LoadFileConfig(filepath.Dir(config.ConfigPath), config.ConfigPath, true)
	if err != nil {
		return fmt.Errorf("reload Orc config before runtime startup: %w", err)
	}
	if loaded.Config.ID != config.InstanceID || loaded.Instance.LocalDir != config.StateDir {
		return fmt.Errorf("Orc config changed before runtime startup; reload the config and retry")
	}
	return nil
}
