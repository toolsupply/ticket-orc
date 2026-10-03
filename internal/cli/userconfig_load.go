package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/jsonx"
)

func resolveLocalDir(configPath, configured string) (string, error) {
	configDir := filepath.Dir(configPath)
	localDir := configured
	if strings.TrimSpace(localDir) == "" {
		localDir = filepath.Join(configDir, defaultRuntimeRootName)
	} else if !filepath.IsAbs(localDir) {
		localDir = filepath.Join(configDir, localDir)
	}
	resolved, err := filepath.Abs(localDir)
	if err != nil {
		return "", fmt.Errorf("resolve local directory %q: %w", localDir, err)
	}
	resolved = filepath.Clean(resolved)
	info, err := os.Lstat(resolved)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("inspect local directory %s: %w", resolved, err)
	}
	if os.IsNotExist(err) {
		for parent := filepath.Dir(resolved); parent != resolved; parent = filepath.Dir(parent) {
			parentInfo, parentErr := os.Stat(parent)
			if parentErr == nil {
				if !parentInfo.IsDir() {
					return "", fmt.Errorf("local directory %s has non-directory parent %s", resolved, parent)
				}
				break
			}
			if !os.IsNotExist(parentErr) {
				return "", fmt.Errorf("inspect local directory %s: %w", resolved, parentErr)
			}
			next := filepath.Dir(parent)
			if next == parent {
				break
			}
		}
	}
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("local_dir must name a directory, not a symlink: %s", resolved)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("local directory %s is not a directory", resolved)
		}
	}
	return resolved, nil
}

// LoadFileConfig loads one strict Orc config file. An omitted path selects the
// existing project-local instance, then the existing user-home instance.
// Relative paths inside the file are rooted at its directory.
func LoadFileConfig(cwd, requestedPath string, explicit bool) (LoadedFileConfig, error) {
	path, err := resolveConfigPath(cwd, requestedPath, explicit)
	if err != nil {
		return LoadedFileConfig{}, err
	}
	return loadInstanceConfig(instanceContextForPath(path, explicit))
}

func loadInstanceConfig(instance InstanceContext) (LoadedFileConfig, error) {
	path := instance.ConfigPath
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return LoadedFileConfig{}, fmt.Errorf("config file not found: %s (run ticket-orc init or select an instance with TICKET_ORC)", path)
		}
		return LoadedFileConfig{}, fmt.Errorf("read config file %s: %w", path, err)
	}
	var config FileConfig
	if err := jsonx.Validate(data); err != nil {
		return LoadedFileConfig{}, fmt.Errorf("config file %s rejected: %w", path, err)
	}
	if err := validateConfigJSONFields(data); err != nil {
		return LoadedFileConfig{}, fmt.Errorf("config file %s rejected: %w", path, err)
	}
	if err := jsonx.Decode(data, &config); err != nil {
		return LoadedFileConfig{}, fmt.Errorf("config file %s rejected: %w", path, err)
	}
	if config.Version != 1 {
		return LoadedFileConfig{}, fmt.Errorf("config file %s has unsupported version %d; expected 1", path, config.Version)
	}
	if err := validateAndNormalizeFileConfig(&config, filepath.Dir(path)); err != nil {
		return LoadedFileConfig{}, fmt.Errorf("config file %s rejected: %w", path, err)
	}
	instance.LocalDirConfigured = strings.TrimSpace(config.LocalDir) != ""
	instance.LocalDir, err = resolveLocalDir(path, config.LocalDir)
	if err != nil {
		return LoadedFileConfig{}, fmt.Errorf("config file %s rejected: %w", path, err)
	}
	config.LocalDir = instance.LocalDir
	return LoadedFileConfig{Config: config, Instance: instance}, nil
}

func instanceContextForPath(configPath string, explicit bool) InstanceContext {
	absolute, err := filepath.Abs(configPath)
	if err == nil {
		configPath = filepath.Clean(absolute)
	}
	instanceDir := filepath.Dir(configPath)
	return InstanceContext{ConfigPath: configPath, InstanceDir: instanceDir, Explicit: explicit}
}

func resolveConfigPath(cwd, requestedPath string, explicit bool) (string, error) {
	if strings.TrimSpace(cwd) == "" {
		return "", fmt.Errorf("config working directory must not be empty")
	}
	base, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("resolve config working directory: %w", err)
	}
	path := requestedPath
	if !explicit && strings.TrimSpace(path) == "" {
		instanceDir, err := resolveInstanceDir(base, nil)
		if err != nil {
			return "", err
		}
		path = filepath.Join(instanceDir, instanceConfigFileName)
	}
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("config path must not be empty")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(base, path)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve config path: %w", err)
	}
	return filepath.Clean(path), nil
}

func loadInvocationConfig(values map[string]string, lookupEnv envLookup) (LoadedFileConfig, error) {
	instance, err := resolveInstanceContext(values, lookupEnv)
	if err != nil {
		return LoadedFileConfig{}, err
	}
	return loadInstanceConfig(instance)
}

func invocationConfigPath(values map[string]string, lookupEnv envLookup) (string, bool, error) {
	instance, err := resolveInstanceContext(values, lookupEnv)
	if err != nil {
		return "", false, err
	}
	return instance.ConfigPath, instance.Explicit, nil
}

func resolveInstanceContext(values map[string]string, lookupEnv envLookup) (InstanceContext, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return InstanceContext{}, fmt.Errorf("resolve config working directory: %w", err)
	}
	if path, ok := values["config"]; ok {
		resolved, resolveErr := resolveConfigPath(cwd, path, true)
		if resolveErr != nil {
			return InstanceContext{}, resolveErr
		}
		return instanceContextForPath(resolved, true), nil
	}
	instanceDir, resolveErr := resolveInstanceDir(cwd, lookupEnv)
	if resolveErr != nil {
		return InstanceContext{}, resolveErr
	}
	configPath := filepath.Join(instanceDir, instanceConfigFileName)
	return InstanceContext{ConfigPath: configPath, InstanceDir: instanceDir}, nil
}

func resolveInstanceDir(cwd string, lookupEnv envLookup) (string, error) {
	if lookupEnv != nil {
		if value, ok := lookupEnv("TICKET_ORC"); ok && strings.TrimSpace(value) != "" {
			path := value
			if !filepath.IsAbs(path) {
				path = filepath.Join(cwd, path)
			}
			resolved, err := filepath.Abs(path)
			if err != nil {
				return "", fmt.Errorf("resolve Orc instance directory: %w", err)
			}
			return filepath.Clean(resolved), nil
		}
	}
	localDir := filepath.Join(cwd, defaultInstanceDirectoryName)
	localExists, err := existingInstanceDirectory(localDir)
	if err != nil {
		return "", fmt.Errorf("inspect project Orc instance %s: %w", localDir, err)
	}
	if localExists {
		resolved, err := filepath.Abs(localDir)
		if err != nil {
			return "", fmt.Errorf("resolve Orc instance directory: %w", err)
		}
		return filepath.Clean(resolved), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("no Orc instance found: run ticket-orc init in a project, run ticket-orc init --global, or set TICKET_ORC to an instance directory; the user home directory is unavailable")
	}
	globalDir := filepath.Join(home, defaultInstanceDirectoryName)
	globalExists, err := existingInstanceDirectory(globalDir)
	if err != nil {
		return "", fmt.Errorf("inspect global Orc instance %s: %w", globalDir, err)
	}
	if !globalExists {
		return "", fmt.Errorf("no Orc instance found: run ticket-orc init in a project, run ticket-orc init --global, or set TICKET_ORC to an instance directory")
	}
	instanceDir, err := filepath.Abs(globalDir)
	if err != nil {
		return "", fmt.Errorf("resolve Orc instance directory: %w", err)
	}
	return filepath.Clean(instanceDir), nil
}

func existingInstanceDirectory(path string) (bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return false, fmt.Errorf("instance path is not a real directory")
	}
	return true, nil
}
