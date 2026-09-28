package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const steerCommandTimeout = 30 * time.Second

type ticketCommandRunner func(context.Context, ...string) ([]byte, error)

type currentTicketIdentity struct {
	Actor          string
	RepositoryID   string
	RepositoryPath string
	RepositoryName string
	ThreadID       string
	CodexHome      string
}

func executeJoin(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	args, configPath, err := extractConfigPath(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	roleName, help, err := parseJoinRole(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	if help {
		fmt.Fprint(stdout, joinHelp)
		return 0
	}
	if err := joinWithConfigRunner(context.Background(), roleName, configPath, stdout, lookupEnv, runTicketJSON); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func parseJoinRole(args []string) (string, bool, error) {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		return "", true, nil
	}
	if len(args) > 1 {
		return "", false, errors.New("join accepts at most one role")
	}
	if len(args) == 1 {
		if strings.HasPrefix(args[0], "-") || strings.TrimSpace(args[0]) == "" {
			return "", false, fmt.Errorf("invalid role %q", args[0])
		}
		return args[0], false, nil
	}
	return "", false, nil
}

func extractConfigPath(args []string) ([]string, string, error) {
	remaining := make([]string, 0, len(args))
	configPath := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var value string
		isConfig := false
		switch {
		case arg == "-c" || arg == "--config":
			isConfig = true
			i++
			if i >= len(args) {
				return nil, "", fmt.Errorf("flag --config requires a value")
			}
			value = args[i]
		case strings.HasPrefix(arg, "-c="):
			isConfig = true
			value = strings.TrimPrefix(arg, "-c=")
		case strings.HasPrefix(arg, "--config="):
			isConfig = true
			value = strings.TrimPrefix(arg, "--config=")
		}
		if !isConfig {
			remaining = append(remaining, arg)
			continue
		}
		if strings.TrimSpace(value) == "" {
			return nil, "", fmt.Errorf("--config must not be empty")
		}
		if configPath != "" {
			return nil, "", fmt.Errorf("duplicate flag --config")
		}
		configPath = value
	}
	return remaining, configPath, nil
}

func joinWithRunner(ctx context.Context, requestedRole string, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) error {
	return joinWithConfigRunner(ctx, requestedRole, "", stdout, lookupEnv, runTicket)
}

func joinWithConfigRunner(ctx context.Context, requestedRole, configPath string, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) error {
	ctx, cancel := context.WithTimeout(ctx, steerCommandTimeout)
	defer cancel()
	loaded, err := loadSteerConfigPath(configPath, lookupEnv)
	if err != nil {
		return err
	}
	roleName, explicit, err := resolveExplicitSteerRole(requestedRole, loaded.Config, lookupEnv)
	if err != nil {
		return err
	}
	identity, err := discoverCurrentTicketIdentity(ctx, lookupEnv, runTicket)
	if err != nil {
		return err
	}
	if !explicit {
		if _, ok := loaded.Config.Roles[identity.Actor]; ok {
			roleName = identity.Actor
		} else {
			roleName, err = resolveDefaultSteerRole(loaded.Config)
			if err != nil {
				return err
			}
		}
	}
	checkData, err := runTicket(ctx, "check", "--active", "-j")
	if err != nil {
		return fmt.Errorf("validate active Ticket metadata: %w", err)
	}
	var checkResult struct {
		OK *bool `json:"ok"`
	}
	if err := json.Unmarshal(checkData, &checkResult); err != nil || checkResult.OK == nil || !*checkResult.OK {
		return errors.New("ticket check --active -j did not confirm valid active metadata")
	}
	localDir := loaded.Instance.LocalDir
	registration := state.SteerRegistration{
		RepositoryID: identity.RepositoryID, RepositoryPath: identity.RepositoryPath,
		RepositoryName: identity.RepositoryName, Actor: identity.Actor, Role: roleName,
		CodexHome: identity.CodexHome, ThreadID: identity.ThreadID,
	}
	current, previous, changed, err := state.NewRegistrationStore(localDir).Join(ctx, registration)
	if err != nil {
		return fmt.Errorf("save steer registration: %w", err)
	}
	if previous == nil {
		fmt.Fprintf(stdout, "joined as %s\n", current.Role)
	} else if !changed {
		fmt.Fprintf(stdout, "already joined as %s\n", current.Role)
	} else if previous.Role != current.Role {
		fmt.Fprintf(stdout, "role changed: %s -> %s\n", previous.Role, current.Role)
	} else if previous.ThreadID != current.ThreadID {
		fmt.Fprintf(stdout, "session replaced for %s\n", current.Role)
	} else {
		fmt.Fprintf(stdout, "registration updated for %s\n", current.Role)
	}
	fmt.Fprintf(stdout, "repository: %s\nsession: %s\n", repositoryDisplayLabel(identity.RepositoryName, identity.RepositoryPath, identity.RepositoryID), shortIdentity(identity.ThreadID))
	return nil
}

func resolveSteerRole(requested string, config FileConfig, lookupEnv envLookup) (string, error) {
	role, explicit, err := resolveExplicitSteerRole(requested, config, lookupEnv)
	if err != nil {
		return "", err
	}
	if explicit {
		return role, nil
	}
	return resolveDefaultSteerRole(config)
}

func resolveDefaultSteerRole(config FileConfig) (string, error) {
	role := config.DefaultRole
	if role == "" {
		return "", errors.New("no role selected; pass a role, set TICKET_ORC_ROLE, or configure default_role")
	}
	return validateSteerRole(role, config)
}

func resolveExplicitSteerRole(requested string, config FileConfig, lookupEnv envLookup) (string, bool, error) {
	role := requested
	if role != "" {
		validated, err := validateSteerRole(role, config)
		return validated, true, err
	}
	if value, ok := lookupEnv("TICKET_ORC_ROLE"); ok {
		role = strings.TrimSpace(value)
		if role == "" {
			return "", true, errors.New("TICKET_ORC_ROLE must not be empty")
		}
		validated, err := validateSteerRole(role, config)
		return validated, true, err
	}
	return "", false, nil
}

func validateSteerRole(role string, config FileConfig) (string, error) {
	if role == "" {
		return "", errors.New("no role selected; pass a role, set TICKET_ORC_ROLE, or configure default_role")
	}
	if _, ok := config.Roles[role]; !ok {
		return "", fmt.Errorf("unknown role %q; configure it in roles", role)
	}
	return role, nil
}

func executeLeave(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	args, configPath, err := extractConfigPath(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprint(stdout, leaveHelp)
		return 0
	}
	if len(args) != 0 {
		return usageError(stderr, "leave accepts no arguments")
	}
	removed, err := leaveWithConfigRunner(context.Background(), configPath, stdout, lookupEnv, runTicketJSON)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if !removed {
		fmt.Fprintln(stdout, "not joined (already absent or replaced)")
	}
	return 0
}

func leaveWithRunner(ctx context.Context, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) (bool, error) {
	return leaveWithConfigRunner(ctx, "", stdout, lookupEnv, runTicket)
}

func leaveWithConfigRunner(ctx context.Context, configPath string, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) (bool, error) {
	loaded, err := loadSteerConfigPath(configPath, lookupEnv)
	if err != nil {
		return false, err
	}
	identity, err := discoverCurrentTicketIdentity(ctx, lookupEnv, runTicket)
	if err != nil {
		return false, err
	}
	removed, err := state.NewRegistrationStore(loaded.Instance.LocalDir).Leave(ctx, identity.RepositoryID, identity.Actor, identity.RepositoryPath, identity.CodexHome, identity.ThreadID)
	if err != nil {
		return false, fmt.Errorf("remove steer registration: %w", err)
	}
	if removed {
		fmt.Fprintf(stdout, "left %s in %s\n", shortIdentity(identity.ThreadID), repositoryDisplayLabel(identity.RepositoryName, identity.RepositoryPath, identity.RepositoryID))
	}
	return removed, nil
}

func executeWhoami(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	args, configPath, err := extractConfigPath(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	jsonOutput := false
	for _, arg := range args {
		switch arg {
		case "-h", "--help":
			fmt.Fprint(stdout, whoamiHelp)
			return 0
		case "-j", "--json":
			if jsonOutput {
				return usageError(stderr, "duplicate flag --json")
			}
			jsonOutput = true
		default:
			return usageError(stderr, "unknown whoami argument %q", arg)
		}
	}
	if err := whoamiWithConfigRunner(context.Background(), jsonOutput, configPath, stdout, lookupEnv, runTicketJSON); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func whoamiWithRunner(ctx context.Context, jsonOutput bool, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) error {
	return whoamiWithConfigRunner(ctx, jsonOutput, "", stdout, lookupEnv, runTicket)
}

func whoamiWithConfigRunner(ctx context.Context, jsonOutput bool, configPath string, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) error {
	loaded, err := loadSteerConfigPath(configPath, lookupEnv)
	if err != nil {
		return err
	}
	identity, err := discoverCurrentTicketIdentity(ctx, lookupEnv, runTicket)
	if err != nil {
		return err
	}
	role, roleErr := resolveSteerRole("", loaded.Config, lookupEnv)
	registration, registered, err := state.NewRegistrationStore(loaded.Instance.LocalDir).Find(ctx, identity.RepositoryID, identity.Actor)
	if err != nil {
		return fmt.Errorf("read steer registration: %w", err)
	}
	joined := registered && registration.ThreadID == identity.ThreadID && registration.CodexHome == identity.CodexHome && registration.RepositoryPath == identity.RepositoryPath
	if joined {
		role = registration.Role
		roleErr = nil
	}
	if roleErr != nil {
		role = ""
	}
	result := struct {
		OrcID        string `json:"orc_id"`
		RepositoryID string `json:"repository_id"`
		Repository   string `json:"repository"`
		Actor        string `json:"actor"`
		Role         string `json:"role"`
		Session      string `json:"session"`
		Joined       bool   `json:"joined"`
	}{
		OrcID: loaded.Config.ID, RepositoryID: identity.RepositoryID,
		Repository: repositoryDisplayLabel(identity.RepositoryName, identity.RepositoryPath, identity.RepositoryID), Actor: identity.Actor,
		Role: role, Session: identity.ThreadID, Joined: joined,
	}
	if jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	fmt.Fprintf(stdout, "Orc:        %s\nRepository: %s (%s)\nActor:      %s\nRole:       %s\nSession:    %s\nJoined:     %s\n",
		shortIdentity(result.OrcID), result.Repository, shortIdentity(result.RepositoryID), result.Actor, valueOrNone(result.Role), shortIdentity(result.Session), yesNo(result.Joined))
	return nil
}

func loadSteerConfig(lookupEnv envLookup) (LoadedFileConfig, error) {
	return loadSteerConfigPath("", lookupEnv)
}

func loadSteerConfigPath(configPath string, lookupEnv envLookup) (LoadedFileConfig, error) {
	values := map[string]string{}
	if configPath != "" {
		values["config"] = configPath
	}
	loaded, err := loadInvocationConfig(values, lookupEnv)
	if err != nil {
		return LoadedFileConfig{}, fmt.Errorf("load Orc config: %w", err)
	}
	return loaded, nil
}

func discoverCurrentTicketIdentity(ctx context.Context, lookupEnv envLookup, runTicket ticketCommandRunner) (currentTicketIdentity, error) {
	if lookupEnv == nil || runTicket == nil {
		return currentTicketIdentity{}, errors.New("Ticket identity discovery is unavailable")
	}
	if ctx == nil {
		return currentTicketIdentity{}, errors.New("Ticket identity context is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, steerCommandTimeout)
	defer cancel()
	actorData, err := runTicket(ctx, "actor", "-j")
	if err != nil {
		return currentTicketIdentity{}, fmt.Errorf("read Ticket actor: %w", err)
	}
	var actorResult struct {
		Actor string `json:"actor"`
	}
	if err := json.Unmarshal(actorData, &actorResult); err != nil || strings.TrimSpace(actorResult.Actor) == "" || ticketclient.ValidateActor(actorResult.Actor) != nil {
		return currentTicketIdentity{}, errors.New("ticket actor -j returned invalid actor data")
	}
	infoData, err := runTicket(ctx, "info", "-j")
	if err != nil {
		return currentTicketIdentity{}, fmt.Errorf("read Ticket repository: %w", err)
	}
	var info ticketclient.RepositoryInfo
	if err := json.Unmarshal(infoData, &info); err != nil || !ticketclient.ValidRepositoryID(info.ID) || strings.TrimSpace(info.Path) == "" {
		return currentTicketIdentity{}, errors.New("ticket info -j returned invalid repository data")
	}
	repositoryPath, err := filepath.Abs(info.Path)
	if err != nil {
		return currentTicketIdentity{}, fmt.Errorf("resolve Ticket repository path: %w", err)
	}
	threadID, ok := lookupEnv("CODEX_THREAD_ID")
	if !ok || codex.ValidateSessionTarget(threadID) != nil {
		return currentTicketIdentity{}, errors.New("CODEX_THREAD_ID is unavailable or invalid")
	}
	codexHome := ""
	if value, ok := lookupEnv("CODEX_HOME"); ok && strings.TrimSpace(value) != "" {
		codexHome = value
	} else {
		home, err := os.UserHomeDir()
		if err != nil || strings.TrimSpace(home) == "" {
			return currentTicketIdentity{}, errors.New("effective Codex home is unavailable")
		}
		codexHome = filepath.Join(home, ".codex")
	}
	codexHome, err = filepath.Abs(codexHome)
	if err != nil {
		return currentTicketIdentity{}, fmt.Errorf("resolve Codex home: %w", err)
	}
	name := ""
	if info.Name != nil {
		name = *info.Name
	}
	return currentTicketIdentity{
		Actor: actorResult.Actor, RepositoryID: info.ID, RepositoryPath: filepath.Clean(repositoryPath),
		RepositoryName: name, ThreadID: threadID, CodexHome: filepath.Clean(codexHome),
	}, nil
}

func runTicketJSON(ctx context.Context, args ...string) ([]byte, error) {
	commandName := "command"
	if len(args) > 0 {
		commandName = args[0]
	}
	command := exec.CommandContext(ctx, "ticket", args...)
	stdout := &limitedCommandOutput{limit: 64 << 10}
	stderr := &limitedCommandOutput{limit: 4 << 10}
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, exec.ErrNotFound) {
			return nil, errors.New("ticket executable was not found in PATH")
		}
		if stdout.truncated || stderr.truncated {
			return nil, errors.New("Ticket command output exceeded its size limit")
		}
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return nil, fmt.Errorf("ticket %s failed with exit code %d", commandName, exitError.ExitCode())
		}
		return nil, fmt.Errorf("ticket %s failed", commandName)
	}
	if stdout.truncated || stderr.truncated {
		return nil, errors.New("Ticket command output exceeded its size limit")
	}
	return stdout.Bytes(), nil
}

type limitedCommandOutput struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func (output *limitedCommandOutput) Write(data []byte) (int, error) {
	available := output.limit - output.Len()
	if available > len(data) {
		available = len(data)
	}
	if available > 0 {
		_, _ = output.Buffer.Write(data[:available])
	}
	if available < len(data) {
		output.truncated = true
	}
	return len(data), nil
}

func shortIdentity(value string) string {
	if value == "" {
		return "(none)"
	}
	runes := []rune(value)
	if len(runes) > 8 {
		return string(runes[:8]) + "…"
	}
	return value
}

func valueOrNone(value string) string {
	if value == "" {
		return "(none)"
	}
	return value
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
