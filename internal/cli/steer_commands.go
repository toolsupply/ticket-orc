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
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const steerCommandTimeout = 30 * time.Second

type ticketCommandRunner func(context.Context, ...string) ([]byte, error)

func executeJoin(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	args, selector, err := extractEndpointSelector(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	args, configPath, err := extractConfigPath(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	roleName, jsonOutput, help, err := parseJoinArgs(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	if help {
		fmt.Fprint(stdout, joinHelp)
		return 0
	}
	if err := joinWithEndpointConfigRunner(context.Background(), roleName, configPath, selector, jsonOutput, stdout, lookupEnv, runTicketJSON, nil); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func parseJoinArgs(args []string) (string, bool, bool, error) {
	role, jsonOutput, help := "", false, false
	for _, arg := range args {
		switch arg {
		case "-h", "--help":
			if help {
				return "", false, false, errors.New("duplicate flag --help")
			}
			help = true
		case "-j", "--json":
			if jsonOutput {
				return "", false, false, errors.New("duplicate flag --json")
			}
			jsonOutput = true
		default:
			if strings.HasPrefix(arg, "-") || strings.TrimSpace(arg) == "" || role != "" {
				return "", false, false, fmt.Errorf("join accepts at most one role; unexpected argument %q", arg)
			}
			role = arg
		}
	}
	return role, jsonOutput, help, nil
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
	return joinWithEndpointConfigRunner(ctx, requestedRole, configPath, steerEndpointSelector{}, false, stdout, lookupEnv, runTicket, nil)
}

func joinWithEndpointConfigRunner(ctx context.Context, requestedRole, configPath string, selector steerEndpointSelector, jsonOutput bool, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner, router *steertransport.Router) error {
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
	identity, err := discoverCurrentIdentity(ctx, selector, lookupEnv, runTicket)
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
	registration := steerRegistrationForIdentity(identity, roleName)
	if router == nil {
		router, err = newDefaultSteerTransportRouter()
		if err != nil {
			return err
		}
	}
	var preparedEndpoint steertransport.Endpoint
	var preparedRegistration state.SteerRegistration
	current, previous, changed, err := state.NewRegistrationStore(localDir).JoinWithPreparation(ctx, registration, func(candidate state.SteerRegistration) error {
		preparedRegistration = candidate
		preparedEndpoint, err = router.Prepare(ctx, localDir, candidate)
		return err
	})
	if err != nil {
		if preparedRegistration.IncarnationID != "" && current.IncarnationID != preparedRegistration.IncarnationID {
			_ = router.Retire(context.Background(), localDir, preparedRegistration)
		}
		return fmt.Errorf("join steer registration: %w", err)
	}
	if previous != nil && previous.IncarnationID != current.IncarnationID {
		if err := router.Retire(ctx, localDir, *previous); err != nil {
			return fmt.Errorf("retire previous steer endpoint: %w", err)
		}
	}
	if jsonOutput {
		return json.NewEncoder(stdout).Encode(struct {
			OrcID          string                  `json:"orc_id"`
			RepositoryID   string                  `json:"repository_id"`
			Actor          string                  `json:"actor"`
			Role           string                  `json:"role"`
			Harness        string                  `json:"harness"`
			Session        string                  `json:"session"`
			RegistrationID string                  `json:"registration_id"`
			IncarnationID  string                  `json:"incarnation_id"`
			Transport      steerEndpointDescriptor `json:"transport"`
		}{loaded.Config.ID, current.RepositoryID, current.Actor, current.Role, current.Harness, current.SessionID,
			current.RegistrationID, current.IncarnationID, descriptorFor(preparedEndpoint)})
	}
	if previous == nil {
		fmt.Fprintf(stdout, "joined as %s\n", current.Role)
	} else if !changed {
		fmt.Fprintf(stdout, "already joined as %s\n", current.Role)
	} else if previous.Role != current.Role {
		fmt.Fprintf(stdout, "role changed: %s -> %s\n", previous.Role, current.Role)
	} else if previous.SessionID != current.SessionID {
		fmt.Fprintf(stdout, "session replaced for %s\n", current.Role)
	} else {
		fmt.Fprintf(stdout, "registration updated for %s\n", current.Role)
	}
	fmt.Fprintf(stdout, "repository: %s\nsession: %s\n", repositoryDisplayLabel(identity.RepositoryName, identity.RepositoryPath, identity.RepositoryID), shortIdentity(identity.SessionID))
	return nil
}

func steerRegistrationForIdentity(identity currentTicketIdentity, role string) state.SteerRegistration {
	return state.SteerRegistration{
		RepositoryID: identity.RepositoryID, RepositoryPath: identity.RepositoryPath,
		RepositoryName: identity.RepositoryName, Actor: identity.Actor, Role: role,
		Harness: identity.Harness, SessionID: identity.SessionID, Transport: identity.Transport,
	}
}

func steerRegistrationMatchesIdentity(registration state.SteerRegistration, identity currentTicketIdentity) bool {
	return registration.RepositoryID == identity.RepositoryID && registration.Actor == identity.Actor &&
		registration.RepositoryPath == identity.RepositoryPath && registration.Harness == identity.Harness &&
		registration.SessionID == identity.SessionID && sameTransportRoute(registration.Transport, identity.Transport)
}

func sameTransportRoute(left, right state.SteerTransportRoute) bool {
	if left.Kind != right.Kind || len(left.Params) != len(right.Params) {
		return false
	}
	for key, value := range left.Params {
		if right.Params[key] != value {
			return false
		}
	}
	return true
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
	args, selector, err := extractEndpointSelector(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	args, configPath, err := extractConfigPath(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	var registrationID, incarnationID string
	jsonOutput := false
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-h" || arg == "--help" {
			fmt.Fprint(stdout, leaveHelp)
			return 0
		}
		if arg == "-j" || arg == "--json" {
			if jsonOutput {
				return usageError(stderr, "duplicate flag --json")
			}
			jsonOutput = true
			continue
		}
		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if !strings.HasPrefix(arg, "--") || name != "registration-id" && name != "incarnation-id" {
			filtered = append(filtered, arg)
			continue
		}
		if !hasValue {
			i++
			if i >= len(args) {
				return usageError(stderr, "flag --%s requires a value", name)
			}
			value = args[i]
		}
		if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
			return usageError(stderr, "--%s must be non-empty and trimmed", name)
		}
		if name == "registration-id" {
			if registrationID != "" {
				return usageError(stderr, "duplicate flag --registration-id")
			}
			registrationID = value
		} else {
			if incarnationID != "" {
				return usageError(stderr, "duplicate flag --incarnation-id")
			}
			incarnationID = value
		}
	}
	args = filtered
	if len(args) != 0 {
		return usageError(stderr, "leave accepts no arguments")
	}
	if selector.Explicit && (registrationID == "" || incarnationID == "") {
		return usageError(stderr, "explicit leave requires --registration-id and --incarnation-id")
	}
	if registrationID != "" && (!validSteerIncarnationID(registrationID) || !validSteerIncarnationID(incarnationID)) {
		return usageError(stderr, "registration and incarnation IDs must be 32 lowercase hexadecimal characters")
	}
	if !selector.Explicit && (registrationID != "" || incarnationID != "") {
		return usageError(stderr, "registration IDs require an explicit endpoint selector")
	}
	removed, err := leaveWithEndpointConfigRunner(context.Background(), configPath, selector, registrationID, incarnationID, jsonOutput, stdout, lookupEnv, runTicketJSON, nil)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if !removed && !jsonOutput {
		fmt.Fprintln(stdout, "not joined (already absent or replaced)")
	}
	return 0
}

func leaveWithRunner(ctx context.Context, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) (bool, error) {
	return leaveWithConfigRunner(ctx, "", stdout, lookupEnv, runTicket)
}

func leaveWithConfigRunner(ctx context.Context, configPath string, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) (bool, error) {
	return leaveWithEndpointConfigRunner(ctx, configPath, steerEndpointSelector{}, "", "", false, stdout, lookupEnv, runTicket, nil)
}

func leaveWithEndpointConfigRunner(ctx context.Context, configPath string, selector steerEndpointSelector, registrationID, incarnationID string, jsonOutput bool, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner, router *steertransport.Router) (bool, error) {
	loaded, err := loadSteerConfigPath(configPath, lookupEnv)
	if err != nil {
		return false, err
	}
	identity, err := discoverCurrentIdentity(ctx, selector, lookupEnv, runTicket)
	if err != nil {
		return false, err
	}
	registrations := state.NewRegistrationStore(loaded.Instance.LocalDir)
	registration, found, err := registrations.Find(ctx, identity.RepositoryID, identity.Actor)
	if err != nil {
		return false, fmt.Errorf("read steer registration: %w", err)
	}
	if !found || !steerRegistrationMatchesIdentity(registration, identity) ||
		(selector.Explicit && (registration.RegistrationID != registrationID || registration.IncarnationID != incarnationID)) {
		if jsonOutput {
			return false, json.NewEncoder(stdout).Encode(struct {
				Removed bool `json:"removed"`
			}{Removed: false})
		}
		return false, nil
	}
	if router == nil {
		router, err = newDefaultSteerTransportRouter()
		if err != nil {
			return false, err
		}
	}
	removed, err := registrations.Leave(ctx, registration)
	if err != nil {
		return false, fmt.Errorf("remove steer registration: %w", err)
	}
	if removed {
		if err := router.Retire(ctx, loaded.Instance.LocalDir, registration); err != nil {
			return false, fmt.Errorf("registration was removed but endpoint retirement failed: %w", err)
		}
	}
	if jsonOutput {
		result := struct {
			Removed        bool   `json:"removed"`
			RegistrationID string `json:"registration_id,omitempty"`
			IncarnationID  string `json:"incarnation_id,omitempty"`
		}{Removed: removed}
		if removed {
			result.RegistrationID, result.IncarnationID = registration.RegistrationID, registration.IncarnationID
		}
		return removed, json.NewEncoder(stdout).Encode(result)
	}
	if removed {
		fmt.Fprintf(stdout, "left %s in %s\n", shortIdentity(identity.SessionID), repositoryDisplayLabel(identity.RepositoryName, identity.RepositoryPath, identity.RepositoryID))
	}
	return removed, nil
}

func executeWhoami(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	args, selector, err := extractEndpointSelector(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
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
	if err := whoamiWithEndpointConfigRunner(context.Background(), jsonOutput, configPath, selector, stdout, lookupEnv, runTicketJSON, nil); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func whoamiWithRunner(ctx context.Context, jsonOutput bool, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) error {
	return whoamiWithConfigRunner(ctx, jsonOutput, "", stdout, lookupEnv, runTicket)
}

func whoamiWithConfigRunner(ctx context.Context, jsonOutput bool, configPath string, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) error {
	return whoamiWithEndpointConfigRunner(ctx, jsonOutput, configPath, steerEndpointSelector{}, stdout, lookupEnv, runTicket, nil)
}

func whoamiWithEndpointConfigRunner(ctx context.Context, jsonOutput bool, configPath string, selector steerEndpointSelector, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner, router *steertransport.Router) error {
	loaded, err := loadSteerConfigPath(configPath, lookupEnv)
	if err != nil {
		return err
	}
	identity, err := discoverCurrentIdentity(ctx, selector, lookupEnv, runTicket)
	if err != nil {
		return err
	}
	role, roleErr := resolveSteerRole("", loaded.Config, lookupEnv)
	registration, registered, err := state.NewRegistrationStore(loaded.Instance.LocalDir).Find(ctx, identity.RepositoryID, identity.Actor)
	if err != nil {
		return fmt.Errorf("read steer registration: %w", err)
	}
	joined := registered && steerRegistrationMatchesIdentity(registration, identity)
	var endpoint *steerEndpointDescriptor
	if joined {
		if router == nil {
			router, err = newDefaultSteerTransportRouter()
			if err != nil {
				return err
			}
		}
		prepared, err := router.Verify(ctx, loaded.Instance.LocalDir, registration)
		if err != nil {
			return fmt.Errorf("verify current steer endpoint: %w", err)
		}
		descriptor := descriptorFor(prepared)
		endpoint = &descriptor
	}
	if joined {
		role = registration.Role
		roleErr = nil
	}
	if roleErr != nil {
		role = ""
	}
	result := struct {
		OrcID          string                   `json:"orc_id"`
		RepositoryID   string                   `json:"repository_id"`
		Repository     string                   `json:"repository"`
		Actor          string                   `json:"actor"`
		Role           string                   `json:"role"`
		Session        string                   `json:"session"`
		Joined         bool                     `json:"joined"`
		Harness        string                   `json:"harness,omitempty"`
		RegistrationID string                   `json:"registration_id,omitempty"`
		IncarnationID  string                   `json:"incarnation_id,omitempty"`
		Transport      *steerEndpointDescriptor `json:"transport,omitempty"`
	}{
		OrcID: loaded.Config.ID, RepositoryID: identity.RepositoryID,
		Repository: repositoryDisplayLabel(identity.RepositoryName, identity.RepositoryPath, identity.RepositoryID), Actor: identity.Actor,
		Role: role, Session: identity.SessionID, Joined: joined,
		Harness: identity.Harness, Transport: endpoint,
	}
	if joined {
		result.RegistrationID = registration.RegistrationID
		result.IncarnationID = registration.IncarnationID
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
	if err := ensureLoadedRuntime(loaded); err != nil {
		return LoadedFileConfig{}, err
	}
	return loaded, nil
}

func discoverCurrentIdentity(ctx context.Context, selector steerEndpointSelector, lookupEnv envLookup, runTicket ticketCommandRunner) (currentTicketIdentity, error) {
	routing, err := discoverTicketRoutingIdentity(ctx, runTicket)
	if err != nil {
		return currentTicketIdentity{}, err
	}
	endpoint, err := discoverSteerEndpointIdentity(selector, lookupEnv)
	if err != nil {
		return currentTicketIdentity{}, err
	}
	return currentTicketIdentity{ticketRoutingIdentity: routing, steerEndpointIdentity: endpoint}, nil
}

func discoverCurrentTicketIdentity(ctx context.Context, lookupEnv envLookup, runTicket ticketCommandRunner) (currentTicketIdentity, error) {
	return discoverCurrentIdentity(ctx, steerEndpointSelector{}, lookupEnv, runTicket)
}

func discoverTicketRoutingIdentity(ctx context.Context, runTicket ticketCommandRunner) (ticketRoutingIdentity, error) {
	if runTicket == nil {
		return ticketRoutingIdentity{}, errors.New("Ticket identity discovery is unavailable")
	}
	if ctx == nil {
		return ticketRoutingIdentity{}, errors.New("Ticket identity context is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, steerCommandTimeout)
	defer cancel()
	actorData, err := runTicket(ctx, "actor", "-j")
	if err != nil {
		return ticketRoutingIdentity{}, fmt.Errorf("read Ticket actor: %w", err)
	}
	var actorResult struct {
		Actor string `json:"actor"`
	}
	if err := json.Unmarshal(actorData, &actorResult); err != nil || strings.TrimSpace(actorResult.Actor) == "" || ticketclient.ValidateActor(actorResult.Actor) != nil {
		return ticketRoutingIdentity{}, errors.New("ticket actor -j returned invalid actor data")
	}
	infoData, err := runTicket(ctx, "info", "-j")
	if err != nil {
		return ticketRoutingIdentity{}, fmt.Errorf("read Ticket repository: %w", err)
	}
	var info ticketclient.RepositoryInfo
	if err := json.Unmarshal(infoData, &info); err != nil || !ticketclient.ValidRepositoryID(info.ID) || strings.TrimSpace(info.Path) == "" {
		return ticketRoutingIdentity{}, errors.New("ticket info -j returned invalid repository data")
	}
	repositoryPath, err := filepath.Abs(info.Path)
	if err != nil {
		return ticketRoutingIdentity{}, fmt.Errorf("resolve Ticket repository path: %w", err)
	}
	name := ""
	if info.Name != nil {
		name = *info.Name
	}
	return ticketRoutingIdentity{Actor: actorResult.Actor, RepositoryID: info.ID,
		RepositoryPath: filepath.Clean(repositoryPath), RepositoryName: name}, nil
}

func discoverSteerEndpointIdentity(selector steerEndpointSelector, lookupEnv envLookup) (steerEndpointIdentity, error) {
	if selector.Explicit {
		if err := validateEndpointSelector(selector); err != nil {
			return steerEndpointIdentity{}, fmt.Errorf("invalid explicit steering endpoint: %w", err)
		}
		return steerEndpointIdentity{Harness: selector.Harness, SessionID: selector.SessionID,
			Transport: state.SteerTransportRoute{Kind: selector.Transport}}, nil
	}
	if lookupEnv == nil {
		return steerEndpointIdentity{}, errors.New("Codex endpoint discovery is unavailable")
	}
	threadID, ok := lookupEnv("CODEX_THREAD_ID")
	if !ok || codex.ValidateSessionTarget(threadID) != nil {
		return steerEndpointIdentity{}, errors.New("CODEX_THREAD_ID is unavailable or invalid")
	}
	codexHome := ""
	if value, ok := lookupEnv("CODEX_HOME"); ok && strings.TrimSpace(value) != "" {
		codexHome = value
	} else {
		home, err := os.UserHomeDir()
		if err != nil || strings.TrimSpace(home) == "" {
			return steerEndpointIdentity{}, errors.New("effective Codex home is unavailable")
		}
		codexHome = filepath.Join(home, ".codex")
	}
	codexHome, err := filepath.Abs(codexHome)
	if err != nil {
		return steerEndpointIdentity{}, fmt.Errorf("resolve Codex home: %w", err)
	}
	return steerEndpointIdentity{Harness: "codex", SessionID: threadID,
		Transport: state.SteerTransportRoute{Kind: "codex-queue", Params: map[string]string{"home": filepath.Clean(codexHome)}}}, nil
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
