package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/claude"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/harness/pi"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// StateConfig controls orchestration-state inspection.
type StateConfig struct {
	StateDir   string
	Output     string
	Instance   InstanceContext
	FileConfig FileConfig
}

// GCConfig controls terminal-ticket garbage collection.
type GCConfig struct {
	Worker          string
	Actor           string
	WorkingDir      string
	Target          ticketclient.Target
	SessionCleanup  supervisor.CleanupPolicy
	CleanupExplicit bool
	StateDir        string
}

func roleTicketTarget(config supervisor.RoleConfig) ticketclient.Target {
	return ticketclient.Target{Repository: config.Ticket.Repository, Config: config.Ticket.Config, Scope: config.Ticket.Scope}
}

func resolveMaintenanceWorker(loaded LoadedFileConfig, name string, expected supervisor.Role, lookupEnv envLookup) (supervisor.RoleConfig, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return supervisor.RoleConfig{}, fmt.Errorf("worker is required; use --worker NAME")
	}
	worker, ok := loaded.Config.Workers[name]
	if !ok {
		return supervisor.RoleConfig{}, fmt.Errorf("worker %q is not configured", name)
	}
	role := supervisor.Role(worker.Role)
	if expected != "" && role != expected {
		return supervisor.RoleConfig{}, fmt.Errorf("worker %q has role %q, expected %q", name, role, expected)
	}
	config, _, err := parseRoleConfig(role, []string{"--config", loaded.Instance.ConfigPath, "--worker", name}, lookupEnv)
	if err != nil {
		return supervisor.RoleConfig{}, fmt.Errorf("resolve worker %q: %w", name, err)
	}
	if strings.TrimSpace(config.Actor) == "" {
		return supervisor.RoleConfig{}, fmt.Errorf("worker %q must declare a Ticket actor", name)
	}
	return config, nil
}

func maintenanceStore(ctx context.Context, stateDir, actor, workingDir string, target ticketclient.Target) (*state.Store, error) {
	info, err := ticketclient.ProbeInfo(ctx, actor, workingDir, target)
	if err != nil {
		return nil, fmt.Errorf("resolve Ticket repository identity: %w", err)
	}
	if !ticketclient.ValidRepositoryID(info.ID) {
		return nil, fmt.Errorf("resolve Ticket repository identity: ticket info omitted a stable repository ID")
	}
	return state.NewForRepository(stateDir, info.ID), nil
}

func parseStateConfig(args []string, lookupEnv envLookup) (StateConfig, bool, error) {
	values, help, err := parseMaintenanceFlags(args, map[string]struct{}{"config": {}, "output": {}})
	if err != nil || help {
		return StateConfig{}, help, err
	}
	loaded, err := loadInvocationConfig(values, lookupEnv)
	if err != nil {
		return StateConfig{}, false, err
	}
	output := ""
	if value, ok := lookupEnv("TICKET_ORC_OUTPUT"); ok {
		output = value
	}
	if value, ok := values["output"]; ok {
		output = value
	}
	if output != "" && output != string(OutputJSON) && output != string(OutputCompact) && output != string(OutputQuiet) {
		return StateConfig{}, false, fmt.Errorf("output must be compact, quiet, or json")
	}
	return StateConfig{StateDir: loaded.Instance.LocalDir, Output: output, Instance: loaded.Instance, FileConfig: loaded.Config}, false, nil
}

func parseGCConfig(args []string, lookupEnv envLookup) (GCConfig, bool, error) {
	values, help, err := parseMaintenanceFlags(args, map[string]struct{}{
		"actor": {}, "config": {}, "session-cleanup": {}, "worker": {},
	})
	if err != nil || help {
		return GCConfig{}, help, err
	}
	loaded, err := loadInvocationConfig(values, lookupEnv)
	if err != nil {
		return GCConfig{}, false, err
	}
	value := func(flag, env, fallback string) string {
		if current, ok := values[flag]; ok {
			return current
		}
		if current, ok := lookupEnv(env); ok {
			return current
		}
		return fallback
	}
	cleanup := loaded.Config.Defaults.SessionCleanup
	if cleanup == "" {
		cleanup = defaultSessionCleanup
	}
	worker, err := resolveMaintenanceWorker(loaded, values["worker"], "", lookupEnv)
	if err != nil {
		return GCConfig{}, false, err
	}
	actor := worker.Actor
	if explicit, ok := values["actor"]; ok && explicit != actor {
		return GCConfig{}, false, fmt.Errorf("actor %q does not match selected worker %q actor %q", explicit, values["worker"], actor)
	}
	config := GCConfig{
		Worker:          values["worker"],
		Actor:           actor,
		WorkingDir:      worker.WorkingDir,
		SessionCleanup:  supervisor.CleanupPolicy(value("session-cleanup", "TICKET_ORC_SESSION_CLEANUP", cleanup)),
		CleanupExplicit: hasExplicitValue(values, lookupEnv, "session-cleanup", "TICKET_ORC_SESSION_CLEANUP") || loaded.Config.Defaults.SessionCleanup != "",
		StateDir:        loaded.Instance.LocalDir,
	}
	config.Target = roleTicketTarget(worker)
	if !oneOf(string(config.SessionCleanup), string(CleanupDelete), string(CleanupArchive), string(CleanupKeep)) {
		return GCConfig{}, false, fmt.Errorf("session-cleanup must be delete, archive, or keep")
	}
	return config, false, nil
}

func parseMaintenanceFlags(args []string, allowed map[string]struct{}) (map[string]string, bool, error) {
	values := make(map[string]string)
	help := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-h" || arg == "--help" {
			if help {
				return nil, false, fmt.Errorf("duplicate flag --help")
			}
			help = true
			continue
		}
		if arg == "-c" || strings.HasPrefix(arg, "-c=") {
			if _, allowedConfig := allowed["config"]; !allowedConfig {
				return nil, false, fmt.Errorf("unknown flag -c")
			}
			if _, duplicate := values["config"]; duplicate {
				return nil, false, fmt.Errorf("duplicate flag --config")
			}
			value := strings.TrimPrefix(arg, "-c=")
			if arg == "-c" {
				i++
				if i >= len(args) {
					return nil, false, fmt.Errorf("flag -c requires a value")
				}
				value = args[i]
			}
			if strings.TrimSpace(value) == "" {
				return nil, false, fmt.Errorf("-c must not be empty")
			}
			values["config"] = value
			continue
		}
		if arg == "--" {
			if i != len(args)-1 {
				return nil, false, fmt.Errorf("unexpected argument: %s", args[i+1])
			}
			break
		}
		if !strings.HasPrefix(arg, "--") {
			return nil, false, fmt.Errorf("unexpected argument: %s", arg)
		}
		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if _, ok := allowed[name]; !ok {
			return nil, false, fmt.Errorf("unknown flag --%s", name)
		}
		if _, duplicate := values[name]; duplicate {
			return nil, false, fmt.Errorf("duplicate flag --%s", name)
		}
		if !hasValue {
			i++
			if i >= len(args) {
				return nil, false, fmt.Errorf("flag --%s requires a value", name)
			}
			value = args[i]
		}
		values[name] = value
	}
	return values, help, nil
}

func executeState(config StateConfig, stdout io.Writer) error {
	// State inspection intentionally uses the explicit administrative view so
	// operators can see every repository namespace without routing worker state.
	snapshot, err := state.NewAdministrative(config.StateDir).Read(context.Background())
	if err != nil {
		return err
	}
	if config.Output == string(OutputJSON) {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(snapshot)
	}
	return renderState(stdout, snapshot)
}

func executeGC(config GCConfig, stdout, stderr io.Writer) error {
	tickets, err := ticketclient.NewWithWorkingDirAndTarget(config.Actor, config.WorkingDir, config.Target)
	if err != nil {
		return err
	}
	defer tickets.Close()
	policyDefaults := func(session state.Session) harness.CleanupPolicy {
		if session.Harness == "pi" || session.Harness == "claude" {
			return harness.CleanupKeep
		}
		return harness.CleanupDelete
	}
	cleaners := map[string]orc.SessionCleaner{
		"claude": claude.New(),
		"codex":  codex.New(),
		"pi":     pi.New(),
	}
	store, err := maintenanceStore(context.Background(), config.StateDir, config.Actor, config.WorkingDir, config.Target)
	if err != nil {
		return err
	}
	var cleaner *orc.Cleaner
	if config.CleanupExplicit {
		cleaner, err = orc.NewCleaner(
			store, tickets, cleaners,
			harness.CleanupPolicy(config.SessionCleanup), stderr,
		)
	} else {
		cleaner, err = orc.NewCleanerWithSessionPolicy(
			store, tickets, cleaners,
			harness.CleanupPolicy(config.SessionCleanup), policyDefaults, stderr,
		)
	}
	if err != nil {
		return err
	}
	result, sweepErr := cleaner.Sweep(context.Background())
	if _, err := fmt.Fprintf(stdout, "checked %d ticket(s); removed %d ticket(s), %d session(s); %d cleanup failure(s)\n",
		result.Checked, result.RemovedTickets, result.RemovedSessions, result.CleanupFailures); err != nil {
		return fmt.Errorf("write gc summary: %w", err)
	}
	return sweepErr
}

func renderState(output io.Writer, snapshot state.Snapshot) error {
	sessions := append([]state.Session(nil), snapshot.Sessions...)
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].Repository != sessions[j].Repository {
			return sessions[i].Repository < sessions[j].Repository
		}
		if sessions[i].Ticket != sessions[j].Ticket {
			return sessions[i].Ticket < sessions[j].Ticket
		}
		if sessions[i].Role != sessions[j].Role {
			return sessions[i].Role < sessions[j].Role
		}
		return sessions[i].Harness < sessions[j].Harness
	})
	type bounceRow struct {
		repository, ticket string
		count              int
	}
	bounceRows := make([]bounceRow, 0, len(snapshot.Bounces))
	for key, count := range snapshot.Bounces {
		repository, ticket := splitAdministrativeBounceKey(key)
		bounceRows = append(bounceRows, bounceRow{repository, ticket, count})
	}
	sort.Slice(bounceRows, func(i, j int) bool {
		if bounceRows[i].repository != bounceRows[j].repository {
			return bounceRows[i].repository < bounceRows[j].repository
		}
		return bounceRows[i].ticket < bounceRows[j].ticket
	})
	writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "Sessions:"); err != nil {
		return err
	}
	if len(sessions) == 0 {
		if _, err := fmt.Fprintln(writer, "  (none)"); err != nil {
			return err
		}
	} else {
		for _, session := range sessions {
			status, reason, replacement := "current", "-", "-"
			if !session.IsCurrent() {
				status, reason, replacement = "superseded", session.SupersededReason, session.ReplacedBy
			}
			owner := session.Owner
			if owner == "" {
				owner = "unknown"
			}
			if _, err := fmt.Fprintf(writer, "  %s\t%s\t%s\t%s\trepository=%s\towner=%s\t%s\treason=%s\treplaced_by=%s\tcontext=%s\n", stateValue(session.Ticket), stateValue(session.Role), stateValue(session.Harness), stateValue(session.ID), stateValue(session.Repository), stateValue(owner), stateValue(status), stateValue(reason), stateValue(replacement), stateValue(contextTelemetrySummary(session.ContextTelemetry, time.Now().UTC()))); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(writer, "\nReview bounces:"); err != nil {
		return err
	}
	if len(bounceRows) == 0 {
		if _, err := fmt.Fprintln(writer, "  (none)"); err != nil {
			return err
		}
	} else {
		for _, row := range bounceRows {
			if _, err := fmt.Fprintf(writer, "  repository=%s\tticket=%s\tbounces=%s\n", stateValue(row.repository), stateValue(row.ticket), strconv.Itoa(row.count)); err != nil {
				return err
			}
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("write state output: %w", err)
	}
	return nil
}

func splitAdministrativeBounceKey(key string) (repository, ticket string) {
	if index := strings.IndexByte(key, '\x00'); index >= 0 {
		return key[:index], key[index+1:]
	}
	return "unknown", key
}

func stateRepository(repository string) string { return stateValue(repository) }

const maxHumanStateRunes = 256

func contextTelemetrySummary(telemetry contextheadroom.Telemetry, now time.Time) string {
	if !telemetry.Known || !telemetry.Valid() {
		return "unknown"
	}
	status := "known"
	if now.Before(telemetry.ObservedAt) || now.Sub(telemetry.ObservedAt) > 24*time.Hour {
		status = "stale"
	}
	return fmt.Sprintf("%s used=%d window=%d remaining=%d observed_at=%s", status, telemetry.Used, telemetry.Window, telemetry.Remaining, telemetry.ObservedAt.UTC().Format(time.RFC3339))
}

func stateValue(value string) string {
	if value == "" {
		return "-"
	}
	var safe strings.Builder
	outputRunes := 0
	input := []rune(value)
	for index, r := range input {
		encoded := string(r)
		if unicode.IsControl(r) {
			encoded = fmt.Sprintf("\\u%04x", r)
		}
		width := utf8.RuneCountInString(encoded)
		hasMore := index+1 < len(input)
		limit := maxHumanStateRunes
		if hasMore {
			limit-- // leave room for the truncation marker
		}
		if outputRunes+width > limit {
			if hasMore || outputRunes < maxHumanStateRunes {
				safe.WriteRune('…')
			}
			break
		}
		safe.WriteString(encoded)
		outputRunes += width
	}
	return safe.String()
}

func stateTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339Nano)
}
