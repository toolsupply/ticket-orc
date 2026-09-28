package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const localTicketLimit = 20

type localTicket struct {
	ID       string `json:"id"`
	Title    string `json:"title,omitempty"`
	State    string `json:"state"`
	Assignee string `json:"assignee,omitempty"`
	Priority int    `json:"priority"`
}

type localTicketReader interface {
	ActiveClaims(context.Context, string, []string, int) (ticketclient.ListResult, error)
	ReadyFrontier(context.Context, string, []string, int) (ticketclient.ListResult, error)
	Close() error
}

type localTicketReaderFactory func(currentTicketIdentity) (localTicketReader, error)
type localDaemonStatusReader func(context.Context, string) (daemon.Status, error)

type currentSessionState struct {
	OrcID            string        `json:"orc_id"`
	RepositoryID     string        `json:"repository_id"`
	Repository       string        `json:"repository"`
	Actor            string        `json:"actor"`
	Role             string        `json:"role,omitempty"`
	Session          string        `json:"session"`
	Joined           bool          `json:"joined"`
	Queue            string        `json:"queue,omitempty"`
	ReadyCount       int           `json:"ready_count"`
	ReadyMore        bool          `json:"ready_more,omitempty"`
	ActiveClaims     []localTicket `json:"active_claims"`
	Delivery         string        `json:"delivery,omitempty"`
	DeliveryCode     string        `json:"delivery_code,omitempty"`
	ManagedOwner     string        `json:"managed_owner,omitempty"`
	ManagedOwnership string        `json:"managed_ownership"`
	StatusCode       string        `json:"session_status_code,omitempty"`
	PersistenceCode  string        `json:"persistence_code,omitempty"`
}

func executeSessionState(config StateConfig, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner) error {
	ctx, cancel := context.WithTimeout(context.Background(), steerCommandTimeout)
	defer cancel()
	result, err := currentSessionStateWithConfig(ctx, config.StateDir, LoadedFileConfig{Instance: config.Instance, Config: config.FileConfig}, lookupEnv, runTicket, openLocalTicketReader, readLocalDaemonStatus)
	if err != nil {
		return err
	}
	if config.Output == string(OutputQuiet) {
		return nil
	}
	if config.Output == string(OutputJSON) {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	return renderCurrentSessionState(stdout, result)
}

func currentSessionStateWithRunner(ctx context.Context, stateDir string, lookupEnv envLookup, runTicket ticketCommandRunner, open localTicketReaderFactory, readStatus localDaemonStatusReader) (currentSessionState, error) {
	loaded, err := loadSteerConfig(lookupEnv)
	if err != nil {
		return currentSessionState{}, err
	}
	return currentSessionStateWithConfig(ctx, stateDir, loaded, lookupEnv, runTicket, open, readStatus)
}

func currentSessionStateWithConfig(ctx context.Context, stateDir string, loaded LoadedFileConfig, lookupEnv envLookup, runTicket ticketCommandRunner, open localTicketReaderFactory, readStatus localDaemonStatusReader) (currentSessionState, error) {
	identity, err := discoverCurrentTicketIdentity(ctx, lookupEnv, runTicket)
	if err != nil {
		return currentSessionState{}, err
	}
	localDir := loaded.Instance.LocalDir
	registration, registered, err := state.NewRegistrationStore(localDir).Find(ctx, identity.RepositoryID, identity.Actor)
	if err != nil {
		return currentSessionState{}, fmt.Errorf("read steer registration: %w", err)
	}
	joined := registered && registration.ThreadID == identity.ThreadID && registration.CodexHome == identity.CodexHome && registration.RepositoryPath == identity.RepositoryPath
	result := currentSessionState{
		OrcID: loaded.Config.ID, RepositoryID: identity.RepositoryID, Repository: repositoryDisplayLabel(identity.RepositoryName, identity.RepositoryPath, identity.RepositoryID),
		Actor: identity.Actor, Session: identity.ThreadID, Joined: joined,
		ActiveClaims: []localTicket{},
	}
	if !joined {
		return result, nil
	}
	result.ManagedOwnership = "unknown"
	result.Role = registration.Role
	role, ok := loaded.Config.Roles[registration.Role]
	if !ok || (role.TicketQueue != "open" && role.TicketQueue != "review") {
		return result, fmt.Errorf("registered role %q has no supported Ticket queue", registration.Role)
	}
	result.Queue = role.TicketQueue
	client, err := open(identity)
	if err != nil {
		return result, fmt.Errorf("open Ticket reader: %w", err)
	}
	defer client.Close()
	active, err := client.ActiveClaims(ctx, role.TicketQueue, nil, localTicketLimit)
	if err != nil {
		return result, fmt.Errorf("read active Ticket claims: %w", err)
	}
	ready, err := client.ReadyFrontier(ctx, role.TicketQueue, nil, localTicketLimit)
	if err != nil {
		return result, fmt.Errorf("read Ticket ready frontier: %w", err)
	}
	result.ActiveClaims = localTickets(active.Items)
	result.ReadyCount, result.ReadyMore = len(ready.Items), ready.More
	delivery, ok, err := findCurrentDelivery(ctx, localDir, registration)
	if err != nil {
		return result, fmt.Errorf("read delivery status: %w", err)
	}
	if ok {
		result.Delivery, result.DeliveryCode = delivery.State, delivery.Code
	}
	if readStatus == nil {
		result.StatusCode = "daemon_status_unavailable"
		return result, nil
	}
	statusCtx, cancel := context.WithTimeout(ctx, time.Second)
	status, statusErr := readStatus(statusCtx, stateDir)
	cancel()
	if statusErr != nil {
		result.StatusCode = "daemon_status_unavailable"
		var clientErr *daemonclient.Error
		if errors.As(statusErr, &clientErr) && clientErr.Kind == daemonclient.ErrorDiscovery {
			result.StatusCode = "daemon_endpoint_unavailable"
		}
		return result, nil
	}
	matched := false
	for _, session := range status.Steer {
		if session.RepositoryID == registration.RepositoryID && session.Actor == registration.Actor && session.Session == registration.ThreadID {
			matched = true
			result.StatusCode = session.Code
			result.PersistenceCode = session.PersistenceCode
			if session.State == "conflict" {
				result.ManagedOwnership = "conflict"
				result.ManagedOwner = session.ManagedOwner
				if result.ManagedOwner == "" {
					result.ManagedOwner = "unknown"
				}
			} else {
				result.ManagedOwnership = "clear"
			}
			break
		}
	}
	if !matched {
		result.StatusCode = "session_status_missing"
	}
	return result, nil
}

func readLocalDaemonStatus(ctx context.Context, stateDir string) (daemon.Status, error) {
	client, err := daemonclient.New(stateDir)
	if err != nil {
		return daemon.Status{}, err
	}
	return client.Status(ctx)
}

func findCurrentDelivery(ctx context.Context, dir string, registration state.SteerRegistration) (state.SteerDelivery, bool, error) {
	snapshot, err := state.NewSteerRuntimeStore(dir).Snapshot(ctx)
	if err != nil {
		return state.SteerDelivery{}, false, err
	}
	for _, delivery := range snapshot.Deliveries {
		if delivery.RepositoryID == registration.RepositoryID && delivery.Actor == registration.Actor && delivery.RegistrationID == registration.RegistrationID {
			return delivery, true, nil
		}
	}
	return state.SteerDelivery{}, false, nil
}

func openLocalTicketReader(identity currentTicketIdentity) (localTicketReader, error) {
	return ticketclient.NewWithWorkingDirAndTarget(identity.Actor, identity.RepositoryPath, ticketclient.Target{Repository: identity.RepositoryPath})
}

func localTickets(items []ticketclient.Ticket) []localTicket {
	result := make([]localTicket, 0, len(items))
	for _, item := range items {
		result = append(result, localTicket{ID: item.ID, Title: item.Title, State: item.State, Assignee: item.Assignee, Priority: item.Priority})
	}
	return result
}

func renderCurrentSessionState(out io.Writer, result currentSessionState) error {
	fmt.Fprintf(out, "Orc: %s\nRepository: %s (%s)\nActor: %s\nSession: %s\nJoined: %s\n",
		shortIdentity(result.OrcID), result.Repository, shortIdentity(result.RepositoryID), result.Actor, shortIdentity(result.Session), yesNo(result.Joined))
	if !result.Joined {
		fmt.Fprintln(out, "Work: not joined")
		return nil
	}
	work := "none"
	if result.ReadyCount > 0 {
		work = readyCountLabel(result.ReadyCount, result.ReadyMore) + " ready"
	}
	fmt.Fprintf(out, "Role: %s\nWork: %s\n", result.Role, work)
	if len(result.ActiveClaims) == 0 {
		fmt.Fprintln(out, "Active: none")
	} else {
		for i, ticket := range result.ActiveClaims {
			if i == 0 {
				fmt.Fprintf(out, "Active: %s\n", ticket.ID)
			} else {
				fmt.Fprintf(out, "  %s\n", ticket.ID)
			}
		}
	}
	fmt.Fprintf(out, "Delivery: %s\n", humanDelivery(result.Delivery))
	if result.ManagedOwner != "" {
		fmt.Fprintln(out, "State: conflict")
		fmt.Fprintf(out, "Reason: actor is owned by managed worker %s\n", result.ManagedOwner)
	} else if result.ManagedOwnership == "unknown" {
		fmt.Fprintf(out, "Managed ownership: unknown (%s)\n", humanStatusCode(result.StatusCode))
	} else if result.ManagedOwnership == "clear" {
		fmt.Fprintln(out, "Managed ownership: clear")
	}
	if result.DeliveryCode != "" {
		fmt.Fprintf(out, "Delivery detail: %s\n", humanDeliveryCode(result.DeliveryCode))
	}
	if result.StatusCode != "" && result.ManagedOwnership != "unknown" {
		fmt.Fprintf(out, "Session status: %s\n", humanDeliveryCode(result.StatusCode))
	}
	if result.PersistenceCode != "" {
		fmt.Fprintf(out, "Persistence: %s\n", humanDeliveryCode(result.PersistenceCode))
	}
	return nil
}

func humanStatusCode(code string) string {
	switch code {
	case "daemon_endpoint_unavailable":
		return "daemon endpoint unavailable"
	case "daemon_status_unavailable":
		return "daemon status unavailable"
	case "session_status_missing":
		return "session status unavailable"
	default:
		return "daemon status unavailable"
	}
}

func humanDeliveryCode(code string) string {
	switch code {
	case "managed_owner":
		return "actor is already owned by a managed worker"
	case "queue_rejected":
		return "notification was rejected"
	case "queue_uncertain":
		return "notification outcome is uncertain"
	case "registration_state_unavailable", "runtime_state_unavailable":
		return "Orc session state is unavailable"
	case "registration_state_lost":
		return "registration state disappeared; retained registrations are unavailable until steer.json is restored"
	case "registration_state_malformed":
		return "registration state is malformed; repair steer.json"
	case "runtime_state_write_failed":
		return "Orc could not save notification status"
	case "ticket_client_failed", "ticket_observation_failed":
		return "Ticket check failed; retrying"
	case "unknown_role":
		return "session role is no longer configured"
	case "busy":
		return "Ticket has an active claim"
	case "ready":
		return "ready work is available"
	case "idle":
		return "no ready work is available"
	case "awaiting_claim":
		return "waiting for a Ticket claim"
	case "consumed":
		return "Ticket work is active"
	case "invalid_state":
		return "delivery status is invalid"
	case "registration_replaced":
		return "session registration was replaced"
	default:
		return "Orc reported a delivery issue"
	}
}

func readyCountLabel(count int, more bool) string {
	if more {
		return fmt.Sprintf("%d+", count)
	}
	return fmt.Sprint(count)
}

func humanDelivery(state string) string {
	switch state {
	case "", "none", "idle":
		if state == "idle" {
			return "waiting"
		}
		return "none"
	case "sending":
		return "sending notification"
	case "queued":
		return "notification queued"
	case "consumed":
		return "work observed"
	case "degraded":
		return "needs attention"
	default:
		return "needs attention"
	}
}

func executeNext(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	args, configPath, err := extractConfigPath(args)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	jsonOutput := false
	for _, arg := range args {
		switch arg {
		case "-h", "--help":
			fmt.Fprint(stdout, nextHelp)
			return 0
		case "-j", "--json":
			if jsonOutput {
				return usageError(stderr, "duplicate flag --json")
			}
			jsonOutput = true
		default:
			return usageError(stderr, "unknown next argument %q", arg)
		}
	}
	if err := nextWithConfigRunner(context.Background(), jsonOutput, configPath, stdout, lookupEnv, runTicketJSON, openLocalTicketReader); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func nextWithRunner(ctx context.Context, jsonOutput bool, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner, open localTicketReaderFactory) error {
	return nextWithConfigRunner(ctx, jsonOutput, "", stdout, lookupEnv, runTicket, open)
}

func nextWithConfigRunner(ctx context.Context, jsonOutput bool, configPath string, stdout io.Writer, lookupEnv envLookup, runTicket ticketCommandRunner, open localTicketReaderFactory) error {
	ctx, cancel := context.WithTimeout(ctx, steerCommandTimeout)
	defer cancel()
	loaded, err := loadSteerConfigPath(configPath, lookupEnv)
	if err != nil {
		return err
	}
	identity, err := discoverCurrentTicketIdentity(ctx, lookupEnv, runTicket)
	if err != nil {
		return err
	}
	registration, ok, err := state.NewRegistrationStore(loaded.Instance.LocalDir).Find(ctx, identity.RepositoryID, identity.Actor)
	if err != nil {
		return fmt.Errorf("read steer registration: %w", err)
	}
	if !ok || registration.ThreadID != identity.ThreadID || registration.CodexHome != identity.CodexHome || registration.RepositoryPath != identity.RepositoryPath {
		return errors.New("current session is not joined; run ticket-orc join first")
	}
	role, ok := loaded.Config.Roles[registration.Role]
	if !ok || (role.TicketQueue != "open" && role.TicketQueue != "review") {
		return fmt.Errorf("registered role %q has no supported Ticket queue", registration.Role)
	}
	client, err := open(identity)
	if err != nil {
		return fmt.Errorf("open Ticket reader: %w", err)
	}
	defer client.Close()
	active, err := client.ActiveClaims(ctx, role.TicketQueue, nil, localTicketLimit)
	if err != nil {
		return fmt.Errorf("read active Ticket claims: %w", err)
	}
	ready, err := client.ReadyFrontier(ctx, role.TicketQueue, nil, localTicketLimit)
	if err != nil {
		return fmt.Errorf("read Ticket ready frontier: %w", err)
	}
	items := localTickets(active.Items)
	if len(items) == 0 {
		items = localTickets(ready.Items)
	}
	result := struct {
		Role     string        `json:"role"`
		Session  string        `json:"session"`
		Delivery string        `json:"delivery"`
		Queue    string        `json:"queue"`
		Active   bool          `json:"active_claim"`
		Items    []localTicket `json:"items"`
		More     bool          `json:"more,omitempty"`
	}{Role: registration.Role, Session: identity.ThreadID, Queue: role.TicketQueue, Active: len(active.Items) > 0, Items: items, More: ready.More}
	if delivery, found, deliveryErr := findCurrentDelivery(ctx, loaded.Instance.LocalDir, registration); deliveryErr != nil {
		return fmt.Errorf("read delivery status: %w", deliveryErr)
	} else if found {
		result.Delivery = humanDelivery(delivery.State)
	} else {
		result.Delivery = "none"
	}
	if jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	fmt.Fprintf(stdout, "Role: %s\nSession: %s\nDelivery: %s\n\n", result.Role, shortIdentity(result.Session), result.Delivery)
	if len(items) == 0 {
		fmt.Fprintln(stdout, "none ready")
		return nil
	}
	if result.Active {
		fmt.Fprintln(stdout, "Active claim:")
	} else {
		fmt.Fprintf(stdout, "%s ready\n", readyCountLabel(len(ready.Items), ready.More))
	}
	for _, item := range items {
		prefix := fmt.Sprintf("P%d  %s", item.Priority, item.ID)
		if strings.TrimSpace(item.Title) == "" {
			fmt.Fprintln(stdout, prefix)
		} else {
			fmt.Fprintf(stdout, "%s  %s\n", prefix, item.Title)
		}
	}
	if result.More {
		fmt.Fprintln(stdout, "more ready work exists")
	}
	return nil
}
