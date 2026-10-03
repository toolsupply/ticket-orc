package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type queueForecastOwner struct {
	Owner        string                `json:"owner"`
	Kind         string                `json:"kind"`
	RepositoryID string                `json:"repository_id"`
	Repository   string                `json:"repository"`
	Role         string                `json:"role"`
	Actor        string                `json:"actor"`
	Session      string                `json:"session,omitempty"`
	Queue        string                `json:"queue"`
	State        string                `json:"state,omitempty"`
	Reason       string                `json:"reason,omitempty"`
	Delivery     string                `json:"delivery,omitempty"`
	Active       []ticketclient.Ticket `json:"active"`
	Next         *ticketclient.Ticket  `json:"next,omitempty"`
	MoreReady    bool                  `json:"more_ready,omitempty"`

	repositoryPath string
	deliveryCode   string
	filters        ticketclient.QueueFilters
}

func executeQueue(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	jsonOutput := false
	optionsArgs := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "-j" || arg == "--json" {
			if jsonOutput {
				return usageError(stderr, "duplicate flag --json")
			}
			jsonOutput = true
			continue
		}
		optionsArgs = append(optionsArgs, arg)
	}
	options, positional, help, err := parseDaemonCommandOptions(optionsArgs, lookupEnv, false)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	if help {
		fmt.Fprint(stdout, queueHelp)
		return 0
	}
	if len(positional) != 0 {
		return usageError(stderr, "queue accepts no positional arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), steerCommandTimeout)
	defer cancel()
	values := map[string]string{}
	if options.configPath != "" {
		values["config"] = options.configPath
	}
	loaded, err := loadInvocationConfig(values, lookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if err := ensureLoadedRuntime(loaded); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	client, statusCtx, clientCancel, err := daemonClient(options)
	if err != nil {
		fmt.Fprintf(stderr, "error: connect to Orc status: %v\n", err)
		return 1
	}
	defer clientCancel()
	status, err := client.Status(statusCtx)
	if err != nil {
		fmt.Fprintf(stderr, "error: read runtime status: %v\n", err)
		return 1
	}
	if err := writeQueueForecast(ctx, stdout, jsonOutput, loaded, status, openLocalTicketReader); err != nil {
		fmt.Fprintf(stderr, "error: queue forecast: %v\n", err)
		return 1
	}
	return 0
}

func writeQueueForecast(ctx context.Context, out io.Writer, jsonOutput bool, loaded LoadedFileConfig, status daemon.Status, open localTicketReaderFactory) error {
	owners, err := queueForecast(ctx, loaded, status, open)
	if err != nil {
		return err
	}
	if jsonOutput {
		mode := status.Mode
		if mode == "" {
			mode = "running"
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(struct {
			Mode              string               `json:"daemon_mode"`
			DispatchInhibited bool                 `json:"dispatch_inhibited"`
			Owners            []queueForecastOwner `json:"owners"`
		}{Mode: mode, DispatchInhibited: mode != "running", Owners: owners})
	}
	if status.Mode != "" && status.Mode != "running" {
		fmt.Fprintf(out, "\ndaemon mode=%s dispatch=inhibited\n", status.Mode)
	}
	return renderQueueForecast(out, owners)
}

func queueForecast(ctx context.Context, loaded LoadedFileConfig, status daemon.Status, open localTicketReaderFactory) ([]queueForecastOwner, error) {
	if open == nil {
		return nil, errors.New("Ticket reader is unavailable")
	}
	registrations, err := state.NewRegistrationStore(loaded.Instance.LocalDir).Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("read steer registrations: %w", err)
	}
	registrationByKey := make(map[string]state.SteerRegistration, len(registrations.Registrations))
	for _, registration := range registrations.Registrations {
		registrationByKey[registration.RepositoryID+"\x00"+registration.Actor] = registration
	}
	owners := make([]queueForecastOwner, 0, len(status.Workers)+len(status.Steer))
	for _, worker := range status.Workers {
		roleName, queue := worker.EffectiveRoleName, worker.EffectiveTicketQueue
		if roleName == "" || queue == "" {
			// Older status snapshots lack effective policy. Resolve them from the
			// desired config only as a compatibility fallback.
			if configured, ok := loaded.Config.Workers[worker.Name]; ok {
				if roleName == "" {
					roleName = configured.Role
				}
			}
			if roleName == "" {
				roleName = worker.Role
			}
			if role, ok := loaded.Config.Roles[roleName]; ok && queue == "" {
				queue = role.TicketQueue
			}
		}
		if roleName == "" || (queue != "open" && queue != "review") {
			return nil, fmt.Errorf("managed worker %q has unsupported role or queue %q/%q", worker.Name, roleName, queue)
		}
		repoID, repoPath, repoName := worker.RepositoryID, worker.RepositoryPath, worker.RepositoryName
		if repoID == "" && worker.RepositoryKey != "" {
			if repository, found := loaded.Config.Repositories[worker.RepositoryKey]; found {
				repoPath = repository.Repository
			}
		}
		if repoID == "" || repoPath == "" || worker.TicketActor == "" {
			continue
		}
		filters, err := managedForecastFilters(queue, worker)
		if err != nil {
			return nil, fmt.Errorf("resolve effective Ticket filters for managed worker %q: %w", worker.Name, err)
		}
		owners = append(owners, queueForecastOwner{
			Owner: worker.Name, Kind: "managed", RepositoryID: repoID,
			Repository: repositoryDisplayLabel(repoName, repoPath, repoID), Role: roleName,
			Actor: worker.TicketActor, Queue: queue, State: worker.State,
			Reason: managedForecastReason(worker), Active: []ticketclient.Ticket{}, repositoryPath: repoPath,
			filters: filters,
		})
	}
	for _, session := range status.Steer {
		registration, ok := registrationByKey[session.RepositoryID+"\x00"+session.Actor]
		if !ok || registration.Harness != session.Harness || registration.SessionID != session.Session {
			continue
		}
		queue := session.EffectiveTicketQueue
		filters := ticketclient.QueueFilters{
			Tags:        append([]string(nil), session.EffectiveTicketTags...),
			WithoutTags: append([]string(nil), session.EffectiveReviewSkipTags...),
		}
		if queue != "" && queue != "open" && queue != "review" {
			return nil, fmt.Errorf("steer session %q has unsupported role or queue %q/%q", session.Actor, registration.Role, queue)
		}
		ownerName := repositoryDisplayLabel(registration.RepositoryName, registration.RepositoryPath, registration.RepositoryID) + "/" + session.Actor
		owner := queueForecastOwner{
			Owner: ownerName, Kind: "steer", RepositoryID: registration.RepositoryID,
			Repository: repositoryDisplayLabel(registration.RepositoryName, registration.RepositoryPath, registration.RepositoryID),
			Role:       registration.Role, Actor: session.Actor, Session: session.Session, Queue: queue,
			State: session.State, Reason: steerForecastReason(session), Delivery: session.State,
			Active: []ticketclient.Ticket{}, repositoryPath: registration.RepositoryPath, deliveryCode: session.Code,
			filters: filters,
		}
		if queue == "" {
			owner.Reason = "role policy unavailable"
		}
		if session.ManagedOwner != "" {
			owner.Reason = "actor is owned by managed worker " + session.ManagedOwner
		}
		owners = append(owners, owner)
	}
	sort.SliceStable(owners, func(i, j int) bool {
		if owners[i].RepositoryID != owners[j].RepositoryID {
			return owners[i].RepositoryID < owners[j].RepositoryID
		}
		return owners[i].Queue < owners[j].Queue
	})

	frontiers := make(map[string]ticketclient.ListResult)
	for index := range owners {
		owner := &owners[index]
		if owner.Kind == "steer" && owner.State == "conflict" {
			// The matching managed owner is the execution owner for this actor;
			// avoid displaying its active claim twice under the rejected session.
			continue
		}
		identity := currentTicketIdentity{ticketRoutingIdentity: ticketRoutingIdentity{
			Actor: owner.Actor, RepositoryID: owner.RepositoryID, RepositoryPath: owner.repositoryPath, RepositoryName: owner.Repository,
		}}
		client, err := open(identity)
		if err != nil {
			return nil, fmt.Errorf("open Ticket reader for %s/%s: %w", owner.Repository, owner.Actor, err)
		}
		activeOpen, err := client.ActiveClaims(ctx, "open", localTicketLimit)
		if err == nil {
			activeReview, reviewErr := client.ActiveClaims(ctx, "review", localTicketLimit)
			err = reviewErr
			if err == nil {
				owner.Active = append(owner.Active, activeOpen.Items...)
				owner.Active = append(owner.Active, activeReview.Items...)
			}
		}
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("read active claims for %s/%s: %w", owner.Repository, owner.Actor, err)
		}
		if owner.Queue != "open" && owner.Queue != "review" {
			_ = client.Close()
			continue
		}
		frontierKey := queueForecastFrontierKey(*owner)
		frontier, found := frontiers[frontierKey]
		if !found {
			frontier, err = client.ReadyFrontier(ctx, owner.Queue, owner.filters, localTicketLimit)
			if err == nil {
				frontiers[frontierKey] = frontier
			}
		}
		_ = client.Close()
		if err != nil {
			return nil, fmt.Errorf("read %s ready frontier for %s: %w", owner.Queue, owner.Repository, err)
		}
	}
	// Ticket supplies each selector's ordered ready frontier. Assign IDs once
	// per repository/queue in stable owner order because selector frontiers can
	// overlap.
	frontierOffsets := make(map[string]int)
	assignedByQueue := make(map[string]map[string]struct{})
	for index := range owners {
		owner := &owners[index]
		if owner.Reason != "" && owner.Reason != "already notified" {
			continue
		}
		key := queueForecastFrontierKey(*owner)
		frontier := frontiers[key]
		offset := frontierOffsets[key]
		queueKey := queueForecastDedupKey(*owner)
		assigned := assignedByQueue[queueKey]
		if assigned == nil {
			assigned = make(map[string]struct{})
			assignedByQueue[queueKey] = assigned
		}
		for offset < len(frontier.Items) {
			item := frontier.Items[offset]
			offset++
			if _, exists := assigned[item.ID]; exists {
				continue
			}
			owner.Next = &item
			assigned[item.ID] = struct{}{}
			break
		}
		frontierOffsets[key] = offset
		owner.MoreReady = frontier.More || offset < len(frontier.Items)
		if owner.Reason == "" {
			if owner.Kind == "steer" {
				action := orc.AdvanceSteerDelivery(orc.SteerDeliveryState(owner.Delivery), orc.Evidence{Active: len(owner.Active) > 0, Ready: owner.Next != nil})
				if action.Code == "awaiting_claim" || owner.Delivery == string(orc.SteerQueued) {
					owner.Reason = "already notified"
				}
			}
			if owner.Next == nil && len(owner.Active) == 0 {
				owner.Reason = "no actionable work"
			}
		}
	}
	return owners, nil
}

func managedForecastFilters(queue string, worker daemon.WorkerStatus) (ticketclient.QueueFilters, error) {
	filters := ticketclient.QueueFilters{Tags: append([]string(nil), worker.EffectiveTicketTags...)}
	if queue == "review" {
		filters.WithoutTags = append([]string(nil), worker.EffectiveReviewSkipTags...)
	}
	return ticketclient.CanonicalQueueFilters(filters)
}

func queueForecastFrontierKey(owner queueForecastOwner) string {
	filters, err := ticketclient.CanonicalQueueFilters(owner.filters)
	if err != nil {
		filters = owner.filters
	}
	encoded, _ := json.Marshal(filters)
	return owner.RepositoryID + "\x00" + owner.Queue + "\x00" + string(encoded)
}

func queueForecastDedupKey(owner queueForecastOwner) string {
	return owner.RepositoryID + "\x00" + owner.Queue
}

func managedForecastReason(worker daemon.WorkerStatus) string {
	switch worker.State {
	case "paused":
		return "managed worker is paused"
	case "stopped", "failed", "conflict":
		return "managed worker is " + worker.State
	default:
		return ""
	}
}

func steerForecastReason(session daemon.SteerStatus) string {
	if session.State == "conflict" {
		return "actor ownership conflict"
	}
	if session.State == "degraded" || session.PersistenceCode != "" {
		return "degraded delivery"
	}
	if session.State == "queued" {
		return "already notified"
	}
	if session.State == "sending" {
		return "notification delivery is uncertain"
	}
	return ""
}

func renderQueueForecast(out io.Writer, owners []queueForecastOwner) error {
	if len(owners) == 0 {
		_, _ = io.WriteString(out, "\nNo configured or registered execution owners.\n\n")
		return nil
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	rowsByRepository := make(map[string][]queueForecastTicket)
	repositoryOwners := make(map[string]queueForecastOwner)
	repositoryOrder := make([]string, 0)
	for _, owner := range owners {
		if _, exists := rowsByRepository[owner.RepositoryID]; !exists {
			repositoryOrder = append(repositoryOrder, owner.RepositoryID)
			rowsByRepository[owner.RepositoryID] = nil
			repositoryOwners[owner.RepositoryID] = owner
		}
		for _, ticket := range owner.Active {
			rowsByRepository[owner.RepositoryID] = append(rowsByRepository[owner.RepositoryID], queueForecastTicket{Actor: owner.Actor, Ticket: ticket, State: "claimed"})
		}
		if owner.Next != nil {
			rowsByRepository[owner.RepositoryID] = append(rowsByRepository[owner.RepositoryID], queueForecastTicket{Actor: owner.Actor, Ticket: *owner.Next, State: "queued"})
		}
	}
	rowCount := 0
	for _, repositoryID := range repositoryOrder {
		rowCount += len(rowsByRepository[repositoryID])
	}
	if rowCount == 0 {
		_, _ = io.WriteString(out, "\nNo non-terminal tickets.\n\n")
		return nil
	}
	seenTickets := make(map[string]bool, rowCount)
	for _, repositoryID := range repositoryOrder {
		repositoryRows := rowsByRepository[repositoryID]
		if len(repositoryRows) == 0 {
			continue
		}
		owner := repositoryOwners[repositoryID]
		fmt.Fprintf(w, "\nRepository: %s (%s) %s\n\n", consoleField(owner.Repository), shortIdentity(repositoryID), consoleField(owner.repositoryPath))
		fmt.Fprintln(w, "Actor\tTicket\tTitle\tState")
		for _, row := range repositoryRows {
			ticketKey := repositoryID + "\x00" + row.Ticket.ID
			if seenTickets[ticketKey] {
				continue
			}
			seenTickets[ticketKey] = true
			title := consoleEllipsize(consoleField(strings.TrimSpace(row.Ticket.Title)), 19)
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", consoleField(row.Actor), row.Ticket.ID, title, row.State)
		}
	}
	fmt.Fprintln(w)
	return w.Flush()
}

type queueForecastTicket struct {
	Actor  string
	Ticket ticketclient.Ticket
	State  string
}

func queueStatus(ctx context.Context, client *daemonclient.Client) (daemon.Status, error) {
	if client == nil {
		return daemon.Status{}, errors.New("daemon client is unavailable")
	}
	return client.Status(ctx)
}
