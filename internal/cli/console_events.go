package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

func readConsoleEvents(ctx context.Context, client *daemonclient.Client, events chan<- daemon.Event, errorsOut chan<- error) {
	const reconnectDelay = 100 * time.Millisecond
	consecutiveFailures := 0
	connected := false
	for {
		stream, err := client.Events(ctx)
		if err != nil {
			consecutiveFailures++
			if consecutiveFailures >= 3 {
				errorsOut <- err
				return
			}
			if !waitConsoleReconnect(ctx, reconnectDelay) {
				return
			}
			continue
		}
		if connected {
			resyncConsoleStatus(ctx, client, events, errorsOut)
		}
		connected = true
		consecutiveFailures = 0
		event, nextErr := stream.Next()
		if nextErr != nil && !errors.Is(nextErr, daemonclient.ErrEventGap) {
			_ = stream.Close()
			if errors.Is(nextErr, context.Canceled) || errors.Is(nextErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			consecutiveFailures++
			if consecutiveFailures >= 3 {
				errorsOut <- nextErr
				return
			}
			if !waitConsoleReconnect(ctx, reconnectDelay) {
				return
			}
			continue
		}
		if errors.Is(nextErr, daemonclient.ErrEventGap) {
			resyncConsoleStatus(ctx, client, events, errorsOut)
		}
		select {
		case events <- event:
		case <-ctx.Done():
			_ = stream.Close()
			return
		}
	}
}

func waitConsoleReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func resyncConsoleStatus(ctx context.Context, client *daemonclient.Client, events chan<- daemon.Event, errorsOut chan<- error) {
	status, statusErr := client.Status(ctx)
	if statusErr != nil {
		select {
		case errorsOut <- statusErr:
		case <-ctx.Done():
		}
		return
	}
	for _, worker := range status.Workers {
		select {
		case events <- daemon.Event{Type: "status.resync", Worker: worker.Name, Role: worker.Role, Actor: worker.TicketActor, State: worker.State, RepositoryID: worker.RepositoryID, RepositoryName: worker.RepositoryName}:
		case <-ctx.Done():
			return
		}
	}
	for _, repository := range status.Repositories {
		select {
		case events <- daemon.Event{Type: "status.resync", RepositoryID: repository.ID, RepositoryKey: repository.Key, State: repository.State}:
		case <-ctx.Done():
			return
		}
	}
	for _, session := range status.Steer {
		select {
		case events <- daemon.Event{Type: "status.resync", Role: session.Role, Actor: session.Actor, Session: shortIdentity(session.Session), State: session.State, Code: session.Code, RepositoryID: session.RepositoryID, RepositoryName: session.RepositoryName}:
		case <-ctx.Done():
			return
		}
	}
}

func renderConsoleEvent(out io.Writer, event daemon.Event) {
	renderConsoleEventAt(out, event, time.Now(), nil)
}

type consoleWatchRenderer struct {
	date         string
	repositories map[string]daemon.RepositoryStatus
}

func consoleWatchHeader() string {
	return renderColumnRow([]string{"Time", "Repository", "Session", "Role", "Actor", "Event"}, []int{8, consoleWatchRepositoryCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, 0})
}

func (renderer *consoleWatchRenderer) seedRepositories(repositories []daemon.RepositoryStatus) {
	renderer.repositories = make(map[string]daemon.RepositoryStatus, len(repositories))
	for _, repository := range repositories {
		if repository.ID != "" {
			renderer.repositories[repository.ID] = repository
		}
	}
}

func (renderer *consoleWatchRenderer) render(out io.Writer, event daemon.Event, at time.Time) {
	date := at.Local().Format("2006-01-02")
	if renderer.date != "" && renderer.date != date {
		fmt.Fprintf(out, "--- %s ---\n", date)
	}
	renderer.date = date
	renderConsoleEventAt(out, event, at, renderer.repositories)
}

func renderConsoleEventAt(out io.Writer, event daemon.Event, at time.Time, repositories map[string]daemon.RepositoryStatus) {
	message := consoleEventMessage(event)
	if message != "" {
		if ticket := consoleEventTicket(event); ticket != "" && !strings.Contains(message, consoleField(ticket)) {
			message += " " + consoleField(ticket)
		}
		repositoryPath := ""
		if repository, ok := repositories[event.RepositoryID]; ok {
			repositoryPath = repository.Path
		}
		repository := repositoryDisplayLabel(event.RepositoryName, repositoryPath, event.RepositoryID)
		if event.RepositoryID == "" && event.RepositoryName == "" {
			repository = "—"
		}
		role := valueOrDash(event.Role)
		actor := event.Actor
		if actor == "" {
			actor = event.Worker
		}
		actor = valueOrDash(actor)
		session := valueOrDash(shortEventSession(event.Session))
		fmt.Fprintln(out, renderColumnRow([]string{
			at.Local().Format("15:04:05"),
			consoleEllipsize(repository, consoleWatchRepositoryCol),
			session,
			consoleEllipsize(consoleField(role), consoleStatusRoleCol),
			consoleEllipsize(consoleField(actor), consoleStatusRoleCol),
			message,
		}, []int{8, consoleWatchRepositoryCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, 0}))
	}
}

func shortEventSession(value string) string {
	if value == "" {
		return ""
	}
	return shortIdentity(value)
}

func consoleEventTicket(event daemon.Event) string {
	if event.Ticket != "" {
		return event.Ticket
	}
	if event.Failure != nil {
		return event.Failure.Ticket
	}
	return ""
}

func valueOrDash(value string) string {
	if value == "" {
		return "—"
	}
	return consoleField(value)
}

type consoleEventTracker struct {
	states           map[string]string
	repositories     map[string]string
	workers          map[string]consoleWatchWorker
	steerSessions    map[string]steerWatchIdentity
	lastEvent        string
	lastEventWorker  string
	lastEventMessage string
}

type steerWatchIdentity struct{ Role, Session string }
type consoleWatchWorker struct{ Role, Actor, RepositoryID, RepositoryName string }

func newConsoleEventTracker() *consoleEventTracker {
	return &consoleEventTracker{states: make(map[string]string), repositories: make(map[string]string), workers: make(map[string]consoleWatchWorker), steerSessions: make(map[string]steerWatchIdentity)}
}

func (tracker *consoleEventTracker) seed(status daemon.Status) {
	for _, worker := range status.Workers {
		if worker.Name != "" {
			tracker.states[worker.Name] = worker.State
			tracker.workers[worker.Name] = consoleWatchWorker{Role: worker.Role, Actor: worker.TicketActor, RepositoryID: worker.RepositoryID, RepositoryName: worker.RepositoryName}
		}
	}
	for _, repository := range status.Repositories {
		if repository.ID != "" {
			tracker.repositories[repository.ID] = repository.State
		}
	}
	for _, session := range status.Steer {
		key := session.RepositoryID + "\x00" + session.Actor
		tracker.steerSessions[key] = steerWatchIdentity{Role: session.Role, Session: shortIdentity(session.Session)}
	}
}

// prepare enriches the operator-local stream using adjacent status snapshots;
// the daemon event envelope and HTTP API remain unchanged.
func (tracker *consoleEventTracker) prepare(event daemon.Event) daemon.Event {
	if event.Type == "status.resync" {
		if event.Worker != "" {
			tracker.workers[event.Worker] = consoleWatchWorker{Role: event.Role, Actor: event.Actor, RepositoryID: event.RepositoryID, RepositoryName: event.RepositoryName}
		}
		if event.Actor != "" && event.Worker == "" {
			key := event.RepositoryID + "\x00" + event.Actor
			tracker.steerSessions[key] = steerWatchIdentity{Role: event.Role, Session: event.Session}
		}
		return event
	}
	if event.Worker != "" {
		if worker, ok := tracker.workers[event.Worker]; ok {
			if event.Role == "" {
				event.Role = worker.Role
			}
			if event.Actor == "" {
				event.Actor = worker.Actor
			}
			if event.RepositoryID == "" {
				event.RepositoryID = worker.RepositoryID
			}
			if event.RepositoryName == "" {
				event.RepositoryName = worker.RepositoryName
			}
		}
	}
	if event.Type != "steer.status" || event.Actor == "" {
		return event
	}
	key := event.RepositoryID + "\x00" + event.Actor
	if event.State == "left" {
		delete(tracker.steerSessions, key)
		return event
	}
	previous, found := tracker.steerSessions[key]
	if event.Code == "" || event.Code == "ready" || event.Code == "busy" || event.Code == "idle" || event.Code == "consumed" || event.Code == "awaiting_claim" {
		switch {
		case !found:
			event.Phase = "session_joined"
		case previous.Session != event.Session:
			event.Phase = "session_replaced"
		case previous.Role != event.Role:
			event.Phase = "role_changed"
		}
	}
	tracker.steerSessions[key] = steerWatchIdentity{Role: event.Role, Session: event.Session}
	return event
}

func (tracker *consoleEventTracker) accept(event daemon.Event) bool {
	if event.Type == "status.resync" {
		if event.Worker != "" && event.State != "" {
			tracker.states[event.Worker] = event.State
		}
		if event.RepositoryID != "" && event.State != "" {
			tracker.repositories[event.RepositoryID] = event.State
		}
		return false
	}
	if event.Type == "worker.state" && event.Worker != "" && event.State != "" {
		if tracker.states[event.Worker] == event.State {
			return false
		}
		tracker.states[event.Worker] = event.State
	}
	message := consoleEventMessage(event)
	key := strings.Join([]string{event.Type, event.Worker, event.Ticket, event.State, event.Phase, event.Code, event.RepositoryID, event.RepositoryKey, event.Actor, event.Session, message}, "\x00")
	if key == tracker.lastEvent && !consoleEventMustRemainVisible(event) {
		return false
	}
	tracker.lastEvent = key
	tracker.lastEventWorker = event.Worker
	tracker.lastEventMessage = message
	return true
}

func consoleEventMustRemainVisible(event daemon.Event) bool {
	if event.Type == "worker.failure" {
		return true
	}
	return false
}

func consoleEventMessage(event daemon.Event) string {
	switch event.Type {
	case "worker.state":
		switch event.State {
		case "running":
			return "worker ready"
		case "starting":
			return "worker starting"
		case "stopped":
			return "worker stopped"
		case "paused":
			return "worker paused"
		case "failed":
			return "worker failed"
		default:
			return "worker state changed"
		}
	case "worker.failure":
		return "worker failed"
	case "ticket.claim":
		return watchTicketMessage("claimed", event)
	case "ticket.lifecycle":
		if event.State == "review" {
			return watchTicketMessage("entered review", event)
		}
		switch event.State {
		case "closed":
			return watchTicketMessage("closed", event)
		case "open":
			return watchTicketMessage("reopened", event)
		case "approved":
			return watchTicketMessage("approved", event)
		case "returned":
			return watchTicketMessage("returned for changes", event)
		default:
			return watchTicketMessage("Ticket updated", event)
		}
	case "ticket.repository_changed":
		message := "ticket"
		if ticket := consoleEventTicket(event); ticket != "" {
			message += " " + consoleField(ticket)
		}
		message += " modified"
		details := make([]string, 0, 2)
		if event.State != "" {
			details = append(details, consoleField(event.State))
		}
		// Ticket watch attribution can name the previous assignee for a state
		// transition. Only a claimed event identifies the current claimant.
		if event.Code == "claimed" && event.Actor != "" {
			details = append(details, consoleField(event.Actor))
		}
		if len(details) != 0 {
			message += " (" + strings.Join(details, ", ") + ")"
		}
		return message
	case "ticket.repository_observer":
		switch event.Code {
		case "observer_recovered":
			return "repository watch recovered"
		case "observer_restarted":
			return "repository watch restarted"
		}
		if event.State == "degraded" {
			return "repository watch needs attention"
		}
		return ""
	case "daemon.started":
		return "daemon started"
	case "daemon.stopping":
		return "daemon stopping"
	case "config.reloaded":
		return "configuration reloaded"
	case "config.reload_failed":
		return "configuration reload failed"
	case "doctor.completed":
		return "worker recovery completed"
	case "doctor.failed":
		return "worker recovery failed"
	case "steer.status":
		switch event.Phase {
		case "session_joined":
			if event.Code == "ready" {
				return "session joined; ready work available"
			}
			if event.Code == "busy" || event.Code == "consumed" {
				return "session joined; " + watchTicketMessage("claimed", event)
			}
			return "session joined"
		case "session_replaced":
			if event.Code == "busy" || event.Code == "consumed" {
				return "session replaced; " + watchTicketMessage("claimed", event)
			}
			if event.Code == "ready" {
				return "session replaced; ready work available"
			}
			return "session replaced"
		case "role_changed":
			if event.Code == "busy" || event.Code == "consumed" {
				return "role changed; " + watchTicketMessage("claimed", event)
			}
			if event.Code == "ready" {
				return "role changed; ready work available"
			}
			return "role changed"
		}
		if event.Code != "" && event.Code != "ready" && event.Code != "busy" && event.Code != "idle" && event.Code != "consumed" && event.Code != "awaiting_claim" {
			return humanDeliveryCode(event.Code)
		}
		if event.State == "left" {
			return "session left"
		}
		if event.State == "conflict" {
			return "managed worker owns this actor"
		}
		if event.State == "none" && event.Code == "ready" {
			return "ready work available"
		}
		if event.State == "none" && event.Code == "busy" {
			return watchTicketMessage("claimed", event)
		}
		switch event.State {
		case "none":
			return "session joined; no work available"
		case "sending":
			return watchNotificationMessage("sending", event)
		case "queued":
			return ""
		case "consumed":
			return watchTicketMessage("claimed", event)
		case "degraded":
			return "session needs attention"
		default:
			return ""
		}
	case "steer.delivery":
		switch event.State {
		case "sending":
			return watchNotificationMessage("sending", event)
		case "queued":
			return watchNotificationMessage("queued", event)
		}
	default:
		return ""
	}
	return ""
}

func watchTicketMessage(action string, event daemon.Event) string {
	if ticket := consoleEventTicket(event); ticket != "" {
		return action + " " + consoleField(ticket)
	}
	return action
}

func watchNotificationMessage(state string, event daemon.Event) string {
	if ticket := consoleEventTicket(event); ticket != "" {
		if state == "queued" {
			return "queued notification for " + consoleField(ticket)
		}
		return "sending notification for " + consoleField(ticket)
	}
	if state == "queued" {
		return "notification queued"
	}
	return "notification sending"
}

func consoleWatchEvent(event daemon.Event) bool {
	switch event.Type {
	case "worker.state", "worker.failure", "ticket.claim", "ticket.lifecycle", "ticket.repository_changed", "ticket.repository_observer", "daemon.started", "daemon.stopping", "config.reloaded", "config.reload_failed", "doctor.completed", "doctor.failed", "steer.status", "steer.delivery":
		return true
	default:
		return false
	}
}
