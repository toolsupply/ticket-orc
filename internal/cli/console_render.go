package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/columnrender"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

const (
	consoleWorkerColumn       = 24
	consoleActivityColumn     = 96
	consoleTicketColumn       = 20
	consoleStatusNameCol      = 20
	consoleRepositoryLabelCol = 16
	consoleStatusRoleCol      = 12
	consoleStatusSession      = 11
	consoleStatusState        = 14
	consoleRepositoryStateCol = 16
	consoleWatchRepositoryCol = consoleRepositoryLabelCol
)

func consoleTableRow(worker, activity, ticket string) string {
	return renderColumnRow([]string{worker, activity, ticket}, []int{consoleWorkerColumn, consoleActivityColumn, consoleTicketColumn})
}

func consoleEllipsize(value string, width int) string {
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	if width <= 3 {
		return string(runes[:width])
	}
	return string(runes[:width-3]) + "..."
}

func renderConsoleVerboseStatus(out io.Writer, status daemon.Status) {
	fmt.Fprintln(out)
	writeConsoleDaemonMode(out, status.Mode)
	renderConsoleRepositorySummary(out, status.Repositories)
	if len(status.Repositories) == 0 {
		fmt.Fprintln(out, "Repositories: none")
		fmt.Fprintln(out)
	}
	if len(status.Steer) == 0 {
		fmt.Fprintln(out, "Sessions: none")
		fmt.Fprintln(out)
	} else {
		renderConsoleSteerStatus(out, status.Steer, status.Repositories, true)
	}
	renderConsoleManagedWorkers(out, status.Workers)
}

func renderConsoleSteerStatus(out io.Writer, sessions []daemon.SteerStatus, repositories []daemon.RepositoryStatus, verbose bool) {
	if len(sessions) == 0 {
		return
	}
	sessions = append([]daemon.SteerStatus(nil), sessions...)
	sort.SliceStable(sessions, func(i, j int) bool {
		if sessions[i].RepositoryName != sessions[j].RepositoryName {
			return sessions[i].RepositoryName < sessions[j].RepositoryName
		}
		if sessions[i].Role != sessions[j].Role {
			return sessions[i].Role < sessions[j].Role
		}
		return sessions[i].Actor < sessions[j].Actor
	})
	if verbose {
		fmt.Fprintln(out, "Sessions:")
		fmt.Fprintln(out)
		for _, session := range sessions {
			fmt.Fprintf(out, "Repository: %s\n", consoleField(sessionRepositoryLabel(session, repositories)))
			fmt.Fprintf(out, "  Codex session: %s\n", consoleField(shortIdentity(session.Session)))
			fmt.Fprintf(out, "Role: %s\n", consoleField(session.Role))
			fmt.Fprintf(out, "Ticket actor: %s\n", consoleField(session.Actor))
			fmt.Fprintf(out, "State: %s\n", consoleSteerState(session.State))
			fmt.Fprintf(out, "Activity: %s\n", consoleSteerActivity(session.State, session.Code))
			if session.ManagedOwner != "" {
				fmt.Fprintf(out, "  Conflict: actor %s is owned by managed worker %s\n", consoleField(session.Actor), consoleField(session.ManagedOwner))
			}
			if session.Code != "" {
				fmt.Fprintf(out, "  Detail: %s\n", consoleField(humanDeliveryCode(session.Code)))
			}
			fmt.Fprintln(out)
		}
		return
	}
	fmt.Fprintln(out, "Sessions:")
	fmt.Fprintln(out)
	fmt.Fprintln(out, renderColumnRow([]string{"Repository", "Session", "Role", "Ticket actor", "State", "Activity"}, []int{consoleRepositoryLabelCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusState, 0}))
	for _, session := range sessions {
		fmt.Fprintln(out, renderColumnRow([]string{sessionRepositoryLabel(session, repositories), shortIdentity(session.Session), session.Role, session.Actor, consoleSteerState(session.State), consoleSteerActivity(session.State, session.Code)}, []int{consoleRepositoryLabelCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusState, 0}))
	}
	fmt.Fprintln(out)
}

func renderConsoleWorkers(out io.Writer, status daemon.Status) {
	fmt.Fprintln(out)
	sessions := append([]daemon.SteerStatus(nil), status.Steer...)
	sort.SliceStable(sessions, func(i, j int) bool {
		if sessions[i].RepositoryName != sessions[j].RepositoryName {
			return sessions[i].RepositoryName < sessions[j].RepositoryName
		}
		if sessions[i].Role != sessions[j].Role {
			return sessions[i].Role < sessions[j].Role
		}
		return sessions[i].Actor < sessions[j].Actor
	})
	fmt.Fprintln(out, renderColumnRow([]string{"Repository", "Session", "Role", "Ticket actor", "State", "Activity"}, []int{consoleRepositoryLabelCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusState, 0}))
	for _, session := range sessions {
		fmt.Fprintln(out, renderColumnRow([]string{sessionRepositoryLabel(session, status.Repositories), shortIdentity(session.Session), session.Role, session.Actor, consoleSteerState(session.State), consoleSteerActivity(session.State, session.Code)}, []int{consoleRepositoryLabelCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusState, 0}))
	}
	fmt.Fprintln(out)
}

func sessionRepositoryLabel(session daemon.SteerStatus, repositories []daemon.RepositoryStatus) string {
	for _, repository := range repositories {
		if repository.ID == session.RepositoryID {
			return repositoryDisplayLabel(repository.Name, repository.Path, repository.ID)
		}
	}
	return repositoryDisplayLabel(session.RepositoryName, "", session.RepositoryID)
}

func consoleSteerActivity(state, code string) string {
	switch code {
	case "busy", "consumed":
		return "active Ticket claim"
	case "ready":
		return "actionable work"
	case "awaiting_claim":
		return "waiting for Ticket claim"
	case "queue_uncertain":
		return "delivery uncertain"
	case "queue_rejected":
		return "notification rejected"
	case "runtime_state_unavailable", "runtime_state_write_failed", "ticket_observation_failed", "ticket_client_failed":
		return humanDeliveryCode(code)
	}
	switch state {
	case "none", "idle":
		return "no work"
	case "sending":
		return "sending notification"
	case "queued":
		return "notification queued"
	case "consumed":
		return "active Ticket claim"
	case "conflict":
		return "actor owned by managed worker"
	case "degraded":
		return "delivery needs attention"
	default:
		return "checking Ticket"
	}
}

func renderConsoleManagedWorkers(out io.Writer, workers []daemon.WorkerStatus) {
	if len(workers) == 0 {
		return
	}
	workers = append([]daemon.WorkerStatus(nil), workers...)
	sort.SliceStable(workers, func(i, j int) bool { return workers[i].Name < workers[j].Name })
	fmt.Fprintln(out, "Managed workers:")
	fmt.Fprintln(out)
	fmt.Fprintln(out, renderColumnRow([]string{"Worker", "Repository", "Role", "Ticket actor", "Session", "State", "Activity"}, []int{consoleStatusNameCol, consoleStatusNameCol, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusSession, consoleStatusState, 0}))
	for _, worker := range workers {
		fmt.Fprintln(out, renderColumnRow([]string{worker.Name, consoleWorkerRepository(worker), worker.Role, worker.TicketActor, "—", consoleWorkerState(worker), consoleWorkerStatusActivity(worker)}, []int{consoleStatusNameCol, consoleStatusNameCol, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusSession, consoleStatusState, 0}))
	}
	fmt.Fprintln(out)
}

func consoleSteerState(state string) string {
	switch state {
	case "none", "idle":
		return "idle"
	case "checking":
		return "checking"
	case "sending":
		return "notifying"
	case "queued":
		return "notified"
	case "consumed":
		return "working"
	case "conflict":
		return "conflict"
	case "degraded":
		return "degraded"
	default:
		return "needs attention"
	}
}

func renderConsoleRepositories(out io.Writer, repositories []daemon.RepositoryStatus) {
	if len(repositories) == 0 {
		return
	}
	repositories = append([]daemon.RepositoryStatus(nil), repositories...)
	sort.SliceStable(repositories, func(i, j int) bool { return repositories[i].Key < repositories[j].Key })
	fmt.Fprintln(out, "Repositories:")
	fmt.Fprintln(out)
	for _, repository := range repositories {
		fmt.Fprintf(out, "Repository: %s\n", consoleField(repository.Key))
		if repository.ID != "" {
			fmt.Fprintf(out, "  Repository ID: %s\n", consoleField(repository.ID))
		}
		if repository.Name != "" {
			fmt.Fprintf(out, "  Name: %s\n", consoleField(repository.Name))
		}
		if repository.Path != "" {
			fmt.Fprintf(out, "  Path: %s\n", consoleField(repository.Path))
		}
		fmt.Fprintf(out, "  State: %s\n", consoleField(repository.State))
		if repository.Failure != "" {
			fmt.Fprintf(out, "  Failure: %s\n", consoleField(repository.Failure))
		}
		if !repository.LastEventAt.IsZero() {
			fmt.Fprintf(out, "  Last change: %s\n", repository.LastEventAt.Local().Format(time.RFC3339))
		}
		if !repository.LastRestartAt.IsZero() {
			fmt.Fprintf(out, "  Last restart: %s\n", repository.LastRestartAt.Local().Format(time.RFC3339))
		}
		if repository.RestartCount != 0 {
			fmt.Fprintf(out, "  Restart count: %d\n", repository.RestartCount)
		}
		fmt.Fprintln(out)
	}
}

func renderConsoleRepositorySummary(out io.Writer, repositories []daemon.RepositoryStatus) {
	if len(repositories) == 0 {
		return
	}
	repositories = append([]daemon.RepositoryStatus(nil), repositories...)
	sort.SliceStable(repositories, func(i, j int) bool { return repositories[i].Key < repositories[j].Key })
	fmt.Fprintln(out, "Repositories:")
	fmt.Fprintln(out)
	widths := []int{consoleRepositoryLabelCol, consoleRepositoryStateCol, 0}
	fmt.Fprintln(out, renderColumnRow([]string{"Repository", "State", "Repository path"}, widths))
	for _, repository := range repositories {
		fmt.Fprintln(out, renderColumnRow([]string{repositoryDisplayLabel(repository.Name, repository.Path, repository.ID), consoleRepositoryState(repository.State), repository.Path}, widths))
	}
	fmt.Fprintln(out)
}

func consoleRepositoryState(state string) string {
	switch state {
	case "healthy":
		return "ready"
	case "degraded":
		return "needs attention"
	case "stopped":
		return "stopped"
	default:
		return consoleField(state)
	}
}

func renderConsoleStatus(out io.Writer, status daemon.Status) {
	renderConsoleStatusSnapshot(out, status)
}

func renderConsoleStatusSnapshot(out io.Writer, status daemon.Status) {
	fmt.Fprintln(out)
	writeConsoleDaemonMode(out, status.Mode)
	renderConsoleRepositorySummary(out, status.Repositories)
	if len(status.Repositories) == 0 {
		fmt.Fprintln(out, "Repositories: none")
		fmt.Fprintln(out)
	}
	if len(status.Steer) == 0 {
		fmt.Fprintln(out, "Sessions: none")
		fmt.Fprintln(out)
	} else {
		renderConsoleSteerStatus(out, status.Steer, status.Repositories, false)
	}
	renderConsoleManagedWorkers(out, status.Workers)
}

func renderColumnRow(values []string, widths []int) string {
	var output bytes.Buffer
	columnrender.Render(&output, [][]string{values}, widths, " ")
	return strings.TrimSuffix(output.String(), "\n")
}

func renderConsoleDetailStatus(out io.Writer, status daemon.Status) {
	fmt.Fprintln(out)
	writeConsoleDaemonMode(out, status.Mode)
	if len(status.Repositories) != 0 {
		renderConsoleRepositories(out, status.Repositories)
	}
	workers := append([]daemon.WorkerStatus(nil), status.Workers...)
	sort.SliceStable(workers, func(i, j int) bool { return workers[i].Name < workers[j].Name })
	for i, worker := range workers {
		if i > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintf(out, "Worker: %s\n", consoleField(worker.Name))
		fmt.Fprintf(out, "  State: %s\n", consoleField(worker.State))
		fmt.Fprintf(out, "  Activity/state: %s\n", consoleWorkerActivity(worker))
		if worker.Role != "" {
			fmt.Fprintf(out, "  Role: %s\n", consoleField(worker.Role))
		}
		if worker.TicketActor != "" {
			fmt.Fprintf(out, "  Ticket actor: %s\n", consoleField(worker.TicketActor))
		}
		if worker.Harness != "" {
			fmt.Fprintf(out, "  Harness: %s\n", consoleField(worker.Harness))
		}
		if worker.TicketFrontierState != "" {
			fmt.Fprintf(out, "  Ticket work: %s\n", consoleField(worker.TicketFrontierState))
		}
		if repository := consoleWorkerRepository(worker); repository != "" {
			fmt.Fprintf(out, "  Ticket repository: %s\n", repository)
		}
		if worker.RepositoryID != "" {
			fmt.Fprintf(out, "  Ticket repository ID: %s\n", consoleField(worker.RepositoryID))
		}
		if !worker.TicketActivityAt.IsZero() {
			activity := consoleField(worker.TicketActivity)
			if worker.TicketActivityTicket != "" {
				activity += " (ticket " + consoleField(worker.TicketActivityTicket) + ")"
			}
			fmt.Fprintf(out, "  Ticket activity: %s at %s\n", activity, worker.TicketActivityAt.Local().Format(time.RFC3339))
		}
		if worker.Reason != "" {
			fmt.Fprintf(out, "  Failure reason: %s\n", consoleField(worker.Reason))
		}
		if worker.Failure != nil {
			fmt.Fprintf(out, "  Failure: %s\n", consoleWorkerFailureDetail(worker.Failure))
		}
	}
	if len(status.Steer) != 0 {
		renderConsoleSteerStatus(out, status.Steer, status.Repositories, true)
	}
	fmt.Fprintln(out)
}

func writeConsoleDaemonMode(out io.Writer, mode string) {
	if mode != "" {
		fmt.Fprintf(out, "Daemon mode: %s\n\n", consoleField(mode))
	}
}

func consoleWorkerFailureDetail(failure *daemon.WorkerFailure) string {
	if failure == nil {
		return ""
	}
	parts := []string{consoleField(failure.Classification), "during", consoleField(failure.Phase)}
	if failure.ExitCode != 0 {
		parts = append(parts, fmt.Sprintf("exit_code=%d", failure.ExitCode))
	}
	if failure.Signal != "" {
		parts = append(parts, "signal="+consoleField(failure.Signal))
	}
	if failure.TicketCode != "" {
		parts = append(parts, "Ticket="+consoleField(failure.TicketCode))
	}
	if failure.TransportCategory != "" {
		parts = append(parts, "transport="+consoleField(failure.TransportCategory))
	}
	if failure.Origin != "" {
		parts = append(parts, "origin="+consoleField(failure.Origin))
	}
	if failure.Operation != "" {
		parts = append(parts, "operation="+consoleField(failure.Operation))
	}
	if failure.NestedExitCode != 0 {
		parts = append(parts, fmt.Sprintf("nested_exit_code=%d", failure.NestedExitCode))
	}
	if failure.Ticket != "" {
		parts = append(parts, "ticket="+consoleField(failure.Ticket))
	}
	if failure.Remediation != "" {
		parts = append(parts, "remediation="+consoleField(failure.Remediation))
	}
	if failure.Contained {
		parts = append(parts, "contained")
	}
	return strings.Join(parts, " ")
}

func consoleWorkerRepository(worker daemon.WorkerStatus) string {
	if worker.RepositoryName == "" && worker.RepositoryPath == "" && worker.RepositoryID == "" {
		return "—"
	}
	return repositoryDisplayLabel(worker.RepositoryName, worker.RepositoryPath, worker.RepositoryID)
}

func repositoryDisplayLabel(name, path, id string) string {
	if strings.TrimSpace(name) != "" {
		return consoleField(name)
	}
	if strings.TrimSpace(path) != "" {
		base := filepath.Base(filepath.Clean(path))
		if base != "." && base != string(filepath.Separator) && base != "" {
			return consoleField(base)
		}
	}
	return consoleField(shortIdentity(id))
}

func consoleWorkerActivity(worker daemon.WorkerStatus) string {
	activity := consoleWorkerSummary(worker)
	if worker.TicketActivity != "" {
		activity += "; " + consoleField(worker.TicketActivity)
		if worker.TicketActivityTicket != "" {
			activity += " " + consoleField(worker.TicketActivityTicket)
		}
	}
	return activity
}

func consoleWorkerState(worker daemon.WorkerStatus) string {
	switch worker.State {
	case "failed", "paused", "stopped", "running", "conflict":
		return worker.State
	case "starting":
		return "waking up"
	case "":
		return "unknown"
	default:
		return "needs attention"
	}
}

func consoleWorkerStatusActivity(worker daemon.WorkerStatus) string {
	activity := ""
	if worker.TicketActivity != "" {
		activity = consoleField(worker.TicketActivity)
		if worker.TicketActivityTicket != "" {
			activity += " " + consoleField(worker.TicketActivityTicket)
		}
	} else if worker.TicketActivityTicket != "" {
		activity = "Ticket " + consoleField(worker.TicketActivityTicket)
	} else if worker.TicketFrontierState != "" {
		activity = consoleWorkerSummary(worker)
	} else if worker.Reason != "" {
		activity = consoleField(worker.Reason)
	} else {
		activity = "no recent Ticket activity"
	}
	return activity
}

func consoleSummaryName(name string) string {
	name = consoleField(name)
	if len(name) > 24 {
		return name[:21] + "..."
	}
	return name
}

func consoleWorkerSummary(worker daemon.WorkerStatus) string {
	if worker.State == "failed" {
		return "failed"
	}
	if worker.State == "paused" {
		return "paused"
	}
	switch worker.TicketFrontierState {
	case "actionable":
		return "actionable work"
	case "quiescent":
		return "no actionable work"
	case "busy":
		return "active work"
	}
	if worker.State == "stopped" {
		return "stopped"
	}
	if worker.State == "starting" {
		return "starting"
	}
	if worker.State != "" {
		return consoleField(worker.State)
	}
	return "unknown"
}

func consoleField(value string) string {
	return terminaltext.Sanitize(value, false)
}

func renderConsoleMutation(out io.Writer, result daemon.MutationResult, operation string) {
	worker := consoleSummaryName(result.Worker)
	if result.Applied {
		fmt.Fprintf(out, "worker %s %s\n", worker, consoleMutationVerb(operation))
		return
	}
	if result.State != "" {
		fmt.Fprintf(out, "worker %s remains %s\n", worker, consoleField(result.State))
		return
	}
	fmt.Fprintf(out, "worker %s request was not applied\n", worker)
}

func renderConsoleDoctor(out io.Writer, result daemon.DoctorResult) {
	if result.Reloaded {
		fmt.Fprintln(out, "doctor: configuration reloaded")
	} else {
		fmt.Fprintln(out, "doctor: configuration validated")
	}
	for _, worker := range result.Workers {
		name := consoleField(worker.Worker)
		description := consoleField(worker.Outcome)
		if action := consoleDoctorAction(worker.Action); action != "" {
			description = action
		}
		if worker.Outcome == "failed" {
			description = "failed"
			if worker.Reason != "" {
				description += " — " + consoleField(worker.Reason)
			} else if worker.Failure != nil {
				description += " — " + consoleWorkerFailureDetail(worker.Failure)
			}
		}
		fmt.Fprintf(out, "%s: %s\n", name, description)
	}
}

func consoleDoctorAction(action string) string {
	action = strings.TrimSpace(consoleField(action))
	action = strings.TrimSpace(strings.TrimPrefix(action, ";"))
	action = strings.ReplaceAll(action, "; start", "; started")
	if action == "start" {
		action = "started"
	}
	return action
}

func renderConsoleGroup(out io.Writer, result daemon.GroupResult, operation string) {
	verb := "requested"
	if groupApplied(result) {
		verb = consoleMutationVerb(operation)
	}
	fmt.Fprintf(out, "group %s %s\n", consoleSummaryName(result.Group), verb)
	for _, item := range result.Results {
		renderConsoleMutation(out, item, operation)
	}
}

func consoleMutationVerb(operation string) string {
	switch operation {
	case "start":
		return "started"
	case "stop":
		return "stopped"
	case "restart":
		return "restarted"
	case "pause":
		return "paused"
	case "resume":
		return "resumed"
	default:
		return "updated"
	}
}

func writeConsoleError(out io.Writer, err error) {
	fmt.Fprintf(out, "error: %s\n", safeConsoleError(err))
}

func safeConsoleError(err error) string {
	if err == nil {
		return ""
	}
	var clientErr *daemonclient.Error
	if errors.As(err, &clientErr) {
		return clientErr.Error()
	}
	return "daemon request failed"
}
