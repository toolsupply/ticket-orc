package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

const remoteWorkerWatchError = "worker output watch is only available for the local daemon; use explicit watch orc for remote Orc activity"

// Human labels are bounded independently of table widths. They never replace
// repository IDs or stable worker names used for selection and log matching.
func watchLabel(value string) string {
	value = strings.TrimSpace(terminaltext.Sanitize(value, false))
	value = strings.NewReplacer("[", "(", "]", ")", ":", "∶").Replace(value)
	runes := []rune(value)
	if len(runes) > 128 {
		value = string(runes[:128]) + "…"
	}
	return value
}

func (renderer *consoleWatchRenderer) repositoryLabel(id, name, path string) string {
	if repository, ok := renderer.repositories[id]; ok {
		if strings.TrimSpace(repository.Name) != "" {
			name = repository.Name
		}
		path = repository.Path
	}
	name = watchLabel(name)
	path = terminaltext.Sanitize(path, false)
	if id == "" && name == "" && strings.TrimSpace(path) == "" {
		return "unknown repository"
	}
	label := watchLabel(repositoryDisplayLabel(name, path, id))
	if label == "" {
		return "unknown repository"
	}
	return label
}

func (renderer *consoleWatchRenderer) orcLabel(event daemon.Event) string {
	if event.RepositoryID == "" && event.RepositoryName == "" {
		return "orc"
	}
	return renderer.repositoryLabel(event.RepositoryID, event.RepositoryName, "") + ":orc"
}

func (renderer *consoleWatchRenderer) workerLabel(worker daemon.WorkerStatus) string {
	actor := watchLabel(worker.TicketActor)
	if actor == "" {
		actor = watchLabel(worker.Name)
	}
	return renderer.repositoryLabel(worker.RepositoryID, worker.RepositoryName, worker.RepositoryPath) + ":" + actor
}

// Every physical content line carries its local observation time and identity,
// including multiline provider output and viewer diagnostics.
func writeWatchLines(out io.Writer, at time.Time, label, message string) error {
	message = terminaltext.Sanitize(message, true)
	var rows strings.Builder
	for _, line := range strings.Split(strings.TrimSuffix(message, "\n"), "\n") {
		if line != "" {
			fmt.Fprintf(&rows, "%s [%s] %s\n", at.Local().Format("15:04:05"), label, line)
		}
	}
	return writeWatchOutput(out, rows.String())
}

func writeWatchOutput(out io.Writer, output string) error {
	if output == "" {
		return nil
	}
	n, err := io.WriteString(out, output)
	if err != nil {
		return fmt.Errorf("write watch output: %w", err)
	}
	if n != len(output) {
		return fmt.Errorf("write watch output: %w", io.ErrShortWrite)
	}
	return nil
}

func unifiedEventMessage(event daemon.Event) string {
	message := consoleEventMessage(event)
	if message == "" {
		return ""
	}
	if event.Type == "ticket.lifecycle" {
		switch event.State {
		case "signoff", "rejected":
			message = watchTicketMessage("entered "+event.State, event)
		default:
			if message == watchTicketMessage("Ticket updated", event) && event.State != "" {
				message = watchTicketMessage("entered "+watchLabel(event.State), event)
			}
		}
	}
	if event.Type == "steer.status" {
		switch event.Phase {
		case "session_joined", "session_replaced", "role_changed":
			if event.Failure == nil {
				action := "joined"
				switch event.Phase {
				case "session_replaced":
					action = "replaced"
				case "role_changed":
					action = "changed role"
				}
				activity := "no work available"
				switch {
				case event.Code == "busy" || event.Code == "consumed":
					activity = watchTicketMessage("claimed", event)
				case event.Code == "ready":
					activity = "ready work available"
				case event.State != "none" && event.State != "idle":
					activity = consoleSteerActivity(event.State, event.Code)
				}
				message = action + ", " + activity
			}
		default:
			if event.State == "left" {
				message = "left"
			}
		}
	}
	// The old table carried these fields in separate columns. Keep all
	// supplied identity in prose, including distinct worker and Ticket actor.
	var subject []string
	if event.Role != "" {
		subject = append(subject, watchLabel(event.Role))
	}
	if event.Worker != "" {
		subject = append(subject, "worker", watchLabel(event.Worker))
	}
	if event.Session != "" {
		subject = append(subject, "session", watchLabel(shortEventSession(event.Session)))
	}
	if event.Actor != "" {
		subject = append(subject, "(actor "+watchLabel(event.Actor)+")")
	}
	if len(subject) != 0 {
		// Avoid 'worker NAME worker ready' and 'session ID session left'.
		if event.Worker != "" {
			message = strings.TrimPrefix(message, "worker ")
		}
		if event.Session != "" {
			message = strings.TrimPrefix(message, "session ")
		}
		message = strings.Join(subject, " ") + " " + message
	}
	if ticket := consoleEventTicket(event); ticket != "" && !strings.Contains(message, consoleField(ticket)) {
		message += " for ticket " + consoleField(ticket)
	}
	if event.Failure != nil && event.Type != "steer.status" {
		message += "; " + consoleWorkerFailureDetail(event.Failure)
	}
	// Bound diagnostics without admitting raw subprocess errors or targets.
	return boundedWatchDiagnostic(message)
}

func boundedWatchDiagnostic(message string) string {
	message = terminaltext.Sanitize(message, false)
	runes := []rune(message)
	if len(runes) > 2048 {
		message = string(runes[:2048]) + "…"
	}
	return message
}
