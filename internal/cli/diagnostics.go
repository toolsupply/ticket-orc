package cli

import (
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type DiagnosticSeverity string

const (
	DiagnosticError   DiagnosticSeverity = "error"
	DiagnosticWarning DiagnosticSeverity = "warning"
)

// ConfigDiagnostic is a safe, deterministic configuration finding. Values
// supplied by operators (prompts, actors, and queue targets) are deliberately
// omitted from the message.
type ConfigDiagnostic struct {
	Severity    DiagnosticSeverity `json:"severity"`
	Code        string             `json:"code"`
	Path        string             `json:"path"`
	Worker      string             `json:"worker,omitempty"`
	Message     string             `json:"message"`
	Remediation string             `json:"remediation"`
}

type ConfigDiagnostics []ConfigDiagnostic

func (d ConfigDiagnostics) HasErrors() bool {
	for _, item := range d {
		if item.Severity == DiagnosticError {
			return true
		}
	}
	return false
}

func (d ConfigDiagnostics) Error() string {
	if len(d) == 0 {
		return "configuration is invalid"
	}
	ordered := append(ConfigDiagnostics(nil), d...)
	sortDiagnostics(ordered)
	parts := make([]string, 0, len(ordered))
	for _, item := range ordered {
		parts = append(parts, item.String())
	}
	return strings.Join(parts, "; ")
}

func (d ConfigDiagnostic) String() string {
	path := d.Path
	if path == "" {
		path = "config"
	}
	return fmt.Sprintf("%s [%s] %s: %s (%s)", d.Severity, d.Code, path, d.Message, d.Remediation)
}

func sortDiagnostics(d ConfigDiagnostics) {
	sort.SliceStable(d, func(i, j int) bool {
		if d[i].Severity != d[j].Severity {
			return d[i].Severity == DiagnosticError
		}
		if d[i].Path != d[j].Path {
			return d[i].Path < d[j].Path
		}
		return d[i].Code < d[j].Code
	})
}

func renderConfigDiagnostics(out io.Writer, diagnostics ConfigDiagnostics) {
	ordered := append(ConfigDiagnostics(nil), diagnostics...)
	sortDiagnostics(ordered)
	for _, item := range ordered {
		_, _ = fmt.Fprintf(out, "config %s code=%s path=%s message=%s remediation=%s\n", item.Severity, item.Code, item.Path, item.Message, item.Remediation)
	}
}

// renderRuntimeConfigWarnings emits the short, human-facing form used by a
// supervisor run. The structured fields remain available from config check
// and control API responses; they do not belong in ordinary service logs.
func renderRuntimeConfigWarnings(out io.Writer, diagnostics ConfigDiagnostics) {
	ordered := append(ConfigDiagnostics(nil), diagnostics...)
	sortDiagnostics(ordered)
	for _, item := range ordered {
		if item.Severity != DiagnosticWarning {
			continue
		}
		_, _ = fmt.Fprintf(out, "[ticket-orc] config warning: %s\n", runtimeConfigWarningMessage(item.Code, item.Worker, item.Message, item.Remediation))
	}
}

func runtimeConfigWarningMessage(code, worker, message, remediation string) string {
	worker = sanitizeServiceToken(worker)
	switch code {
	case "reload.restart_required":
		if worker != "" {
			if remediation != "" {
				return fmt.Sprintf("worker %s configuration requires restart; %s", worker, sanitizeRuntimeWarningMessage(remediation))
			}
			return fmt.Sprintf("worker %s configuration requires restart", worker)
		}
	}
	return sanitizeRuntimeWarningMessage(message)
}

func sanitizeRuntimeWarningMessage(message string) string {
	message = strings.Join(strings.Fields(message), " ")
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

// AnalyzeRunWorkers checks for duplicate Ticket actors before selected workers start.
func AnalyzeRunWorkers(workers map[string]supervisor.RunWorker, selected map[string]bool) ConfigDiagnostics {
	return analyzeRunWorkers(workers, selected)
}

// AnalyzeConfiguredWorkers runs intrinsic lints for every configured worker
// without inventing a run-selection warning. Collision findings remain
// warnings because no worker has been selected for launch yet.
func AnalyzeConfiguredWorkers(workers map[string]supervisor.RunWorker) ConfigDiagnostics {
	return analyzeRunWorkers(workers, nil)
}

func analyzeRunWorkers(workers map[string]supervisor.RunWorker, selected map[string]bool) ConfigDiagnostics {
	names := make([]string, 0, len(workers))
	for name := range workers {
		names = append(names, name)
	}
	sort.Strings(names)
	var diagnostics ConfigDiagnostics
	for i, leftName := range names {
		left := workers[leftName]
		for _, rightName := range names[i+1:] {
			right := workers[rightName]
			bothSelected := selected[leftName] && selected[rightName]
			severity := DiagnosticWarning
			if bothSelected {
				severity = DiagnosticError
			}
			if left.Config.Actor != "" && left.Config.Actor == right.Config.Actor && ticketRepositoriesConflict(workerRepositoryIdentity(left), workerRepositoryIdentity(right)) {
				code := "duplicate_ticket_actor"
				message := "workers share a Ticket actor identity and cannot safely run together"
				if !bothSelected && severity != DiagnosticError {
					message = "workers share a Ticket actor identity; selecting them together is unsafe"
				}
				if bothSelected && unresolvedTicketTarget(left, right) {
					// Scoped targets do not reveal their repository identity until
					// Ticket preflight. Keep the diagnostic visible, but defer its
					// fatal decision until the resolved paths can be compared.
					severity = DiagnosticWarning
					message = "workers share a Ticket actor identity; repository identity will be checked after Ticket target preflight"
				}
				diagnostics = append(diagnostics, ConfigDiagnostic{Severity: severity, Code: code, Path: "workers." + leftName + ".actor", Worker: leftName, Message: message, Remediation: "assign distinct actor identities"})
			}
		}
	}
	return diagnostics
}

func workerRepositoryIdentity(worker supervisor.RunWorker) string {
	if worker.TicketInfo != nil && ticketclient.ValidRepositoryID(worker.TicketInfo.ID) {
		return worker.TicketInfo.ID
	}
	if ticketclient.ValidRepositoryID(worker.Config.RepositoryIdentity) {
		return worker.Config.RepositoryIdentity
	}
	return worker.Config.Repository
}

func unresolvedTicketTarget(left, right supervisor.RunWorker) bool {
	return (ticketTarget(left.Config).Config != "" && left.TicketInfo == nil) || (ticketTarget(right.Config).Config != "" && right.TicketInfo == nil)
}

// ticketRepositoriesConflict is conservative when either worker leaves
// repository discovery to Ticket. Explicit distinct repositories isolate actor
// ownership; an implicit identity cannot be safely compared ahead of launch.
func ticketRepositoriesConflict(left, right string) bool {
	if left == "" || right == "" {
		return true
	}
	leftID, rightID := ticketclient.ValidRepositoryID(left), ticketclient.ValidRepositoryID(right)
	if leftID || rightID {
		return !leftID || !rightID || left == right
	}
	return filepath.Clean(left) == filepath.Clean(right)
}
