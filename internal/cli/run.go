package cli

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

var (
	runChildStopTimeout = 5 * time.Second
	// Worker readiness includes Ticket target and harness preflight. Keep this bounded for
	// genuine hangs while allowing real Ticket/Codex round trips; tests may
	// override the package variable with a short duration.
	workerStartupVerificationTime = 30 * time.Second
)

const (
	workerLeaseConflictExitCode = 74
)

const (
	WorkerStarting = supervisor.WorkerStarting
	WorkerRunning  = supervisor.WorkerRunning
	WorkerStopped  = supervisor.WorkerStopped
	WorkerPaused   = supervisor.WorkerPaused
	WorkerFailed   = supervisor.WorkerFailed
	WorkerConflict = supervisor.WorkerConflict
)

var ErrWorkerLeaseConflict = errors.New("worker lease conflict")

func renderRunBanner(out io.Writer, configPath, listener string) {
	fmt.Fprintf(out, "[ticket-orc] loaded %s\n", sanitizeServiceToken(configPath))
	endpoint := "unavailable"
	if parsed, err := url.Parse(listener); err == nil && parsed.Host != "" {
		endpoint = "orc://" + sanitizeServiceToken(parsed.Host)
	}
	fmt.Fprintf(out, "[ticket-orc] endpoint: %s\n", endpoint)
}

func renderServiceReady(out io.Writer, configPath, listener string, status daemon.Status) {
	renderRunBanner(out, configPath, listener)
	fmt.Fprintln(out)
	renderConsoleRepositorySummary(out, status.Repositories)
	if len(status.Repositories) == 0 {
		fmt.Fprintln(out, "Repositories: none")
		fmt.Fprintln(out)
	}
	if len(status.Steer) != 0 {
		renderConsoleSteerStatus(out, status.Steer, status.Repositories, false)
	}
	renderConsoleManagedWorkers(out, status.Workers)
}

func runtimeDaemonStatus(runtime *supervisor.RuntimeState[supervisor.RunWorker], sessions []daemon.SteerStatus) daemon.Status {
	if runtime == nil {
		return daemon.Status{Workers: []daemon.WorkerStatus{}, Repositories: []daemon.RepositoryStatus{}, Steer: sessions}
	}
	snapshot := runtime.Snapshot()
	configured := runtime.EffectiveWorkers()
	repositories := runtime.RepositoryStatuses()
	status := daemon.Status{ConfigRevision: runtime.Revision(), Workers: make([]daemon.WorkerStatus, 0, len(configured)), Repositories: make([]daemon.RepositoryStatus, 0, len(repositories)), Steer: sessions}
	for _, repository := range repositories {
		status.Repositories = append(status.Repositories, daemon.RepositoryStatusDTO(repository))
	}
	for _, worker := range configured {
		transition := snapshot[worker.Name]
		observation := runtime.Observed(worker.Name)
		item := daemon.WorkerStatus{
			Name: worker.Name, Groups: append([]string(nil), worker.Groups...),
			Role: string(worker.Config.Role), EffectiveRoleName: worker.Config.RoleName,
			EffectiveTicketQueue: worker.Config.TicketQueue, Harness: worker.Config.Harness, TicketActor: worker.Config.Actor,
			EffectiveTicketTags: splitTicketTags(worker.Config.TicketTags), EffectiveReviewSkipTags: splitReviewSkipTags(worker.Config.ReviewSkipTags),
			TicketActivityAt: observation.TicketAt, TicketActivity: observation.TicketEvent,
			TicketActivityTicket: observation.TicketID, TicketActivitySource: observation.TicketSource,
			State: string(transition.State), Reason: workerStatusReason(transition), Failure: daemon.WorkerFailureDTO(transition.Failure),
		}
		addWorkerRepositoryStatus(&item, worker)
		status.Workers = append(status.Workers, item)
	}
	return status
}

func runtimeDaemonStatusWithSteerPolicies(runtime *supervisor.RuntimeState[supervisor.RunWorker], sessions []daemon.SteerStatus, policies *steerPolicyStore) daemon.Status {
	projected := append([]daemon.SteerStatus(nil), sessions...)
	if policies != nil {
		policySnapshot := policies.Snapshot()
		for index := range projected {
			projected[index].EffectiveTicketQueue = ""
			projected[index].EffectiveTicketTags = nil
			projected[index].EffectiveReviewSkipTags = nil
			if policy, ok := policySnapshot.roles[projected[index].Role]; ok {
				applySteerStatusPolicy(&projected[index], policy)
			}
		}
	}
	return runtimeDaemonStatus(runtime, projected)
}

func sanitizeServiceToken(value string) string {
	var builder strings.Builder
	for _, char := range value {
		if char < 0x20 || char == 0x7f || unicode.IsSpace(char) {
			builder.WriteByte('_')
			continue
		}
		builder.WriteRune(char)
	}
	return builder.String()
}

func renderServiceFailure(out io.Writer, worker, phase string) {
	_, _ = fmt.Fprintf(out, "[ticket-orc] startup failed worker=%s phase=%s\n", worker, phase)
}

func renderWorkerFailure(out io.Writer, worker string, state supervisor.WorkerState, failure *supervisor.WorkerFailure) {
	if failure == nil {
		return
	}
	if failure.Classification == "lease_conflict" {
		_, _ = fmt.Fprintf(out, "[ticket-orc] worker=%s containment conflict; another worker process owns its lease; reconcile the surviving worker before retrying", worker)
	} else {
		_, _ = fmt.Fprintf(out, "[ticket-orc] worker=%s unexpected_exit state=%s", worker, state)
	}
	_, _ = fmt.Fprintf(out, " classification=%s phase=%s", failure.Classification, failure.Phase)
	if failure.ExitCode != 0 {
		_, _ = fmt.Fprintf(out, " exit_code=%d", failure.ExitCode)
	}
	if failure.Signal != "" {
		_, _ = fmt.Fprintf(out, " signal=%s", failure.Signal)
	}
	if failure.TicketCode != "" {
		_, _ = fmt.Fprintf(out, " ticket_code=%s", failure.TicketCode)
	}
	if failure.TransportCategory != "" {
		_, _ = fmt.Fprintf(out, " transport_category=%s", failure.TransportCategory)
	}
	if failure.Origin != "" {
		_, _ = fmt.Fprintf(out, " origin=%s", failure.Origin)
	}
	if failure.Operation != "" {
		_, _ = fmt.Fprintf(out, " operation=%s", failure.Operation)
	}
	if failure.NestedExitCode != 0 {
		_, _ = fmt.Fprintf(out, " nested_exit_code=%d", failure.NestedExitCode)
	}
	if failure.Ticket != "" {
		_, _ = fmt.Fprintf(out, " ticket=%s", failure.Ticket)
	}
	if failure.Remediation != "" {
		_, _ = fmt.Fprintf(out, " remediation=%s", failure.Remediation)
	}
	if failure.Contained {
		_, _ = io.WriteString(out, " contained=true")
	}
	_, _ = io.WriteString(out, "\n")
}

func renderTicketPreflightFailure(out io.Writer, worker string, err error) {
	renderServiceFailure(out, worker, "preflight")
	var probeErr *ticketclient.ProbeError
	if errors.As(err, &probeErr) && probeErr != nil {
		_, _ = fmt.Fprintf(out, "[ticket-orc] Ticket target preflight worker=%s diagnostic=%s\n", sanitizeServiceToken(worker), probeErr.Error())
	}
}
