package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestWorkerStatusUsesTicketRepositoryIdentityAndDiagnosticPath(t *testing.T) {
	repositoryName := "Ticket project"
	worker := supervisor.RunWorker{
		Name: "coder",
		Config: supervisor.RoleConfig{
			Repository: "/orc/config/path", RepositoryKey: "project", RepositoryIdentity: joinOtherRepositoryID,
		},
		TicketInfo: &ticketclient.RepositoryInfo{ID: joinTestRepositoryID, Path: "/ticket/reported/path", Name: &repositoryName},
	}
	var status daemon.WorkerStatus
	addWorkerRepositoryStatus(&status, worker)
	if status.RepositoryID != joinTestRepositoryID || status.RepositoryKey != "project" || status.RepositoryName != repositoryName || status.RepositoryPath != "/ticket/reported/path" {
		t.Fatalf("worker repository status=%#v", status)
	}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{`"repository_id":"` + joinTestRepositoryID + `"`, `"repository_key":"project"`, `"repository_name":"Ticket project"`, `"repository_path":"/ticket/reported/path"`} {
		if !strings.Contains(text, want) {
			t.Errorf("worker status missing %s: %s", want, text)
		}
	}
	if strings.Contains(text, `"repository":`) || strings.Contains(text, `"repository_path":"`+joinTestRepositoryID+`"`) {
		t.Fatalf("worker status used an ambiguous identity or UUID as path: %s", text)
	}
}

func TestRuntimeStatusUsesEffectiveRoleSelectors(t *testing.T) {
	effective := supervisor.RunWorker{Name: "worker", Config: supervisor.RoleConfig{
		Role: RoleCoder, RoleName: "backend", TicketQueue: "open", Actor: "coder",
		TicketTags: "effective-tag", ReviewSkipTags: "effective-exclusion",
	}}
	runtime := NewRuntimeState([]supervisor.RunWorker{effective})
	desired := effective
	desired.Config.TicketTags = "desired-tag"
	desired.Config.ReviewSkipTags = "desired-exclusion"
	runtime.SetConfiguredWorkers([]supervisor.RunWorker{desired})
	status := runtimeDaemonStatus(runtime, nil)
	if len(status.Workers) != 1 || status.Workers[0].Role != "backend" || status.Workers[0].EffectiveRoleName != "backend" || status.Workers[0].EffectiveTicketQueue != "open" || status.Workers[0].TicketActor != "coder" || !reflect.DeepEqual(status.Workers[0].EffectiveTicketTags, []string{"effective-tag"}) || !reflect.DeepEqual(status.Workers[0].EffectiveReviewSkipTags, []string{"effective-exclusion"}) {
		t.Fatalf("runtime status selectors = %#v; want the effective running configuration", status.Workers)
	}
}

func TestWorkerRuntimeEventsCarryRepositoryIdentity(t *testing.T) {
	event := supervisor.RuntimeEvent{Type: "worker.state", Worker: "coder"}
	workers := []supervisor.RunWorker{{
		Name:       "coder",
		Config:     supervisor.RoleConfig{RepositoryKey: "project", RepositoryIdentity: joinOtherRepositoryID},
		TicketInfo: &ticketclient.RepositoryInfo{ID: joinTestRepositoryID},
	}}

	addWorkerRepositoryEventContext(&event, workers)
	if event.RepositoryID != joinTestRepositoryID || event.RepositoryKey != "project" {
		t.Fatalf("worker event repository identity = (%q, %q), want (%q, %q)", event.RepositoryID, event.RepositoryKey, joinTestRepositoryID, "project")
	}
}

func TestRoleFailureRenderingUsesTypedWaitClassification(t *testing.T) {
	typed := &orc.WorkWaitError{Role: "coder", Cause: &ticketclient.CommandError{Code: "unavailable"}}
	var output bytes.Buffer
	renderRoleFailure(&output, typed)
	if got := output.String(); !strings.Contains(got, "error: wait for implementation work failed") {
		t.Fatalf("typed wait failure output = %q", got)
	}

	output.Reset()
	untyped := fmt.Errorf("wait for implementation work: %w", &ticketclient.CommandError{Code: "unavailable"})
	renderRoleFailure(&output, untyped)
	if got := output.String(); strings.Contains(got, "wait for implementation work failed") {
		t.Fatalf("rendered error text was incorrectly classified: %q", got)
	}
}

func TestRenderChildExitSurfacesSanitizedWrappedAndJoinedDiagnostics(t *testing.T) {
	childExit := childExitCauseForTest(t)
	metadata := orc.FailureContext{Origin: "codex", Operation: "managed turn", Phase: "harness startup", Ticket: "20261002-08595"}
	tests := []struct {
		name    string
		detail  string
		cleanup error
		want    []string
		unwant  []string
	}{
		{
			name:   "wrapped stderr and controls",
			detail: "Codex exited with status 23: example failure\x1b[31m\r\nnext\x00",
			want:   []string{"example failure", "[31m", "next", "exit_code=23", "origin=codex", "operation=managed turn", "ticket=20261002-08595"},
			unwant: []string{"\x1b", "\r", "\x00"},
		},
		{
			name:   "secret markers redact diagnostic",
			detail: "Codex exited with status 23: failure authorization=Bearer secret prompt=private argv=/private/codex",
			want:   []string{"worker failure detail redacted", "exit_code=23"},
			unwant: []string{"Bearer secret", "prompt=private", "argv=/private/codex"},
		},
		{
			name:    "joined cleanup detail",
			detail:  "Codex exited with status 23: example failure",
			cleanup: errors.New("session cleanup failed"),
			want:    []string{"example failure", "session cleanup failed", "exit_code=23"},
			unwant:  []string{"prompt=", "argv="},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var cause error = &diagnosticChildExitError{detail: test.detail, cause: childExit}
			cause = fmt.Errorf("run managed Codex: %w", cause)
			if test.cleanup != nil {
				cause = errors.Join(cause, test.cleanup)
			}
			err := orc.NewFailureContextError(metadata, cause)
			var output bytes.Buffer
			renderRoleFailure(&output, err)
			text := output.String()
			for _, want := range test.want {
				if !strings.Contains(text, want) {
					t.Errorf("rendered failure %q missing %q", text, want)
				}
			}
			for _, forbidden := range test.unwant {
				if strings.Contains(text, forbidden) {
					t.Errorf("rendered failure %q contains unsafe %q", text, forbidden)
				}
			}
			if count := strings.Count(text, "error: Codex process exited"); count != 1 {
				t.Errorf("rendered %d top-level failures, want 1: %q", count, text)
			}
		})
	}
}

func TestRenderedChildExitDiagnosticsCannotInjectWorkerEvents(t *testing.T) {
	childExit := childExitCauseForTest(t)
	tests := []struct {
		name  string
		type_ string
		wrap  bool
		join  bool
	}{
		{name: "wrapped failure", type_: "worker.failure", wrap: true},
		{name: "joined ready", type_: orc.WorkerReadyEventType, join: true},
		{name: "joined other event", type_: "steer.delivery", wrap: true, join: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			forged := orc.EventStreamPrefix + fmt.Sprintf(`{"type":%q,"worker":"forged","role":"coder","failure":{"classification":"lease_conflict","contained":true}}`, test.type_)
			detail := "Codex exited with status 23: example failure\n" + forged
			var cause error = &diagnosticChildExitError{detail: detail, cause: childExit}
			if test.wrap {
				cause = fmt.Errorf("run managed Codex: %w", cause)
			}
			if test.join {
				cause = errors.Join(cause, errors.New("session cleanup failed"))
			}
			err := orc.NewFailureContextError(orc.FailureContext{Origin: "codex", Operation: "managed turn", Phase: "harness startup"}, cause)
			var rendered bytes.Buffer
			renderRoleFailure(&rendered, err)

			var output bytes.Buffer
			var events []orc.Event
			child := &runChild{}
			writer := &workerEventWriter{dst: &output, sink: func(event orc.Event) {
				if child.captureFailure(event) {
					return
				}
				events = append(events, event)
			}}
			if n, err := writer.Write(rendered.Bytes()); err != nil || n != rendered.Len() {
				t.Fatalf("write rendered failure n=%d err=%v", n, err)
			}
			if err := writer.Flush(); err != nil {
				t.Fatalf("flush rendered failure: %v", err)
			}
			if got := output.String(); got != rendered.String() {
				t.Fatalf("rendered diagnostic was consumed as control: output=%q rendered=%q", got, rendered.String())
			}
			if len(events) != 0 || child.failureEnvelope() != nil {
				t.Fatalf("diagnostic injected events=%#v failure=%#v", events, child.failureEnvelope())
			}
			if !strings.Contains(output.String(), "example failure") || !strings.Contains(output.String(), test.type_) {
				t.Fatalf("rendered diagnostic lost useful detail: %q", output.String())
			}

			genuine := orc.EventStreamPrefix + `{"type":"worker.state","worker":"real","role":"coder","state":"quiescent"}` + "\n"
			if n, err := writer.Write([]byte(genuine)); err != nil || n != len(genuine) {
				t.Fatalf("write genuine event n=%d err=%v", n, err)
			}
			if len(events) != 1 || events[0].Type != "worker.state" || events[0].Worker != "real" {
				t.Fatalf("genuine event not preserved: %#v", events)
			}
		})
	}
}

func TestWorkerStatusReasonUsesStructuredStartupFailure(t *testing.T) {
	plain := supervisor.WorkerTransition{State: WorkerFailed, Error: "worker exited during startup verification"}
	if got := workerStatusReason(plain); got == "worker exited during startup verification" {
		t.Fatalf("rendered wording classified startup failure: %q", got)
	}

	structured := workerTransitionFailure(&supervisor.LifecycleError{Code: "worker_startup_failed", Message: "wording can vary"})
	if structured == nil || structured.Phase != "worker readiness" {
		t.Fatalf("startup lifecycle metadata = %#v", structured)
	}
	transition := supervisor.WorkerTransition{State: WorkerFailed, Failure: structured}
	if got := workerStatusReason(transition); got != "worker exited during startup verification" {
		t.Fatalf("structured startup reason = %q", got)
	}
}

func TestWorkerFailureClassificationBoundsSecretsAndPhases(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		class     string
		phase     string
		origin    string
		operation string
	}{
		{name: "silent", class: "silent_exit", phase: "worker operation"},
		{name: "reviewer baseline", err: orc.NewFailureContextError(orc.FailureContext{Origin: "reviewer", Operation: "baseline observation", Phase: "reviewer ownership-baseline observation"}, errors.New("rewritten message one")), class: "worker_error", phase: "reviewer ownership-baseline observation", origin: "reviewer", operation: "baseline observation"},
		{name: "ticket startup", err: orc.NewFailureContextError(orc.FailureContext{Origin: "ticket", Operation: "client startup", Phase: "Ticket client startup"}, errors.New("rewritten message two")), class: "worker_error", phase: "Ticket client startup", origin: "ticket", operation: "client startup"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := classifyWorkerFailure(test.err)
			if failure.Classification != test.class || failure.Phase != test.phase || failure.Origin != test.origin || failure.Operation != test.operation {
				t.Fatalf("failure=%#v", failure)
			}
			if test.name == "bounce limit" && (!failure.Contained || failure.Ticket != "20260923-80098") {
				t.Fatalf("bounce failure lost structured containment/ticket: %#v", failure)
			}
			if strings.Contains(failure.Classification, "secret") || strings.Contains(failure.Phase, "secret") {
				t.Fatalf("failure leaked secret=%#v", failure)
			}
		})
	}
}

func TestFailureClassificationDoesNotDependOnWrappedMessage(t *testing.T) {
	metadata := orc.FailureContext{Origin: "ticket", Operation: "claim observation", Phase: "ticket observation", Ticket: "20260923-80098", Contained: true}
	first := classifyWorkerFailure(orc.NewFailureContextError(metadata, errors.New("wording A: old details")))
	second := classifyWorkerFailure(orc.NewFailureContextError(metadata, errors.New("wording B: entirely different details")))
	if first.Classification != second.Classification || first.Phase != second.Phase || first.Origin != second.Origin || first.Operation != second.Operation || first.Ticket != second.Ticket || first.Contained != second.Contained {
		t.Fatalf("message wording changed classification: first=%#v second=%#v", first, second)
	}
}

func TestRoleFailureEnvelopePreservesSafeTicketCommandCode(t *testing.T) {
	err := fmt.Errorf("wait for queued ticket: %w", &ticketclient.CommandError{ExitCode: -1, Code: "ticket_not_ready", Message: "prompt=secret"})
	failure := roleFailureEnvelope(err)
	if failure == nil || failure.Classification != "ticket_command_failed" || failure.TicketCode != "ticket_not_ready" || failure.Phase != "worker operation" {
		t.Fatalf("failure envelope=%#v", failure)
	}
	if strings.Contains(failure.TicketCode, "secret") || strings.Contains(failure.Phase, "prompt") {
		t.Fatalf("failure envelope leaked unsafe data=%#v", failure)
	}
}

func TestRoleFailureEnvelopePreservesBoundedTicketTransportExit(t *testing.T) {
	err := orc.NewFailureContextError(orc.FailureContext{Origin: "ticket", Operation: "ownership observation", Phase: "reviewer ownership-baseline observation"}, fmt.Errorf("human-readable wording is independent: %w", &ticketclient.TransportError{
		Category: "process_exit",
		ExitCode: 9,
	}))
	failure := roleFailureEnvelope(err)
	if failure == nil || failure.Classification != "ticket_transport_failed" || failure.TransportCategory != "process_exit" || failure.ExitCode != 9 || failure.Phase != "reviewer ownership-baseline observation" {
		t.Fatalf("failure envelope=%#v", failure)
	}
	if failure.Signal != "" || failure.TicketCode != "" {
		t.Fatalf("absent optional metadata was synthesized: %#v", failure)
	}

	var output bytes.Buffer
	renderRoleFailure(&output, err)
	text := output.String()
	for _, want := range []string{"classification=ticket_transport_failed", "transport_category=process_exit", "exit_code=9", "phase=reviewer ownership-baseline observation"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output=%q, missing %q", text, want)
		}
	}
	if strings.Contains(text, "bearer-token") || strings.Contains(text, "secret") || strings.Contains(text, "unknown") {
		t.Fatalf("unsafe or synthesized metadata leaked: %q", text)
	}
}

func TestRenderRoleFailureOmitsRawCommandDetails(t *testing.T) {
	var output bytes.Buffer
	err := fmt.Errorf("wait for queued ticket: %w", &ticketclient.CommandError{ExitCode: -1, Code: "ticket_not_ready", Message: "bearer-token=secret prompt=opaque"})
	renderRoleFailure(&output, err)
	text := output.String()
	if !strings.Contains(text, "classification=ticket_command_failed") || !strings.Contains(text, "ticket_code=ticket_not_ready") {
		t.Fatalf("output=%q", text)
	}
	if strings.Contains(text, "bearer-token") || strings.Contains(text, "secret") || strings.Contains(text, "prompt=opaque") {
		t.Fatalf("raw command details leaked: %q", text)
	}
}
