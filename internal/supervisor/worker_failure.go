package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// LatestFailure is historical evidence, independent of current lifecycle state.
// Reference is a validated filename relative to the instance-local directory.
// Details are at most 2048 bytes; the complete artifact is at most 16 KiB.
type LatestFailure struct {
	Version    int           `json:"version"`
	ObservedAt time.Time     `json:"observed_at"`
	Reference  string        `json:"reference"`
	Failure    WorkerFailure `json:"failure"`
}

func SaveLatestFailure(ctx context.Context, dir string, failure WorkerFailure) error {
	ref, err := state.WorkerFailureReference(failure.RepositoryID, failure.Worker)
	if err != nil {
		return err
	}
	failure = boundedDiagnosticFailure(failure)
	record := LatestFailure{Version: 1, ObservedAt: time.Now().UTC(), Reference: ref, Failure: failure}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return state.WriteWorkerFailure(ctx, dir, failure.RepositoryID, failure.Worker, data)
}

func LoadLatestFailure(ctx context.Context, dir, repository, worker string) (*LatestFailure, error) {
	ref, err := state.WorkerFailureReference(repository, worker)
	if err != nil {
		return nil, err
	}
	data, err := state.ReadWorkerFailure(ctx, dir, repository, worker)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record LatestFailure
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, fmt.Errorf("invalid worker diagnostic: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("invalid worker diagnostic trailing data")
	}
	if record.Version != 1 {
		return nil, fmt.Errorf("unsupported worker diagnostic version %d; expected 1", record.Version)
	}
	if record.ObservedAt.IsZero() || record.Reference != ref || record.Failure.RepositoryID != repository || record.Failure.Worker != worker {
		return nil, fmt.Errorf("invalid worker diagnostic version or identity")
	}
	record.Failure = boundedDiagnosticFailure(record.Failure)
	return &record, nil
}

func boundedDiagnosticFailure(f WorkerFailure) WorkerFailure {
	f.Detail = terminaltext.Diagnostic(f.Detail)
	// Identity tokens are validated as identity, not inspected for prose markers.
	// A worker named token-reviewer is still the same nonsecret stable worker.
	if f.Role != "" {
		if _, err := state.WorkerFailureReference(f.RepositoryID, f.Role); err != nil {
			f.Role = "unknown"
		}
	}
	if f.Actor != "" && ticketclient.ValidateActor(f.Actor) != nil {
		f.Actor = ""
	}
	for _, field := range []*string{&f.Classification, &f.Signal, &f.Origin, &f.TicketCode, &f.TransportCategory} {
		if len(*field) > 128 {
			*field = "unknown"
		} else {
			*field = terminaltext.Diagnostic(*field)
		}
	}
	if f.Ticket != "" && ticketclient.ValidateFullID(f.Ticket) != nil {
		f.Ticket = ""
	}
	if f.OuterProcess != nil {
		e := (harness.ExecutionEvidence{Process: *f.OuterProcess}).Bounded()
		f.OuterProcess = &e.Process
	}
	f.Phase = terminaltext.Diagnostic(f.Phase)
	f.Operation = terminaltext.Diagnostic(f.Operation)
	f.Remediation = terminaltext.Diagnostic(f.Remediation)
	if f.Execution != nil {
		e := f.Execution.Bounded()
		f.Execution = &e
	}
	return f
}
