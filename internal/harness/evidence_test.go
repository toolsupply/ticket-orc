package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestEvidenceBoundaryKeepsUnknownAndRejectsFreeFormFacts(t *testing.T) {
	evidence := ExecutionEvidence{
		LogID: "/private/prompt=secret.jsonl", Phase: "prompt=secret",
		Process: ProcessOutcome{Start: "argv=secret", Cancellation: "authorization=secret", Signal: 999},
	}
	bounded := evidence.Bounded()
	if bounded.Process.Start != "unknown" || bounded.Process.Cancellation != "unknown" || bounded.Phase != "unknown" || bounded.Process.Signal != 0 {
		t.Fatalf("unknown was reinterpreted: %+v", bounded)
	}
	data, _ := json.Marshal(bounded)
	if strings.Contains(string(data), "secret") || evidence.Process.Signal != 999 {
		t.Fatalf("boundary leaked input or mutated evidence: %s", data)
	}
}

func TestProcessCancellationDoesNotInterpretCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("shutdown prompt=secret"))
	if got := ProcessCancellation(ctx); got != "canceled" {
		t.Fatalf("cancellation=%s want canceled", got)
	}
}
