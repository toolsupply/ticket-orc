package supervisor

import (
	"testing"
)

func TestStartupAttemptPublishesOnlyFirstClassifiedOutcome(t *testing.T) {
	attempt := NewStartupAttempt()
	attempt.MarkResultPending()
	pending, done := attempt.PendingResult()
	if !pending {
		t.Fatal("result should be pending before completion")
	}

	attempt.Complete(&WorkerFailure{Classification: "startup_failure", Phase: "readiness"})
	attempt.Complete(&WorkerFailure{Classification: "shutdown", Phase: "daemon shutdown"})

	select {
	case <-done:
	default:
		t.Fatal("completion signal was not closed")
	}
	if pending, _ := attempt.PendingResult(); pending {
		t.Fatal("completed result remained pending")
	}
	completed, failure := attempt.Result()
	if !completed || failure == nil || failure.Classification != "startup_failure" || failure.Phase != "readiness" {
		t.Fatalf("startup result = completed:%v failure:%#v", completed, failure)
	}
}
