package supervisor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
)

func TestLatestFailureSanitizesPrivateAndOversizedEvidence(t *testing.T) {
	const repo = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	dir := t.TempDir()
	for _, detail := range []string{"authorization: private-value", "api_key=private-value", "password: private-value", "prompt private-value", "argv private-value", "environment private-value", "session_id private-value", "/private/path", "\x1b\n[ticket-orc-event] private-value", strings.Repeat("x", 50000)} {
		f := WorkerFailure{RepositoryID: repo, Worker: "worker", Role: "coder", Classification: "worker_error", Operation: "register managed session", Detail: detail}
		if err := SaveLatestFailure(t.Context(), dir, f); err != nil {
			t.Fatal(err)
		}
		record, err := LoadLatestFailure(t.Context(), dir, repo, "worker")
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, record.Reference))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > state.MaxWorkerFailureBytes || strings.Contains(string(data), "private-value") || strings.Contains(string(data), "/private/path") || record.Failure.Operation != f.Operation || !strings.Contains(record.Failure.Detail, "redacted") {
			t.Fatalf("unsafe or unhelpful diagnostic: %s", data)
		}
	}
	if err := SaveLatestFailure(t.Context(), dir, WorkerFailure{RepositoryID: repo, Worker: "worker", Detail: "registry\x1b[31m unavailable\nnext"}); err != nil {
		t.Fatal(err)
	}
	record, err := LoadLatestFailure(t.Context(), dir, repo, "worker")
	if err != nil || strings.ContainsAny(record.Failure.Detail, "\x1b\n") || !strings.Contains(record.Failure.Detail, "unavailable") {
		t.Fatalf("terminal controls or useful context lost: %+v %v", record, err)
	}
}

func TestLatestFailureConcurrentReplacementAndStrictRead(t *testing.T) {
	const repo = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	dir := t.TempDir()
	f := WorkerFailure{RepositoryID: repo, Worker: "worker", Classification: "worker_error", Detail: "initial cause"}
	if err := SaveLatestFailure(t.Context(), dir, f); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 5 {
				if err := SaveLatestFailure(t.Context(), dir, f); err != nil {
					t.Error(err)
					return
				}
				if record, err := LoadLatestFailure(t.Context(), dir, repo, "worker"); err != nil || record == nil || record.Failure.Detail != f.Detail {
					t.Errorf("partial replacement: %+v %v", record, err)
					return
				}
			}
		})
	}
	wg.Wait()
	record, err := LoadLatestFailure(t.Context(), dir, repo, "worker")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*LatestFailure){
		func(r *LatestFailure) { r.Version++ },
		func(r *LatestFailure) { r.Failure.RepositoryID = "other" },
		func(r *LatestFailure) { r.Failure.Worker = "other" },
		func(r *LatestFailure) { r.Reference = "../escape" },
	} {
		copy := *record
		mutate(&copy)
		data, _ := json.Marshal(copy)
		if err := state.WriteWorkerFailure(t.Context(), dir, repo, "worker", data); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadLatestFailure(t.Context(), dir, repo, "worker"); err == nil {
			t.Fatal("malformed or foreign diagnostic accepted")
		}
	}
}
