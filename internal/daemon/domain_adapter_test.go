package daemon

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestMutationAndGroupDTOsPreserveControlJSONShape(t *testing.T) {
	mutation := MutationResultDTO(supervisor.MutationResult{
		Worker: "coder", State: "running", Applied: true,
	})
	data, err := json.Marshal(mutation)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{
		"worker": "coder", "state": "running", "mutation_applied": true,
	} {
		if got[key] != want {
			t.Errorf("mutation DTO %q = %#v, want %#v", key, got[key], want)
		}
	}

	group := GroupResultDTO(supervisor.GroupResult{Group: "backend", Results: []supervisor.MutationResult{{Worker: "coder", Applied: true}}})
	if group.Group != "backend" || len(group.Results) != 1 || group.Results[0].Worker != "coder" || !group.Results[0].Applied {
		t.Fatalf("group DTO = %#v", group)
	}
}

func TestReloadAndDoctorDTOsAdaptSupervisorOutcomes(t *testing.T) {
	reload := ReloadResultDTO(supervisor.ReloadResult{
		Revision: "revision-2", Applied: true,
		Warnings: []supervisor.Diagnostic{{Severity: "warning", Code: "reload.restart_required", Path: "workers.coder", Worker: "coder", Message: "restart required", Remediation: "restart worker"}},
	})
	if reload.Revision != "revision-2" || !reload.Applied || len(reload.Warnings) != 1 || reload.Warnings[0].Code != "reload.restart_required" || reload.Warnings[0].Remediation != "restart worker" {
		t.Fatalf("reload DTO = %#v", reload)
	}
	doctor := DoctorResultDTO(supervisor.DoctorResult{
		Reloaded: true,
		Workers: []supervisor.DoctorWorkerResult{{
			Worker: "coder", Outcome: "failed", Failure: &supervisor.WorkerFailure{Classification: "child_exit", Phase: "startup", ExitCode: 7},
		}},
	})
	if !doctor.Reloaded || len(doctor.Workers) != 1 || doctor.Workers[0].Failure == nil || doctor.Workers[0].Failure.Classification != "child_exit" || doctor.Workers[0].Failure.ExitCode != 7 {
		t.Fatalf("doctor DTO = %#v", doctor)
	}
}

func TestLifecycleErrorStatusMapping(t *testing.T) {
	for _, test := range []struct {
		code     string
		conflict bool
		want     int
	}{
		{code: "invalid_config", want: http.StatusBadRequest},
		{code: "invalid_config", conflict: true, want: http.StatusConflict},
		{code: "worker_not_found", want: http.StatusNotFound},
		{code: "capability_unavailable", want: http.StatusUnprocessableEntity},
	} {
		t.Run(test.code+map[bool]string{true: "_conflict"}[test.conflict], func(t *testing.T) {
			got := lifecycleHTTPStatus(&supervisor.LifecycleError{Code: test.code, Conflict: test.conflict})
			if got != test.want {
				t.Fatalf("HTTP status = %d, want %d", got, test.want)
			}
		})
	}
}
