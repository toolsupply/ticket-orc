package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunTicketVersionFailurePrecedesRuntime(t *testing.T) {
	runCalled := false
	var stdout, stderr bytes.Buffer
	err := errors.New("incompatible Ticket version: ticket-orc requires ticket >= v0.2.4; found v0.2.3")
	code := runWithExecutors([]string{"run"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
		ticketCompatibility: func() error { return err },
		run: func(RunConfig, io.Writer, io.Writer) error {
			runCalled = true
			return nil
		},
	})
	if code != 1 || runCalled || stdout.Len() != 0 || !strings.Contains(stderr.String(), err.Error()) {
		t.Fatalf("run preflight code=%d called=%v stdout=%q stderr=%q", code, runCalled, stdout.String(), stderr.String())
	}
}

func TestConfigCheckDoesNotRunTicketVersionPreflight(t *testing.T) {
	checks := 0
	var stdout, stderr bytes.Buffer
	code := runWithExecutors([]string{"config", "check"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
		ticketCompatibility: func() error { checks++; return errors.New("must not be called") },
		configCheck:         func(ConfigCheckConfig, io.Writer, io.Writer, envLookup) error { return nil },
	})
	if code != 0 || checks != 0 || stderr.Len() != 0 {
		t.Fatalf("config check code=%d checks=%d stderr=%q", code, checks, stderr.String())
	}
}

func TestDoctorRemainsAvailableWhenTicketVersionCheckWouldFail(t *testing.T) {
	checks, doctorCalled := 0, false
	var stdout, stderr bytes.Buffer
	code := runWithExecutors([]string{"doctor"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
		ticketCompatibility: func() error { checks++; return errors.New("old Ticket") },
		doctor:              func(doctorConfig, io.Writer, io.Writer, envLookup) error { doctorCalled = true; return nil },
	})
	if code != 0 || checks != 0 || !doctorCalled || stderr.Len() != 0 {
		t.Fatalf("doctor code=%d checks=%d called=%v stderr=%q", code, checks, doctorCalled, stderr.String())
	}
}

func TestTicketDependentCommandsUseOneSharedPreflight(t *testing.T) {
	checks, runCalls := 0, 0
	root := t.TempDir()
	firstRepo, secondRepo := filepath.Join(root, "first"), filepath.Join(root, "second")
	if err := os.MkdirAll(firstRepo, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(secondRepo, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "run.json")
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"repositories":{"first":{"repository":%q},"second":{"repository":%q}},"workers":{"one":{"role":"coder","repository":"first","actor":"one"},"two":{"role":"coder","repository":"second","actor":"two"}}}`, firstRepo, secondRepo))
	var stdout, stderr bytes.Buffer
	code := runWithExecutors([]string{"run", "--config", configPath}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
		ticketCompatibility: func() error { checks++; return nil },
		run: func(config RunConfig, _, _ io.Writer) error {
			runCalls++
			if len(config.Repositories) != 2 {
				t.Errorf("repositories = %d, want 2", len(config.Repositories))
			}
			return nil
		},
	})
	if code != 0 || checks != 1 || runCalls != 1 {
		t.Fatalf("run code=%d version checks=%d run calls=%d stderr=%q", code, checks, runCalls, stderr.String())
	}
}

func TestManagedRoleReusesParentPreflight(t *testing.T) {
	check := ticketVersionCompatibility(mapEnv(map[string]string{supervisedRoleEnv: "1"}))
	if err := check(); err != nil {
		t.Fatalf("managed role version gate = %v, want parent preflight reuse", err)
	}
}

func TestDirectTicketCommandFailsBeforeIdentityLookup(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runWithExecutors([]string{"join"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
		ticketCompatibility: func() error { return errors.New("old Ticket") },
	})
	if code != 1 || !strings.Contains(stderr.String(), "old Ticket") || strings.Contains(stderr.String(), "identity") {
		t.Fatalf("join preflight code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}
