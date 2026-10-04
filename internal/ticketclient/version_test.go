package ticketclient

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func configureVersionHelper(t *testing.T, output, stderr string, exit string) string {
	t.Helper()
	t.Setenv(versionHelperEnv, "1")
	t.Setenv(versionOutputEnv, output)
	t.Setenv(versionStderrEnv, stderr)
	t.Setenv(versionExitEnv, exit)
	t.Setenv(versionRepeatEnv, "")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return executable
}

func TestRequireMinimumVersionWithExecutable(t *testing.T) {
	for _, test := range []struct {
		name    string
		output  string
		wantErr string
	}{
		{name: "below minimum", output: "ticket 0.2.3 (api 2, storage 3)\n", wantErr: "incompatible Ticket version: ticket-orc requires ticket >= v0.2.4; found v0.2.3"},
		{name: "minimum", output: "ticket 0.2.4 (api 2, storage 3)\n"},
		{name: "newer patch", output: "ticket v0.2.5 (api 2, storage 3)\n"},
		{name: "newer minor", output: "ticket 0.3.0 (api 2, storage 3)\n"},
		{name: "newer major", output: "ticket 1.0.0 (api 2, storage 3)\n"},
		{name: "build metadata", output: "ticket 0.2.4+release.1 (api 2, storage 3)\n"},
		{name: "prerelease", output: "ticket 0.2.4-rc.1 (api 2, storage 3)\n", wantErr: "incompatible Ticket version: ticket-orc requires ticket >= v0.2.4; found v0.2.4-rc.1"},
		{name: "malformed", output: "Ticket version is 0.2.4\n", wantErr: "could not determine Ticket version"},
	} {
		t.Run(test.name, func(t *testing.T) {
			executable := configureVersionHelper(t, test.output, "secret diagnostic", "")
			err := RequireMinimumVersionWithExecutable(context.Background(), executable)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("RequireMinimumVersionWithExecutable: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, test.wantErr)
			}
			if strings.Contains(err.Error(), "secret diagnostic") {
				t.Fatalf("error exposed stderr: %v", err)
			}
		})
	}
}

func TestTicketVersionProbeFailuresAreBoundedAndSafe(t *testing.T) {
	t.Run("command failure", func(t *testing.T) {
		executable := configureVersionHelper(t, "", "secret stderr", "7")
		_, err := TicketVersionWithExecutable(context.Background(), executable)
		if err == nil || !strings.Contains(err.Error(), "command_failed") || strings.Contains(err.Error(), "secret stderr") {
			t.Fatalf("TicketVersionWithExecutable error = %v", err)
		}
	})
	t.Run("oversized stdout", func(t *testing.T) {
		executable := configureVersionHelper(t, "", "", "")
		t.Setenv(versionRepeatEnv, stringInt(maxProbeOutputBytes+1))
		_, err := TicketVersionWithExecutable(context.Background(), executable)
		if err == nil || !strings.Contains(err.Error(), "output_too_large") {
			t.Fatalf("TicketVersionWithExecutable error = %v, want output_too_large", err)
		}
	})
}

func TestMinimumVersionCheckCachesPerExecutablePath(t *testing.T) {
	executable := configureVersionHelper(t, "ticket 0.2.4 (api 2, storage 3)\n", "", "")
	countFile := filepath.Join(t.TempDir(), "calls")
	t.Setenv(versionCountFileEnv, countFile)
	absExecutable, err := filepath.Abs(executable)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := requireMinimumVersionCached(context.Background(), absExecutable); err != nil {
			t.Fatalf("RequireMinimumVersion: %v", err)
		}
	}
	data, err := os.ReadFile(countFile)
	if err != nil || string(data) != "x" {
		t.Fatalf("version subprocess calls = %q, err=%v; want one", data, err)
	}
}

func TestTicketVersionProbeHonorsContext(t *testing.T) {
	executable := configureVersionHelper(t, "ticket 0.2.4 (api 2, storage 3)\n", "", "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	_, err := TicketVersionWithExecutable(ctx, executable)
	if err == nil {
		t.Fatal("TicketVersionWithExecutable succeeded with expired context")
	}
}

func stringInt(value int) string {
	return strconv.Itoa(value)
}
