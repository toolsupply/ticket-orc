//go:build darwin

package cli

import (
	"os"
	"syscall"
	"testing"
)

func TestDarwinForceAfterLeaderExitSignalsVerifiedGroup(t *testing.T) {
	parentGroup, err := syscall.Getpgid(0)
	if err != nil {
		t.Fatal(err)
	}
	pgid := parentGroup + 1
	if pgid <= 0 {
		pgid = parentGroup - 1
	}
	containment := childContainment{pgid: pgid}
	var signalled bool
	previous := signalProcessGroupID
	signalProcessGroupID = func(got int, signal syscall.Signal) error {
		if got != pgid || signal != syscall.SIGKILL {
			t.Fatalf("signal = (%d, %v), want (%d, SIGKILL)", got, signal, pgid)
		}
		signalled = true
		return nil
	}
	t.Cleanup(func() { signalProcessGroupID = previous })
	if err := containment.forceAfterLeaderExit(&os.Process{Pid: 1 << 30}); err != nil {
		t.Fatalf("forceAfterLeaderExit = %v", err)
	}
	if !signalled {
		t.Fatal("verified group was not signalled")
	}
}
