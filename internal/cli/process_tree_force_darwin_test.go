package cli

import (
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

func darwinGroupRecord(pid int, status byte) []byte {
	record := make([]byte, darwinKinfoProcSize)
	record[36] = status
	binary.LittleEndian.PutUint32(record[40:44], uint32(pid))
	return record
}

func TestChildContainmentDarwinZombieOnlyGroup(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		want bool
	}{
		{"leader", darwinGroupRecord(123, 5), true},
		{"zombie_descendant", append(darwinGroupRecord(123, 5), darwinGroupRecord(124, 5)...), true},
		{"live_descendant", append(darwinGroupRecord(123, 5), darwinGroupRecord(124, 2)...), false},
		{"stopped_descendant", append(darwinGroupRecord(123, 5), darwinGroupRecord(124, 4)...), false},
		{"unknown_state", darwinGroupRecord(123, 0), false},
		{"missing_leader", darwinGroupRecord(124, 5), false},
		{"live_leader", darwinGroupRecord(123, 2), false},
		{"invalid_pid", append(darwinGroupRecord(123, 5), darwinGroupRecord(0, 5)...), false},
		{"empty", nil, false},
		{"partial", darwinGroupRecord(123, 5)[:darwinKinfoProcSize-1], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := darwinZombieOnlyGroup(tc.data, 123); got != tc.want {
				t.Fatalf("zombie-only=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestChildContainmentDarwinPostExitErrors(t *testing.T) {
	previousSignal, previousRead := signalProcessGroupID, readDarwinProcessGroup
	t.Cleanup(func() { signalProcessGroupID, readDarwinProcessGroup = previousSignal, previousRead })
	for _, tc := range []struct {
		name       string
		signalErr  error
		data       []byte
		inspectErr error
		wantErr    error
		wantReads  int
	}{
		{"submitted", nil, nil, nil, nil, 0},
		{"other_errno", syscall.EIO, nil, nil, syscall.EIO, 0},
		{"missing_group", syscall.ESRCH, nil, nil, syscall.ESRCH, 0},
		{"zombie_only", syscall.EPERM, darwinGroupRecord(123, 5), nil, nil, 1},
		{"denied_live", syscall.EPERM, append(darwinGroupRecord(123, 5), darwinGroupRecord(124, 2)...), nil, syscall.EPERM, 1},
		{"unknown_group", syscall.EPERM, nil, nil, syscall.EPERM, 1},
		{"malformed", syscall.EPERM, []byte{5}, nil, syscall.EPERM, 1},
		{"denied_inspection", syscall.EPERM, nil, syscall.EACCES, syscall.EPERM, 1},
		{"growing_group", syscall.EPERM, nil, syscall.ENOMEM, syscall.EPERM, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signals, reads := 0, 0
			signalProcessGroupID = func(pgid int, signal syscall.Signal) error {
				signals++
				if pgid != 123 || signal != syscall.SIGKILL {
					t.Errorf("signal=(%d, %d)", pgid, signal)
				}
				return tc.signalErr
			}
			readDarwinProcessGroup = func(pgid int) ([]byte, error) {
				reads++
				if pgid != 123 {
					t.Errorf("snapshot group=%d", pgid)
				}
				return tc.data, tc.inspectErr
			}
			err := forceExitedProcessGroup(123)
			if !errors.Is(err, tc.wantErr) || signals != 1 || reads != tc.wantReads {
				t.Fatalf("err=%v signals=%d reads=%d", err, signals, reads)
			}
			if tc.inspectErr != nil && (!errors.Is(err, tc.inspectErr) || !strings.Contains(err.Error(), "inspect post-exit group 123")) {
				t.Fatalf("lost inspection operation/errno: %v", err)
			}
		})
	}
}

func TestChildContainmentDarwinLeaderOnlyNaturalExit(t *testing.T) {
	cmd := exec.Command("sh", "-c", "read value; exit 0")
	configureChildProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	containment, err := attachChildContainment(cmd.Process)
	if err != nil {
		t.Fatal(err)
	}
	_ = stdin.Close()
	if err := waitForChildExitBeforeReap(cmd.Process); err != nil {
		t.Fatal(err)
	}
	data, err := darwinProcessGroupSnapshot(cmd.Process.Pid)
	if err != nil || !darwinZombieOnlyGroup(data, cmd.Process.Pid) {
		t.Fatalf("retained leader missing from zombie snapshot: bytes=%d err=%v", len(data), err)
	}
	if err := containment.forceAfterLeaderExit(cmd.Process); err != nil {
		t.Fatalf("redundant cleanup of exited leader: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("lost natural exit: %v", err)
	}
	if err := containment.forceAfterLeaderExit(cmd.Process); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("released leader allowed cleanup: %v", err)
	}
}
