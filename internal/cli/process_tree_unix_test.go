//go:build !windows

package cli

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/testutil"
)

func TestChildContainmentDoesNotForceUnverifiedGroupAfterLeaderExit(t *testing.T) {
	pgid, err := syscall.Getpgid(0)
	if err != nil {
		t.Fatal(err)
	}
	containment := childContainment{pgid: pgid}
	var signalled bool
	oldSignal := signalProcessGroupID
	signalProcessGroupID = func(int, syscall.Signal) error {
		signalled = true
		return nil
	}
	t.Cleanup(func() { signalProcessGroupID = oldSignal })
	if err := containment.force(&os.Process{Pid: 1 << 30}); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("unverified group force error=%v, want process done", err)
	}
	if signalled {
		t.Fatal("force signalled an unverified remembered process group")
	}
}

func TestConfiguredChildProcessStopsDescendants(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	command := exec.Command("sh", "-c", `sleep 30 & echo $! > "$1"; wait`, "ticket-orc-tree", pidFile)
	configureChildProcess(command)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	containment, err := attachChildContainment(command.Process)
	if err != nil {
		t.Fatal(err)
	}
	childPID := waitForProcessPID(t, pidFile)
	if err := requestChildStop(command.Process); err != nil {
		t.Fatalf("graceful tree stop: %v", err)
	}
	// A descendant may ignore the graceful interrupt. Force the same process
	// group before reaping the supervisor child so inherited output handles
	// cannot keep the wait blocked.
	if err := containment.force(command.Process); err != nil {
		t.Fatalf("force tree stop: %v", err)
	}
	waitForCommandExit(t, command)
	testutil.Await(t, time.Second, "descendant process exit", func() bool {
		return !processExists(childPID)
	}, func() string { return "descendant PID=" + strconv.Itoa(childPID) })
}

func TestConfiguredChildProcessStopsDescendantAfterLeaderExit(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	readOutput, writeOutput, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readOutput.Close()
	command := exec.Command("sh", "-c", `sleep 30 & echo $! > "$1"; sleep 0.5; exit 0`, "ticket-orc-tree", pidFile)
	command.Stdout = writeOutput
	configureChildProcess(command)
	if err := command.Start(); err != nil {
		writeOutput.Close()
		t.Fatal(err)
	}
	if err := writeOutput.Close(); err != nil {
		t.Fatal(err)
	}
	containment, err := attachChildContainment(command.Process)
	if err != nil {
		t.Fatal(err)
	}
	var observedExit <-chan bool
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		observed := make(chan bool, 1)
		observedExit = observed
		go func() { observed <- waitForChildExitBeforeReap(command.Process) }()
	}
	childPID := waitForProcessPID(t, pidFile)
	if observedExit != nil {
		if !<-observedExit {
			t.Fatal("leader exit observer did not report the unreaped child")
		}
	}
	var forceErr error
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		forceErr = containment.forceAfterLeaderExit(command.Process)
	} else {
		forceErr = containment.force(command.Process)
	}
	if err := forceErr; err != nil {
		t.Fatalf("force tree stop after leader exit: %v", err)
	}
	waitForCommandExit(t, command)
	outputDone := make(chan error, 1)
	go func() {
		_, readErr := io.Copy(io.Discard, readOutput)
		outputDone <- readErr
	}()
	select {
	case err := <-outputDone:
		if err != nil {
			t.Fatalf("read descendant output handle: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("descendant retained inherited output handle after process-group stop")
	}
	testutil.Await(t, time.Second, "descendant process exit after leader exit", func() bool {
		return !processExists(childPID)
	}, func() string { return "descendant PID=" + strconv.Itoa(childPID) })
}

func waitForProcessPID(t *testing.T, path string) int {
	t.Helper()
	var pid int
	testutil.Await(t, 2*time.Second, "descendant PID file", func() bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		parsed, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
		if parseErr != nil || parsed <= 0 {
			return false
		}
		pid = parsed
		return true
	}, func() string { return "path=" + path })
	return pid
}

func waitForCommandExit(t *testing.T, command *exec.Cmd) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case <-done:
	case <-time.After(runChildStopTimeout):
		if err := forceChildStop(command.Process); err != nil && !strings.Contains(err.Error(), "finished") {
			t.Fatalf("force tree stop: %v", err)
		}
		select {
		case <-done:
		case <-time.After(runChildStopTimeout):
			t.Fatal("process tree did not exit")
		}
	}
}

func processExists(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
