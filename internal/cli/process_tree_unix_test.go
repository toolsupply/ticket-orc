//go:build linux || darwin

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestChildContainmentDoesNotForceUnverifiedGroupAfterLeaderExit(t *testing.T) {
	containment := childContainment{pgid: syscall.Getpgrp()}
	previous := signalProcessGroupID
	signalProcessGroupID = func(int, syscall.Signal) error {
		t.Error("signaled an unverified remembered group")
		return nil
	}
	t.Cleanup(func() { signalProcessGroupID = previous })
	if err := containment.force(&os.Process{Pid: 1 << 30}); err == nil {
		t.Fatal("unverified group force succeeded")
	}
	if err := containment.forceAfterLeaderExit(&os.Process{Pid: 1 << 30}); err == nil {
		t.Fatal("unverified post-exit group force succeeded")
	}
}

func TestChildContainmentAttachRetainsLookupOperation(t *testing.T) {
	process := &os.Process{Pid: 1 << 30}
	_, err := attachChildContainment(process)
	wantContext := fmt.Sprintf("attach worker group: getpgid leader %d:", process.Pid)
	if !errors.Is(err, syscall.ESRCH) || !strings.Contains(err.Error(), wantContext) {
		t.Fatalf("failed group lookup lost operation or errno: %v", err)
	}
}

func TestChildContainmentDeniedSignalRetainsOperation(t *testing.T) {
	for _, operation := range []string{"request", "force", "post-exit"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "sh", "-c", "read value; exit 17")
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
			if operation == "post-exit" {
				// Keep a live member in the group after the exact leader exits.
				// A zombie-only Darwin group has a distinct, benign EPERM case.
				member := exec.Command("sh", "-c", "read value")
				member.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: cmd.Process.Pid}
				memberInput, err := member.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := member.Start(); err != nil {
					_ = memberInput.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = memberInput.Close()
					_ = member.Process.Kill()
					_ = member.Wait()
				})
				_ = stdin.Close()
				if err := waitForChildExitBeforeReap(cmd.Process); err != nil {
					t.Fatal(err)
				}
			}
			previous := signalProcessGroupID
			t.Cleanup(func() { signalProcessGroupID = previous })
			calls := 0
			wantSignal := syscall.SIGKILL
			if operation == "request" {
				wantSignal = syscall.SIGINT
			}
			signalProcessGroupID = func(pgid int, signal syscall.Signal) error {
				calls++
				if pgid != cmd.Process.Pid || signal != wantSignal {
					t.Errorf("group signal=(%d, %d), want (%d, %d)", pgid, signal, cmd.Process.Pid, wantSignal)
				}
				return syscall.EPERM
			}
			var stopErr error
			prefix := "signal worker group"
			switch operation {
			case "request":
				stopErr = containment.request(cmd.Process)
			case "force":
				stopErr = containment.force(cmd.Process)
			case "post-exit":
				prefix = "post-exit worker cleanup"
				stopErr = containment.forceAfterLeaderExit(cmd.Process)
			}
			wantContext := fmt.Sprintf("%s: kill group %d with signal %d:", prefix, cmd.Process.Pid, wantSignal)
			if calls != 1 || !errors.Is(stopErr, syscall.EPERM) || !strings.Contains(stopErr.Error(), wantContext) {
				t.Fatalf("denied group signal lost stage or errno: calls=%d err=%v", calls, stopErr)
			}
			_ = stdin.Close()
			if err := cmd.Wait(); err == nil || cmd.ProcessState.ExitCode() != 17 {
				t.Fatalf("lost leader exit status: %v / %v", err, cmd.ProcessState)
			}
			for _, stop := range []func(*os.Process) error{containment.force, containment.forceAfterLeaderExit} {
				err := stop(cmd.Process)
				if !errors.Is(err, os.ErrProcessDone) || !strings.Contains(err.Error(), "probe leader") || calls != 1 {
					t.Fatalf("released leader lost probe context or authorized group signal: calls=%d err=%v", calls, err)
				}
			}
		})
	}
}

// This is the one outer-boundary smoke scenario, exercised both by explicit
// stop and by abnormal leader exit. All subprocesses are controlled fixtures;
// no Ticket CLI, harness or live model is involved.
func TestConfiguredChildProcessStopsDescendants(t *testing.T) {
	testOuterProcessBoundary(t, true)
}

func TestConfiguredChildProcessStopsDescendantAfterLeaderExit(t *testing.T) {
	testOuterProcessBoundary(t, false)
}

type outerTreePeer struct {
	Role string
	PID  int
	PGID int
}

func init() {
	role := os.Getenv("ORC_TEST_OUTER_TREE_ROLE")
	if role == "" {
		return
	}
	// Exercise forced shutdown rather than relying on the default SIGINT
	// action. The socket EOF path also lets fixture cleanup retire a survivor.
	signal.Ignore(syscall.SIGINT)
	conn, err := net.Dial("tcp", os.Getenv("ORC_TEST_OUTER_TREE_ADDRESS"))
	if err != nil {
		os.Exit(90)
	}
	if err := json.NewEncoder(conn).Encode(outerTreePeer{role, os.Getpid(), syscall.Getpgrp()}); err != nil {
		os.Exit(91)
	}
	next := map[string]string{"worker": "child", "child": "grandchild"}[role]
	if next != "" {
		command := exec.Command(os.Args[0])
		command.Env = setChildEnvironment(os.Environ(), "ORC_TEST_OUTER_TREE_ROLE", next)
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		if err := command.Start(); err != nil {
			os.Exit(92)
		}
	}
	for {
		var instruction [1]byte
		if _, err := io.ReadFull(conn, instruction[:]); err != nil {
			os.Exit(0)
		}
		if instruction[0] == 'e' {
			os.Exit(7)
		}
		if instruction[0] != 'w' {
			os.Exit(93)
		}
		file, err := os.OpenFile(os.Getenv("ORC_TEST_OUTER_TREE_WRITES"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			os.Exit(94)
		}
		_, writeErr := fmt.Fprintln(file, role)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			os.Exit(95)
		}
		fmt.Fprintln(os.Stdout, role)
		if _, err := conn.Write([]byte{'w'}); err != nil {
			os.Exit(96)
		}
	}
}

func testOuterProcessBoundary(t *testing.T, stop bool) {
	t.Helper()
	// Isolate Linux subreaper adoption from all other tests. Darwin delegates
	// orphan reaping to launchd and uses retained kqueue exit observations.
	if os.Getenv("ORC_TEST_OUTER_TREE_OWNER") != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$")
		command.Env = setChildEnvironment(os.Environ(), "ORC_TEST_OUTER_TREE_OWNER", "1")
		configureChildProcess(command)
		command.Cancel = func() error { return forceChildStop(command.Process) }
		command.WaitDelay = time.Second
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("outer process fixture: %v\n%s", err, output)
		}
		return
	}
	prepareOuterTreeReaper(t)
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	writes := filepath.Join(t.TempDir(), "writes")
	t.Setenv("ORC_TEST_OUTER_TREE_ADDRESS", listener.Addr().String())
	t.Setenv("ORC_TEST_OUTER_TREE_WRITES", writes)
	t.Setenv("ORC_TEST_OUTER_TREE_ROLE", "worker")
	child, err := startRunChild(t.Context(), supervisorTestConfigPath(t), os.Args[0], supervisorTestWorker("tree", t.TempDir()), io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatal(err)
	}
	peers := map[string]net.Conn{}
	waits := map[string]func() error{}
	await := func(done <-chan struct{}, message string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal(message)
		}
	}
	// Cleanup is installed before any socket/read/wait barrier. Retire socket
	// controls, then join the production wait and known descendants before
	// TempDir removal. No recorded PID is required to disappear on a timer.
	t.Cleanup(func() {
		for _, conn := range peers {
			_ = conn.Close()
		}
		_ = child.forceStop()
		await(child.waited, "production child did not finish during fixture cleanup")
		for _, role := range []string{"child", "grandchild"} {
			if wait := waits[role]; wait != nil {
				done := make(chan struct{})
				go func() { defer close(done); _ = wait() }()
				await(done, "fixture descendant did not exit: "+role)
			}
		}
	})
	for range 3 {
		_ = listener.SetDeadline(time.Now().Add(5 * time.Second))
		conn, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		var peer outerTreePeer
		if err := json.NewDecoder(conn).Decode(&peer); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		peers[peer.Role] = conn
		if peer.PGID != child.cmd.Process.Pid {
			t.Fatalf("fixture escaped the outer group: %+v", peer)
		}
		if peer.Role != "worker" {
			waits[peer.Role] = outerTreeWaitOnce(observeOuterTreeExit(t, peer.PID))
		}
	}
	for _, role := range []string{"worker", "child", "grandchild"} {
		conn := peers[role]
		if conn == nil {
			t.Fatalf("missing fixture role %s", role)
		}
		if _, err := conn.Write([]byte{'w'}); err != nil {
			t.Fatal(err)
		}
		var ack [1]byte
		if _, err := io.ReadFull(conn, ack[:]); err != nil || ack[0] != 'w' {
			t.Fatalf("writer %s did not complete its write: %v", role, err)
		}
	}
	if child.boundaryResult().Confirmed {
		t.Fatal("running writers were reported confirmed stopped")
	}
	if stop {
		if err := child.requestStop(); err != nil {
			t.Fatal(err)
		}
		if err := child.forceStop(); err != nil {
			t.Fatal(err)
		}
	} else if _, err := peers["worker"].Write([]byte{'e'}); err != nil {
		t.Fatal(err)
	}
	await(child.waited, "outer production wait did not finish")
	var exit *exec.ExitError
	if !errors.As(child.exitResult(), &exit) || (!stop && exit.ExitCode() != 7) {
		t.Fatalf("lost worker failure outcome: %v", child.exitResult())
	}
	// The fixture's retained OS observations establish nonexecution independently
	// of stdout/socket EOF. Reap child before grandchild for Linux adoption.
	for _, role := range []string{"child", "grandchild"} {
		wait := waits[role]
		done := make(chan struct{})
		var waitErr error
		go func() { defer close(done); waitErr = wait() }()
		await(done, "descendant remained executable: "+role)
		delete(waits, role)
		if waitErr != nil {
			t.Fatal(waitErr)
		}
		// After the exit barrier this peer can no longer service a write command.
		_, _ = peers[role].Write([]byte{'w'})
		var ack [1]byte
		if _, err := io.ReadFull(peers[role], ack[:]); err == nil {
			t.Fatalf("stopped %s accepted another write", role)
		}
	}
	// The explicit SIGKILL can finish the group before post-exit cleanup
	// issues its redundant kill. Darwin may return EPERM for that request;
	// the later snapshot need not prove the earlier group's zombie state.
	// Accept that retained diagnostic only after our independent exit barriers
	// prove every fixture descendant stopped. It never confirms production
	// cleanup or suppresses permission failures in the containment code.
	result := child.boundaryResult()
	if result.Confirmed || result.Err != nil && !(runtime.GOOS == "darwin" && stop && errors.Is(result.Err, syscall.EPERM)) {
		t.Fatalf("group stop must remain explicitly uncertain: %+v", result)
	}
	if result.Err != nil {
		t.Logf("retained redundant post-exit cleanup diagnostic after fixture exits: %v", result.Err)
	}
	data, err := os.ReadFile(writes)
	if err != nil {
		t.Fatal(err)
	}
	if len(strings.Fields(string(data))) != 3 {
		t.Fatalf("unexpected writes across the completion barrier: %q", data)
	}
	// The released os.Process handle must refuse signals even if its stored
	// numeric process group is still present as unreaped descendant zombies.
	if err := child.containment.forceAfterLeaderExit(child.cmd.Process); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("post-reap containment did not refuse the old identity: %v", err)
	}
}

// Main assertions and failure cleanup join the same observation/reap. In
// particular, cleanup must not start a second wait after an assertion timeout.
func outerTreeWaitOnce(wait func() error) func() error {
	var once sync.Once
	done := make(chan struct{})
	var err error
	return func() error {
		once.Do(func() { go func() { err = wait(); close(done) }() })
		<-done
		return err
	}
}
