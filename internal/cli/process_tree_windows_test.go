//go:build windows

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
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func init() {
	role := os.Getenv("ORC_TEST_WINDOWS_TREE_ROLE")
	if role == "" {
		return
	}
	conn, err := net.Dial("tcp", os.Getenv("ORC_TEST_WINDOWS_TREE_ADDRESS"))
	if err != nil {
		os.Exit(90)
	}
	if err := json.NewEncoder(conn).Encode(windowsTreeHello{role, os.Getpid()}); err != nil {
		os.Exit(91)
	}
	for {
		var instruction [1]byte
		if _, err := io.ReadFull(conn, instruction[:]); err != nil {
			os.Exit(0)
		}
		switch instruction[0] {
		case 's':
			next := map[string]string{"worker": "child", "child": "grandchild"}[role]
			if next != "" {
				cmd := exec.Command(os.Args[0])
				cmd.Env = setChildEnvironment(os.Environ(), "ORC_TEST_WINDOWS_TREE_ROLE", next)
				cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
				if err := cmd.Start(); err != nil {
					os.Exit(92)
				}
				_ = cmd.Process.Release()
			}
			if os.Getenv("ORC_TEST_WINDOWS_TREE_CLOSE_OUTPUT") == "1" {
				_ = os.Stdout.Close()
				_ = os.Stderr.Close()
			}
		case 'w':
			file, err := os.OpenFile(os.Getenv("ORC_TEST_WINDOWS_TREE_WRITES"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				os.Exit(93)
			}
			_, err = fmt.Fprintln(file, role)
			closeErr := file.Close()
			if err != nil || closeErr != nil {
				os.Exit(94)
			}
		case 'e':
			os.Exit(0)
		case 'x':
			os.Exit(7)
		default:
			os.Exit(95)
		}
		if _, err := conn.Write(instruction[:]); err != nil {
			os.Exit(96)
		}
	}
}

type windowsTreeHello struct {
	Role string
	PID  int
}

type windowsTreePeer struct {
	conn   net.Conn
	pid    int
	handle syscall.Handle
}

func windowsTreeListener(t *testing.T) (*net.TCPListener, string) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	writes := filepath.Join(t.TempDir(), "writes")
	t.Setenv("ORC_TEST_WINDOWS_TREE_ROLE", "worker")
	t.Setenv("ORC_TEST_WINDOWS_TREE_ADDRESS", listener.Addr().String())
	t.Setenv("ORC_TEST_WINDOWS_TREE_WRITES", writes)
	return listener, writes
}

// Retain handles before sending any spawn/exit/stop command. Cleanup is installed
// before accepting and releases all barriers before joining any process. A PID
// is never reopened after termination, and an OpenProcess error never means dead.
func windowsTreePeers(t *testing.T, listener *net.TCPListener) map[string]windowsTreePeer {
	t.Helper()
	peers := map[string]windowsTreePeer{}
	var connections []net.Conn
	t.Cleanup(func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
		for role, peer := range peers {
			stopped, err := windowsProcessStopped(peer.handle, uint32(runChildStopTimeout/time.Millisecond))
			if err != nil || !stopped {
				t.Errorf("join %s PID %d: stopped=%v error=%v", role, peer.pid, stopped, err)
			}
			if err := syscall.CloseHandle(peer.handle); err != nil {
				t.Errorf("close %s PID %d handle: %v", role, peer.pid, err)
			}
		}
	})
	for range 3 {
		_ = listener.SetDeadline(time.Now().Add(runChildStopTimeout))
		conn, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		_ = conn.SetDeadline(time.Now().Add(runChildStopTimeout))
		var hello windowsTreeHello
		if err := json.NewDecoder(conn).Decode(&hello); err != nil {
			t.Fatal(err)
		}
		if hello.PID <= 0 || peers[hello.Role].handle != 0 || (hello.Role != "worker" && hello.Role != "child" && hello.Role != "grandchild") {
			t.Fatalf("invalid or duplicate fixture peer: %+v", hello)
		}
		handle, err := syscall.OpenProcess(syscall.SYNCHRONIZE|syscall.PROCESS_QUERY_INFORMATION, false, uint32(hello.PID))
		if err != nil {
			t.Fatalf("OpenProcess for held %s PID %d: %v", hello.Role, hello.PID, err)
		}
		peers[hello.Role] = windowsTreePeer{conn, hello.PID, handle}
		windowsTreeCommand(t, conn, 's')
	}
	for _, role := range []string{"worker", "child", "grandchild"} {
		if peers[role].handle == 0 {
			t.Fatalf("missing %s handshake", role)
		}
	}
	return peers
}

func windowsTreeCommand(t *testing.T, conn net.Conn, command byte) {
	t.Helper()
	if _, err := conn.Write([]byte{command}); err != nil {
		t.Fatal(err)
	}
	var ack [1]byte
	if _, err := io.ReadFull(conn, ack[:]); err != nil || ack[0] != command {
		t.Fatalf("fixture command %q: ack=%q error=%v", command, ack, err)
	}
}

func windowsTreeInJob(t *testing.T, peer windowsTreePeer, job syscall.Handle) bool {
	t.Helper()
	var inJob int32
	if ok, _, err := kernel32.NewProc("IsProcessInJob").Call(uintptr(peer.handle), uintptr(job), uintptr(unsafe.Pointer(&inJob))); ok == 0 {
		t.Fatal(err)
	}
	return inJob != 0
}

// One actual outer worker/child/grandchild scenario covers ordinary exit,
// failure and supervisor stop. Children inherit the ordinary outer job. The
// process-handle waits, rather than job accounting or pipe EOF, establish the
// fixture's no-further-work milestone. Production remains conservatively
// uncertain because its uncontrolled startup can create earlier descendants.
func TestWindowsOuterWorkerBoundary(t *testing.T) {
	for _, mode := range []string{"success", "abnormal", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			listener, writes := windowsTreeListener(t)
			if mode == "success" {
				t.Setenv("ORC_TEST_WINDOWS_TREE_CLOSE_OUTPUT", "1")
			}
			child, err := startRunChild(context.Background(), supervisorTestConfigPath(t), os.Args[0], supervisorTestWorker("tree", t.TempDir()), io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
			if err != nil {
				t.Fatal(err)
			}
			stopRunChildAtTestCleanup(t, child)
			peers := windowsTreePeers(t, listener)
			for role, peer := range peers {
				if !windowsTreeInJob(t, peer, child.containment.handle) {
					t.Fatalf("%s escaped the outer worker job", role)
				}
				if stopped, err := windowsProcessStopped(peer.handle, 0); stopped || err != nil {
					t.Fatalf("held %s: stopped=%v error=%v", role, stopped, err)
				}
				windowsTreeCommand(t, peer.conn, 'w')
			}
			if mode == "shutdown" {
				if err := child.requestStop(); err != nil {
					t.Fatal(err)
				}
			} else {
				command := byte('e')
				if mode == "abnormal" {
					command = 'x'
				}
				if _, err := peers["worker"].conn.Write([]byte{command}); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-child.waited:
			case <-time.After(runChildStopTimeout):
				t.Fatal("outer worker wait blocked on descendant output")
			}
			if result := child.boundaryResult(); result.Confirmed || result.Err != nil {
				t.Fatalf("non-atomic launch must remain uncertain without inventing a failure: %+v", result)
			}
			if mode == "success" && child.exitResult() != nil {
				t.Fatal(child.exitResult())
			}
			if mode == "abnormal" {
				var exit *exec.ExitError
				if !errors.As(child.exitResult(), &exit) || exit.ExitCode() != 7 {
					t.Fatalf("lost worker exit: %v", child.exitResult())
				}
			}
			for role, peer := range peers {
				stopped, err := windowsProcessStopped(peer.handle, uint32(runChildStopTimeout/time.Millisecond))
				if err != nil || !stopped {
					t.Fatalf("%s still able to execute: stopped=%v error=%v", role, stopped, err)
				}
			}
			data, err := os.ReadFile(writes)
			if err != nil || len(strings.Fields(string(data))) != 3 {
				t.Fatalf("unexpected writes after termination barrier: %q error=%v", data, err)
			}
		})
	}
}

// Deliberately hold the same Start-then-Assign interval open until the worker
// has made its descendants. Assignment is not retroactive. This is why even a
// successfully terminated outer job cannot authorize automatic claim release.
func TestWindowsOuterJobDoesNotContainPreassignmentChildren(t *testing.T) {
	listener, _ := windowsTreeListener(t)
	cmd := exec.Command(os.Args[0])
	configureChildProcess(cmd)
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	peers := windowsTreePeers(t, listener)
	containment, err := attachChildContainment(cmd.Process)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = containment.close() })
	for role, peer := range peers {
		if got := windowsTreeInJob(t, peer, containment.handle); got != (role == "worker") {
			t.Fatalf("unexpected retroactive job assignment for %s: %v", role, got)
		}
	}
	if err := containment.force(cmd.Process); err != nil {
		t.Fatal(err)
	}
	if stopped, err := windowsProcessStopped(peers["worker"].handle, uint32(runChildStopTimeout/time.Millisecond)); err != nil || !stopped {
		t.Fatalf("worker job termination: stopped=%v error=%v", stopped, err)
	}
	for _, role := range []string{"child", "grandchild"} {
		peer := peers[role]
		if stopped, err := windowsProcessStopped(peer.handle, 0); stopped || err != nil {
			t.Fatalf("preassignment %s should survive job termination: stopped=%v error=%v", role, stopped, err)
		}
		windowsTreeCommand(t, peer.conn, 'w')
	}
}

func TestWindowsBoundaryOperationErrors(t *testing.T) {
	if stopped, err := windowsProcessStopped(0, 0); stopped || !errors.Is(err, syscall.Errno(6)) { // ERROR_INVALID_HANDLE
		t.Fatalf("invalid handle established observation: stopped=%v error=%v", stopped, err)
	}
	if err := (childContainment{handle: syscall.Handle(0x12345678)}).force(nil); !errors.Is(err, syscall.Errno(6)) {
		t.Fatalf("job termination failure was lost: %v", err)
	}
	failed := &runChild{cmd: &exec.Cmd{}, containment: childContainment{handle: syscall.Handle(0x12345678)}}
	if err := failed.forceStop(); !errors.Is(err, syscall.Errno(6)) || !errors.Is(failed.boundaryResult().Err, syscall.Errno(6)) || failed.boundaryResult().Confirmed {
		t.Fatalf("job failure became a benign late stop: error=%v result=%+v", err, failed.boundaryResult())
	}
	previous := assignChildToJob
	sentinel := errors.New("assignment rejected")
	assignChildToJob = func(syscall.Handle, syscall.Handle) error { return sentinel }
	t.Cleanup(func() { assignChildToJob = previous })
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", t.TempDir())
	child, err := startRunChild(context.Background(), supervisorTestConfigPath(t), os.Args[0], supervisorTestWorker("assignment", t.TempDir()), io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
	if child != nil || !errors.Is(err, sentinel) {
		t.Fatalf("failed assignment authorized a worker: child=%v error=%v", child, err)
	}
}
