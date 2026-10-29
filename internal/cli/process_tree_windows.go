//go:build windows

package cli

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	jobObjectExtendedLimitInformation = 9
	jobObjectLimitKillOnClose         = 0x2000
	processSetQuota                   = 0x0100
)

type childContainment struct {
	handle syscall.Handle
}

// Observe leader exit without closing its stdout pipe. Conversation execution
// can then stop the job descendants before draining output and calling cmd.Wait.
func waitForChildExitBeforeReap(process *os.Process) error {
	if process == nil {
		return os.ErrProcessDone
	}
	handle, err := syscall.OpenProcess(syscall.SYNCHRONIZE, false, uint32(process.Pid))
	if err != nil {
		return err
	}
	defer syscall.CloseHandle(handle)
	stopped, err := windowsProcessStopped(handle, syscall.INFINITE)
	if err != nil {
		return err
	}
	if !stopped {
		return errors.New("child termination wait timed out")
	}
	return nil
}

// Only a signaled retained process handle establishes termination of that exact
// process. Timeout is an observation of an unsignaled handle; errors establish
// neither liveness nor death. Retain the actual result and system diagnostic.
func windowsProcessStopped(handle syscall.Handle, timeout uint32) (bool, error) {
	event, err := syscall.WaitForSingleObject(handle, timeout)
	if err != nil {
		return false, fmt.Errorf("WaitForSingleObject result=%#x: %w", event, err)
	}
	switch event {
	case syscall.WAIT_OBJECT_0:
		return true, nil
	case syscall.WAIT_TIMEOUT:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected process wait result=%#x", event)
	}
}

type windowsJobBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type windowsIOCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type windowsJobExtendedLimitInformation struct {
	BasicInfo             windowsJobBasicLimitInformation
	IOInfo                windowsIOCounters
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	createJobObject          = kernel32.NewProc("CreateJobObjectW")
	setInformationJobObject  = kernel32.NewProc("SetInformationJobObject")
	assignProcessToJobObject = kernel32.NewProc("AssignProcessToJobObject")
	terminateJobObject       = kernel32.NewProc("TerminateJobObject")
	closeKernelHandle        = kernel32.NewProc("CloseHandle")
)

// Keep the assignment syscall controllable so the launch interval and its
// failure path can be tested with real children held behind explicit barriers.
var assignChildToJob = func(job, process syscall.Handle) error {
	if result, _, err := assignProcessToJobObject.Call(uintptr(job), uintptr(process)); result == 0 {
		return fmt.Errorf("AssignProcessToJobObject: %w", err)
	}
	return nil
}

// A successful attachment is containment from this point forward, not proof
// of ownership of earlier descendants. exec.Cmd.Start has already allowed the
// worker to execute. Keep processBoundaryResult uncertain for this launch path;
// neither empty-job accounting nor a successful TerminateJobObject could prove
// that a child created before assignment stopped. Automatic claim release must
// remain disabled until creation and job assignment are atomic.
// https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject
func attachChildContainment(process *os.Process) (childContainment, error) {
	if process == nil {
		return childContainment{}, fmt.Errorf("worker process is unavailable")
	}
	job, _, createErr := createJobObject.Call(0, 0)
	if job == 0 {
		return childContainment{}, createErr
	}
	containment := childContainment{handle: syscall.Handle(job)}
	limits := windowsJobExtendedLimitInformation{}
	limits.BasicInfo.LimitFlags = jobObjectLimitKillOnClose
	if result, _, setErr := setInformationJobObject.Call(job, jobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), unsafe.Sizeof(limits)); result == 0 {
		_ = containment.close()
		return childContainment{}, setErr
	}
	processHandle, openErr := syscall.OpenProcess(syscall.PROCESS_TERMINATE|syscall.PROCESS_QUERY_INFORMATION|processSetQuota, false, uint32(process.Pid))
	if openErr != nil {
		_ = containment.close()
		return childContainment{}, openErr
	}
	defer syscall.CloseHandle(processHandle)
	if assignErr := assignChildToJob(syscall.Handle(job), processHandle); assignErr != nil {
		_ = containment.close()
		return childContainment{}, assignErr
	}
	return containment, nil
}

func (containment childContainment) force(process *os.Process) error {
	if containment.handle != 0 {
		if result, _, err := terminateJobObject.Call(uintptr(containment.handle), 1); result == 0 {
			// Still try to stop the leader, but never erase the job failure
			// merely because that fallback succeeded.
			stopErr := forceChildStop(process)
			if errors.Is(stopErr, os.ErrProcessDone) {
				stopErr = nil // The job failure must not look like a benign late stop.
			}
			return errors.Join(fmt.Errorf("TerminateJobObject: %w", err), stopErr)
		}
		return nil // Request accepted, not descendant completion.
	}
	return forceChildStop(process)
}

func (containment childContainment) forceAfterLeaderExit(process *os.Process) error {
	return containment.force(process)
}

func (containment childContainment) request(process *os.Process) error {
	return requestChildStop(process)
}

func (containment childContainment) close() error {
	if containment.handle == 0 {
		return nil
	}
	if result, _, err := closeKernelHandle.Call(uintptr(containment.handle)); result == 0 {
		return err
	}
	return nil
}
