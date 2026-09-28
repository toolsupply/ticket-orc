//go:build windows

package cli

import (
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

// Windows job objects terminate descendants after the leader is reaped, so
// the Unix pre-reap identity window has no work to do on this platform.
func waitForChildExitBeforeReap(_ *os.Process) bool { return false }

// processAlive is only reached from the Darwin-specific fallback branch in
// startRunChild. The runtime guard is not enough to hide the symbol from the
// Windows compiler, so keep the platform stub false where that branch cannot
// execute.
func processAlive(_ *os.Process) bool { return false }

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
	if result, _, assignErr := assignProcessToJobObject.Call(job, uintptr(processHandle)); result == 0 {
		_ = containment.close()
		return childContainment{}, assignErr
	}
	return containment, nil
}

func (containment childContainment) force(process *os.Process) error {
	if containment.handle != 0 {
		if result, _, err := terminateJobObject.Call(uintptr(containment.handle), 1); result != 0 {
			return nil
		} else if err != syscall.Errno(0) {
			return err
		}
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
