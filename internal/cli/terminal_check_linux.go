package cli

import (
	"os"
	"syscall"
	"unsafe"
)

func isTerminalFile(file *os.File) bool {
	var attributes syscall.Termios
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, file.Fd(), uintptr(syscall.TCGETS), uintptr(unsafe.Pointer(&attributes)))
	return errno == 0
}
