//go:build windows

package cli

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Windows inherits the user's profile ACL. POSIX mode bits do not provide
// privacy there, so only filesystem structure is checked.
func secureInstanceDirectory(string) error          { return nil }
func instanceDirectoryModeMatches(os.FileInfo) bool { return true }

func secureInstanceFile(file *os.File, path string, _ os.FileMode) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("config path is not a regular file: %s", path)
	}
	return nil
}

func verifyInstanceFile(path string, _ os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("config path is not a regular file: %s", path)
	}
	return nil
}

var procMoveFileExW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func replaceInitConfig(replacement, target string) error {
	replacementUTF16, err := syscall.UTF16PtrFromString(replacement)
	if err != nil {
		return err
	}
	targetUTF16, err := syscall.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	const moveFileReplaceExisting = 0x1
	const moveFileWriteThrough = 0x8
	result, _, callErr := procMoveFileExW.Call(
		uintptr(unsafe.Pointer(replacementUTF16)),
		uintptr(unsafe.Pointer(targetUTF16)),
		moveFileReplaceExisting|moveFileWriteThrough,
	)
	if result == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.GetLastError()
		}
		return callErr
	}
	return nil
}
