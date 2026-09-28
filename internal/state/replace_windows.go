//go:build windows

package state

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var procReplaceFileW = kernel32.NewProc("ReplaceFileW")

func publishReplace(target, replacement string) error {
	if _, err := os.Lstat(target); os.IsNotExist(err) {
		return os.Rename(replacement, target)
	} else if err != nil {
		return err
	}
	targetUTF16, err := syscall.UTF16FromString(target)
	if err != nil {
		return fmt.Errorf("encode target path: %w", err)
	}
	replacementUTF16, err := syscall.UTF16FromString(replacement)
	if err != nil {
		return fmt.Errorf("encode replacement path: %w", err)
	}
	result, _, callErr := procReplaceFileW.Call(
		uintptr(unsafe.Pointer(&targetUTF16[0])),
		uintptr(unsafe.Pointer(&replacementUTF16[0])),
		0,
		0,
		0,
		0,
	)
	if result == 0 {
		return fmt.Errorf("ReplaceFileW failed: %w", windowsLastError(callErr))
	}
	return nil
}

func windowsLastError(err error) error {
	if err == nil {
		return syscall.GetLastError()
	}
	return err
}
