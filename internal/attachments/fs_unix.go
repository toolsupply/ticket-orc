//go:build linux || darwin

package attachments

import (
	"os"
	"syscall"
)

const safeMutationSupported = true

const safeOpenFlags = syscall.O_NOFOLLOW | syscall.O_NONBLOCK

func redirected(info os.FileInfo) bool { return info.Mode()&os.ModeSymlink != 0 }

func checkRegular(_ *os.File, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Nlink != 1 {
		return failure("unsafe_attachment")
	}
	return nil
}

func syncDir(f *os.File) error { return f.Sync() }
