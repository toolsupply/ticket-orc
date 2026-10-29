package attachments

import (
	"os"
	"syscall"
)

// os.Root rejects escaping reparse traversal; Lstat/open identity checks reject
// in-root redirection. Native handle metadata supplies the hardlink count.
// os.Root.Rename uses os.Rename, which does not guarantee atomic publication
// on Windows. Keep mutations unavailable until a safe native primitive exists.
const safeMutationSupported = false

const safeOpenFlags = 0

func redirected(info os.FileInfo) bool {
	attributes, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return !ok || attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

func checkRegular(file *os.File, info os.FileInfo) error {
	var native syscall.ByHandleFileInformation
	if !info.Mode().IsRegular() || syscall.GetFileInformationByHandle(syscall.Handle(file.Fd()), &native) != nil || native.NumberOfLinks != 1 || native.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return failure("unsafe_attachment")
	}
	return nil
}

// This mutation-only durability primitive is unavailable with publication.
func syncDir(_ *os.File) error { return failure("capability_unavailable") }
