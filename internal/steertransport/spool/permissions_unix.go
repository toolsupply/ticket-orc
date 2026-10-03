//go:build unix

package spool

import "os"

func privateFileMode(info os.FileInfo) bool { return info.Mode().Perm() == 0o600 }

func securePrivateDirectory(path string) error { return os.Chmod(path, 0o700) }

func privateDirectoryMode(info os.FileInfo) bool { return info.Mode().Perm() == 0o700 }
