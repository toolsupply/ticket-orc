//go:build windows

package spool

import "os"

// Windows permission privacy comes from the private Orc instance ACL; mode
// bits do not represent that ACL.
func privateFileMode(os.FileInfo) bool { return true }

func securePrivateDirectory(string) error { return nil }

func privateDirectoryMode(os.FileInfo) bool { return true }
