//go:build !unix

package harness

import "os"

func processSignal(*os.ProcessState) int { return 0 }
