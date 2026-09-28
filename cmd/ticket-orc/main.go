// Command ticket-orc is the thin process entry point for the orchestration CLI.
package main

import (
	"os"

	"github.com/toolsupply/ticket-orc/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
