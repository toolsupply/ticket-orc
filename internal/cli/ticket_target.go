package cli

import (
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// ticketTarget returns the resolved target used by every Ticket client and
// child environment. The Repository fallback keeps manually constructed
// supervisor.RoleConfig values compatible with the pre-target configuration boundary.
func ticketTarget(config supervisor.RoleConfig) ticketclient.Target {
	target := ticketclient.Target{
		Repository: config.Ticket.Repository,
		Config:     config.Ticket.Config,
		Scope:      config.Ticket.Scope,
	}
	if target.Repository == "" && target.Config == "" && target.Scope == "" {
		target.Repository = config.Repository
	}
	return target
}
