package supervisor

import "github.com/toolsupply/ticket-orc/internal/ticketclient"

// TicketTarget is the resolved Ticket routing mode used by application
// workers and repository observers.
type TicketTarget struct {
	Mode       TicketTargetMode
	Repository string
	Config     string
	Scope      string
}

type TicketTargetMode string

const (
	TicketTargetImplicit   TicketTargetMode = ""
	TicketTargetRepository TicketTargetMode = "repository"
	TicketTargetScoped     TicketTargetMode = "config_scope"
)

// ConfiguredRepository is the supervisor-owned view of one repository
// resource. Target comes from configuration; Info and ID are populated only
// after Ticket's info probe.
type ConfiguredRepository struct {
	Key    string
	ID     string
	Target TicketTarget
	Info   *ticketclient.RepositoryInfo
}

type RepositoryRegistry map[string]ConfiguredRepository

func CloneRepositoryRegistry(registry RepositoryRegistry) RepositoryRegistry {
	copy := make(RepositoryRegistry, len(registry))
	for key, repository := range registry {
		if repository.Info != nil {
			info := *repository.Info
			repository.Info = &info
		}
		copy[key] = repository
	}
	return copy
}
