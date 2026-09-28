package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const repositoryProbeActor = "ticket-orc"

func resolveConfiguredRepositories(config FileConfig) (supervisor.RepositoryRegistry, error) {
	registry := make(supervisor.RepositoryRegistry, len(config.Repositories))
	names := make([]string, 0, len(config.Repositories))
	for name := range config.Repositories {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := config.Repositories[name]
		if strings.TrimSpace(entry.Repository) == "" && (strings.TrimSpace(entry.Ticket.Config) == "" || strings.TrimSpace(entry.Ticket.Scope) == "") {
			return nil, configValidation("repository.target_required", "repositories."+name, "repository must configure a direct target or ticket config and scope", "set repository or both ticket.config and ticket.scope")
		}
		target, err := resolveTicketTarget(entry.Repository, entry.Ticket)
		if err != nil {
			return nil, configValidationDetail("repository.target_invalid", "repositories."+name, "repository target is invalid", "configure a direct repository or ticket config and scope", err.Error())
		}
		registry[name] = supervisor.ConfiguredRepository{Key: name, Target: target}
	}
	return registry, nil
}

func cloneRepositoryRegistry(registry supervisor.RepositoryRegistry) supervisor.RepositoryRegistry {
	return supervisor.CloneRepositoryRegistry(registry)
}

// repositoryIdentity returns Ticket's stable ID; info.Path is diagnostic and
// routing metadata, not repository identity.
func repositoryIdentity(info ticketclient.RepositoryInfo) string {
	identity := strings.TrimSpace(info.ID)
	if !ticketclient.ValidRepositoryID(identity) {
		return ""
	}
	return identity
}

func validateResolvedRepositories(registry supervisor.RepositoryRegistry) error {
	seen := make(map[string]string, len(registry))
	keys := make([]string, 0, len(registry))
	for key := range registry {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		repository := registry[key]
		if repository.Info == nil {
			return fmt.Errorf("repository %q has not been resolved through Ticket", key)
		}
		identity := repositoryIdentity(*repository.Info)
		if identity == "" {
			return fmt.Errorf("repository %q returned no stable Ticket repository ID", key)
		}
		if repository.ID != "" && repository.ID != identity {
			return fmt.Errorf("repository %q ID does not match Ticket's reported repository ID", key)
		}
		if previous, ok := seen[identity]; ok {
			return configValidationDetail("repository.duplicate", "repositories."+key, "configured repositories resolve to the same Ticket repository", "remove the duplicate repository entry", previous+" and "+key+" resolve to "+identity)
		}
		seen[identity] = key
	}
	return nil
}

func repositoryRegistryByID(registry supervisor.RepositoryRegistry) map[string]supervisor.ConfiguredRepository {
	byID := make(map[string]supervisor.ConfiguredRepository, len(registry))
	for _, repository := range registry {
		if repository.ID != "" {
			byID[repository.ID] = repository
		}
	}
	return byID
}

type repositoryProbe func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error)

func probeConfiguredRepository(ctx context.Context, repository supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
	return ticketclient.ProbeInfo(ctx, repositoryProbeActor, "", ticketTargetFromConfiguredRepository(repository))
}

func ticketTargetFromConfiguredRepository(repository supervisor.ConfiguredRepository) ticketclient.Target {
	return ticketclient.Target{Repository: repository.Target.Repository, Config: repository.Target.Config, Scope: repository.Target.Scope}
}

func repositoryKeys(registry supervisor.RepositoryRegistry) []string {
	keys := make([]string, 0, len(registry))
	for key := range registry {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
