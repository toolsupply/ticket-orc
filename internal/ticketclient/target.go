package ticketclient

import (
	"fmt"
	"runtime"
	"strings"
)

const defaultExecutable = "ticket"

type environFunc func() []string

// Target selects the Ticket repository context for a child process. An empty
// target preserves Ticket's legacy implicit discovery from the inherited
// environment and working directory. Repository is mutually exclusive with
// the explicit Config/Scope pair.
type Target struct {
	Repository string
	Config     string
	Scope      string
}

// RepositoryInfo is the metadata returned by Ticket's info command. ID is the
// stable repository identity; Path is retained for diagnostics and process
// routing. Name and Scope are nil when the repository is unnamed or no scope
// was selected.
type RepositoryInfo struct {
	Path           string  `json:"path"`
	Name           *string `json:"name"`
	ID             string  `json:"id"`
	FormatVersion  int     `json:"format_version"`
	StorageVersion int     `json:"storage_version"`
	Scope          *string `json:"scope"`
}

// ValidRepositoryID reports whether value is the canonical lowercase UUIDv4
// used by Ticket for stable repository identity.
func ValidRepositoryID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || value[14] != '4' {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (i == 19 && r != '8' && r != '9' && r != 'a' && r != 'b') || (i != 19 && !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func withActor(base []string, actor string) []string {
	return ApplyTargetEnvironment(base, actor, Target{})
}

func withActorAndRepository(base []string, actor, repository string) []string {
	return ApplyTargetEnvironment(base, actor, Target{Repository: repository})
}

// ApplyTargetEnvironment returns a child environment with the actor and
// explicit target isolated from inherited Ticket selection variables.
// Implicit targets deliberately preserve repository and scope discovery for
// compatibility.
func ApplyTargetEnvironment(base []string, actor string, target Target) []string {
	env := make([]string, 0, len(base)+1)
	explicitRepository := strings.TrimSpace(target.Repository) != ""
	explicitScope := strings.TrimSpace(target.Config) != "" || strings.TrimSpace(target.Scope) != ""
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok && (sameEnvKey(key, "TICKET_ACTOR") ||
			((explicitRepository || explicitScope) && conflictingTargetEnvKey(key))) {
			continue
		}
		env = append(env, entry)
	}
	env = append(env, "TICKET_ACTOR="+actor)
	if explicitRepository {
		env = append(env, "TICKET_REPOSITORY="+target.Repository)
	} else if explicitScope {
		env = append(env, "TICKET_CONFIG="+target.Config, "TICKET_SCOPE="+target.Scope)
	}
	return env
}

func conflictingTargetEnvKey(key string) bool {
	return sameEnvKey(key, "TICKET_REPOSITORY") || sameEnvKey(key, "TICKET_CONFIG") || sameEnvKey(key, "TICKET_SCOPE") || sameEnvKey(key, "TICKET_ROOT")
}

func (target Target) validate() error {
	hasRepository := strings.TrimSpace(target.Repository) != ""
	hasConfig := strings.TrimSpace(target.Config) != ""
	hasScope := strings.TrimSpace(target.Scope) != ""
	if hasConfig != hasScope {
		return fmt.Errorf("ticket target config and scope must be configured together")
	}
	if hasRepository && (hasConfig || hasScope) {
		return fmt.Errorf("ticket target repository and config/scope are mutually exclusive")
	}
	return nil
}

func (target Target) args() []string {
	if strings.TrimSpace(target.Config) == "" {
		return nil
	}
	return []string{"--config", target.Config, "--scope", target.Scope}
}

func sameEnvKey(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}
