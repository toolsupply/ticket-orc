package cli

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
)

type ticketRoutingIdentity struct {
	Actor          string
	RepositoryID   string
	RepositoryPath string
	RepositoryName string
}

type steerEndpointIdentity struct {
	Harness   string
	SessionID string
	Transport state.SteerTransportRoute
}

type currentTicketIdentity struct {
	ticketRoutingIdentity
	steerEndpointIdentity
}

type steerEndpointSelector struct {
	Harness   string
	SessionID string
	Transport string
	Explicit  bool
}

type steerEndpointDescriptor struct {
	Kind     string `json:"kind"`
	Protocol int    `json:"protocol,omitempty"`
	Root     string `json:"root,omitempty"`
	Pending  string `json:"pending,omitempty"`
	Control  string `json:"control,omitempty"`
	Rejected string `json:"rejected,omitempty"`
}

func descriptorFor(endpoint steertransport.Endpoint) steerEndpointDescriptor {
	return steerEndpointDescriptor{Kind: endpoint.Kind, Protocol: endpoint.Protocol, Root: endpoint.Root,
		Pending: endpoint.Pending, Control: endpoint.Control, Rejected: endpoint.Rejected}
}

// extractEndpointSelector removes generic endpoint flags and requires all
// three together whenever any is supplied.
func extractEndpointSelector(args []string) ([]string, steerEndpointSelector, error) {
	remaining := make([]string, 0, len(args))
	values := map[string]string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "--") {
			remaining = append(remaining, arg)
			continue
		}
		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if name != "harness" && name != "session" && name != "transport" {
			remaining = append(remaining, arg)
			continue
		}
		if !hasValue {
			i++
			if i >= len(args) {
				return nil, steerEndpointSelector{}, fmt.Errorf("flag --%s requires a value", name)
			}
			value = args[i]
		}
		if _, exists := values[name]; exists {
			return nil, steerEndpointSelector{}, fmt.Errorf("duplicate flag --%s", name)
		}
		if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
			return nil, steerEndpointSelector{}, fmt.Errorf("--%s must be non-empty and trimmed", name)
		}
		values[name] = value
	}
	selector := steerEndpointSelector{Harness: values["harness"], SessionID: values["session"], Transport: values["transport"], Explicit: len(values) != 0}
	if !selector.Explicit {
		return remaining, selector, nil
	}
	if err := validateEndpointSelector(selector); err != nil {
		return nil, steerEndpointSelector{}, err
	}
	return remaining, selector, nil
}

func validateEndpointSelector(selector steerEndpointSelector) error {
	if selector.Harness == "" || selector.SessionID == "" || selector.Transport == "" {
		return errors.New("--harness, --session, and --transport must be supplied together")
	}
	if !validHarnessLabel(selector.Harness) || !validSessionLabel(selector.SessionID) {
		return errors.New("invalid steering harness or session")
	}
	if selector.Transport != "spool" {
		return fmt.Errorf("unsupported explicit steering transport %q (want spool)", selector.Transport)
	}
	return nil
}

func validHarnessLabel(value string) bool {
	if value == "" || len(value) > 64 || value == "." || value == ".." || strings.TrimSpace(value) != value {
		return false
	}
	for index, char := range value {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || (index > 0 && (char == '.' || char == '_' || char == '-'))) {
			return false
		}
	}
	return true
}

func validSessionLabel(value string) bool {
	if value == "" || len(value) > 4096 || strings.TrimSpace(value) != value {
		return false
	}
	return strings.IndexFunc(value, unicode.IsControl) < 0
}

func validSteerIncarnationID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}
