package cli

import (
	"fmt"
	"strings"
)

var roleFlags = map[string]struct{}{
	"config":                        {},
	"worker":                        {},
	"harness":                       {},
	"actor":                         {},
	"model":                         {},
	"reasoning":                     {},
	"max-bounces":                   {},
	"session-policy":                {},
	"session-cleanup":               {},
	"minimum-reuse-context-percent": {},
	"review-completion":             {},
	"codex-sandbox":                 {},
	"pi-provider":                   {},
	"claude-permission-mode":        {},
	"output":                        {},
	"ticket-prompt":                 {},
}

func parseRoleFlags(args []string) (map[string]string, bool, error) {
	values := make(map[string]string)
	help := false

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-h" || arg == "--help" {
			if help {
				return nil, false, fmt.Errorf("duplicate flag --help")
			}
			help = true
			continue
		}
		if arg == "--" {
			if i != len(args)-1 {
				return nil, false, fmt.Errorf("%s accepts no positional arguments", args[i+1])
			}
			break
		}
		if arg == "-c" || strings.HasPrefix(arg, "-c=") {
			value := strings.TrimPrefix(arg, "-c=")
			if arg == "-c" {
				i++
				if i >= len(args) {
					return nil, false, fmt.Errorf("flag -c requires a value")
				}
				value = args[i]
			}
			if _, duplicate := values["config"]; duplicate {
				return nil, false, fmt.Errorf("duplicate flag --config")
			}
			if value == "" {
				return nil, false, fmt.Errorf("-c must not be empty")
			}
			values["config"] = value
			continue
		}
		if !strings.HasPrefix(arg, "--") {
			return nil, false, fmt.Errorf("unexpected argument: %s", arg)
		}

		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if _, ok := roleFlags[name]; !ok {
			return nil, false, fmt.Errorf("unknown flag --%s", name)
		}
		if _, duplicate := values[name]; duplicate {
			return nil, false, fmt.Errorf("duplicate flag --%s", name)
		}
		if !hasValue {
			i++
			if i >= len(args) {
				return nil, false, fmt.Errorf("flag --%s requires a value", name)
			}
			value = args[i]
		}
		values[name] = value
	}

	return values, help, nil
}
