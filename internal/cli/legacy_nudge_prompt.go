package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

// These exact prompts were shipped in the built-in config and documented
// README example before dynamic nudges became one-ticket turns. Keep this
// recognition finite: custom prompt text is operator-owned.
const (
	legacyCoderNudgePrompt          = "[ticket-orc] Coding work is available. Process all actionable Ticket work for your current actor until none remains. Before submitting each ticket, review your changes against the objective and acceptance criteria and run the relevant checks. Fix obvious issues before handing it to review. Set a goal before starting."
	legacyReviewerNudgePrompt       = "[ticket-orc] Review work is available. Perform a substantive review of all actionable Ticket work for your current actor until none remains. Assume the coder has already run the routine/basic test suite; do not rerun the same tests by default. Focus on specification compliance, correctness, edge cases, integration behavior, error handling, and code quality. Run targeted tests only when they help investigate a specific concern. Set a goal before starting."
	legacyReadmeCoderNudgePrompt    = "Coding work is available. Process actionable Ticket work for your current actor until none remains."
	legacyReadmeReviewerNudgePrompt = "Review work is available. Perform substantive review of actionable Ticket work for your current actor until none remains."
)

func obsoleteNudgePromptDiagnostic(config FileConfig, instance InstanceContext, lookupEnv envLookup) ConfigDiagnostics {
	roles := make([]string, 0)
	for name, role := range config.Roles {
		if knownObsoleteNudgePrompt(role.TicketQueue, role.NudgePrompt) {
			roles = append(roles, "roles."+name+".nudge_prompt")
		}
	}
	if len(roles) == 0 {
		return nil
	}
	sort.Strings(roles)
	return ConfigDiagnostics{{
		Severity:    DiagnosticWarning,
		Code:        "run.obsolete_nudge_prompt",
		Path:        instance.ConfigPath,
		Message:     "obsolete dynamic nudge prompt found in " + strings.Join(roles, ", "),
		Remediation: obsoleteNudgePromptRemediation(instance, lookupEnv),
	}}
}

func knownObsoleteNudgePrompt(queue, prompt string) bool {
	prompt = strings.TrimSpace(prompt)
	switch queue {
	case "open":
		return prompt == legacyCoderNudgePrompt || prompt == legacyReadmeCoderNudgePrompt
	case "review":
		return prompt == legacyReviewerNudgePrompt || prompt == legacyReadmeReviewerNudgePrompt
	default:
		return false
	}
}

func obsoleteNudgePromptRemediation(instance InstanceContext, lookupEnv envLookup) string {
	manual := "update only the affected nudge_prompt field manually"
	reset := ""
	if pathContainsControl(instance.InstanceDir) {
		reset = "stop Orc; this instance directory contains control characters, so no reset target is suggested; " + manual
	} else if instance.Explicit && filepath.Base(instance.ConfigPath) != instanceConfigFileName {
		reset = "ticket-orc init --force writes config.json and will not replace this explicitly named file; " + manual
	} else if home, err := os.UserHomeDir(); err == nil && filepath.Clean(instance.ConfigPath) == filepath.Join(home, defaultInstanceDirectoryName, instanceConfigFileName) {
		reset = "stop Orc, then run ticket-orc init --global --force for this global instance"
	} else if instance.Explicit {
		reset = targetedInstanceResetAdvice()
	} else if configured, ok := lookupInstanceEnv(lookupEnv); ok && strings.TrimSpace(configured) != "" {
		reset = targetedInstanceResetAdvice()
	} else if !instance.Explicit && filepath.Base(instance.ConfigPath) == instanceConfigFileName {
		reset = "stop Orc, then run ticket-orc init --force from the project root that selected this local instance"
	} else if filepath.Base(instance.ConfigPath) == instanceConfigFileName {
		reset = targetedInstanceResetAdvice()
	} else {
		reset = "ticket-orc init --force writes config.json and will not replace this explicitly named file; " + manual
	}
	return reset + ". Dynamic turns now handle at most one ticket. A reset replaces that instance's config.json, generates a new instance ID, and removes its owned runtime state; back up customizations first, or " + manual + "."
}

func targetedInstanceResetAdvice() string {
	return "stop Orc, set TICKET_ORC to the directory containing the loaded config file above, then run ticket-orc init --force"
}

func pathContainsControl(value string) bool {
	return strings.ContainsFunc(value, unicode.IsControl)
}

func lookupInstanceEnv(lookupEnv envLookup) (string, bool) {
	if lookupEnv == nil {
		return "", false
	}
	return lookupEnv("TICKET_ORC")
}

func renderObsoleteNudgePromptWarning(out io.Writer, path, message, remediation string) {
	_, _ = fmt.Fprintf(out, "[ticket-orc] config warning: %s: %s\n", escapeObsoleteNudgeWarningControls(path), escapeObsoleteNudgeWarningControls(message))
	_, _ = fmt.Fprintf(out, "[ticket-orc] %s\n", escapeObsoleteNudgeWarningControls(remediation))
}

func escapeObsoleteNudgeWarningControls(value string) string {
	var builder strings.Builder
	for _, char := range value {
		if unicode.IsControl(char) {
			if char <= 0xffff {
				fmt.Fprintf(&builder, "\\u%04x", char)
			} else {
				fmt.Fprintf(&builder, "\\U%08x", char)
			}
			continue
		}
		builder.WriteRune(char)
	}
	return builder.String()
}
