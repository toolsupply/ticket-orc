package terminaltext

import "strings"

// DiagnosticDetailBytes is the maximum retained explanatory text, in bytes.
// Diagnostic applies the existing marker-based diagnostic policy, not a
// guarantee that arbitrary free text is secret-free. Structured context must
// remain useful when text is withheld. Oversized input is withheld wholesale
// so truncation cannot hide a privacy marker beyond the retained prefix.
const DiagnosticDetailBytes = 2048

func Diagnostic(value string) string {
	if len(value) > DiagnosticDetailBytes {
		return "diagnostic detail redacted (truncated: exceeds 2048 bytes)"
	}
	value = strings.TrimSpace(Sanitize(value, false))
	lower := strings.ToLower(value)
	for _, marker := range []string{"authorization", "bearer", "token", "password", "secret", "api_key", "api key", "access_key", "access-key", "session_id", "session id", "api-key", "apikey", "credential", "prompt", "argv", "environment", "session content", "[ticket-orc-event]"} {
		if strings.Contains(lower, marker) {
			return "diagnostic detail redacted"
		}
	}
	// Paths and opaque targets are private. Withhold the text rather than
	// attempting to extract potentially sensitive fragments of a quoted path.
	for _, word := range strings.Fields(value) {
		if strings.ContainsAny(word, "/\\") || strings.Contains(word, "://") || strings.Contains(word, "=") {
			return "diagnostic detail redacted"
		}
	}
	return value
}
