// Package terminaltext contains safeguards for untrusted text rendered to a
// human terminal.
package terminaltext

import "strings"

// Sanitize removes terminal controls from untrusted human-readable text.
// Newlines are retained for multiline output; carriage returns and tabs are
// rendered as spaces so they cannot rewrite or reshape a terminal line.
// Ordinary Unicode, including CJK and emoji, is preserved. This deliberately
// matches Ticket's human rendering policy: bytes left after an escape byte are
// ordinary visible text rather than an executable terminal sequence.
func Sanitize(value string, multiline bool) string {
	var out strings.Builder
	for _, r := range value {
		switch {
		case r == '\n' && multiline:
			out.WriteRune(r)
		case r == '\n' || r == '\r' || r == '\t':
			out.WriteByte(' ')
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f):
			// Drop C0/C1 controls, ESC, BEL, and DEL. Any remaining bytes
			// from an escape sequence are ordinary visible text.
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}
