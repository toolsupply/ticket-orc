package harness

// LogRecord is semantic provider output, before terminal formatting or truncation.
// Kind is message, diagnostic, command, or file_change. Only applicable fields
// are populated. Diagnostic levels are info, warning, or error; command phases
// are started or completed. A nil ExitCode means the provider did not report it.
type LogRecord struct {
	Kind     string       `json:"kind"`
	Text     string       `json:"text,omitempty"`
	Level    string       `json:"level,omitempty"`
	Code     string       `json:"code,omitempty"`
	Phase    string       `json:"phase,omitempty"`
	Command  string       `json:"command,omitempty"`
	ToolName string       `json:"tool_name,omitempty"`
	ExitCode *int         `json:"exit_code,omitempty"`
	Changes  []FileChange `json:"changes,omitempty"`
}

// FileChange describes a provider-reported file operation and path.
type FileChange struct {
	Kind string `json:"kind"`
	Path string `json:"path"`
}
