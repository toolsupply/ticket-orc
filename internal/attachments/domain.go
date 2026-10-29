// Package attachments owns supplementary file storage only. Ticket location
// observation and HTTP presentation live outside this package. No Orc state or
// runtime mutex may be held during these operations. Admission precedes Ticket
// resolution (BeginDownload), which precedes the exclusive per-ticket lock.
package attachments

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Limits is the single v1 policy used by storage and future transport wiring.
type Limits struct {
	MaxFileBytes, MaxTotalBytes                                          int64
	MaxEntries, MaxDepth, MaxNameBytes, MaxPathBytes                     int
	DefaultPageEntries, MaxPageEntries, MaxCursorBytes, MaxJSONBodyBytes int
	MaxActiveTransfers, MaxStagingFilesPerTicket                         int
	MaxStagingBytesPerTicket                                             int64
	TransferTimeout, CommitTimeout, LockWait                             time.Duration
}

func V1Limits() Limits {
	return Limits{
		MaxFileBytes: 64 << 20, MaxTotalBytes: 512 << 20,
		MaxEntries: 2000, MaxDepth: 16, MaxNameBytes: 255, MaxPathBytes: 4096,
		DefaultPageEntries: 50, MaxPageEntries: 200, MaxCursorBytes: 36864, MaxJSONBodyBytes: 65536,
		MaxActiveTransfers: 4, MaxStagingFilesPerTicket: 1, MaxStagingBytesPerTicket: 64 << 20,
		TransferTimeout: 5 * time.Minute, CommitTimeout: 5 * time.Second, LockWait: 0,
	}
}

// Error contains only safe domain information. Private OS errors and paths
// never cross this boundary. Read errors omit MutationApplied. Mutations set
// it only with evidence; nil retains an unknown publication outcome.
type Error struct {
	Code            string
	MutationApplied *bool
	// FileSizeLimit distinguishes transfer bytes from tree/path limits without HTTP policy.
	FileSizeLimit bool
}

func (e *Error) Error() string  { return "attachment operation failed: " + e.Code }
func failure(code string) error { return &Error{Code: code} }

// MutationsAvailable reports whether this platform supplies safe atomic publication.
// Capability composition must consult this domain policy rather than infer it.
func MutationsAvailable() bool { return safeMutationSupported }

// Location is supplied afresh by the private Ticket resolver for each operation.
// TicketDirectory is relative to RepositoryRoot, never derived from TicketID.
type Location struct{ RepositoryID, TicketID, RepositoryRoot, TicketDirectory string }

type Entry struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	SizeBytes *int64 `json:"size_bytes,omitempty"`
	Revision  string `json:"revision"`
}
type Listing struct {
	RepositoryID string  `json:"repository_id"`
	TicketID     string  `json:"ticket_id"`
	Directory    string  `json:"directory"`
	Revision     string  `json:"revision"`
	Entries      []Entry `json:"entries"`
	NextCursor   *string `json:"next_cursor"`
}

// ValidatePath receives already-decoded UTF-8. Percent signs are rejected so
// residual query encoding cannot acquire filesystem meaning at another layer.
func ValidatePath(value string, allowRoot bool) error {
	limits := V1Limits()
	if value == "" && allowRoot {
		return nil
	}
	if value == "" || !utf8.ValidString(value) || strings.ContainsAny(value, "\\:%") {
		return failure("invalid_path")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return failure("invalid_path")
		}
	}
	parts := strings.Split(value, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") || strings.EqualFold(part, ".ticket-orc") {
			return failure("invalid_path")
		}
		base, _, _ := strings.Cut(strings.ToUpper(part), ".")
		if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || (len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && (base[3] >= '1' && base[3] <= '9')) || base == "COM¹" || base == "COM²" || base == "COM³" || base == "LPT¹" || base == "LPT²" || base == "LPT³" {
			return failure("invalid_path")
		}
		if len(part) > limits.MaxNameBytes {
			return failure("limit_exceeded")
		}
	}
	if len(value) > limits.MaxPathBytes || len(parts) > limits.MaxDepth {
		return failure("limit_exceeded")
	}
	return nil
}
