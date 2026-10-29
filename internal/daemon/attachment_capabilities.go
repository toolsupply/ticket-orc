package daemon

import "github.com/toolsupply/ticket-orc/internal/attachments"

// AttachmentCapabilities reports only wired operations; limits come from the
// same storage policy that enforces them. Recursive removal is unavailable.
type AttachmentCapabilities struct {
	List                     bool   `json:"list"`
	Download                 bool   `json:"download"`
	Upload                   bool   `json:"upload"`
	Mkdir                    bool   `json:"mkdir"`
	Move                     bool   `json:"move"`
	Delete                   bool   `json:"delete"`
	RecursiveDelete          bool   `json:"recursive_delete"`
	RevisionScope            string `json:"revision_scope"`
	MaxFileBytes             int64  `json:"max_file_bytes"`
	MaxTotalBytes            int64  `json:"max_total_bytes"`
	MaxEntries               int    `json:"max_entries"`
	MaxDepth                 int    `json:"max_depth"`
	MaxNameBytes             int    `json:"max_name_bytes"`
	MaxPathBytes             int    `json:"max_path_bytes"`
	DefaultPageEntries       int    `json:"default_page_entries"`
	MaxPageEntries           int    `json:"max_page_entries"`
	MaxCursorBytes           int    `json:"max_cursor_bytes"`
	MaxJSONBodyBytes         int    `json:"max_json_body_bytes"`
	MaxActiveTransfers       int    `json:"max_active_transfers"`
	MaxStagingFilesPerTicket int    `json:"max_staging_files_per_ticket"`
	MaxStagingBytesPerTicket int64  `json:"max_staging_bytes_per_ticket"`
	TransferTimeoutSeconds   int64  `json:"transfer_timeout_seconds"`
	CommitTimeoutSeconds     int64  `json:"commit_timeout_seconds"`
	LockWaitSeconds          int64  `json:"lock_wait_seconds"`
}

func (s *Server) attachmentCapabilities() *AttachmentCapabilities {
	g := s.config.RepositoryGateway
	safe := attachments.MutationsAvailable()
	c := &AttachmentCapabilities{List: g.ListAttachments != nil, Download: g.ReadAttachment != nil, Upload: safe && g.WriteAttachment != nil, Mkdir: safe && g.CreateAttachmentDirectory != nil, Move: safe && g.MoveAttachment != nil, Delete: safe && g.DeleteAttachment != nil}
	if !c.List && !c.Download && !c.Upload && !c.Mkdir && !c.Move && !c.Delete {
		return nil
	}
	l := attachments.V1Limits()
	c.RevisionScope = "ticket"
	c.MaxFileBytes = l.MaxFileBytes
	c.MaxTotalBytes = l.MaxTotalBytes
	c.MaxEntries = l.MaxEntries
	c.MaxDepth = l.MaxDepth
	c.MaxNameBytes = l.MaxNameBytes
	c.MaxPathBytes = l.MaxPathBytes
	c.DefaultPageEntries = l.DefaultPageEntries
	c.MaxPageEntries = l.MaxPageEntries
	c.MaxCursorBytes = l.MaxCursorBytes
	c.MaxJSONBodyBytes = l.MaxJSONBodyBytes
	c.MaxActiveTransfers = l.MaxActiveTransfers
	c.MaxStagingFilesPerTicket = l.MaxStagingFilesPerTicket
	c.MaxStagingBytesPerTicket = l.MaxStagingBytesPerTicket
	c.TransferTimeoutSeconds = int64(l.TransferTimeout.Seconds())
	c.CommitTimeoutSeconds = int64(l.CommitTimeout.Seconds())
	c.LockWaitSeconds = int64(l.LockWait.Seconds())
	return c
}
