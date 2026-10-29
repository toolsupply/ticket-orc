package ticketclient

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// AttachmentLocation is private integration data, never a public TicketDetail
// projection. Consumers must open RepositoryRoot and TicketDirectory through
// confined, no-redirection directory handles; these strings authorize no raw IO.
type AttachmentLocation struct {
	RepositoryID    string
	TicketID        string
	RepositoryRoot  string
	TicketDirectory string // slash relative to RepositoryRoot, including archive
	Archived        bool
}

// ResolveAttachmentLocation observes only public Ticket JSON. Callers own and
// close this client before taking attachment locks or starting a transfer.
func (c *Client) ResolveAttachmentLocation(ctx context.Context, repositoryID, id string) (AttachmentLocation, error) {
	if !ValidRepositoryID(repositoryID) || validateFullID(id) != nil {
		return AttachmentLocation{}, fmt.Errorf("%w: invalid attachment identity", ErrProtocol)
	}
	var info RepositoryInfo
	if err := c.invoke(ctx, []string{"info"}, &info); err != nil {
		return AttachmentLocation{}, err
	}
	if info.ID != repositoryID || !filepath.IsAbs(info.Path) || filepath.Clean(info.Path) != info.Path || (c.target.Scope != "" && (info.Scope == nil || *info.Scope != c.target.Scope)) {
		return AttachmentLocation{}, fmt.Errorf("%w: attachment repository mismatch", ErrProtocol)
	}
	var show struct {
		ID             string `json:"id"`
		Path           string `json:"path"`
		Archived       *bool  `json:"archived"`
		AttachmentPath string `json:"attachment_path"`
	}
	if err := c.invoke(ctx, []string{"show", id, "--max-bytes", "1"}, &show); err != nil {
		return AttachmentLocation{}, err
	}
	var location struct {
		ID   string `json:"id"`
		Path string `json:"path"`
	}
	if err := c.invoke(ctx, []string{"path", id}, &location); err != nil {
		return AttachmentLocation{}, err
	}
	if show.ID != id || location.ID != id || show.Archived == nil || show.Path != location.Path || !safeTaskLocation(show.Path) {
		return AttachmentLocation{}, fmt.Errorf("%w: inconsistent attachment ticket location", ErrProtocol)
	}
	directory := path.Dir(show.Path)
	if show.AttachmentPath != "" {
		cwd := c.workingDir
		cwd, err := filepath.Abs(cwd)
		if err != nil {
			return AttachmentLocation{}, err
		}
		advisory := filepath.FromSlash(show.AttachmentPath)
		if !filepath.IsAbs(advisory) {
			advisory = filepath.Join(cwd, advisory)
		}
		if filepath.Clean(advisory) != filepath.Join(info.Path, filepath.FromSlash(directory), "attachments") {
			return AttachmentLocation{}, fmt.Errorf("%w: inconsistent advisory attachment path", ErrProtocol)
		}
	}
	return AttachmentLocation{repositoryID, id, info.Path, directory, *show.Archived}, nil
}

func safeTaskLocation(value string) bool {
	if !strings.HasSuffix(value, "/TASK.md") || strings.ContainsAny(value, "\\:\x00") || path.IsAbs(value) {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
