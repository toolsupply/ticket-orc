package attachments

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strings"
)

// List holds only the per-ticket filesystem lock. It never visits descendants
// of immediate children or reads file contents to compute revisions.
func List(ctx context.Context, loc Location, directory string, limit int, token string) (Listing, error) {
	var empty Listing
	if err := ValidatePath(directory, true); err != nil {
		return empty, err
	}
	if limit == 0 {
		limit = V1Limits().DefaultPageEntries
	}
	if limit < 1 || limit > V1Limits().MaxPageEntries {
		return empty, failure("invalid_request")
	}
	t, absent, err := openTree(ctx, loc)
	if err != nil {
		return empty, err
	}
	defer t.close()
	var gen [16]byte
	if !absent {
		gen, err = generation(t)
		if err != nil {
			return empty, err
		}
	}
	rev := revision(loc, directory, "directory", gen)
	position, err := parseCursor(token, loc, directory, limit, rev)
	if err != nil {
		return empty, err
	}
	result := Listing{loc.RepositoryID, loc.TicketID, directory, rev, []Entry{}, nil}
	if absent {
		if directory != "" {
			return empty, failure("entry_not_found")
		}
		if position != nil {
			return empty, failure("invalid_cursor")
		}
		return result, nil
	}
	dir := t.root
	if directory != "" {
		for _, part := range strings.Split(directory, "/") {
			dir, err = t.descend(dir, part)
			if err != nil {
				return empty, storageError(err)
			}
		}
	}
	file, err := dir.Open(".")
	if err != nil {
		return empty, storageError(err)
	}
	defer file.Close()
	entries := []Entry{}
	for {
		if err = ctx.Err(); err != nil {
			return empty, err
		}
		names, readErr := file.Readdirnames(1)
		if readErr != nil && readErr != io.EOF {
			return empty, storageError(readErr)
		}
		if len(names) == 0 {
			break
		}
		name := names[0]
		if directory == "" && name == ".ticket-orc" {
			continue
		}
		path := name
		if directory != "" {
			path = directory + "/" + name
		}
		if err = ValidatePath(path, false); err != nil {
			return empty, failure("unsafe_attachment")
		}
		info, err := dir.Lstat(name)
		if err != nil {
			return empty, storageError(err)
		}
		entry := Entry{Name: name, Path: path, Kind: "directory"}
		if info.Mode().IsRegular() {
			opened, err := openRegular(dir, name, os.O_RDONLY)
			if err != nil {
				return empty, storageError(err)
			}
			info, err = opened.Stat()
			opened.Close()
			if err != nil {
				return empty, storageError(err)
			}
			size := info.Size()
			entry.Kind = "file"
			entry.SizeBytes = &size
		} else if !info.IsDir() || redirected(info) {
			return empty, failure("unsafe_attachment")
		}
		entry.Revision = revision(loc, path, entry.Kind, gen)
		entries = append(entries, entry)
		if len(entries) > V1Limits().MaxEntries {
			return empty, failure("limit_exceeded")
		}
	}
	sort.Slice(entries, func(i, j int) bool { return less(entries[i].Kind, entries[i].Name, entries[j].Kind, entries[j].Name) })
	start := 0
	if position != nil {
		found := false
		for i, entry := range entries {
			if entry.Kind == position.LastKind && entry.Name == position.LastName {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return empty, failure("invalid_cursor")
		}
	}
	end := min(start+limit, len(entries))
	result.Entries = entries[start:end]
	if end < len(entries) {
		last := entries[end-1]
		data, _ := json.Marshal(cursor{1, loc.RepositoryID, loc.TicketID, directory, rev, limit, last.Kind, last.Name})
		next := base64.RawURLEncoding.EncodeToString(data)
		if len(next) > V1Limits().MaxCursorBytes {
			return empty, failure("limit_exceeded")
		}
		result.NextCursor = &next
	}
	if err = t.verify(); err != nil {
		return empty, err
	}
	return result, nil
}
func less(kind, name, otherKind, otherName string) bool {
	if kind != otherKind {
		return kind == "directory"
	}
	return name < otherName
}
