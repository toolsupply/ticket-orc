package attachments

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"unicode/utf8"
)

func revision(loc Location, path, kind string, generation [16]byte) string {
	h := sha256.New()
	for _, field := range [][]byte{[]byte(loc.RepositoryID), []byte(loc.TicketID), []byte(path), []byte(kind), generation[:]} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(field)))
		h.Write(size[:])
		h.Write(field)
	}
	return "a1_" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// strictObject rejects duplicates as well as unknown fields and trailing data.
func strictObject(data []byte, fields []string, out any) error {
	if !utf8.Valid(data) {
		return failure("unsafe_attachment")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return failure("unsafe_attachment")
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return failure("unsafe_attachment")
		}
		allowed := false
		for _, field := range fields {
			if field == key {
				allowed = true
			}
		}
		if !allowed {
			return failure("unsafe_attachment")
		}
		seen[key] = true
		var raw json.RawMessage
		if err = decoder.Decode(&raw); err != nil {
			return err
		}
	}
	if len(seen) != len(fields) {
		return failure("unsafe_attachment")
	}
	if _, err = decoder.Token(); err != nil {
		return err
	}
	if _, err = decoder.Token(); err != io.EOF {
		return failure("unsafe_attachment")
	}
	return json.Unmarshal(data, out)
}

func generation(t *tree) ([16]byte, error) {
	var value [16]byte
	private := t.private
	dir, err := private.Open(".")
	if err != nil {
		return value, storageError(err)
	}
	defer dir.Close()
	names, err := dir.Readdirnames(5)
	if err != nil && err != io.EOF {
		return value, storageError(err)
	}
	if len(names) > 4 {
		return value, failure("unsafe_attachment")
	}
	hasGeneration, hasStage := false, false
	for _, name := range names {
		switch name {
		case "lock":
		case "generation.json":
			hasGeneration = true
		case "upload.tmp", "generation.tmp":
			hasStage = true
		default:
			return value, failure("unsafe_attachment")
		}
		file, err := openRegular(private, name, os.O_RDONLY)
		if err != nil {
			return value, storageError(err)
		}
		info, err := file.Stat()
		file.Close()
		if err != nil {
			return value, storageError(err)
		}
		if (name == "generation.json" || name == "generation.tmp") && info.Size() > 4096 {
			return value, failure("unsafe_attachment")
		}
		if name == "upload.tmp" && info.Size() > V1Limits().MaxStagingBytesPerTicket {
			return value, failure("unsafe_attachment")
		}
	}
	if !hasGeneration {
		if hasStage {
			return value, failure("unsafe_attachment")
		}
		return value, nil
	}
	file, err := openRegular(private, "generation.json", os.O_RDONLY)
	if err != nil {
		return value, storageError(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return value, storageError(err)
	}
	var record struct {
		Version    int    `json:"version"`
		Generation string `json:"generation"`
	}
	if len(data) > 4096 || strictObject(data, []string{"version", "generation"}, &record) != nil || record.Version != 1 {
		return value, failure("unsafe_attachment")
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(record.Generation)
	if err != nil || len(raw) != 16 || base64.RawURLEncoding.EncodeToString(raw) != record.Generation {
		return value, failure("unsafe_attachment")
	}
	copy(value[:], raw)
	return value, nil
}

type cursor struct {
	Version      int    `json:"version"`
	RepositoryID string `json:"repository_id"`
	TicketID     string `json:"ticket_id"`
	Directory    string `json:"directory"`
	Revision     string `json:"revision"`
	Limit        int    `json:"limit"`
	LastKind     string `json:"last_kind"`
	LastName     string `json:"last_name"`
}

func parseCursor(token string, loc Location, directory string, limit int, rev string) (*cursor, error) {
	if token == "" {
		return nil, nil
	}
	if len(token) > V1Limits().MaxCursorBytes {
		return nil, failure("invalid_cursor")
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || base64.RawURLEncoding.EncodeToString(data) != token {
		return nil, failure("invalid_cursor")
	}
	var c cursor
	fields := []string{"version", "repository_id", "ticket_id", "directory", "revision", "limit", "last_kind", "last_name"}
	if strictObject(data, fields, &c) != nil || c.Version != 1 {
		return nil, failure("invalid_cursor")
	}
	if c.RepositoryID != loc.RepositoryID || c.TicketID != loc.TicketID || c.Directory != directory || c.Limit != limit {
		return nil, failure("invalid_cursor")
	}
	if (c.LastKind != "file" && c.LastKind != "directory") || ValidatePath(c.LastName, false) != nil || bytes.ContainsRune([]byte(c.LastName), '/') {
		return nil, failure("invalid_cursor")
	}
	if len(c.Revision) != 46 || c.Revision[:3] != "a1_" {
		return nil, failure("invalid_cursor")
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(c.Revision[3:])
	if err != nil || len(raw) != 32 {
		return nil, failure("invalid_cursor")
	}
	if c.Revision != rev {
		return nil, failure("cursor_stale")
	}
	return &c, nil
}
