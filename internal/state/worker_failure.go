package state

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MaxWorkerFailureBytes bounds each latest-failure artifact (including JSON
// escaping). There is exactly one file per stable worker/repository, replaced
// atomically. These artifacts are evidence, never general or steering state.
const MaxWorkerFailureBytes = 16 << 10

// WorkerFailureReference is relative to the instance-local directory. The
// repository UUID and restricted worker key make the namespace unambiguous.
// Hex encoding preserves case-sensitive worker identity on every filesystem.
func WorkerFailureReference(repository, worker string) (string, error) {
	if !isStableTicketRepositoryID(repository) || worker == "" || len(worker) > 64 || worker == "." || worker == ".." {
		return "", fmt.Errorf("invalid worker diagnostic identity")
	}
	for _, r := range worker {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return "", fmt.Errorf("invalid worker diagnostic key")
		}
	}
	return "worker-failure-" + repository + "-" + hex.EncodeToString([]byte(worker)) + ".json", nil
}

func WriteWorkerFailure(ctx context.Context, dir, repository, worker string, data []byte) error {
	ref, err := WorkerFailureReference(repository, worker)
	if err != nil {
		return err
	}
	if len(data) > MaxWorkerFailureBytes || !json.Valid(data) {
		return fmt.Errorf("invalid or oversized worker diagnostic")
	}
	s := NewAdministrative(dir)
	return s.withLock(ctx, func() error {
		path := filepath.Join(dir, ref)
		if info, err := os.Lstat(path); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("worker diagnostic must be a regular file")
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		return writeStateAtomic(path, data)
	})
}

func ReadWorkerFailure(ctx context.Context, dir, repository, worker string) ([]byte, error) {
	ref, err := WorkerFailureReference(repository, worker)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("diagnostic directory is unavailable")
	}
	var data []byte
	s := NewAdministrative(dir)
	err = s.withLock(ctx, func() error {
		path := filepath.Join(dir, ref)
		if err := verifyPrivateFilePath(path, 0600); err != nil {
			return err
		}
		var err error
		data, err = readBoundedStateFileLimit(path, MaxWorkerFailureBytes)
		return err
	})
	return data, err
}
