package state

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWorkerFailureArtifactNamespacesBoundsAndPermissions(t *testing.T) {
	const repo = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	const other = "7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	dir := t.TempDir()
	for _, key := range [][2]string{{repo, "coder"}, {other, "coder"}, {repo, "reviewer"}, {repo, "Coder"}} {
		data := []byte(`{"worker":"` + key[1] + `","repository":"` + key[0] + `"}`)
		if err := WriteWorkerFailure(t.Context(), dir, key[0], key[1], data); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range [][2]string{{repo, "coder"}, {other, "coder"}, {repo, "reviewer"}, {repo, "Coder"}} {
		data, err := ReadWorkerFailure(t.Context(), dir, key[0], key[1])
		if err != nil || !bytes.Contains(data, []byte(key[0])) || !bytes.Contains(data, []byte(key[1])) {
			t.Fatalf("namespace collision: %s %v", data, err)
		}
		ref, _ := WorkerFailureReference(key[0], key[1])
		info, err := os.Stat(filepath.Join(dir, ref))
		if err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
			t.Fatalf("public diagnostic mode: %v", info.Mode())
		}
	}
	if err := WriteWorkerFailure(t.Context(), dir, repo, "coder", []byte(`{"latest":true}`)); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "worker-failure-") {
			count++
		}
	}
	if count != 4 {
		t.Fatalf("unbounded failure history: %d artifacts", count)
	}
	if err := WriteWorkerFailure(t.Context(), dir, repo, "coder", []byte(`"`+strings.Repeat("x", MaxWorkerFailureBytes)+`"`)); err == nil {
		t.Fatal("accepted oversized artifact")
	}
	ref, _ := WorkerFailureReference(repo, "coder")
	if err := os.WriteFile(filepath.Join(dir, ref), bytes.Repeat([]byte("x"), MaxWorkerFailureBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadWorkerFailure(t.Context(), dir, repo, "coder"); err == nil {
		t.Fatal("read oversized artifact")
	}
	for _, worker := range []string{"../escape", "a/b", `a\b`, "..", strings.Repeat("x", 65), "worker\n"} {
		if _, err := WorkerFailureReference(repo, worker); err == nil {
			t.Fatalf("accepted unsafe key %q", worker)
		}
	}
	if _, err := WorkerFailureReference("../repository", "coder"); err == nil {
		t.Fatal("accepted unsafe repository")
	}
}

func TestWorkerFailureArtifactRejectsSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires Windows privilege")
	}
	const repo = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "private.json")
	original := []byte(`{"private":"untouched"}`)
	if err := os.WriteFile(target, original, 0600); err != nil {
		t.Fatal(err)
	}
	ref, _ := WorkerFailureReference(repo, "coder")
	if err := os.Symlink(target, filepath.Join(dir, ref)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadWorkerFailure(t.Context(), dir, repo, "coder"); err == nil {
		t.Fatal("read symlink")
	}
	if err := WriteWorkerFailure(t.Context(), dir, repo, "coder", []byte(`{}`)); err == nil {
		t.Fatal("replaced symlink")
	}
	data, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(data, original) {
		t.Fatalf("modified symlink target: %s %v", data, err)
	}
	link := filepath.Join(t.TempDir(), "linked-dir")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if err := WriteWorkerFailure(t.Context(), link, repo, "coder", []byte(`{}`)); err == nil {
		t.Fatal("accepted symlink directory")
	}
}
