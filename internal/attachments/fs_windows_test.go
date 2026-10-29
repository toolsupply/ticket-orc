package attachments

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestWindowsPrivateShortNameAliasRejected(t *testing.T) {
	loc, root := fixture(t, true)
	list(t, loc, "", 0, "") // Bootstrap the actual private directory/lock.
	privatePath := filepath.Join(root, ".ticket-orc")
	longPath, err := syscall.UTF16PtrFromString(privatePath)
	if err != nil {
		t.Fatal(err)
	}
	needed, err := syscall.GetShortPathName(longPath, nil, 0)
	if err != nil || needed == 0 {
		t.Fatalf("GetShortPathName size: %d %v", needed, err)
	}
	buffer := make([]uint16, needed)
	n, err := syscall.GetShortPathName(longPath, &buffer[0], uint32(len(buffer)))
	if err != nil || n == 0 || n >= uint32(len(buffer)) {
		t.Fatalf("GetShortPathName: %d %v", n, err)
	}
	alias := filepath.Base(syscall.UTF16ToString(buffer[:n]))
	if strings.EqualFold(alias, ".ticket-orc") {
		t.Skip("test volume has no distinct 8.3 alias for .ticket-orc; identity boundary remains covered by common traversal tests")
	}
	if err = ValidatePath(alias, false); err != nil {
		t.Fatalf("actual alias %q does not exercise the lexical bypass: %v", alias, err)
	}
	actual, err := os.Stat(privatePath)
	if err != nil {
		t.Fatal(err)
	}
	aliased, err := os.Stat(filepath.Join(root, alias))
	if err != nil || !os.SameFile(actual, aliased) {
		t.Fatalf("actual short-name alias does not identify private directory: %v", err)
	}
	write(t, filepath.Join(privatePath, "generation.json"), []byte(`{"version":1,"generation":"AAAAAAAAAAAAAAAAAAAAAA"}`))
	// This is valid private metadata: failure must come from the user/private
	// boundary, not malformed generation or another unrelated safety check.
	list(t, loc, "", 0, "")
	_, err = List(context.Background(), loc, alias, 0, "")
	code(t, err, "unsafe_attachment")
	d, err := NewStore().BeginDownload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	code(t, d.Open(loc, alias+"/generation.json"), "unsafe_attachment")
}

func TestWindowsHardlinkAndJunctionSafety(t *testing.T) {
	for _, kind := range []string{"hardlink", "junction"} {
		t.Run(kind, func(t *testing.T) {
			loc, root := fixture(t, true)
			outside := t.TempDir()
			write(t, filepath.Join(outside, "secret"), []byte("secret"))
			if kind == "hardlink" {
				if err := os.Link(filepath.Join(outside, "secret"), filepath.Join(root, "secret")); err != nil {
					t.Fatal(err)
				}
			} else {
				cmd := exec.Command("cmd", "/c", "mklink", "/J", filepath.Join(root, "redirect"), outside)
				if data, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("native junction fixture unavailable: %v %s", err, data)
				}
			}
			_, err := List(context.Background(), loc, "", 0, "")
			code(t, err, "unsafe_attachment")
			d, _ := NewStore().BeginDownload(context.Background())
			defer d.Close()
			path := "secret"
			if kind == "junction" {
				path = "redirect/secret"
			}
			code(t, d.Open(loc, path), "unsafe_attachment")
		})
	}
}
