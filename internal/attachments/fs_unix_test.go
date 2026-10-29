//go:build linux || darwin

package attachments

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRealLinksAndRootRedirection(t *testing.T) {
	for _, kind := range []string{"root", "directory", "file", "hardlink", "private", "lock", "generation"} {
		t.Run(kind, func(t *testing.T) {
			loc, root := fixture(t, true)
			outside := t.TempDir()
			write(t, filepath.Join(outside, "secret"), []byte("secret"))
			switch kind {
			case "root":
				if err := os.Remove(root); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, root); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Symlink(outside, filepath.Join(root, "sub")); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "secret")); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(filepath.Join(outside, "secret"), filepath.Join(root, "secret")); err != nil {
					t.Fatal(err)
				}
			case "private":
				if err := os.Symlink(outside, filepath.Join(root, ".ticket-orc")); err != nil {
					t.Fatal(err)
				}
			case "lock", "generation":
				list(t, loc, "", 0, "")
				name := "lock"
				if kind == "generation" {
					name = "generation.json"
				} else {
					if err := os.Remove(filepath.Join(root, ".ticket-orc", name)); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, ".ticket-orc", name)); err != nil {
					t.Fatal(err)
				}
			}
			_, err := List(context.Background(), loc, "", 0, "")
			code(t, err, "unsafe_attachment")
			d, _ := NewStore().BeginDownload(context.Background())
			defer d.Close()
			if err = d.Open(loc, "secret"); err == nil {
				t.Fatal("download accepted unsafe tree")
			}
		})
	}
}

func TestPinnedDirectorySwapFailsBeforePublication(t *testing.T) {
	loc, root := fixture(t, true)
	mkdir(t, filepath.Join(root, "sub"))
	write(t, filepath.Join(root, "sub", "file"), []byte("safe"))
	tree, _, err := openTree(context.Background(), loc)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.close()
	pinned, err := tree.descend(tree.root, "sub")
	if err != nil {
		t.Fatal(err)
	}
	// Explicit interleaving: replace the traversed name after its handle is
	// pinned and before publication. The old handle cannot be redirected.
	if err = os.Rename(filepath.Join(root, "sub"), filepath.Join(root, "old")); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	write(t, filepath.Join(outside, "file"), []byte("secret"))
	if err = os.Symlink(outside, filepath.Join(root, "sub")); err != nil {
		t.Fatal(err)
	}
	file, err := openRegular(pinned, "file", os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var buf [4]byte
	if _, err = file.Read(buf[:]); err != nil || string(buf[:]) != "safe" {
		t.Fatal("pinned read redirected", err)
	}
	code(t, tree.verify(), "unsafe_attachment")
}

func TestPrivateIdentitySurvivesDirectoryRename(t *testing.T) {
	loc, root := fixture(t, true)
	owned, _, err := openTree(context.Background(), loc)
	if err != nil {
		t.Fatal(err)
	}
	defer owned.close()
	// Model an alternate name of the same pinned object without requiring
	// Windows aliases: rename after bootstrap, then attempt user traversal.
	if err = os.Rename(filepath.Join(root, ".ticket-orc"), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if err = ValidatePath("alias", false); err != nil {
		t.Fatal(err)
	}
	_, err = owned.descend(owned.root, "alias")
	code(t, err, "unsafe_attachment")
}

func TestInRootSymlinkAndSpecialFileRejected(t *testing.T) {
	loc, root := fixture(t, true)
	write(t, filepath.Join(root, "original"), nil)
	if err := os.Symlink("original", filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	d, _ := NewStore().BeginDownload(context.Background())
	defer d.Close()
	code(t, d.Open(loc, "alias"), "unsafe_attachment")
	if err := syscall.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	d2, _ := NewStore().BeginDownload(context.Background())
	defer d2.Close()
	code(t, d2.Open(loc, "fifo"), "unsafe_attachment")
}

func TestOpenDuringRealFileSymlinkReplacement(t *testing.T) {
	loc, root := fixture(t, true)
	write(t, filepath.Join(root, "target"), []byte("safe"))
	outside := filepath.Join(t.TempDir(), "secret")
	write(t, outside, []byte("secret"))
	owned, _, err := openTree(context.Background(), loc)
	if err != nil {
		t.Fatal(err)
	}
	defer owned.close()
	start := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-start
		for i := 0; i < 200; i++ {
			if err := os.Symlink(outside, filepath.Join(root, "swap-link")); err != nil {
				done <- err
				return
			}
			if err := os.Rename(filepath.Join(root, "swap-link"), filepath.Join(root, "target")); err != nil {
				done <- err
				return
			}
			if err := os.WriteFile(filepath.Join(root, "swap-file"), []byte("safe"), 0600); err != nil {
				done <- err
				return
			}
			if err := os.Rename(filepath.Join(root, "swap-file"), filepath.Join(root, "target")); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	// Register the completion barrier before starting the concurrent host edits.
	t.Cleanup(func() {
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("file swap process did not finish")
		}
	})
	close(start)
	for i := 0; i < 200; i++ {
		file, err := openRegular(owned.root, "target", os.O_RDONLY)
		if err != nil {
			continue
		}
		data, err := io.ReadAll(file)
		file.Close()
		if err != nil || !bytes.Equal(data, []byte("safe")) {
			t.Fatalf("redirection escaped confinement: %q %v", data, err)
		}
	}
}
