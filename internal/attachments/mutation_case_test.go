package attachments

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Both destinations are valid directory paths. Their child exceeds depth; the
// second also exceeds path bytes. With v1's 16*255+15 maximum valid path, the
// path-byte boundary cannot be exceeded without also exceeding depth.
func moveLimitDestination(long bool) (parent, destination string) {
	component, leaf := "d", "moved"
	if long {
		component = strings.Repeat("d", V1Limits().MaxNameBytes)
		leaf = strings.Repeat("m", V1Limits().MaxNameBytes)
	}
	parent = strings.TrimSuffix(strings.Repeat(component+"/", V1Limits().MaxDepth-1), "/")
	return parent, parent + "/" + leaf
}

func TestUsageMatchesResolvedObjects(t *testing.T) {
	if !MutationsAvailable() {
		t.Skip("safe atomic publication unavailable")
	}
	ctx := context.Background()
	loc, root := fixture(t, true)
	mkdir(t, filepath.Join(root, "SOURCE"))
	write(t, filepath.Join(root, "SOURCE", "file"), []byte("payload"))
	tree, _, err := openTree(ctx, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.close()
	// The accounting interface takes resolved identity, not the caller path.
	// A fresh Stat from a pinned handle must match the enumerated object even
	// though no source spelling is supplied to the walker. This runs on Linux
	// as well as case-insensitive volumes without simulating filesystem lookup.
	source, err := tree.descend(tree.root, "SOURCE")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := source.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, long := range []bool{false, true} {
		_, to := moveLimitDestination(long)
		if err = ValidatePath(to, false); err != nil {
			t.Fatal(err)
		}
		if long && len(to+"/file") <= V1Limits().MaxPathBytes {
			t.Fatal("test did not exceed path byte limit")
		}
		_, _, err = tree.usage(ctx, identity, to, nil)
		code(t, err, "limit_exceeded")
	}
	if bytes, count, err := tree.usage(ctx, identity, "destination", nil); err != nil || bytes != 7 || count != 2 {
		t.Fatal(bytes, count, err)
	}
	// Replacement exemption uses the opened object's identity too. No string
	// comparison may make reduction depend on an accepted path alias spelling.
	f, err := source.OpenFile("file", os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(V1Limits().MaxFileBytes + 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	replacing, err := f.Stat()
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = tree.usage(ctx, nil, "", nil)
	code(t, err, "limit_exceeded")
	if bytes, count, err := tree.usage(ctx, nil, "", replacing); err != nil || bytes != V1Limits().MaxFileBytes+1 || count != 2 {
		t.Fatal(bytes, count, err)
	}
}

func TestMoveCaseAliasChecksResultingSubtree(t *testing.T) {
	if !MutationsAvailable() {
		t.Skip("safe atomic publication unavailable")
	}
	for _, long := range []bool{false, true} {
		name := "depth"
		if long {
			name = "path_bytes"
		}
		for _, from := range []string{"SOURCE", "source"} {
			t.Run(name+"/"+from, func(t *testing.T) {
				ctx := context.Background()
				loc, root := fixture(t, false)
				if _, err := Mkdir(ctx, loc, "tester", "SOURCE", create); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(root, from)); os.IsNotExist(err) {
					t.Skip("backing filesystem is case sensitive")
				} else if err != nil {
					t.Fatal(err)
				}
				put(t, NewStore(), loc, "SOURCE/file", create, []byte("unchanged"))
				parent, to := moveLimitDestination(long)
				// Create each parent through confined handles; the maximum path must not
				// be narrowed by an absolute-path syscall in this test fixture.
				p := ""
				for _, component := range strings.Split(parent, "/") {
					if p != "" {
						p += "/"
					}
					p += component
					if _, err := Mkdir(ctx, loc, "tester", p, create); err != nil {
						t.Fatal(err)
					}
				}
				before := list(t, loc, "", 0, "")
				alias := list(t, loc, from, 0, "")
				if len(alias.Entries) != 1 {
					t.Fatal(alias)
				}
				_, err := Move(ctx, loc, "tester", from, to, alias.Revision, create)
				rejected(t, err, "limit_exceeded")
				after := list(t, loc, "", 0, "")
				if after.Revision != before.Revision {
					t.Fatal("rejected move rotated generation")
				}
				if len(list(t, loc, parent, 0, "").Entries) != 0 {
					t.Fatal("rejected move published destination")
				}
				bytes, err := os.ReadFile(filepath.Join(root, "SOURCE", "file"))
				if err != nil || string(bytes) != "unchanged" {
					t.Fatal(string(bytes), err)
				}
			})
		}
	}
}

func TestCaseAliasOversizedReplacementReduction(t *testing.T) {
	if !MutationsAvailable() {
		t.Skip("safe atomic publication unavailable")
	}
	loc, root := fixture(t, true)
	f, err := os.Create(filepath.Join(root, "PAYLOAD"))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(V1Limits().MaxFileBytes + 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	if _, err = os.Stat(filepath.Join(root, "payload")); os.IsNotExist(err) {
		t.Skip("backing filesystem is case sensitive")
	} else if err != nil {
		t.Fatal(err)
	}
	d := download(t, NewStore(), loc, "payload")
	rev := d.Entry.Revision
	d.Close()
	receipt := put(t, NewStore(), loc, "payload", Condition{Revision: rev}, []byte("reduced"))
	if receipt.Created {
		t.Fatal("alias replacement reported creation")
	}
	b, err := os.ReadFile(filepath.Join(root, "PAYLOAD"))
	if err != nil || string(b) != "reduced" {
		t.Fatal(string(b), err)
	}
	if entries := list(t, loc, "", 0, "").Entries; len(entries) != 1 || !strings.EqualFold(entries[0].Name, "payload") {
		t.Fatal(entries)
	}
}

func TestMoveAccountingKeepsDistinctCaseSensitiveNames(t *testing.T) {
	if !MutationsAvailable() {
		t.Skip("safe atomic publication unavailable")
	}
	ctx := context.Background()
	loc, root := fixture(t, true)
	mkdir(t, filepath.Join(root, "SOURCE"))
	if _, err := os.Stat(filepath.Join(root, "source")); err == nil {
		t.Skip("backing filesystem is case insensitive")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	mkdir(t, filepath.Join(root, "source"))
	write(t, filepath.Join(root, "source", "file"), nil)
	tree, _, err := openTree(ctx, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.close()
	identity, err := tree.root.Lstat("SOURCE")
	if err != nil {
		t.Fatal(err)
	}
	_, to := moveLimitDestination(false)
	// Only empty SOURCE moves to depth 16. The distinct source/file remains at
	// depth 2; lowercasing either spelling would incorrectly reject this move.
	if _, _, err = tree.usage(ctx, identity, to, nil); err != nil {
		t.Fatal(err)
	}
}
