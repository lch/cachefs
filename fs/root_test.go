package fs

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/store"
)

func TestRootNode_Lookup(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	var out fuse.EntryOut
	// Test lookup non-existent
	_, errno := root.Lookup(ctx, "01", &out)
	if errno == 0 {
		t.Errorf("expected error for non-existent prefix")
	}

	// Create it
	err := st.Create("01")
	if err != nil {
		t.Fatalf("failed to create prefix: %v", err)
	}

	// Test lookup existent
	child, errno := root.Lookup(ctx, "01", &out)
	if errno != 0 {
		t.Errorf("lookup failed: %v", errno)
	}
	if child == nil {
		t.Errorf("lookup returned nil child")
	}
	if out.Ino == 0 {
		t.Errorf("lookup did not fill Ino")
	}

	// Test lookup invalid name
	_, errno = root.Lookup(ctx, "xyz", &out)
	if errno == 0 {
		t.Errorf("expected error for invalid prefix name")
	}
}

func TestRootNode_Mkdir(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	var out fuse.EntryOut
	child, errno := root.Mkdir(ctx, "aa", 0755, &out)
	if errno != 0 {
		t.Fatalf("mkdir failed: %v", errno)
	}
	if child == nil {
		t.Errorf("mkdir returned nil child")
	}

	exists, _ := st.Exists("aa")
	if !exists {
		t.Errorf("prefix not created in store")
	}
}

func TestRootNode_Rmdir(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	_ = st.Create("bb")
	errno := root.Rmdir(ctx, "bb")
	if errno != 0 {
		t.Fatalf("rmdir failed: %v", errno)
	}

	exists, _ := st.Exists("bb")
	if exists {
		t.Errorf("prefix still exists in store")
	}
}

func TestRootNode_Readdir(t *testing.T) {
	root, st := newTestRoot(t)

	_ = st.Create("01")
	_ = st.Create("02")

	names, err := readDirEntries(root)
	if err != nil {
		t.Fatalf("readdir failed: %v", err)
	}

	found01 := false
	found02 := false
	for _, n := range names {
		if n == "01" {
			found01 = true
		}
		if n == "02" {
			found02 = true
		}
	}
	if !found01 || !found02 {
		t.Errorf("expected to find 01 and 02, got %v", names)
	}
}

func TestRootNode_Getattr(t *testing.T) {
	root, _ := newTestRoot(t)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := root.Getattr(ctx, nil, &out)
	if errno != 0 {
		t.Fatalf("getattr failed: %v", errno)
	}

	if out.Mode&fuse.S_IFDIR == 0 {
		t.Errorf("expected S_IFDIR in mode")
	}
	if out.Ino != InodeRoot {
		t.Errorf("expected Ino %d, got %d", InodeRoot, out.Ino)
	}
}

func TestRootNode_Statfs(t *testing.T) {
	root, _ := newTestRoot(t)
	ctx := context.Background()

	var out fuse.StatfsOut
	errno := root.Statfs(ctx, &out)
	if errno != 0 {
		t.Fatalf("statfs failed: %v", errno)
	}

	if out.Bsize != 4096 {
		t.Errorf("expected Bsize 4096, got %d", out.Bsize)
	}
}

func readDirEntries(root *RootNode) ([]string, error) {
	ds, errno := root.Readdir(context.Background())
	if errno != 0 {
		return nil, errno
	}
	defer ds.Close()

	var names []string
	for ds.HasNext() {
		entry, errno := ds.Next()
		if errno != 0 {
			return nil, errno
		}
		names = append(names, entry.Name)
	}
	return names, nil
}

func newTestRoot(t *testing.T) (*RootNode, store.Store) {
	t.Helper()
	st, err := store.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	return NewRootNode(NewCacheFS(st, 1000, 1001)), st
}
