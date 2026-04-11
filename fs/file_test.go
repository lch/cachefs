package fs

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)


func TestFileNode_DirectoryOps(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	// 1. Create a prefix directory via RootNode
	prefix := "01"
	_, errno := root.Mkdir(ctx, prefix, 0755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("RootNode.Mkdir failed: %v", errno)
	}

	// 2. Create prefixNode manually for testing
	prefixNode := &FileNode{
		cfs:  root.cfs,
		path: prefix + "/",
	}

	// 3. Create a subdirectory via FileNode (Mkdir)
	subDirName := "subdir"
	var out fuse.EntryOut
	_, errno = prefixNode.Mkdir(ctx, subDirName, 0755, &out)
	if errno != 0 {
		t.Fatalf("FileNode.Mkdir failed: %v", errno)
	}

	// Verify in store
	exists, _ := st.Exists(prefix + "/" + subDirName + "/")
	if !exists {
		t.Errorf("Subdirectory not found in store")
	}

	// 4. Lookup the subdirectory
	_, errno = prefixNode.Lookup(ctx, subDirName, &out)
	if errno != 0 {
		t.Fatalf("FileNode.Lookup failed: %v", errno)
	}

	// 5. Rename subdirectory
	newName := "newdir"
	errno = prefixNode.Rename(ctx, subDirName, prefixNode, newName, 0)
	if errno != 0 {
		t.Fatalf("FileNode.Rename failed: %v", errno)
	}

	// Verify rename in store
	exists, _ = st.Exists(prefix + "/" + subDirName + "/")
	if exists {
		t.Errorf("Old subdirectory still exists in store")
	}
	exists, _ = st.Exists(prefix + "/" + newName + "/")
	if !exists {
		t.Errorf("New subdirectory not found in store")
	}

	// 6. Rmdir
	errno = prefixNode.Rmdir(ctx, newName)
	if errno != 0 {
		t.Fatalf("FileNode.Rmdir failed: %v", errno)
	}
	exists, _ = st.Exists(prefix + "/" + newName + "/")
	if exists {
		t.Errorf("Subdirectory still exists after Rmdir")
	}
}

func readResultBytes(t *testing.T, rr fuse.ReadResult) []byte {
	t.Helper()
	buf := make([]byte, rr.Size())
	got, status := rr.Bytes(buf)
	if status != fuse.OK {
		t.Fatalf("ReadResult.Bytes status = %v, want OK", status)
	}
	return append([]byte(nil), got...)
}
