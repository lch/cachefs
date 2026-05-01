package fs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

func TestFileNode_DirectoryOps(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	// 1. Create a prefix directory via RootNode
	prefix := "01"
	_, errno := root.Mkdir(ctx, prefix, 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("RootNode.Mkdir failed: %v", errno)
	}

	// 2. Create prefixNode manually for testing
	prefixNode := &FileNode{
		cfs:  root.cfs,
		path: meta.Path{Prefix: prefix, Key: "", Kind: meta.PathIsPrefixFolder},
	}

	// 3. Create a subdirectory via FileNode (Mkdir)
	subDirName := "subdir"
	var out fuse.EntryOut
	_, errno = prefixNode.Mkdir(ctx, subDirName, 0o755, &out)
	if errno != 0 {
		t.Fatalf("FileNode.Mkdir failed: %v", errno)
	}

	// Verify in store
	p := meta.Path{Kind: meta.PathIsSubFolder, Prefix: prefix, Key: subDirName + "/"}
	exists, _ := st.Exists(p)
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
	exists, _ = st.Exists(p)
	if exists {
		t.Errorf("Old subdirectory still exists in store")
	}
	newP := meta.Path{Kind: meta.PathIsSubFolder, Prefix: prefix, Key: newName + "/"}
	exists, _ = st.Exists(newP)
	if !exists {
		t.Errorf("New subdirectory not found in store")
	}

	// 6. Rmdir
	errno = prefixNode.Rmdir(ctx, newName)
	if errno != 0 {
		t.Fatalf("FileNode.Rmdir failed: %v", errno)
	}
	exists, _ = st.Exists(newP)
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

func TestFileNode_FlushFsync(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	// 1. Create a prefix directory via RootNode
	prefix := "01"
	_, errno := root.Mkdir(ctx, prefix, 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("Mkdir failed: %v", errno)
	}

	// 2. Create prefix node manually for testing
	prefixNode := &FileNode{
		cfs:  root.cfs,
		path: meta.Path{Prefix: prefix, Key: "", Kind: meta.PathIsPrefixFolder},
	}

	// 3. Create a file via prefixNode
	fileName := "testfile"
	var out fuse.EntryOut
	_, fh, _, errno := prefixNode.Create(ctx, fileName, 0, 0o644, &out)
	if errno != 0 {
		t.Fatalf("Create failed: %v", errno)
	}
	h := fh.(*CacheFileHandle)

	// Create the FileNode manually for testing methods
	fnode := &FileNode{
		cfs:  root.cfs,
		path: h.path,
	}

	// 4. Write some data to the handle (buffer)
	data := []byte("hello world")
	_, errno = h.Write(ctx, data, 0)
	if errno != 0 {
		t.Fatalf("Write failed: %v", errno)
	}

	// 5. Verify it's NOT in the store yet (since Flush hasn't been called)
	p := h.path
	gotData, _, err := st.Read(p)
	// In BoltDB store, if it doesn't exist it returns an error
	if err == nil && len(gotData) > 0 {
		t.Errorf("Data should not be in store yet")
	}

	// 6. Call Flush on the node
	errno = fnode.Flush(ctx, fh)
	if errno != 0 {
		t.Fatalf("Flush failed: %v", errno)
	}

	// 7. Verify it IS in the store now
	gotData, _, err = st.Read(p)
	if err != nil {
		t.Fatalf("Read from store failed: %v", err)
	}
	if string(gotData) != string(data) {
		t.Errorf("Got data %q, want %q", string(gotData), string(data))
	}

	// 8. Write more data
	data2 := []byte("foobar")
	_, errno = h.Write(ctx, data2, int64(len(data)))
	if errno != 0 {
		t.Fatalf("Write2 failed: %v", errno)
	}

	// 9. Call Fsync on the node
	errno = fnode.Fsync(ctx, fh, 0)
	if errno != 0 {
		t.Fatalf("Fsync failed: %v", errno)
	}

	// 10. Verify it IS in the store now
	gotData, _, err = st.Read(p)
	if err != nil {
		t.Fatalf("Read from store failed: %v", err)
	}
	expected := string(data) + string(data2)
	if string(gotData) != expected {
		t.Errorf("Got data %q, want %q", string(gotData), expected)
	}
}

func TestFileNode_FlushFsync_NilHandle(t *testing.T) {
	root, _ := newTestRoot(t)
	ctx := context.Background()
	fnode := &FileNode{cfs: root.cfs}

	if errno := fnode.Flush(ctx, nil); errno != 0 {
		t.Errorf("Flush(nil) = %v, want 0", errno)
	}
	if errno := fnode.Fsync(ctx, nil, 0); errno != 0 {
		t.Errorf("Fsync(nil) = %v, want 0", errno)
	}
}

func TestFileNode_Xattr(t *testing.T) {
	root, _ := newTestRoot(t)
	ctx := context.Background()

	// 1. Create a prefix directory via RootNode
	prefix := "01"
	_, errno := root.Mkdir(ctx, prefix, 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("Mkdir failed: %v", errno)
	}

	prefixNode := &FileNode{
		cfs:  root.cfs,
		path: meta.Path{Prefix: prefix, Key: "", Kind: meta.PathIsPrefixFolder},
	}

	// 2. Create a file
	fileName := "xattrfile"
	var out fuse.EntryOut
	_, _, _, errno = prefixNode.Create(ctx, fileName, 0, 0o644, &out)
	if errno != 0 {
		t.Fatalf("Create failed: %v", errno)
	}

	// Create the FileNode manually for testing methods
	childKey := meta.ChildKey(prefixNode.path, fileName, false)
	fnode := &FileNode{
		cfs:  root.cfs,
		path: meta.Path{Kind: meta.PathIsFile, Prefix: prefix, Key: childKey},
	}

	// Setxattr
	xattrName := "user.test"
	xattrVal := []byte("test_value")
	errno = fnode.Setxattr(ctx, xattrName, xattrVal, 0)
	if errno != 0 {
		t.Fatalf("Setxattr failed: %v", errno)
	}

	// Getxattr
	var getBuf []byte
	size, errno := fnode.Getxattr(ctx, xattrName, getBuf)
	if errno != syscall.ERANGE {
		t.Fatalf("Getxattr for size failed: %v", errno)
	}
	getBuf = make([]byte, size)
	_, errno = fnode.Getxattr(ctx, xattrName, getBuf)
	if errno != 0 {
		t.Fatalf("Getxattr failed: %v", errno)
	}
	if string(getBuf) != string(xattrVal) {
		t.Errorf("Getxattr = %q, want %q", getBuf, xattrVal)
	}

	// Listxattr
	var listBuf []byte
	size, errno = fnode.Listxattr(ctx, listBuf)
	if errno != syscall.ERANGE {
		t.Fatalf("Listxattr for size failed: %v", errno)
	}
	listBuf = make([]byte, size)
	_, errno = fnode.Listxattr(ctx, listBuf)
	if errno != 0 {
		t.Fatalf("Listxattr failed: %v", errno)
	}
	expectedList := "user.test\x00"
	if string(listBuf) != expectedList {
		t.Errorf("Listxattr = %q, want %q", listBuf, expectedList)
	}

	// Removexattr
	errno = fnode.Removexattr(ctx, xattrName)
	if errno != 0 {
		t.Fatalf("Removexattr failed: %v", errno)
	}

	// Getxattr again
	_, errno = fnode.Getxattr(ctx, xattrName, getBuf)
	if errno != syscall.ENODATA {
		t.Errorf("Getxattr after Removexattr = %v, want ENODATA", errno)
	}
}
