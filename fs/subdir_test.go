package fs

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

func TestPrefixDirSubdirLifecycle(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	dir := &PrefixDirNode{cfs: root.cfs, prefix: "aa"}
	ctx := context.Background()

	var out fuse.EntryOut
	if inode, errno := dir.Mkdir(ctx, "foo", 0, &out); errno != 0 || inode == nil {
		t.Fatalf("Mkdir foo = (%v, %v), want inode and OK", inode, errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Mkdir out mode = %#o, want directory", out.Mode)
	}

	if inode, errno := dir.Lookup(ctx, "foo", &out); errno != 0 || inode == nil {
		t.Fatalf("Lookup subdir = (%v, %v), want inode and OK", inode, errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Lookup subdir mode = %#o, want directory", out.Mode)
	}

	subdir := &SubdirNode{cfs: root.cfs, prefix: "aa", dirname: "foo"}
	if _, fh, _, errno := subdir.Create(ctx, "nested", 0, 0o644, &fuse.EntryOut{}); errno != 0 || fh == nil {
		t.Fatalf("Subdir Create nested = (%v, %v), want handle and OK", fh, errno)
	}

	entries, err := collectDirEntries(t, func() (gfs.DirStream, syscall.Errno) { return subdir.Readdir(ctx) })
	if err != nil {
		t.Fatalf("Subdir Readdir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "nested" || entries[0].Mode&syscall.S_IFREG == 0 {
		t.Fatalf("Subdir Readdir = %#v, want one regular file nested", entries)
	}

	if _, fh, _, errno := dir.Create(ctx, "foo", 0, 0o644, &fuse.EntryOut{}); errno != 0 || fh == nil {
		t.Fatalf("Create direct foo = (%v, %v), want handle and OK", fh, errno)
	}

	entries, err = collectDirEntries(t, func() (gfs.DirStream, syscall.Errno) { return dir.Readdir(ctx) })
	if err != nil {
		t.Fatalf("Prefix Readdir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Prefix Readdir len = %d, want 2", len(entries))
	}
	if entries[0].Name != "foo" || entries[0].Mode&syscall.S_IFREG == 0 {
		t.Fatalf("Prefix Readdir[0] = %#v, want regular foo", entries[0])
	}
	if entries[1].Name != "foo" || entries[1].Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Prefix Readdir[1] = %#v, want directory foo", entries[1])
	}

	if inode, errno := dir.Lookup(ctx, "foo", &out); errno != 0 || inode == nil {
		t.Fatalf("Lookup foo after direct create = (%v, %v), want inode and OK", inode, errno)
	}
	if out.Mode&syscall.S_IFREG == 0 {
		t.Fatalf("Lookup foo after direct create mode = %#o, want regular file", out.Mode)
	}

	if errno := dir.Rmdir(ctx, "foo"); errno != syscall.ENOTEMPTY {
		t.Fatalf("Rmdir non-empty foo errno = %v, want ENOTEMPTY", errno)
	}

	if errno := subdir.Unlink(ctx, "nested"); errno != 0 {
		t.Fatalf("Subdir Unlink nested errno = %v, want OK", errno)
	}

	if errno := dir.Rmdir(ctx, "foo"); errno != 0 {
		t.Fatalf("Rmdir empty foo errno = %v, want OK", errno)
	}

	if inode, errno := dir.Lookup(ctx, "foo", &out); errno != 0 || inode == nil {
		t.Fatalf("Lookup foo after Rmdir = (%v, %v), want direct file inode and OK", inode, errno)
	}
	if out.Mode&syscall.S_IFREG == 0 {
		t.Fatalf("Lookup foo after Rmdir mode = %#o, want regular file", out.Mode)
	}

	if errno := dir.Unlink(ctx, "foo"); errno != 0 {
		t.Fatalf("Unlink direct foo errno = %v, want OK", errno)
	}
	if inode, errno := dir.Lookup(ctx, "foo", &out); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup foo after final unlink = (%v, %v), want (nil, ENOENT)", inode, errno)
	}
}

func TestPrefixAndSubdirRenameMovesFiles(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	dir := &PrefixDirNode{cfs: root.cfs, prefix: "aa"}
	ctx := context.Background()

	if inode, errno := dir.Mkdir(ctx, "foo", 0, &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Mkdir foo = (%v, %v), want inode and OK", inode, errno)
	}
	if inode, errno := dir.Mkdir(ctx, "bar", 0, &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Mkdir bar = (%v, %v), want inode and OK", inode, errno)
	}

	foo := &SubdirNode{cfs: root.cfs, prefix: "aa", dirname: "foo"}
	bar := &SubdirNode{cfs: root.cfs, prefix: "aa", dirname: "bar"}

	payload := []byte("payload")
	if err := st.WriteFile("aa", "src", payload, meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile src: %v", err)
	}

	if errno := dir.Rename(ctx, "src", foo, "nested", 0); errno != 0 {
		t.Fatalf("Rename direct->subdir errno = %v, want OK", errno)
	}
	got, _, err := st.ReadSubdirFile("aa", "foo", "nested")
	if err != nil {
		t.Fatalf("ReadSubdirFile after direct->subdir rename: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("subdir content after direct->subdir rename = %q, want %q", got, payload)
	}
	if inode, errno := dir.Lookup(ctx, "src", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup src after direct->subdir rename = (%v, %v), want (nil, ENOENT)", inode, errno)
	}

	if errno := foo.Rename(ctx, "nested", bar, "other", 0); errno != 0 {
		t.Fatalf("Rename subdir->subdir errno = %v, want OK", errno)
	}
	got, _, err = st.ReadSubdirFile("aa", "bar", "other")
	if err != nil {
		t.Fatalf("ReadSubdirFile after subdir->subdir rename: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("subdir content after subdir->subdir rename = %q, want %q", got, payload)
	}
	if inode, errno := foo.Lookup(ctx, "nested", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup nested after subdir->subdir rename = (%v, %v), want (nil, ENOENT)", inode, errno)
	}

	if errno := bar.Rename(ctx, "other", dir, "moved", 0); errno != 0 {
		t.Fatalf("Rename subdir->direct errno = %v, want OK", errno)
	}
	got, _, err = st.ReadFile("aa", "moved")
	if err != nil {
		t.Fatalf("ReadFile after subdir->direct rename: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("direct content after subdir->direct rename = %q, want %q", got, payload)
	}
	if inode, errno := bar.Lookup(ctx, "other", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup other after subdir->direct rename = (%v, %v), want (nil, ENOENT)", inode, errno)
	}
}

func collectDirEntries(t *testing.T, next func() (gfs.DirStream, syscall.Errno)) ([]fuse.DirEntry, error) {
	t.Helper()
	ds, errno := next()
	if errno != 0 {
		return nil, errno
	}
	defer ds.Close()

	var entries []fuse.DirEntry
	for ds.HasNext() {
		entry, errno := ds.Next()
		if errno != 0 {
			return nil, errno
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
