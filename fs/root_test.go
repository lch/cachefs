package fs

import (
	"context"
	"reflect"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
	"github.com/lch/cachefs/store"
)

func TestRootLookupMkdirAndReaddir(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	if inode, errno := root.Lookup(ctx, "zz", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup invalid prefix = (%v, %v), want (nil, ENOENT)", inode, errno)
	}
	if inode, errno := root.Lookup(ctx, "AA", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup uppercase prefix = (%v, %v), want (nil, ENOENT)", inode, errno)
	}

	if inode, errno := root.Lookup(ctx, "aa", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup missing prefix = (%v, %v), want (nil, ENOENT)", inode, errno)
	}

	created, errno := root.Mkdir(ctx, "aa", 0o700, &fuse.EntryOut{})
	if errno != 0 || created == nil {
		t.Fatalf("Mkdir = (%v, %v), want non-nil inode and OK", created, errno)
	}

	if inode, errno := root.Lookup(ctx, "aa", &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Lookup existing prefix = (%v, %v), want inode and OK", inode, errno)
	}

	entries, err := readDirEntries(root)
	if err != nil {
		t.Fatalf("Readdir: %v", err)
	}
	if !reflect.DeepEqual(entries, []string{"aa"}) {
		t.Fatalf("Readdir = %v, want [aa]", entries)
	}

	if inode, errno := root.Mkdir(ctx, "bb", 0o755, &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Mkdir bb = (%v, %v), want inode and OK", inode, errno)
	}
	entries, err = readDirEntries(root)
	if err != nil {
		t.Fatalf("Readdir after second mkdir: %v", err)
	}
	if !reflect.DeepEqual(entries, []string{"aa", "bb"}) {
		t.Fatalf("Readdir after second mkdir = %v, want [aa bb]", entries)
	}

	if err := st.CreatePrefix("cc"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	entries, err = readDirEntries(root)
	if err != nil {
		t.Fatalf("Readdir after direct store create: %v", err)
	}
	if !reflect.DeepEqual(entries, []string{"aa", "bb", "cc"}) {
		t.Fatalf("Readdir after direct store create = %v, want [aa bb cc]", entries)
	}
}

func TestRootRmdirAndGetattrStatfs(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	if inode, errno := root.Mkdir(ctx, "aa", 0o755, &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Mkdir aa = (%v, %v), want inode and OK", inode, errno)
	}
	if inode, errno := root.Mkdir(ctx, "bb", 0o755, &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Mkdir bb = (%v, %v), want inode and OK", inode, errno)
	}

	if err := st.WriteFile("aa", "alpha", []byte("x"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if errno := root.Rmdir(ctx, "aa"); errno != syscall.ENOTEMPTY {
		t.Fatalf("Rmdir non-empty errno = %v, want ENOTEMPTY", errno)
	}
	if errno := root.Rmdir(ctx, "zz"); errno != syscall.EINVAL {
		t.Fatalf("Rmdir invalid errno = %v, want EINVAL", errno)
	}

	var attr fuse.AttrOut
	if errno := root.Getattr(ctx, nil, &attr); errno != 0 {
		t.Fatalf("Getattr errno = %v", errno)
	}
	if attr.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Getattr mode = %#o, want directory", attr.Mode)
	}
	if attr.Uid != 1000 || attr.Gid != 1001 {
		t.Fatalf("Getattr owner = %d:%d, want 1000:1001", attr.Uid, attr.Gid)
	}

	var statfs fuse.StatfsOut
	if errno := root.Statfs(ctx, &statfs); errno != 0 {
		t.Fatalf("Statfs errno = %v", errno)
	}
	if statfs.Blocks == 0 || statfs.Bsize == 0 {
		t.Fatalf("Statfs unexpectedly empty: %#v", statfs)
	}
	if statfs.Files != 1 {
		t.Fatalf("Statfs files = %d, want 1", statfs.Files)
	}

	if errno := root.Rmdir(ctx, "aa"); errno != syscall.ENOTEMPTY {
		t.Fatalf("Rmdir non-empty errno = %v, want ENOTEMPTY", errno)
	}
	if err := st.DeleteFile("aa", "alpha"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if errno := root.Rmdir(ctx, "aa"); errno != 0 {
		t.Fatalf("Rmdir empty errno = %v, want OK", errno)
	}

	entries, err := readDirEntries(root)
	if err != nil {
		t.Fatalf("Readdir after rmdir: %v", err)
	}
	if !reflect.DeepEqual(entries, []string{"bb"}) {
		t.Fatalf("Readdir after rmdir = %v, want [bb]", entries)
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
