package fs

import (
	"context"
	"reflect"
	"syscall"
	"testing"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestPrefixLookupCreateReaddirAndUnlink(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	dir := &PrefixDirNode{cfs: root.cfs, prefix: "aa"}
	ctx := context.Background()

	if inode, errno := dir.Lookup(ctx, "alpha", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup missing file = (%v, %v), want (nil, ENOENT)", inode, errno)
	}

	created, fh, fuseFlags, errno := dir.Create(ctx, "alpha", 0, 0o640, &fuse.EntryOut{})
	if errno != 0 || created == nil || fh == nil || fuseFlags != 0 {
		t.Fatalf("Create = (%v, %v, %v, %v), want inode+fh and OK", created, fh, fuseFlags, errno)
	}

	var out fuse.EntryOut
	if inode, errno := dir.Lookup(ctx, "alpha", &out); errno != 0 || inode == nil {
		t.Fatalf("Lookup created file = (%v, %v), want inode and OK", inode, errno)
	}
	if out.Mode&syscall.S_IFREG == 0 || out.Mode&0o777 != 0o640 {
		t.Fatalf("Lookup mode = %#o, want regular file 0640", out.Mode)
	}

	names, err := readPrefixNames(dir)
	if err != nil {
		t.Fatalf("Readdir: %v", err)
	}
	if !reflect.DeepEqual(names, []string{"alpha"}) {
		t.Fatalf("Readdir = %v, want [alpha]", names)
	}

	if _, _, _, errno := dir.Create(ctx, "alpha", 0, 0o600, &fuse.EntryOut{}); errno != syscall.EEXIST {
		t.Fatalf("Create duplicate errno = %v, want EEXIST", errno)
	}

	if errno := dir.Unlink(ctx, "alpha"); errno != 0 {
		t.Fatalf("Unlink errno = %v, want OK", errno)
	}
	if inode, errno := dir.Lookup(ctx, "alpha", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup after unlink = (%v, %v), want (nil, ENOENT)", inode, errno)
	}

	names, err = readPrefixNames(dir)
	if err != nil {
		t.Fatalf("Readdir after unlink err = %v, want nil", err)
	}
	if len(names) != 0 {
		t.Fatalf("Readdir after unlink = %v, want empty", names)
	}
}

func TestPrefixGetattrAndRename(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix aa: %v", err)
	}
	if err := st.CreatePrefix("bb"); err != nil {
		t.Fatalf("CreatePrefix bb: %v", err)
	}

	aa := &PrefixDirNode{cfs: root.cfs, prefix: "aa"}
	bb := &PrefixDirNode{cfs: root.cfs, prefix: "bb"}
	ctx := context.Background()

	if _, _, _, errno := aa.Create(ctx, "alpha", 0, 0o644, &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("Create alpha errno = %v", errno)
	}

	var attr fuse.AttrOut
	if errno := aa.Getattr(ctx, nil, &attr); errno != 0 {
		t.Fatalf("Getattr errno = %v", errno)
	}
	if attr.Mode&syscall.S_IFDIR == 0 || attr.Uid != 1000 || attr.Gid != 1001 {
		t.Fatalf("Getattr = %#v, want directory attrs for 1000:1001", attr)
	}

	if errno := aa.Rename(ctx, "alpha", aa, "beta", 0); errno != 0 {
		t.Fatalf("Rename within prefix errno = %v", errno)
	}
	if inode, errno := aa.Lookup(ctx, "alpha", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup old name after rename = (%v, %v), want (nil, ENOENT)", inode, errno)
	}
	if inode, errno := aa.Lookup(ctx, "beta", &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Lookup new name after rename = (%v, %v), want inode and OK", inode, errno)
	}

	if errno := aa.Rename(ctx, "beta", bb, "gamma", 0); errno != 0 {
		t.Fatalf("Rename across prefix errno = %v", errno)
	}
	if inode, errno := aa.Lookup(ctx, "beta", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup source after cross-prefix rename = (%v, %v), want (nil, ENOENT)", inode, errno)
	}
	if inode, errno := bb.Lookup(ctx, "gamma", &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Lookup destination after cross-prefix rename = (%v, %v), want inode and OK", inode, errno)
	}

	names, err := readPrefixNames(bb)
	if err != nil {
		t.Fatalf("Readdir target prefix: %v", err)
	}
	if !reflect.DeepEqual(names, []string{"gamma"}) {
		t.Fatalf("Readdir target prefix = %v, want [gamma]", names)
	}
}

func readPrefixNames(dir *PrefixDirNode) ([]string, error) {
	ds, errno := dir.Readdir(context.Background())
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

var _ gfs.FileHandle = (*CacheFileHandle)(nil)
