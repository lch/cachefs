package fs

import (
	"context"
	"errors"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
	"github.com/lch/cachefs/store"
)


var (
	_ = (fs.InodeEmbedder)((*RootNode)(nil))
	_ = (fs.NodeLookuper)((*RootNode)(nil))
	_ = (fs.NodeReaddirer)((*RootNode)(nil))
	_ = (fs.NodeMkdirer)((*RootNode)(nil))
	_ = (fs.NodeRmdirer)((*RootNode)(nil))
	_ = (fs.NodeGetattrer)((*RootNode)(nil))
	_ = (fs.NodeStatfser)((*RootNode)(nil))
)

// RootNode represents the filesystem root and lists prefix directories.
type RootNode struct {
	fs.Inode
	cfs *CacheFS
}

// NewRootNode builds the root node for a CacheFS instance.
func NewRootNode(cfs *CacheFS) *RootNode {
	return &RootNode{cfs: cfs}
}

// isValidPrefix reports whether name is a 2-character hex prefix.
func isValidPrefix(name string) bool {
	if len(name) != 2 {
		return false
	}
	for i := range 2 {
		c := name[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (n *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !isValidPrefix(name) {
		return nil, syscall.ENOENT
	}

	exists, err := n.cfs.Store.Exists(name)
	if err != nil {
		return nil, syscall.EIO
	}
	if !exists {
		return nil, syscall.ENOENT
	}

	ino := prefixDirIno(name)
	child := n.GetChild(name)
	if child != nil {
		return child, 0
	}

	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  ino,
	}
	ops := &FileNode{
		cfs:  n.cfs,
		path: name,
	}
	child = newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|meta.DefaultDirMode, ino)
	return child, 0
}

func (n *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	prefixList, err := n.cfs.Store.List("")
	if err != nil {
		return nil, syscall.EIO
	}
	r := make([]fuse.DirEntry, 0, len(prefixList))
	for _, v := range prefixList {
		d := fuse.DirEntry{
			Name: v,
			Ino:  prefixDirIno(v),
			Mode: meta.DefaultDirMode,
		}
		r = append(r, d)
	}
	return fs.NewListDirStream(r), 0
}

func (n *RootNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !isValidPrefix(name) {
		return nil, syscall.EINVAL
	}

	err := n.cfs.Store.Create(name)
	if err != nil {
		return nil, syscall.EIO
	}

	ino := prefixDirIno(name)
	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  ino,
	}
	ops := &FileNode{
		cfs:  n.cfs,
		path: name,
	}
	child := newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|(mode&0o777), ino)
	return child, 0
}

func (n *RootNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if !isValidPrefix(name) {
		return syscall.EINVAL
	}
	err := n.cfs.Store.Remove(name)
	if err != nil {
		if errors.Is(err, store.ErrPrefixNotEmpty) {
			return syscall.ENOTEMPTY
		}
		return syscall.EIO
	}
	return 0
}

func (n *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0o755
	out.Uid = n.cfs.Uid
	out.Gid = n.cfs.Gid
	out.Ino = InodeRoot
	return 0
}

func (n *RootNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Blocks = 100 * 1024 * 1024 // 100M blocks
	out.Bfree = 80 * 1024 * 1024   // 80M free
	out.Bavail = 80 * 1024 * 1024  // 80M available
	out.Files = 10 * 1024 * 1024   // 10M files
	out.Ffree = 9 * 1024 * 1024    // 9M free files
	out.Bsize = 4096               // 4K block size
	out.NameLen = 255
	return 0
}
