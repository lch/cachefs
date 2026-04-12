package fs

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

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

func (n *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !meta.IsHexPrefix(name) {
		return nil, syscall.ENOENT
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	path := meta.Path{Kind: meta.PathIsPrefixFolder, Prefix: name}
	exists, err := n.cfs.Store.Exists(path)
	if err != nil {
		return nil, fs.ToErrno(err)
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
		path: path,
	}
	child = newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|meta.DefaultDirMode, ino)
	return child, 0
}

func (n *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}
	prefixList, err := n.cfs.Store.List(
		meta.Path{Kind: meta.PathIsRootFolder},
	)
	if err != nil {
		return nil, syscall.EIO
	}
	r := make([]fuse.DirEntry, 0, len(prefixList))
	for _, prefix := range prefixList {
		d := fuse.DirEntry{
			Name: prefix,
			Ino:  prefixDirIno(prefix),
			Mode: fuse.S_IFDIR,
		}
		r = append(r, d)
	}
	return fs.NewListDirStream(r), 0
}

func (n *RootNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !meta.IsHexPrefix(name) {
		return nil, syscall.EINVAL
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	path := meta.Path{Kind: meta.PathIsPrefixFolder, Prefix: name, Key: ""}
	err := n.cfs.Store.Create(path)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, syscall.EEXIST
		}
		return nil, fs.ToErrno(err)
	}

	ino := prefixDirIno(name)
	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  ino,
	}
	ops := &FileNode{
		cfs:  n.cfs,
		path: path,
	}
	child := newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|(mode&0o777), ino)
	return child, 0
}

func (n *RootNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if !meta.IsHexPrefix(name) {
		return syscall.EINVAL
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}
	path := meta.Path{Kind: meta.PathIsPrefixFolder, Prefix: name, Key: ""}
	err := n.cfs.Store.Delete(path)
	if err != nil {
		if errors.Is(err, store.ErrFolderNotEmpty) {
			return syscall.ENOTEMPTY
		}
		return syscall.EIO
	}
	return 0
}

func (n *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil {
		return syscall.EIO
	}

	now := time.Now()
	out.Mode = fuse.S_IFDIR | 0o755
	out.Uid = n.cfs.Uid
	out.Gid = n.cfs.Gid
	out.Ino = InodeRoot
	out.SetTimes(&now, &now, &now)
	return 0
}

func (n *RootNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	prefixes, err := n.cfs.Store.List(meta.Path{Kind: meta.PathIsRootFolder})
	if err != nil {
		return fs.ToErrno(err)
	}
	var fileCount uint64
	for _, prefix := range prefixes {
		files, err := n.cfs.Store.List(
			meta.Path{Kind: meta.PathIsPrefixFolder, Prefix: prefix},
		)
		if err != nil {
			return fs.ToErrno(err)
		}
		fileCount += uint64(len(files))
	}
	out.Bsize = meta.DefaultBlockSize
	out.Frsize = meta.DefaultBlockSize
	out.Blocks = 1 // 100M blocks
	out.Bfree = 1  // 80M free
	out.Bavail = 1 // 80M available
	out.Files = fileCount
	out.Ffree = 1 // 9M free files
	out.NameLen = 255
	return 0
}
