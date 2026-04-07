package fs

import (
	"context"
	"errors"
	"os"
	"strconv"
	"syscall"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
	"github.com/lch/cachefs/store"
)

// RootNode represents the filesystem root and lists prefix directories.
type RootNode struct {
	gfs.Inode
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

func prefixIno(name string) uint64 {
	ino, err := strconv.ParseUint(name, 16, 8)
	if err != nil {
		return 0
	}
	return ino + 2
}

func (n *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if !isValidPrefix(name) {
		return nil, syscall.ENOENT
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	exists, err := n.cfs.Store.PrefixExists(name)
	if err != nil {
		return nil, gfs.ToErrno(err)
	}
	if !exists {
		return nil, syscall.ENOENT
	}

	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|meta.DefaultDirMode, prefixIno(name))
	return n.newPrefixInode(ctx, name), 0
}

func (n *RootNode) Readdir(ctx context.Context) (gfs.DirStream, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	prefixes, err := n.cfs.Store.ListPrefixes()
	if err != nil {
		return nil, gfs.ToErrno(err)
	}

	entries := make([]fuse.DirEntry, 0, len(prefixes))
	for _, prefix := range prefixes {
		entries = append(entries, fuse.DirEntry{
			Name: prefix,
			Mode: fuse.S_IFDIR,
			Ino:  prefixIno(prefix),
		})
	}
	return gfs.NewListDirStream(entries), 0
}

func (n *RootNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if !isValidPrefix(name) {
		return nil, syscall.EINVAL
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	if err := n.cfs.Store.CreatePrefix(name); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, syscall.EEXIST
		}
		return nil, gfs.ToErrno(err)
	}

	perm := mode & 0o777
	if perm == 0 {
		perm = meta.DefaultDirMode
	}
	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|perm, prefixIno(name))
	return n.newPrefixInode(ctx, name), 0
}

func (n *RootNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if !isValidPrefix(name) {
		return syscall.EINVAL
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	files, err := n.cfs.Store.ListFiles(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		return gfs.ToErrno(err)
	}
	if len(files) != 0 {
		return syscall.ENOTEMPTY
	}

	if err := n.cfs.Store.RemovePrefix(name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		if errors.Is(err, store.ErrPrefixNotEmpty) {
			return syscall.ENOTEMPTY
		}
		return gfs.ToErrno(err)
	}
	return 0
}

func (n *RootNode) Getattr(ctx context.Context, fh gfs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil {
		return syscall.EIO
	}

	now := time.Now()
	out.Mode = fuse.S_IFDIR | meta.DefaultDirMode
	out.Nlink = 2
	out.Size = 0
	out.Blocks = 0
	out.Blksize = 4096
	out.Uid = n.cfs.Uid
	out.Gid = n.cfs.Gid
	out.Ino = 1
	out.SetTimes(&now, &now, &now)
	return 0
}

func (n *RootNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}
	type statfser interface {
		Statfs() (syscall.Statfs_t, error)
	}
	if sf, ok := n.cfs.Store.(statfser); ok {
		st, err := sf.Statfs()
		if err != nil {
			return gfs.ToErrno(err)
		}
		out.FromStatfsT(&st)
		return 0
	}

	prefixes, err := n.cfs.Store.ListPrefixes()
	if err != nil {
		return gfs.ToErrno(err)
	}

	var fileCount uint64
	for _, prefix := range prefixes {
		files, err := n.cfs.Store.ListFiles(prefix)
		if err != nil {
			return gfs.ToErrno(err)
		}
		fileCount += uint64(len(files))
	}

	var st syscall.Statfs_t
	st.Bsize = 4096
	st.Frsize = 4096
	st.Blocks = 1
	st.Bfree = 1
	st.Bavail = 1
	st.Files = fileCount
	st.Ffree = 1
	st.Namelen = 255
	out.FromStatfsT(&st)
	return 0
}

func (n *RootNode) prefixExists(name string) (bool, error) {
	return n.cfs.Store.PrefixExists(name)
}

func (n *RootNode) newPrefixInode(ctx context.Context, name string) (child *gfs.Inode) {
	ops := &PrefixDirNode{cfs: n.cfs, prefix: name}
	return newInodeOrPlaceholder(&n.Inode, ctx, ops, gfs.StableAttr{Mode: fuse.S_IFDIR, Ino: prefixIno(name)})
}

func fillDirEntryOut(out *fuse.EntryOut, cfs *CacheFS, mode uint32, ino uint64) {
	if out == nil || cfs == nil {
		return
	}

	now := time.Now()
	out.Mode = mode
	out.Nlink = 2
	out.Size = 0
	out.Blocks = 0
	out.Blksize = 4096
	out.Uid = cfs.Uid
	out.Gid = cfs.Gid
	out.Ino = ino
	out.SetTimes(&now, &now, &now)
}
