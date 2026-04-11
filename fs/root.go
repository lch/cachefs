package fs

import (
	"context"
	"syscall"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
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

func (n *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	return nil, 0
}

func (n *RootNode) Readdir(ctx context.Context) (gfs.DirStream, syscall.Errno) {
	return nil, 0
}

func (n *RootNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	return nil, 0
}

func (n *RootNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return 0
}

func (n *RootNode) Getattr(ctx context.Context, fh gfs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return 0
}

func (n *RootNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return 0
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
	out.Blksize = meta.DefaultBlockSize
	out.Uid = cfs.Uid
	out.Gid = cfs.Gid
	out.Ino = ino
	out.SetTimes(&now, &now, &now)
}
