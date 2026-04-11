package fs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
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
	return nil, 0
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
	return nil, 0
}

func (n *RootNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return 0
}

func (n *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return 0
}

func (n *RootNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return 0
}
