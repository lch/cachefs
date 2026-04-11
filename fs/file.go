package fs

import (
	"context"
	"syscall"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

// FileNode represents a cached regular file inside a prefix directory.
type FileNode struct {
	gfs.Inode
	cfs      *CacheFS
	prefix   string
	subdir   string
	filename string
}

func (n *FileNode) stableIno() uint64 {
	return 0
}

func (n *FileNode) readData() (data []byte, attr *meta.FileAttr, err error) {
	return
}

func (n *FileNode) getMeta() (attr *meta.FileAttr, err error) {
	return
}

func (n *FileNode) updateMeta(attr *meta.FileAttr) (err error) {
	return
}

func (n *FileNode) truncate(size uint64) (err error) {
	return
}

func (n *FileNode) newHandle(data []byte, attr *meta.FileAttr) *CacheFileHandle {
	return nil
}

func (n *FileNode) updatePath(prefix, subdir, filename string) {
}

func resizeContent(content []byte, size uint64) []byte {
	if uint64(len(content)) == size {
		return content
	}
	if uint64(len(content)) > size {
		return append([]byte(nil), content[:int(size)]...)
	}
	buf := make([]byte, int(size))
	copy(buf, content)
	return buf
}

func (n *FileNode) Open(ctx context.Context, flags uint32) (gfs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, syscall.EIO
}

func (n *FileNode) Getattr(ctx context.Context, fh gfs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return 0
}

func (n *FileNode) Setattr(ctx context.Context, fh gfs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return 0
}
