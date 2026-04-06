package fs

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// FileNode represents a cached regular file inside a prefix directory.
type FileNode struct {
	gfs.Inode
	cfs      *CacheFS
	prefix   string
	filename string
}

func (n *FileNode) Open(ctx context.Context, flags uint32) (gfs.FileHandle, uint32, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, 0, syscall.EIO
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, n.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, syscall.ENOENT
		}
		return nil, 0, gfs.ToErrno(err)
	}

	content, err := n.cfs.Store.GetContent(n.prefix, n.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, syscall.ENOENT
		}
		return nil, 0, gfs.ToErrno(err)
	}
	attrCopy := *attr
	attrCopy.Size = uint64(len(content))

	h := &CacheFileHandle{
		cfs:      n.cfs,
		prefix:   n.prefix,
		filename: n.filename,
		attr:     &attrCopy,
		buf:      content,
	}
	return h, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *FileNode) Getattr(ctx context.Context, fh gfs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}
	if handle, ok := fh.(*CacheFileHandle); ok {
		return handle.Getattr(ctx, out)
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, n.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	fillFileAttrOut(out, n.cfs, attr, fileIno(n.prefix, n.filename))
	return 0
}

func (n *FileNode) Setattr(ctx context.Context, fh gfs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, n.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	content, err := n.cfs.Store.GetContent(n.prefix, n.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	changedSize := false
	changedMeta := false
	mtimeSet := false
	ctimeSet := false
	if size, ok := in.GetSize(); ok {
		changedSize = true
		changedMeta = true
		content = resizeContent(content, size)
		attr.Size = size
	}

	if mode, ok := in.GetMode(); ok {
		changedMeta = true
		attr.Mode = mode & 0o777
	}
	if uid, ok := in.GetUID(); ok {
		changedMeta = true
		attr.Uid = uid
	}
	if gid, ok := in.GetGID(); ok {
		changedMeta = true
		attr.Gid = gid
	}
	if atime, ok := in.GetATime(); ok {
		attr.Atime = atime.Unix()
	}
	if mtime, ok := in.GetMTime(); ok {
		attr.Mtime = mtime.Unix()
		mtimeSet = true
	}
	if ctime, ok := in.GetCTime(); ok {
		attr.Ctime = ctime.Unix()
		ctimeSet = true
	}
	if changedMeta {
		now := time.Now().Unix()
		if changedSize && !mtimeSet {
			attr.Mtime = now
		}
		if !ctimeSet {
			attr.Ctime = now
		}
	}
	attr.Size = uint64(len(content))

	if err := n.cfs.Store.PutFile(n.prefix, n.filename, content, attr); err != nil {
		if errors.Is(err, os.ErrExist) {
			return syscall.EEXIST
		}
		return gfs.ToErrno(err)
	}

	fillFileAttrOut(out, n.cfs, attr, fileIno(n.prefix, n.filename))
	if fh != nil {
		if handle, ok := fh.(*CacheFileHandle); ok {
			handle.mu.Lock()
			handle.buf = content
			attrCopy := *attr
			handle.attr = &attrCopy
			handle.dirty = false
			handle.mu.Unlock()
		}
	}
	return 0
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
