package fs

import (
	"context"
	"errors"
	"os"
	"syscall"

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

	data, attr, err := n.cfs.Store.ReadFile(n.prefix, n.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, syscall.ENOENT
		}
		return nil, 0, gfs.ToErrno(err)
	}
	attrCopy := *attr

	h := &CacheFileHandle{
		cfs:      n.cfs,
		prefix:   n.prefix,
		filename: n.filename,
		attr:     &attrCopy,
		mode:     attr.Mode,
		buf:      data,
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

	if size, ok := in.GetSize(); ok {
		if err := n.cfs.Store.Truncate(n.prefix, n.filename, size); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return gfs.ToErrno(err)
		}
		if handle, ok := fh.(*CacheFileHandle); ok && handle != nil {
			handle.mu.Lock()
			handle.buf = resizeContent(handle.buf, size)
			handle.mode = attr.Mode
			handle.mu.Unlock()
		}
		attr, err = n.cfs.Store.GetMeta(n.prefix, n.filename)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return gfs.ToErrno(err)
		}
	}

	updated := *attr
	changed := false
	if mode, ok := in.GetMode(); ok {
		updated.Mode = mode & 0o777
		changed = true
	}
	if uid, ok := in.GetUID(); ok {
		updated.Uid = uid
		changed = true
	}
	if gid, ok := in.GetGID(); ok {
		updated.Gid = gid
		changed = true
	}
	if atime, ok := in.GetATime(); ok {
		updated.Atime = atime.Unix()
		changed = true
	}
	if mtime, ok := in.GetMTime(); ok {
		updated.Mtime = mtime.Unix()
		changed = true
	}
	if ctime, ok := in.GetCTime(); ok {
		updated.Ctime = ctime.Unix()
		changed = true
	}
	if changed {
		if err := n.cfs.Store.UpdateMeta(n.prefix, n.filename, &updated); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return gfs.ToErrno(err)
		}
		attr = &updated
	}

	fillFileAttrOut(out, n.cfs, attr, fileIno(n.prefix, n.filename))
	if fh != nil {
		if handle, ok := fh.(*CacheFileHandle); ok {
			handle.mu.Lock()
			attrCopy := *attr
			handle.attr = &attrCopy
			handle.mode = attr.Mode
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
