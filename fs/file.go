package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
	"github.com/lch/cachefs/store"
)

// FileNode represents a cached regular file or directory inside a prefix directory.
type FileNode struct {
	fs.Inode
	cfs  *CacheFS
	path meta.Path
}

var (
	_ fs.NodeOpener    = (*FileNode)(nil)
	_ fs.NodeGetattrer = (*FileNode)(nil)
	_ fs.NodeSetattrer = (*FileNode)(nil)
	_ fs.NodeMkdirer   = (*FileNode)(nil)
	_ fs.NodeLookuper  = (*FileNode)(nil)
	_ fs.NodeRmdirer   = (*FileNode)(nil)
	_ fs.NodeRenamer   = (*FileNode)(nil)
	_ fs.NodeReaddirer = (*FileNode)(nil)
	_ fs.NodeOpendirer = (*FileNode)(nil)
	_ fs.NodeCreater   = (*FileNode)(nil)
	_ fs.NodeUnlinker  = (*FileNode)(nil)
)

func (n *FileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, 0, syscall.EIO
	}
	data, attr, err := n.cfs.Store.Read(n.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, syscall.ENOENT
		}
		return nil, 0, fs.ToErrno(err)
	}
	if attr.IsDir() {
		return nil, 0, syscall.EISDIR
	}
	attrCopy := *attr

	h := &CacheFileHandle{
		cfs:  n.cfs,
		path: n.path,
		attr: &attrCopy,
		mode: attr.Mode,
		buf:  data,
	}
	return h, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *FileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}
	if handle, ok := fh.(*CacheFileHandle); ok {
		return handle.Getattr(ctx, out)
	}
	attr, err := n.cfs.Store.GetMeta(n.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return fs.ToErrno(err)
	}
	fillFileAttrOut(out, n.cfs, attr, pathIno(n.path))
	return 0
}

func (n *FileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	attr, err := n.cfs.Store.GetMeta(n.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return fs.ToErrno(err)
	}
	if size, ok := in.GetSize(); ok {
		if err := n.cfs.Store.Truncate(n.path, size); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return fs.ToErrno(err)
		}
		if handle, ok := fh.(*CacheFileHandle); ok && handle != nil {
			handle.mu.Lock()
			handle.buf = resizeContent(handle.buf, size)
			handle.mode = attr.Mode
			handle.mu.Unlock()
		}
		attr, err = n.cfs.Store.GetMeta(n.path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return fs.ToErrno(err)
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
		if err := n.cfs.Store.UpdateMeta(n.path, &updated); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return fs.ToErrno(err)
		}
		attr = &updated
	}
	fillFileAttrOut(out, n.cfs, attr, pathIno(n.path))
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

func (n *FileNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	if !n.IsDir() {
		return nil, syscall.EINVAL
	}

	childP := n.path
	newKey := fmt.Sprintf("%s/%s", childP.Key, name)
	childP.Key = newKey

	exists, err := n.cfs.Store.Exists(childP)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	if !exists {
		return nil, syscall.ENOENT
	}

	attr, err := n.cfs.Store.GetMeta(childP)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	ino := pathIno(childP)
	child := n.GetChild(name)
	if child != nil {
		return child, 0
	}

	stable := fs.StableAttr{
		Ino: ino,
	}
	if attr.IsDir() {
		stable.Mode = fuse.S_IFDIR
	} else {
		stable.Mode = fuse.S_IFREG
	}

	ops := &FileNode{
		cfs:  n.cfs,
		path: childP,
	}
	child = newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	if attr.IsDir() {
		fillDirEntryOut(out, n.cfs, attr.Mode, ino)
	} else {
		fillFileEntryOut(out, n.cfs, attr, ino)
	}
	return child, 0
}

func (n *FileNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}
	childKey := meta.ChildKey(n.path, name)
	childP := meta.Path{Kind: meta.PathIsSubFolder, Prefix: n.path.Prefix, Key: childKey}

	err := n.cfs.Store.Create(childP)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, syscall.EEXIST
		}
		return nil, fs.ToErrno(err)
	}

	attr, _ := n.cfs.Store.GetMeta(childP)
	attr.Mode = uint32(syscall.S_IFDIR) | (mode & 0o777)
	_ = n.cfs.Store.UpdateMeta(childP, attr)

	ino := pathIno(childP)
	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  ino,
	}
	ops := &FileNode{
		cfs:  n.cfs,
		path: childP,
	}
	child := newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	fillDirEntryOut(out, n.cfs, attr.Mode, ino)
	return child, 0
}

func (n *FileNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	childKey := meta.ChildKey(n.path, name)
	childP := meta.Path{Kind: meta.PathIsSubFolder, Prefix: n.path.Prefix, Key: childKey}

	err := n.cfs.Store.Delete(childP)
	if err != nil {
		if errors.Is(err, store.ErrFolderNotEmpty) {
			return syscall.ENOTEMPTY
		}
		return fs.ToErrno(err)
	}
	return 0
}

func (n *FileNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	oldChildKey := fmt.Sprintf("%s/%s/", n.path.Key, name)
	oldChildP := meta.Path{Kind: meta.PathIsSubFolder, Prefix: n.path.Prefix, Key: oldChildKey}
	// Check if oldPath is dir
	isDir := false
	if exists, _ := n.cfs.Store.Exists(oldChildP); exists {
		isDir = true
	} else {
		oldChildP.Key = strings.TrimSuffix(oldChildP.Key, "/")
		oldChildP.Kind = meta.PathIsFile
	}

	_, ok := newParent.(*FileNode)
	if !ok {
		return syscall.EXDEV
	}

	newChildKey := fmt.Sprintf("%s/%s", n.path.Key, newName)
	if isDir {
		newChildKey = newChildKey + "/"
	}
	newChildP := meta.Path{Kind: oldChildP.Kind, Prefix: n.path.Prefix, Key: newChildKey}
	if err := n.cfs.Store.Rename(oldChildP, newChildP); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

func (n *FileNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}
	list, err := n.cfs.Store.List(n.path)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	r := make([]fuse.DirEntry, 0, len(list))
	for _, entryName := range list {
		childP := n.path
		d := fuse.DirEntry{
			Name: strings.TrimSuffix(entryName, "/"),
			Ino:  pathIno(childP),
		}

		attr, err := n.cfs.Store.GetMeta(childP)
		if err == nil {
			d.Mode = attr.Mode
		} else {
			if strings.HasSuffix(entryName, "/") {
				d.Mode = fuse.S_IFDIR | meta.DefaultDirMode
			} else {
				d.Mode = fuse.S_IFREG | meta.DefaultFileMode
			}
		}
		r = append(r, d)
	}
	return fs.NewListDirStream(r), 0
}

func (n *FileNode) Opendir(ctx context.Context) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}
	attr, err := n.cfs.Store.GetMeta(n.path)
	if err != nil {
		return fs.ToErrno(err)
	}
	if !attr.IsDir() {
		return syscall.ENOTDIR
	}
	return 0
}

func (n *FileNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, nil, 0, syscall.EIO
	}

	childKey := meta.ChildKey(n.path, name)
	childP := meta.Path{Kind: meta.PathIsFile, Prefix: n.path.Prefix, Key: childKey}

	err := n.cfs.Store.Create(childP)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, nil, 0, syscall.EEXIST
		}
		return nil, nil, 0, fs.ToErrno(err)
	}

	attr, _ := n.cfs.Store.GetMeta(childP)
	attr.Mode = uint32(syscall.S_IFREG) | (mode & 0o777)
	_ = n.cfs.Store.UpdateMeta(childP, attr)

	ino := pathIno(childP)
	stable := fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  ino,
	}
	ops := &FileNode{
		cfs:  n.cfs,
		path: childP,
	}
	childInode := newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	fillFileEntryOut(out, n.cfs, attr, ino)

	h := &CacheFileHandle{
		cfs:   n.cfs,
		path:  childP,
		attr:  attr,
		mode:  attr.Mode,
		buf:   nil,
		dirty: false,
	}

	return childInode, h, 0, 0
}

func (n *FileNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	childKey := meta.ChildKey(n.path, name)
	childP := meta.Path{Kind: meta.PathIsFile, Prefix: n.path.Prefix, Key: childKey}

	err := n.cfs.Store.Delete(childP)
	if err != nil {
		return fs.ToErrno(err)
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
