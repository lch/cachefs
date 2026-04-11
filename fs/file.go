package fs

import (
	"context"
	"errors"
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
	path string
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
	p, _ := meta.NewPathFromString(n.path)
	fillFileAttrOut(out, n.cfs, attr, pathIno(*p))
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
	p, _ := meta.NewPathFromString(n.path)
	fillFileAttrOut(out, n.cfs, attr, pathIno(*p))
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

	p, err := meta.NewPathFromString(n.path)
	if err != nil {
		return nil, syscall.EINVAL
	}
	childP := *p
	if childP.Key == "" {
		childP.Key = name
	} else {
		// If n.path is a directory it might have a trailing slash
		key := strings.TrimSuffix(childP.Key, "/")
		childP.Key = key + "/" + name
	}

	finalPath := childP.String()
	exists, err := n.cfs.Store.Exists(finalPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	if !exists {
		// Try with trailing slash if it's a directory
		if !strings.HasSuffix(finalPath, "/") {
			finalPath += "/"
			exists, _ = n.cfs.Store.Exists(finalPath)
			if exists {
				childP.Key += "/"
			}
		}
	}

	if !exists {
		return nil, syscall.ENOENT
	}

	attr, err := n.cfs.Store.GetMeta(finalPath)
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
		path: finalPath,
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

	p, err := meta.NewPathFromString(n.path)
	if err != nil {
		return nil, syscall.EINVAL
	}
	childP := *p
	key := strings.TrimSuffix(childP.Key, "/")
	if key == "" {
		childP.Key = name + "/"
	} else {
		childP.Key = key + "/" + name + "/"
	}
	finalPath := childP.String()

	err = n.cfs.Store.Create(finalPath)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, syscall.EEXIST
		}
		return nil, fs.ToErrno(err)
	}

	attr, _ := n.cfs.Store.GetMeta(finalPath)
	attr.Mode = uint32(syscall.S_IFDIR) | (mode & 0o777)
	_ = n.cfs.Store.UpdateMeta(finalPath, attr)

	ino := pathIno(childP)
	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  ino,
	}
	ops := &FileNode{
		cfs:  n.cfs,
		path: finalPath,
	}
	child := newInodeOrPlaceholder(&n.Inode, ctx, ops, stable)
	fillDirEntryOut(out, n.cfs, attr.Mode, ino)
	return child, 0
}

func (n *FileNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	p, _ := meta.NewPathFromString(n.path)
	childP := *p
	key := strings.TrimSuffix(childP.Key, "/")
	if key == "" {
		childP.Key = name + "/"
	} else {
		childP.Key = key + "/" + name + "/"
	}
	finalPath := childP.String()

	err := n.cfs.Store.Remove(finalPath)
	if err != nil {
		if errors.Is(err, store.ErrPrefixNotEmpty) {
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

	p, _ := meta.NewPathFromString(n.path)
	oldChildP := *p
	oldKey := strings.TrimSuffix(oldChildP.Key, "/")
	if oldKey == "" {
		oldChildP.Key = name
	} else {
		oldChildP.Key = oldKey + "/" + name
	}
	oldPath := oldChildP.String()

	// Check if oldPath is dir
	isDir := false
	if exists, _ := n.cfs.Store.Exists(oldPath + "/"); exists {
		oldChildP.Key += "/"
		oldPath += "/"
		isDir = true
	}

	targetNode, ok := newParent.(*FileNode)
	if !ok {
		// Might be root node
		if rn, ok := newParent.(*RootNode); ok {
			targetNode = &FileNode{cfs: rn.cfs, path: ""}
		} else {
			return syscall.EXDEV
		}
	}

	targetP, _ := meta.NewPathFromString(targetNode.path)
	newChildP := *targetP
	newKey := strings.TrimSuffix(newChildP.Key, "/")
	if newKey == "" {
		newChildP.Key = newName
	} else {
		newChildP.Key = newKey + "/" + newName
	}
	if isDir {
		newChildP.Key += "/"
	}
	newPath := newChildP.String()

	if err := n.cfs.Store.Rename(oldPath, newPath); err != nil {
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
		p, _ := meta.NewPathFromString(n.path)
		childP := *p
		key := strings.TrimSuffix(childP.Key, "/")
		if key == "" {
			childP.Key = entryName
		} else {
			childP.Key = key + "/" + entryName
		}

		d := fuse.DirEntry{
			Name: strings.TrimSuffix(entryName, "/"),
			Ino:  pathIno(childP),
		}

		attr, err := n.cfs.Store.GetMeta(childP.String())
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
