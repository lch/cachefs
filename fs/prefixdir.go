package fs

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

// PrefixDirNode represents a virtual directory for a two-character prefix.
type PrefixDirNode struct {
	gfs.Inode
	cfs    *CacheFS
	prefix string
}

func (n *PrefixDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, syscall.ENOENT
		}
		return nil, gfs.ToErrno(err)
	}

	fillFileEntryOut(out, n.cfs, attr, fileIno(n.prefix, name))
	return n.newFileInode(ctx, name), 0
}

func (n *PrefixDirNode) Readdir(ctx context.Context) (gfs.DirStream, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	files, err := n.cfs.Store.ListFiles(n.prefix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, syscall.ENOENT
		}
		return nil, gfs.ToErrno(err)
	}

	entries := make([]fuse.DirEntry, 0, len(files))
	for _, name := range files {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: fuse.S_IFREG,
			Ino:  fileIno(n.prefix, name),
		})
	}
	return gfs.NewListDirStream(entries), 0
}

func (n *PrefixDirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gfs.Inode, gfs.FileHandle, uint32, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, nil, 0, syscall.EIO
	}

	if _, err := n.cfs.Store.GetMeta(n.prefix, name); err == nil {
		return nil, nil, 0, syscall.EEXIST
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, 0, gfs.ToErrno(err)
	}

	perm := mode & 0o777
	if perm == 0 {
		perm = meta.DefaultFileMode
	}
	now := time.Now().Unix()
	attr := &meta.FileAttr{
		Size:  0,
		Mode:  perm,
		Uid:   n.cfs.Uid,
		Gid:   n.cfs.Gid,
		Atime: now,
		Mtime: now,
		Ctime: now,
	}
	if err := n.cfs.Store.PutFile(n.prefix, name, []byte{}, attr); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, nil, 0, syscall.EEXIST
		}
		return nil, nil, 0, gfs.ToErrno(err)
	}

	fillFileEntryOut(out, n.cfs, attr, fileIno(n.prefix, name))
	attrCopy := *attr
	return n.newFileInode(ctx, name), &CacheFileHandle{cfs: n.cfs, prefix: n.prefix, filename: name, attr: &attrCopy, buf: []byte{}}, 0, 0
}

func (n *PrefixDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	if err := n.cfs.Store.DeleteFile(n.prefix, name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}
	return 0
}

func (n *PrefixDirNode) Getattr(ctx context.Context, fh gfs.FileHandle, out *fuse.AttrOut) syscall.Errno {
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
	out.Ino = prefixIno(n.prefix)
	out.SetTimes(&now, &now, &now)
	return 0
}

func (n *PrefixDirNode) Rename(ctx context.Context, name string, newParent gfs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags != 0 {
		return syscall.ENOTSUP
	}

	target, ok := newParent.(*PrefixDirNode)
	if !ok || target == nil || target.cfs == nil || target.cfs.Store == nil {
		return syscall.EXDEV
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}
	if target.cfs != n.cfs {
		return syscall.EXDEV
	}
	if n.prefix == target.prefix && name == newName {
		return 0
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}
	content, err := n.cfs.Store.GetContent(n.prefix, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	attr.Size = uint64(len(content))
	if err := target.cfs.Store.PutFile(target.prefix, newName, content, attr); err != nil {
		return gfs.ToErrno(err)
	}
	if err := n.cfs.Store.DeleteFile(n.prefix, name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}
	return 0
}

func (n *PrefixDirNode) newFileInode(ctx context.Context, name string) *gfs.Inode {
	ops := &FileNode{cfs: n.cfs, prefix: n.prefix, filename: name}
	return newInodeOrPlaceholder(&n.Inode, ctx, ops, gfs.StableAttr{Mode: fuse.S_IFREG, Ino: fileIno(n.prefix, name)})
}
