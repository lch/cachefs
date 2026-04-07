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

var (
	_ gfs.NodeLookuper  = (*PrefixDirNode)(nil)
	_ gfs.NodeReaddirer = (*PrefixDirNode)(nil)
	_ gfs.NodeCreater   = (*PrefixDirNode)(nil)
	_ gfs.NodeUnlinker  = (*PrefixDirNode)(nil)
	_ gfs.NodeGetattrer = (*PrefixDirNode)(nil)
	_ gfs.NodeRenamer   = (*PrefixDirNode)(nil)
	_ gfs.NodeMkdirer   = (*PrefixDirNode)(nil)
	_ gfs.NodeRmdirer   = (*PrefixDirNode)(nil)
)

func (n *PrefixDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, name)
	if err == nil {
		fillFileEntryOut(out, n.cfs, attr, fileIno(n.prefix, name))
		return n.newFileInode(ctx, name), 0
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, gfs.ToErrno(err)
	}

	exists, err := n.cfs.Store.SubdirExists(n.prefix, name)
	if err != nil {
		return nil, gfs.ToErrno(err)
	}
	if !exists {
		return nil, syscall.ENOENT
	}

	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|meta.DefaultDirMode, subdirIno(n.prefix, name))
	return n.newSubdirInode(ctx, name), 0
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
	subdirs, err := n.cfs.Store.ListSubdirs(n.prefix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, syscall.ENOENT
		}
		return nil, gfs.ToErrno(err)
	}

	entries := make([]fuse.DirEntry, 0, len(files)+len(subdirs))
	for _, name := range files {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: fuse.S_IFREG,
			Ino:  fileIno(n.prefix, name),
		})
	}
	for _, name := range subdirs {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: fuse.S_IFDIR,
			Ino:  subdirIno(n.prefix, name),
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
	if err := n.cfs.Store.WriteFile(n.prefix, name, []byte{}, perm); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, nil, 0, syscall.EEXIST
		}
		return nil, nil, 0, gfs.ToErrno(err)
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, 0, syscall.ENOENT
		}
		return nil, nil, 0, gfs.ToErrno(err)
	}

	fillFileEntryOut(out, n.cfs, attr, fileIno(n.prefix, name))
	attrCopy := *attr
	return n.newFileInode(ctx, name), &CacheFileHandle{cfs: n.cfs, prefix: n.prefix, filename: name, attr: &attrCopy, mode: attr.Mode, buf: []byte{}}, 0, 0
}

func (n *PrefixDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	if err := n.cfs.Store.CreateSubdir(n.prefix, name); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, syscall.EEXIST
		}
		return nil, gfs.ToErrno(err)
	}

	fillDirEntryOut(out, n.cfs, fuse.S_IFDIR|meta.DefaultDirMode, subdirIno(n.prefix, name))
	return n.newSubdirInode(ctx, name), 0
}

func (n *PrefixDirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	if err := n.cfs.Store.RemoveSubdir(n.prefix, name); err != nil {
		if errors.Is(err, syscall.ENOTEMPTY) {
			return syscall.ENOTEMPTY
		}
		if errors.Is(err, os.ErrNotExist) {
			fileAttr, fileErr := n.cfs.Store.GetMeta(n.prefix, name)
			if fileErr == nil && fileAttr != nil {
				return syscall.ENOTDIR
			}
			if errors.Is(fileErr, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return gfs.ToErrno(fileErr)
		}
		return gfs.ToErrno(err)
	}
	return 0
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
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	targetPrefix := ""
	targetSubdir := ""
	switch target := newParent.(type) {
	case *PrefixDirNode:
		if target == nil || target.cfs == nil || target.cfs.Store == nil {
			return syscall.EIO
		}
		if target.cfs != n.cfs {
			return syscall.EXDEV
		}
		targetPrefix = target.prefix
	case *SubdirNode:
		if target == nil || target.cfs == nil || target.cfs.Store == nil {
			return syscall.EIO
		}
		if target.cfs != n.cfs {
			return syscall.EXDEV
		}
		targetPrefix = target.prefix
		targetSubdir = target.dirname
	default:
		return syscall.EXDEV
	}

	if targetSubdir == "" && targetPrefix == n.prefix && name == newName {
		return 0
	}

	data, attr, err := n.cfs.Store.ReadFile(n.prefix, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			exists, subdirErr := n.cfs.Store.SubdirExists(n.prefix, name)
			if subdirErr != nil {
				return gfs.ToErrno(subdirErr)
			}
			if exists {
				return syscall.ENOTSUP
			}
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	if err := n.cfs.Store.DeleteFile(n.prefix, name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	if targetSubdir == "" {
		if err := n.cfs.Store.WriteFile(targetPrefix, newName, data, attr.Mode); err != nil {
			return gfs.ToErrno(err)
		}
	} else {
		if err := n.cfs.Store.WriteSubdirFile(targetPrefix, targetSubdir, newName, data, attr.Mode); err != nil {
			return gfs.ToErrno(err)
		}
	}

	if child := n.Inode.GetChild(name); child != nil {
		if fn, ok := child.Operations().(*FileNode); ok {
			fn.updatePath(targetPrefix, targetSubdir, newName)
		}
	}
	return 0
}

func (n *PrefixDirNode) newFileInode(ctx context.Context, name string) *gfs.Inode {
	ops := &FileNode{cfs: n.cfs, prefix: n.prefix, filename: name}
	return newInodeOrPlaceholder(&n.Inode, ctx, ops, gfs.StableAttr{Mode: fuse.S_IFREG, Ino: fileIno(n.prefix, name)})
}

func (n *PrefixDirNode) newSubdirInode(ctx context.Context, name string) *gfs.Inode {
	ops := &SubdirNode{cfs: n.cfs, prefix: n.prefix, dirname: name}
	return newInodeOrPlaceholder(&n.Inode, ctx, ops, gfs.StableAttr{Mode: fuse.S_IFDIR, Ino: subdirIno(n.prefix, name)})
}
