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

// SubdirNode represents one level of subdirectories under a prefix.
type SubdirNode struct {
	gfs.Inode
	cfs     *CacheFS
	prefix  string
	dirname string
}

var (
	_ gfs.InodeEmbedder = (*SubdirNode)(nil)
	_ gfs.NodeLookuper  = (*SubdirNode)(nil)
	_ gfs.NodeReaddirer = (*SubdirNode)(nil)
	_ gfs.NodeCreater   = (*SubdirNode)(nil)
	_ gfs.NodeUnlinker  = (*SubdirNode)(nil)
	_ gfs.NodeGetattrer = (*SubdirNode)(nil)
	_ gfs.NodeSetattrer = (*SubdirNode)(nil)
	_ gfs.NodeRenamer   = (*SubdirNode)(nil)
)

func (n *SubdirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gfs.Inode, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	attr, err := n.cfs.Store.GetSubdirFileMeta(n.prefix, n.dirname, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, syscall.ENOENT
		}
		return nil, gfs.ToErrno(err)
	}

	fillFileEntryOut(out, n.cfs, attr, subdirFileIno(n.prefix, n.dirname, name))
	return n.newFileInode(ctx, name), 0
}

func (n *SubdirNode) Readdir(ctx context.Context) (gfs.DirStream, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, syscall.EIO
	}

	names, err := n.cfs.Store.ListSubdirEntries(n.prefix, n.dirname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, syscall.ENOENT
		}
		return nil, gfs.ToErrno(err)
	}

	entries := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: fuse.S_IFREG,
			Ino:  subdirFileIno(n.prefix, n.dirname, name),
		})
	}
	return gfs.NewListDirStream(entries), 0
}

func (n *SubdirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gfs.Inode, gfs.FileHandle, uint32, syscall.Errno) {
	if n.cfs == nil || n.cfs.Store == nil {
		return nil, nil, 0, syscall.EIO
	}

	if _, err := n.cfs.Store.GetSubdirFileMeta(n.prefix, n.dirname, name); err == nil {
		return nil, nil, 0, syscall.EEXIST
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, 0, gfs.ToErrno(err)
	}

	perm := mode & 0o777
	if perm == 0 {
		perm = meta.DefaultFileMode
	}
	if err := n.cfs.Store.WriteSubdirFile(n.prefix, n.dirname, name, []byte{}, perm); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, nil, 0, syscall.EEXIST
		}
		return nil, nil, 0, gfs.ToErrno(err)
	}

	attr, err := n.cfs.Store.GetSubdirFileMeta(n.prefix, n.dirname, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, 0, syscall.ENOENT
		}
		return nil, nil, 0, gfs.ToErrno(err)
	}

	fillFileEntryOut(out, n.cfs, attr, subdirFileIno(n.prefix, n.dirname, name))
	node := &FileNode{cfs: n.cfs, prefix: n.prefix, subdir: n.dirname, filename: name}
	return n.newFileInode(ctx, name), node.newHandle([]byte{}, attr), 0, 0
}

func (n *SubdirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	if err := n.cfs.Store.DeleteSubdirFile(n.prefix, n.dirname, name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}
	return 0
}

func (n *SubdirNode) Getattr(ctx context.Context, fh gfs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, meta.SubdirMarkerKey(n.dirname))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	fillDirAttrOut(out, n.cfs, attr, subdirIno(n.prefix, n.dirname))
	return 0
}

func (n *SubdirNode) Setattr(ctx context.Context, fh gfs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	attr, err := n.cfs.Store.GetMeta(n.prefix, meta.SubdirMarkerKey(n.dirname))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	updated := *attr
	changed := false
	if mode, ok := in.GetMode(); ok {
		updated.Mode = syscall.S_IFDIR | (mode & 0o777)
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
		if err := n.cfs.Store.UpdateMeta(n.prefix, meta.SubdirMarkerKey(n.dirname), &updated); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return gfs.ToErrno(err)
		}
		attr = &updated
	}

	fillDirAttrOut(out, n.cfs, attr, subdirIno(n.prefix, n.dirname))
	return 0
}

func (n *SubdirNode) Rename(ctx context.Context, name string, newParent gfs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags != 0 {
		return syscall.ENOTSUP
	}
	if n.cfs == nil || n.cfs.Store == nil {
		return syscall.EIO
	}

	var targetPrefix string
	var targetSubdir string
	switch parent := newParent.(type) {
	case *PrefixDirNode:
		if parent == nil || parent.cfs == nil || parent.cfs.Store == nil {
			return syscall.EIO
		}
		if parent.cfs != n.cfs {
			return syscall.EXDEV
		}
		targetPrefix = parent.prefix
	case *SubdirNode:
		if parent == nil || parent.cfs == nil || parent.cfs.Store == nil {
			return syscall.EIO
		}
		if parent.cfs != n.cfs {
			return syscall.EXDEV
		}
		targetPrefix = parent.prefix
		targetSubdir = parent.dirname
	default:
		return syscall.EXDEV
	}

	if targetPrefix == n.prefix && targetSubdir == n.dirname && name == newName {
		return 0
	}

	data, attr, err := n.cfs.Store.ReadSubdirFile(n.prefix, n.dirname, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}
	if err := n.cfs.Store.DeleteSubdirFile(n.prefix, n.dirname, name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	var writeErr error
	if targetSubdir == "" {
		writeErr = n.cfs.Store.WriteFile(targetPrefix, newName, data, attr.Mode)
	} else {
		writeErr = n.cfs.Store.WriteSubdirFile(targetPrefix, targetSubdir, newName, data, attr.Mode)
	}
	if writeErr != nil {
		return gfs.ToErrno(writeErr)
	}

	if child := n.Inode.GetChild(name); child != nil {
		if fn, ok := child.Operations().(*FileNode); ok {
			fn.updatePath(targetPrefix, targetSubdir, newName)
		}
	}
	return 0
}

func (n *SubdirNode) newFileInode(ctx context.Context, name string) *gfs.Inode {
	ops := &FileNode{cfs: n.cfs, prefix: n.prefix, subdir: n.dirname, filename: name}
	return newInodeOrPlaceholder(&n.Inode, ctx, ops, gfs.StableAttr{Mode: fuse.S_IFREG, Ino: subdirFileIno(n.prefix, n.dirname, name)})
}

func fillDirAttrOut(out *fuse.AttrOut, cfs *CacheFS, attr *meta.FileAttr, ino uint64) {
	if out == nil || cfs == nil || attr == nil {
		return
	}

	perm := attr.Mode & 0o777
	if perm == 0 {
		perm = meta.DefaultDirMode
	}

	now := time.Now()
	atime := now
	mtime := now
	ctime := now
	if attr.Atime != 0 {
		atime = time.Unix(attr.Atime, 0)
	}
	if attr.Mtime != 0 {
		mtime = time.Unix(attr.Mtime, 0)
	}
	if attr.Ctime != 0 {
		ctime = time.Unix(attr.Ctime, 0)
	}
	out.Mode = fuse.S_IFDIR | perm
	out.Size = 0
	out.Blocks = 0
	out.Nlink = 2
	out.Blksize = 4096
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Ino = ino
	out.SetTimes(&atime, &mtime, &ctime)
	_ = cfs
}
