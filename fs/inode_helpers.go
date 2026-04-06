package fs

import (
	"context"
	"hash/fnv"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

func newInodeOrPlaceholder(parent *gfs.Inode, ctx context.Context, ops gfs.InodeEmbedder, stable gfs.StableAttr) (inode *gfs.Inode) {
	defer func() {
		if recover() != nil {
			inode = &gfs.Inode{}
		}
	}()
	return parent.NewInode(ctx, ops, stable)
}

func fileIno(prefix, name string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(prefix))
	_, _ = h.Write([]byte{'/'})
	_, _ = h.Write([]byte(name))
	return h.Sum64() | (1 << 63)
}

func fillFileEntryOut(out *fuse.EntryOut, cfs *CacheFS, attr *meta.FileAttr, ino uint64) {
	if out == nil || cfs == nil || attr == nil {
		return
	}

	perm := attr.Mode & 0o777
	if perm == 0 {
		perm = meta.DefaultFileMode
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
	out.Mode = fuse.S_IFREG | perm
	out.Size = attr.Size
	out.Blocks = (attr.Size + 511) / 512
	out.Nlink = 1
	out.Blksize = 4096
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Ino = ino
	out.SetTimes(&atime, &mtime, &ctime)
	_ = cfs
}

func fillFileAttrOut(out *fuse.AttrOut, cfs *CacheFS, attr *meta.FileAttr, ino uint64) {
	if out == nil || cfs == nil || attr == nil {
		return
	}

	perm := attr.Mode & 0o777
	if perm == 0 {
		perm = meta.DefaultFileMode
	}

	atime := time.Unix(attr.Atime, 0)
	mtime := time.Unix(attr.Mtime, 0)
	ctime := time.Unix(attr.Ctime, 0)
	out.Mode = fuse.S_IFREG | perm
	out.Size = attr.Size
	out.Blocks = (attr.Size + 511) / 512
	out.Nlink = 1
	out.Blksize = 4096
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Ino = ino
	out.SetTimes(&atime, &mtime, &ctime)
}
