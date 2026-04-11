package fs

import (
	"context"
	"sync"
	"syscall"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

// CacheFileHandle is the per-open handle state for a cached file.
type CacheFileHandle struct {
	cfs  *CacheFS
	path meta.Path
	attr *meta.FileAttr
	mode uint32

	mu    sync.Mutex
	buf   []byte
	dirty bool
}

func (h *CacheFileHandle) stableIno() uint64 {
	return 0
}

func (h *CacheFileHandle) getMeta() (attr *meta.FileAttr, err error) {
	return
}

func (h *CacheFileHandle) updateMeta(attr *meta.FileAttr) (err error) {
	return
}

func (h *CacheFileHandle) writeBuf(buf []byte, mode uint32) error {
	return nil
}

var (
	_ gfs.FileReader    = (*CacheFileHandle)(nil)
	_ gfs.FileWriter    = (*CacheFileHandle)(nil)
	_ gfs.FileGetattrer = (*CacheFileHandle)(nil)
	_ gfs.FileFlusher   = (*CacheFileHandle)(nil)
	_ gfs.FileReleaser  = (*CacheFileHandle)(nil)
)

func (h *CacheFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return nil, 0
}

func (h *CacheFileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	return 0
}

func (h *CacheFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	return uint32(len(data)), 0
}

func (h *CacheFileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

func (h *CacheFileHandle) Release(ctx context.Context) syscall.Errno {
	return h.Flush(ctx)
}

func (h *CacheFileHandle) touchAtime() error {
	return nil
}
