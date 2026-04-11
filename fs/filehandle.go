package fs

import (
	"context"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

// CacheFileHandle is the per-open handle state for a cached file.
type CacheFileHandle struct {
	cfs  *CacheFS
	path string
	attr *meta.FileAttr
	mode uint32

	mu    sync.Mutex
	buf   []byte
	dirty bool
}

func (h *CacheFileHandle) touchAtime() error {
	if h == nil || h.cfs == nil || h.cfs.Store == nil {
		return nil
	}
	attr, err := h.cfs.Store.GetMeta(h.path)
	if err != nil {
		return err
	}
	attr.Atime = time.Now().Unix()
	if err := h.cfs.Store.UpdateMeta(h.path, attr); err != nil {
		return err
	}
	h.mu.Lock()
	attrCopy := *attr
	h.attr = &attrCopy
	h.mu.Unlock()
	return nil
}

var (
	_ fs.FileReader    = (*CacheFileHandle)(nil)
	_ fs.FileWriter    = (*CacheFileHandle)(nil)
	_ fs.FileGetattrer = (*CacheFileHandle)(nil)
	_ fs.FileFlusher   = (*CacheFileHandle)(nil)
	_ fs.FileReleaser  = (*CacheFileHandle)(nil)
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
