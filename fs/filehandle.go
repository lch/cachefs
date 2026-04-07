package fs

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

// CacheFileHandle is the per-open handle state for a cached file.
type CacheFileHandle struct {
	cfs      *CacheFS
	prefix   string
	filename string
	attr     *meta.FileAttr
	mode     uint32

	mu    sync.Mutex
	buf   []byte
	dirty bool
}

var (
	_ gfs.FileReader    = (*CacheFileHandle)(nil)
	_ gfs.FileWriter    = (*CacheFileHandle)(nil)
	_ gfs.FileGetattrer = (*CacheFileHandle)(nil)
	_ gfs.FileFlusher   = (*CacheFileHandle)(nil)
	_ gfs.FileReleaser  = (*CacheFileHandle)(nil)
)

func (h *CacheFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if h == nil || h.cfs == nil || h.cfs.Store == nil {
		return fuse.ReadResultData(nil), syscall.EIO
	}
	if off < 0 {
		return fuse.ReadResultData(nil), syscall.EINVAL
	}

	if err := h.touchAtime(); err != nil {
		// atime updates are best-effort and must not fail reads
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}

	end := off + int64(len(dest))
	if end < off || end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	start := int(off)
	data := make([]byte, int(end)-start)
	copy(data, h.buf[start:end])
	return fuse.ReadResultData(data), 0
}

func (h *CacheFileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	if h == nil || h.cfs == nil || h.cfs.Store == nil {
		return syscall.EIO
	}

	h.mu.Lock()
	size := uint64(len(h.buf))
	var attr meta.FileAttr
	if h.attr != nil {
		attr = *h.attr
		h.mu.Unlock()
		attr.Length = size
		fillFileAttrOut(out, h.cfs, &attr, fileIno(h.prefix, h.filename))
		return 0
	} else {
		h.mu.Unlock()
		loaded, err := h.cfs.Store.GetMeta(h.prefix, h.filename)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return syscall.ENOENT
			}
			return gfs.ToErrno(err)
		}
		attr = *loaded
	}
	attr.Length = size
	fillFileAttrOut(out, h.cfs, &attr, fileIno(h.prefix, h.filename))
	return 0
}

func (h *CacheFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if h == nil || h.cfs == nil || h.cfs.Store == nil {
		return 0, syscall.EIO
	}
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if len(data) == 0 {
		return 0, 0
	}
	if off > int64(^uint(0)>>1)-int64(len(data)) {
		return 0, syscall.EFBIG
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	end := off + int64(len(data))
	newLen := len(h.buf)
	if end > int64(newLen) {
		newLen = int(end)
	}
	newBuf := make([]byte, newLen)
	copy(newBuf, h.buf)
	copy(newBuf[int(off):], data)

	loaded, err := h.cfs.Store.GetMeta(h.prefix, h.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, syscall.ENOENT
		}
		return 0, gfs.ToErrno(err)
	}
	h.mode = loaded.Mode

	if err := h.cfs.Store.WriteFile(h.prefix, h.filename, newBuf, h.mode); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, syscall.ENOENT
		}
		return 0, gfs.ToErrno(err)
	}
	loaded, err = h.cfs.Store.GetMeta(h.prefix, h.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, syscall.ENOENT
		}
		return 0, gfs.ToErrno(err)
	}

	h.buf = newBuf
	attrCopy := *loaded
	h.attr = &attrCopy
	h.mode = loaded.Mode
	h.dirty = false
	return uint32(len(data)), 0
}

func (h *CacheFileHandle) Flush(ctx context.Context) syscall.Errno {
	if h == nil || h.cfs == nil || h.cfs.Store == nil {
		return syscall.EIO
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.dirty {
		return 0
	}

	loaded, err := h.cfs.Store.GetMeta(h.prefix, h.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}
	h.mode = loaded.Mode

	if err := h.cfs.Store.WriteFile(h.prefix, h.filename, h.buf, h.mode); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	attr, err := h.cfs.Store.GetMeta(h.prefix, h.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return syscall.ENOENT
		}
		return gfs.ToErrno(err)
	}

	attrCopy := *attr
	h.attr = &attrCopy
	h.mode = attr.Mode
	h.dirty = false
	return 0
}

func (h *CacheFileHandle) Release(ctx context.Context) syscall.Errno {
	return h.Flush(ctx)
}

func (h *CacheFileHandle) touchAtime() error {
	if h == nil || h.cfs == nil || h.cfs.Store == nil {
		return nil
	}

	attr, err := h.cfs.Store.GetMeta(h.prefix, h.filename)
	if err != nil {
		return err
	}
	attr.Atime = time.Now().Unix()
	if err := h.cfs.Store.UpdateMeta(h.prefix, h.filename, attr); err != nil {
		return err
	}
	h.mu.Lock()
	attrCopy := *attr
	h.attr = &attrCopy
	h.mu.Unlock()
	return nil
}
