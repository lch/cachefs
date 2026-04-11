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

func (h *CacheFileHandle) touchAtime(locked bool) error {
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

	if !locked {
		h.mu.Lock()
		defer h.mu.Unlock()
	}
	attrCopy := *attr
	h.attr = &attrCopy
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
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.buf == nil {
		data, attr, err := h.cfs.Store.Read(h.path)
		if err != nil {
			return nil, fs.ToErrno(err)
		}
		h.buf = data
		h.attr = attr
	}

	_ = h.touchAtime(true)

	if off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}

	return fuse.ReadResultData(h.buf[off:end]), 0
}

func (h *CacheFileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	attr, err := h.cfs.Store.GetMeta(h.path)
	if err != nil {
		return fs.ToErrno(err)
	}
	h.attr = attr

	p, _ := meta.NewPathFromString(h.path)
	ino := pathIno(*p)

	fillFileAttrOut(out, h.cfs, h.attr, ino)
	return 0
}

func (h *CacheFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.buf == nil {
		dataStore, attr, err := h.cfs.Store.Read(h.path)
		if err != nil {
			// If it doesn't exist, we start with an empty buffer if offset is 0
			if off == 0 {
				h.buf = []byte{}
			} else {
				return 0, fs.ToErrno(err)
			}
		} else {
			h.buf = dataStore
			h.attr = attr
		}
	}

	newEnd := off + int64(len(data))
	if newEnd > int64(len(h.buf)) {
		newBuf := make([]byte, newEnd)
		copy(newBuf, h.buf)
		h.buf = newBuf
	}

	copy(h.buf[off:], data)
	h.dirty = true

	if h.attr != nil {
		h.attr.Length = uint64(len(h.buf))
		now := time.Now().Unix()
		h.attr.Mtime = now
		h.attr.Ctime = now
	}

	return uint32(len(data)), 0
}

func (h *CacheFileHandle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.dirty {
		return 0
	}

	mode := meta.DefaultFileMode
	if h.attr != nil {
		mode = h.attr.Mode
	}

	err := h.cfs.Store.Write(h.path, h.buf, mode)
	if err != nil {
		return fs.ToErrno(err)
	}

	h.dirty = false
	return 0
}

func (h *CacheFileHandle) Release(ctx context.Context) syscall.Errno {
	return h.Flush(ctx)
}
