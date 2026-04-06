package fs

import "github.com/lch/cachefs/store"

// CacheFS holds shared filesystem state passed to all FUSE nodes.
type CacheFS struct {
	Store store.Store
	Uid   uint32
	Gid   uint32
}

// NewCacheFS builds the shared filesystem state container.
func NewCacheFS(st store.Store, uid, gid uint32) *CacheFS {
	return &CacheFS{Store: st, Uid: uid, Gid: gid}
}
