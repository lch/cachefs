package store

import (
	"errors"

	"github.com/lch/cachefs/internal/meta"
)

// ErrFolderNotEmpty is returned when a prefix still has files.
var (
	ErrFolderNotEmpty = errors.New("store: folder not empty")
	ErrStoreCorrupt   = errors.New("store: metadata db corrupt")
)

type StoreStats struct {
	Items           uint64
	AllocatedBlocks uint64
	FreeBlocks      uint64
}

// Store is the storage interface used by the filesystem layer.
type Store interface {
	Read(path meta.Path) (data []byte, attr *meta.FileAttr, err error)
	Write(path meta.Path, data []byte, mode uint32) error
	Delete(path meta.Path) error
	GetMeta(path meta.Path) (*meta.FileAttr, error)
	UpdateMeta(path meta.Path, attr *meta.FileAttr) error
	Truncate(path meta.Path, newSize uint64) error
	List(path meta.Path) ([]string, error)
	Create(path meta.Path) error
	Exists(path meta.Path) (bool, error)
	Rename(oldPath, newPath meta.Path) error
	Stats() StoreStats
	Close() error
}
