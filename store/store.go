package store

import (
	"errors"

	"github.com/lch/cachefs/internal/meta"
)

// ErrPrefixNotEmpty is returned when a prefix still has files.
var ErrPrefixNotEmpty = errors.New("store: prefix not empty")

// Store is the storage interface used by the filesystem layer.
type Store interface {
	Read(path string) (data []byte, attr *meta.FileAttr, err error)
	Write(path string, data []byte, mode uint32) error
	Delete(path string) error
	GetMeta(path string) (*meta.FileAttr, error)
	UpdateMeta(path string, attr *meta.FileAttr) error
	Truncate(path string, newSize uint64) error
	List(path string) ([]string, error)
	Create(path string) error
	Remove(path string) error
	Exists(path string) (bool, error)
	Rename(oldPath, newPath string) error
	Close() error
}
