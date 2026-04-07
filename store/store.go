package store

import (
	"errors"

	"github.com/lch/cachefs/internal/meta"
)

// ErrPrefixNotEmpty is returned when a prefix still has files.
var ErrPrefixNotEmpty = errors.New("store: prefix not empty")

// Store is the storage interface used by the filesystem layer.
type Store interface {
	ReadFile(prefix, filename string) (data []byte, attr *meta.FileAttr, err error)
	WriteFile(prefix, filename string, data []byte, mode uint32) error
	DeleteFile(prefix, filename string) error
	GetMeta(prefix, filename string) (*meta.FileAttr, error)
	UpdateMeta(prefix, filename string, attr *meta.FileAttr) error
	Truncate(prefix, filename string, newSize uint64) error
	ListPrefixes() ([]string, error)
	ListFiles(prefix string) ([]string, error)
	CreatePrefix(prefix string) error
	RemovePrefix(prefix string) error
	PrefixExists(prefix string) (bool, error)
	Close() error
}
