package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lch/cachefs/blob"
	"github.com/lch/cachefs/internal/meta"
	"go.etcd.io/bbolt"
)

const (
	defaultMetadataDB = "metadata.db"
)

type boltDBBlobStore struct {
	db    *bbolt.DB
	blobs *blob.BlobManager
	dir   string
	flMu  sync.Mutex
}

// NewStore opens the database and blob directory rooted at dir.
func NewStore(dir string) (Store, error) {
	if dir == "" {
		return nil, errors.New("store: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db, err := bbolt.Open(filepath.Join(dir, defaultMetadataDB), 0o600, nil)
	if err != nil {
		return nil, err
	}

	s, err := newBoltBlobStore(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *boltDBBlobStore) Read(path string) (data []byte, attr *meta.FileAttr, err error) {
	return
}

func (s *boltDBBlobStore) Write(path string, data []byte, mode uint32) error {
	return nil
}

func (s *boltDBBlobStore) Delete(path string) error {
	return nil
}

func (s *boltDBBlobStore) GetMeta(path string) (*meta.FileAttr, error) {
	return nil, nil
}

func (s *boltDBBlobStore) UpdateMeta(path string, attr *meta.FileAttr) error {
	return nil
}

func (s *boltDBBlobStore) Truncate(path string, newSize uint64) error {
	return nil
}

func (s *boltDBBlobStore) List(path string) ([]string, error) {
	list := make([]string, 0)
	p, err := meta.NewPathFromString(path)
	if err != nil {
		return []string{}, err
	}
	switch p.Kind {
	case meta.PathIsRootFolder:
		s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
			if b == nil {
				return notFound("blob metadata bucket", path)
			}
			err := b.ForEach(func(k, v []byte) error {
				list = append(list, string(k))
				return nil
			})
			if err != nil {
				return err
			}
			return nil
		})
	case meta.PathIsPrefixFolder:
		s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(p.Prefix))
			if b == nil {
				return notFound(p.Prefix, path)
			}
			err := b.ForEach(func(k, v []byte) error {
				keyStr := string(k)
				count := strings.Count(keyStr, "/")
				switch count {
				case 0:
					list = append(list, keyStr)
				case 1:
					if strings.HasSuffix(keyStr, "/") {
						list = append(list, keyStr)
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
			return nil
		})
	default:
	}
	return list, nil
}

func (s *boltDBBlobStore) Create(path string) error {
	return nil
}

func (s *boltDBBlobStore) Remove(path string) error {
	return nil
}

func (s *boltDBBlobStore) Exists(path string) (bool, error) {
	return false, nil
}

func newBoltBlobStore(db *bbolt.DB) (*boltDBBlobStore, error) {
	if db == nil {
		return nil, errors.New("store: nil db")
	}

	dir := filepath.Dir(db.Path())
	if dir == "" {
		return nil, fmt.Errorf("store: unable to determine blob directory from %q", db.Path())
	}

	bm := blob.NewBlobManager(dir, db)

	return &boltDBBlobStore{
		db:    db,
		blobs: bm,
		dir:   dir,
	}, nil
}

func (s *boltDBBlobStore) Close() error {
	if s == nil {
		return nil
	}

	var errs []error
	if s.blobs != nil {
		if err := s.blobs.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func notFound(kind, path string) error {
	return fmt.Errorf("store: %s %q: %w", kind, path, os.ErrNotExist)
}
