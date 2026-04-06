package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"syscall"

	"github.com/lch/cachefs/internal/meta"
	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

const metaBucketName = "_meta"

// ErrPrefixNotEmpty is returned when a prefix bucket still contains files.
var ErrPrefixNotEmpty = errors.New("store: prefix not empty")

// Store is the storage interface used by the filesystem layer.
type Store interface {
	GetContent(prefix, filename string) ([]byte, error)
	PutContent(prefix, filename string, data []byte) error
	DeleteContent(prefix, filename string) error

	GetMeta(prefix, filename string) (*meta.FileAttr, error)
	PutMeta(prefix, filename string, attr *meta.FileAttr) error
	DeleteMeta(prefix, filename string) error

	PutFile(prefix, filename string, data []byte, attr *meta.FileAttr) error
	DeleteFile(prefix, filename string) error

	CreatePrefix(prefix string) error
	DeletePrefix(prefix string) error

	ListPrefixes() ([]string, error)
	ListFiles(prefix string) ([]string, error)

	Close() error
}

type bboltStore struct {
	db *bbolt.DB
}

var (
	afterPutFileContentHook    func() error
	afterDeleteFileContentHook func() error
)

// New wraps an open bbolt database in a Store implementation.
func New(db *bbolt.DB) Store {
	return &bboltStore{db: db}
}

func (s *bboltStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *bboltStore) GetContent(prefix, filename string) ([]byte, error) {
	var out []byte
	if err := s.view(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(prefix))
		if b == nil {
			return notFound("content", prefix, filename)
		}

		key := []byte(filename)
		if !hasKey(b, key) {
			return notFound("content", prefix, filename)
		}

		out = copyBytes(b.Get(key))
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *bboltStore) PutContent(prefix, filename string, data []byte) error {
	return s.update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(prefix))
		if err != nil {
			return err
		}
		return b.Put([]byte(filename), copyBytes(data))
	})
}

func (s *bboltStore) DeleteContent(prefix, filename string) error {
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(prefix))
		if b == nil {
			return notFound("content", prefix, filename)
		}
		key := []byte(filename)
		if !hasKey(b, key) {
			return notFound("content", prefix, filename)
		}
		return b.Delete(key)
	})
}

func (s *bboltStore) GetMeta(prefix, filename string) (*meta.FileAttr, error) {
	var out *meta.FileAttr
	if err := s.view(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(metaBucketName))
		if b == nil {
			return notFound("meta", prefix, filename)
		}

		key := []byte(metaKey(prefix, filename))
		if !hasKey(b, key) {
			return notFound("meta", prefix, filename)
		}

		attr, err := meta.Unmarshal(copyBytes(b.Get(key)))
		if err != nil {
			return err
		}
		out = attr
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *bboltStore) PutMeta(prefix, filename string, attr *meta.FileAttr) error {
	if attr == nil {
		return errors.New("store: nil meta attr")
	}

	return s.update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(metaBucketName))
		if err != nil {
			return err
		}
		return b.Put([]byte(metaKey(prefix, filename)), meta.Marshal(attr))
	})
}

func (s *bboltStore) DeleteMeta(prefix, filename string) error {
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(metaBucketName))
		if b == nil {
			return notFound("meta", prefix, filename)
		}
		key := []byte(metaKey(prefix, filename))
		if !hasKey(b, key) {
			return notFound("meta", prefix, filename)
		}
		return b.Delete(key)
	})
}

func (s *bboltStore) PutFile(prefix, filename string, data []byte, attr *meta.FileAttr) error {
	if attr == nil {
		return errors.New("store: nil meta attr")
	}

	return s.update(func(tx *bbolt.Tx) error {
		contentBucket, err := tx.CreateBucketIfNotExists([]byte(prefix))
		if err != nil {
			return err
		}
		if err = contentBucket.Put([]byte(filename), copyBytes(data)); err != nil {
			return err
		}
		if hook := afterPutFileContentHook; hook != nil {
			if err = hook(); err != nil {
				return err
			}
		}
		metaBucket, err := tx.CreateBucketIfNotExists([]byte(metaBucketName))
		if err != nil {
			return err
		}
		return metaBucket.Put([]byte(metaKey(prefix, filename)), meta.Marshal(attr))
	})
}

func (s *bboltStore) DeleteFile(prefix, filename string) error {
	return s.update(func(tx *bbolt.Tx) error {
		contentBucket := tx.Bucket([]byte(prefix))
		if contentBucket == nil {
			return notFound("content", prefix, filename)
		}
		contentKey := []byte(filename)
		if !hasKey(contentBucket, contentKey) {
			return notFound("content", prefix, filename)
		}
		if err := contentBucket.Delete(contentKey); err != nil {
			return err
		}
		if hook := afterDeleteFileContentHook; hook != nil {
			if err := hook(); err != nil {
				return err
			}
		}

		metaBucket := tx.Bucket([]byte(metaBucketName))
		if metaBucket == nil {
			return notFound("meta", prefix, filename)
		}
		metaKeyBytes := []byte(metaKey(prefix, filename))
		if !hasKey(metaBucket, metaKeyBytes) {
			return notFound("meta", prefix, filename)
		}
		if err := metaBucket.Delete(metaKeyBytes); err != nil {
			return err
		}

		if bucketEmpty(contentBucket) {
			return tx.DeleteBucket([]byte(prefix))
		}
		return nil
	})
}

func (s *bboltStore) CreatePrefix(prefix string) error {
	return s.update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucket([]byte(prefix)); err != nil {
			if errors.Is(err, berrors.ErrBucketExists) {
				return fmt.Errorf("store: prefix %q: %w", prefix, os.ErrExist)
			}
			return err
		}
		return nil
	})
}

func (s *bboltStore) DeletePrefix(prefix string) error {
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(prefix))
		if b == nil {
			return fmt.Errorf("store: prefix %q: %w", prefix, os.ErrNotExist)
		}
		if b.Stats().KeyN != 0 {
			return fmt.Errorf("store: prefix %q: %w", prefix, ErrPrefixNotEmpty)
		}
		if err := tx.DeleteBucket([]byte(prefix)); err != nil {
			if errors.Is(err, berrors.ErrBucketNotFound) {
				return fmt.Errorf("store: prefix %q: %w", prefix, os.ErrNotExist)
			}
			return err
		}
		return nil
	})
}

// Statfs reports basic storage information for the backing database.
func (s *bboltStore) Statfs() (syscall.Statfs_t, error) {
	var st syscall.Statfs_t
	if s == nil || s.db == nil {
		return st, errors.New("store: nil db")
	}

	info, err := os.Stat(s.db.Path())
	if err != nil {
		return st, err
	}

	prefixes, err := s.ListPrefixes()
	if err != nil {
		return st, err
	}

	var fileCount uint64
	for _, prefix := range prefixes {
		files, err := s.ListFiles(prefix)
		if err != nil {
			return st, err
		}
		fileCount += uint64(len(files))
	}

	st.Bsize = 4096
	st.Frsize = 4096
	blocks := uint64((info.Size() + 511) / 512)
	if blocks == 0 {
		blocks = 1
	}
	st.Blocks = blocks
	st.Bfree = 1
	st.Bavail = 1
	st.Files = 1 + uint64(len(prefixes)) + fileCount
	st.Ffree = 1
	st.Namelen = 255
	return st, nil
}

func (s *bboltStore) ListPrefixes() ([]string, error) {
	prefixes := make([]string, 0)
	if err := s.view(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if b == nil {
				return nil
			}
			if string(name) == metaBucketName {
				return nil
			}
			prefixes = append(prefixes, string(name))
			return nil
		})
	}); err != nil {
		return nil, err
	}
	sort.Strings(prefixes)
	return prefixes, nil
}

func (s *bboltStore) ListFiles(prefix string) ([]string, error) {
	files := make([]string, 0)
	if err := s.view(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(prefix))
		if b == nil {
			return notFound("prefix", prefix, "")
		}
		return b.ForEach(func(name, _ []byte) error {
			files = append(files, string(name))
			return nil
		})
	}); err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func (s *bboltStore) view(fn func(*bbolt.Tx) error) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}
	return s.db.View(fn)
}

func (s *bboltStore) update(fn func(*bbolt.Tx) error) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}
	return s.db.Update(fn)
}

func metaKey(prefix, filename string) string {
	return prefix + "/" + filename
}

func copyBytes(data []byte) []byte {
	if len(data) == 0 {
		return []byte{}
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func hasKey(bucket *bbolt.Bucket, key []byte) bool {
	if bucket == nil {
		return false
	}
	k, _ := bucket.Cursor().Seek(key)
	return bytes.Equal(k, key)
}

func bucketEmpty(bucket *bbolt.Bucket) bool {
	if bucket == nil {
		return true
	}
	k, _ := bucket.Cursor().First()
	return k == nil
}

func notFound(kind, prefix, name string) error {
	if name == "" {
		return fmt.Errorf("store: %s %q: %w", kind, prefix, os.ErrNotExist)
	}
	return fmt.Errorf("store: %s %q/%q: %w", kind, prefix, name, os.ErrNotExist)
}
