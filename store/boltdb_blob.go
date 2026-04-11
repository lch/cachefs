package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

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
	attr, err = s.GetMeta(path)
	if err != nil {
		return
	}
	if attr.IsDir() {
		return nil, attr, nil
	}

	p, _ := meta.NewPathFromString(path)
	data, err = s.blobs.Read(p.Prefix, attr.BlockIndices)
	if err != nil {
		return
	}
	if uint64(len(data)) > attr.Length {
		data = data[:attr.Length]
	}
	return
}

func (s *boltDBBlobStore) Write(path string, data []byte, mode uint32) error {
	p, err := meta.NewPathFromString(path)
	if err != nil {
		return err
	}
	if p.Kind == meta.PathIsRootFolder || p.Kind == meta.PathIsPrefixFolder {
		return errors.New("store: cannot write to folder")
	}

	attr, err := s.GetMeta(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "not found") {
		return err
	}

	neededBlocks := (len(data) + meta.DefaultBlockSize - 1) / meta.DefaultBlockSize
	var blocks []uint64
	if attr != nil {
		blocks = attr.BlockIndices
	}

	if len(blocks) < neededBlocks {
		more, err := s.allocateBlocks(p.Prefix, neededBlocks-len(blocks))
		if err != nil {
			return err
		}
		blocks = append(blocks, more...)
	} else if len(blocks) > neededBlocks {
		extra := blocks[neededBlocks:]
		blocks = blocks[:neededBlocks]
		if err := s.releaseBlocks(p.Prefix, extra); err != nil {
			return err
		}
	}

	paddedData := data
	if len(data) < neededBlocks*meta.DefaultBlockSize {
		paddedData = make([]byte, neededBlocks*meta.DefaultBlockSize)
		copy(paddedData, data)
	}

	err = s.blobs.Write(p.Prefix, blocks, paddedData)
	if err != nil {
		return err
	}

	if attr == nil {
		attr = &meta.FileAttr{
			Mode: mode,
		}
	} else {
		attr.Mode = mode
	}
	attr.Length = uint64(len(data))
	attr.Blocks = uint64(len(blocks))
	attr.BlockIndices = blocks
	return s.UpdateMeta(path, attr)
}

func (s *boltDBBlobStore) Delete(path string) error {
	attr, err := s.GetMeta(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}
	if attr.IsDir() {
		return s.Remove(path)
	}

	p, _ := meta.NewPathFromString(path)
	if err := s.releaseBlocks(p.Prefix, attr.BlockIndices); err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Prefix))
		if b != nil {
			return b.Delete([]byte(p.Key))
		}
		return nil
	})
}

func (s *boltDBBlobStore) GetMeta(path string) (*meta.FileAttr, error) {
	p, err := meta.NewPathFromString(path)
	if err != nil {
		return nil, err
	}

	if p.Kind == meta.PathIsRootFolder {
		return &meta.FileAttr{
			Mode: uint32(syscall.S_IFDIR) | 0o755,
		}, nil
	}

	var attr *meta.FileAttr
	err = s.db.View(func(tx *bbolt.Tx) error {
		if p.Kind == meta.PathIsPrefixFolder {
			b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
			if b == nil {
				return notFound("blob metadata bucket", path)
			}
			if b.Get([]byte(p.Prefix)) == nil {
				return notFound("prefix", path)
			}
			attr = &meta.FileAttr{
				Mode: uint32(syscall.S_IFDIR) | 0o755,
			}
			return nil
		}

		b := tx.Bucket([]byte(p.Prefix))
		if b == nil {
			return notFound("prefix bucket", path)
		}
		data := b.Get([]byte(p.Key))
		if data == nil {
			return notFound("key", path)
		}
		attr = new(meta.FileAttr)
		return attr.UnmarshalBinary(data)
	})
	if err != nil {
		return nil, err
	}
	return attr, nil
}

func (s *boltDBBlobStore) UpdateMeta(path string, attr *meta.FileAttr) error {
	p, err := meta.NewPathFromString(path)
	if err != nil {
		return err
	}
	if p.Kind == meta.PathIsRootFolder || p.Kind == meta.PathIsPrefixFolder {
		return nil
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(p.Prefix))
		if err != nil {
			return err
		}
		mb, err := tx.CreateBucketIfNotExists([]byte(blob.BlobMetadataBucketName))
		if err != nil {
			return err
		}
		if mb.Get([]byte(p.Prefix)) == nil {
			bm := blob.BlobFileMeta{}
			data, _ := bm.MarshalBinary()
			if err := mb.Put([]byte(p.Prefix), data); err != nil {
				return err
			}
		}

		b := tx.Bucket([]byte(p.Prefix))
		data, err := attr.MarshalBinary()
		if err != nil {
			return err
		}
		return b.Put([]byte(p.Key), data)
	})
}

func (s *boltDBBlobStore) Truncate(path string, newSize uint64) error {
	attr, err := s.GetMeta(path)
	if err != nil {
		return err
	}
	if attr.IsDir() {
		return errors.New("store: cannot truncate directory")
	}
	if attr.Length == newSize {
		return nil
	}

	p, _ := meta.NewPathFromString(path)
	neededBlocks := (newSize + meta.DefaultBlockSize - 1) / meta.DefaultBlockSize

	if uint64(len(attr.BlockIndices)) < neededBlocks {
		more, err := s.allocateBlocks(p.Prefix, int(neededBlocks)-len(attr.BlockIndices))
		if err != nil {
			return err
		}
		attr.BlockIndices = append(attr.BlockIndices, more...)
	} else if uint64(len(attr.BlockIndices)) > neededBlocks {
		extra := attr.BlockIndices[neededBlocks:]
		attr.BlockIndices = attr.BlockIndices[:neededBlocks]
		if err := s.releaseBlocks(p.Prefix, extra); err != nil {
			return err
		}
	}

	attr.Length = newSize
	attr.Blocks = uint64(len(attr.BlockIndices))
	return s.UpdateMeta(path, attr)
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
	p, err := meta.NewPathFromString(path)
	if err != nil {
		return err
	}
	if p.Kind == meta.PathIsPrefixFolder {
		return s.db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(p.Prefix))
			if err != nil {
				return err
			}
			b, err := tx.CreateBucketIfNotExists([]byte(blob.BlobMetadataBucketName))
			if err != nil {
				return err
			}
			if b.Get([]byte(p.Prefix)) != nil {
				return os.ErrExist
			}
			bm := blob.BlobFileMeta{}
			data, _ := bm.MarshalBinary()
			return b.Put([]byte(p.Prefix), data)
		})
	}

	exists, err := s.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return os.ErrExist
	}

	attr := &meta.FileAttr{
		Mode: meta.DefaultFileMode,
	}
	if p.Kind == meta.PathIsSubFolder {
		attr.Mode = uint32(syscall.S_IFDIR) | meta.DefaultDirMode
	}
	return s.UpdateMeta(path, attr)
}

func (s *boltDBBlobStore) Remove(path string) error {
	attr, err := s.GetMeta(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}
	if !attr.IsDir() {
		return errors.New("store: use Delete for files")
	}

	list, err := s.List(path)
	if err != nil {
		return err
	}
	if len(list) > 0 {
		return ErrPrefixNotEmpty
	}

	p, _ := meta.NewPathFromString(path)
	if p.Kind == meta.PathIsPrefixFolder {
		return s.db.Update(func(tx *bbolt.Tx) error {
			_ = tx.DeleteBucket([]byte(p.Prefix))
			b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
			if b != nil {
				_ = b.Delete([]byte(p.Prefix))
			}
			return s.blobs.RemoveBlob(p.Prefix)
		})
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Prefix))
		if b != nil {
			return b.Delete([]byte(p.Key))
		}
		return nil
	})
}

func (s *boltDBBlobStore) Exists(path string) (bool, error) {
	attr, err := s.GetMeta(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return attr != nil, nil
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

func (s *boltDBBlobStore) allocateBlocks(prefix string, needed int) ([]uint64, error) {
	s.flMu.Lock()
	defer s.flMu.Unlock()

	var bm blob.BlobFileMeta
	_ = bm.Load(prefix, s.db)

	var blocks []uint64
	for len(blocks) < needed && len(bm.RecycledBlocks) > 0 {
		blocks = append(blocks, bm.RecycledBlocks[len(bm.RecycledBlocks)-1])
		bm.RecycledBlocks = bm.RecycledBlocks[:len(bm.RecycledBlocks)-1]
	}

	if len(blocks) < needed {
		path := filepath.Join(s.dir, prefix)
		info, err := os.Stat(path)
		var currentSize int64
		if err == nil {
			currentSize = info.Size()
		}
		nextBlock := uint64(currentSize / meta.DefaultBlockSize)
		if currentSize%meta.DefaultBlockSize != 0 {
			nextBlock++
		}

		for len(blocks) < needed {
			blocks = append(blocks, nextBlock)
			nextBlock++
		}
	}

	err := bm.Save(prefix, s.db)
	return blocks, err
}

func (s *boltDBBlobStore) releaseBlocks(prefix string, blocks []uint64) error {
	if len(blocks) == 0 {
		return nil
	}
	s.flMu.Lock()
	defer s.flMu.Unlock()

	var bm blob.BlobFileMeta
	_ = bm.Load(prefix, s.db)

	bm.RecycledBlocks = append(bm.RecycledBlocks, blocks...)
	return bm.Save(prefix, s.db)
}
