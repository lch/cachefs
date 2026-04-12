package store

import (
	"bytes"
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

func (s *boltDBBlobStore) Read(p meta.Path) (data []byte, attr *meta.FileAttr, err error) {
	attr, err = s.GetMeta(p)
	if err != nil {
		return
	}
	if attr.IsDir() {
		return nil, attr, nil
	}

	data, err = s.blobs.Read(p.Prefix, attr.BlockIndices)
	if err != nil {
		return
	}
	if uint64(len(data)) > attr.Length {
		data = data[:attr.Length]
	}
	return
}

func (s *boltDBBlobStore) Write(p meta.Path, data []byte, mode uint32) error {
	if p.Kind == meta.PathIsRootFolder || p.Kind == meta.PathIsPrefixFolder {
		return errors.New("store: cannot write to folder")
	}

	attr, err := s.GetMeta(p)
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
	return s.UpdateMeta(p, attr)
}

func (s *boltDBBlobStore) Delete(p meta.Path) error {
	switch p.Kind {
	case meta.PathIsRootFolder:

	case meta.PathIsPrefixFolder:
		err := s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(p.Prefix))
			if b == nil {
				return nil
			}
			bs := b.Inspect()
			if bs.KeyN != 0 {
				return ErrFolderNotEmpty
			}
			return nil
		})
		if err != nil {
			return errors.Join(ErrStoreCorrupt, err)
		}
		err = s.db.Update(func(tx *bbolt.Tx) error {
			_ = tx.DeleteBucket([]byte(p.Prefix))
			b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
			if b != nil {
				_ = b.Delete([]byte(p.Prefix))
			}
			return s.blobs.RemoveBlob(p.Prefix)
		})
		return err
	case meta.PathIsSubFolder:
		fileCount := 0
		err := s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(p.Prefix))
			if b == nil {
				return nil
			}
			b.ForEach(func(k, v []byte) error {
				if strings.HasPrefix(string(k), p.Key) {
					fileCount++
				}
				return nil
			})
			return nil
		})
		if err != nil {
			return err
		}
		if fileCount > 1 {
			return ErrFolderNotEmpty
		}
		err = s.db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(p.Prefix))
			if b != nil {
				return b.Delete([]byte(p.Key))
			}
			return nil
		})
		return err
	case meta.PathIsFile:
		attr, err := s.GetMeta(p)
		if err != nil {
			return err
		}
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
	return nil
}

func (s *boltDBBlobStore) GetMeta(p meta.Path) (attr *meta.FileAttr, err error) {
	if p.Kind == meta.PathIsRootFolder {
		return &meta.FileAttr{
			Mode: uint32(syscall.S_IFDIR) | 0o755,
		}, nil
	}

	err = s.db.View(func(tx *bbolt.Tx) error {
		switch p.Kind {
		case meta.PathIsPrefixFolder:
			b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
			if b == nil {
				return notFound("blob metadata bucket", p.String())
			}
			if b.Get([]byte(p.Prefix)) == nil {
				return notFound("prefix", p.String())
			}
			attr = &meta.FileAttr{
				Mode: uint32(syscall.S_IFDIR) | 0o755,
			}
			return nil
		case meta.PathIsSubFolder:
			b := tx.Bucket([]byte(p.Prefix))
			if b == nil {
				return notFound("prefix bucket", p.String())
			}
			data := b.Get([]byte(p.Key))
			if data == nil {
				return notFound("key", p.String())
			}
			attr = &meta.FileAttr{
				Mode: uint32(syscall.S_IFDIR) | 0o755,
			}
			return attr.UnmarshalBinary(data)
		case meta.PathIsFile:
			b := tx.Bucket([]byte(p.Prefix))
			if b == nil {
				return notFound("prefix bucket", p.String())
			}
			data := b.Get([]byte(p.Key))
			if data == nil {
				return notFound("key", p.String())
			}
			attr = &meta.FileAttr{}
			return attr.UnmarshalBinary(data)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attr, nil
}

func (s *boltDBBlobStore) UpdateMeta(p meta.Path, attr *meta.FileAttr) error {
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

func (s *boltDBBlobStore) Truncate(p meta.Path, newSize uint64) error {
	attr, err := s.GetMeta(p)
	if err != nil {
		return err
	}
	if attr.IsDir() {
		return errors.New("store: cannot truncate directory")
	}
	if attr.Length == newSize {
		return nil
	}

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
	return s.UpdateMeta(p, attr)
}

func (s *boltDBBlobStore) List(p meta.Path) ([]string, error) {
	list := make([]string, 0)
	switch p.Kind {
	case meta.PathIsRootFolder:
		s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
			if b == nil {
				return notFound("blob metadata bucket", p.String())
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
				return notFound("prefix", p.String())
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
	case meta.PathIsSubFolder:
		s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(p.Prefix))
			if b == nil {
				return notFound("prefix", p.String())
			}
			err := b.ForEach(func(k, v []byte) error {
				keyStr := string(k)
				if strings.HasPrefix(keyStr, p.Key) {
					list = append(list, keyStr)
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

func (s *boltDBBlobStore) Create(p meta.Path) error {
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

	exists, err := s.Exists(p)
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
	return s.UpdateMeta(p, attr)
}

func (s *boltDBBlobStore) Exists(p meta.Path) (bool, error) {
	attr, err := s.GetMeta(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return attr != nil, nil
}

func (s *boltDBBlobStore) Rename(oldPath, newPath meta.Path) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		oldB := tx.Bucket([]byte(oldPath.Prefix))
		if oldB == nil {
			return notFound("prefix bucket", oldPath.String())
		}

		// Check if it's a directory (trailing slash in key or isDir attr)
		isDir := strings.HasSuffix(oldPath.Key, "/")
		if !isDir {
			data := oldB.Get([]byte(oldPath.Key))
			if data != nil {
				attr := new(meta.FileAttr)
				if err := attr.UnmarshalBinary(data); err == nil && attr.IsDir() {
					isDir = true
				}
			}
		}

		newB, err := tx.CreateBucketIfNotExists([]byte(newPath.Prefix))
		if err != nil {
			return err
		}

		if !isDir {
			data := oldB.Get([]byte(oldPath.Key))
			if data == nil {
				return notFound("key", oldPath.String())
			}
			if err := newB.Put([]byte(newPath.Key), data); err != nil {
				return err
			}
			return oldB.Delete([]byte(oldPath.Key))
		}

		// Handle directory rename: move all keys starting with oldPath.Key
		cursor := oldB.Cursor()
		prefix := []byte(oldPath.Key)
		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			newKey := append([]byte(newPath.Key), k[len(prefix):]...)
			if err := newB.Put(newKey, v); err != nil {
				return err
			}
			if err := oldB.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})
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
