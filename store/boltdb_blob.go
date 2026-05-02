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
	"time"

	"github.com/lch/cachefs/blob"
	"github.com/lch/cachefs/internal/meta"
	"go.etcd.io/bbolt"
)

const (
	defaultMetadataDB = "metadata.db"
)

type BoltDBBlobStore struct {
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

func NewStoreForRead(dir string) (Store, error) {
	if dir == "" {
		return nil, errors.New("store: empty dir")
	}
	dbPath := filepath.Join(dir, defaultMetadataDB)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, errors.New("store: cachefs dir doesn't contains metadata.db")
	}
	db, err := bbolt.Open(dbPath, 0o600, nil)
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

func (s *BoltDBBlobStore) Read(p meta.Path) (data []byte, attr *meta.FileAttr, err error) {
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

func (s *BoltDBBlobStore) Write(p meta.Path, data []byte, mode uint32) error {
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

	now := time.Now().UnixNano()
	if attr == nil {
		attr = &meta.FileAttr{
			Mode:  mode,
			Atime: now,
			Mtime: now,
			Ctime: now,
		}
	} else {
		attr.Mode = mode
		attr.Mtime = now
		attr.Ctime = now
	}
	attr.Length = uint64(len(data))
	attr.BlockIndices = blocks
	return s.UpdateMeta(p, attr)
}

func (s *BoltDBBlobStore) Delete(p meta.Path) error {
	switch p.Kind {
	case meta.PathIsRootFolder:
		return nil
	case meta.PathIsPrefixFolder:
		return s.deletePrefixFolder(p)
	case meta.PathIsSubFolder:
		return s.deleteSubFolder(p)
	case meta.PathIsFile:
		return s.deleteFile(p)
	default:
		return nil
	}
}

func (s *BoltDBBlobStore) deletePrefixFolder(p meta.Path) error {
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
	return s.db.Update(func(tx *bbolt.Tx) error {
		_ = tx.DeleteBucket([]byte(p.Prefix))
		b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
		if b != nil {
			_ = b.Delete([]byte(p.Prefix))
		}
		return s.blobs.RemoveBlob(p.Prefix)
	})
}

func (s *BoltDBBlobStore) deleteSubFolder(p meta.Path) error {
	fileCount := 0
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Prefix))
		if b == nil {
			return nil
		}
		_ = b.ForEach(func(k, v []byte) error {
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
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Prefix))
		if b != nil {
			return b.Delete([]byte(p.Key))
		}
		return nil
	})
}

func (s *BoltDBBlobStore) deleteFile(p meta.Path) error {
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

func (s *BoltDBBlobStore) getRootFolderMeta() (*meta.FileAttr, error) {
	return &meta.FileAttr{
		Mode: uint32(syscall.S_IFDIR) | meta.DefaultDirMode,
	}, nil
}

func (s *BoltDBBlobStore) getPrefixFolderMeta(p meta.Path) (*meta.FileAttr, error) {
	var attr *meta.FileAttr
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
		if b == nil {
			return notFound("blob metadata bucket", p.String())
		}
		if b.Get([]byte(p.Prefix)) == nil {
			return notFound("prefix", p.String())
		}
		attr = &meta.FileAttr{
			Mode: uint32(syscall.S_IFDIR) | meta.DefaultDirMode,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attr, nil
}

func (s *BoltDBBlobStore) getChildMeta(p meta.Path) (*meta.FileAttr, error) {
	var attr *meta.FileAttr
	err := s.db.View(func(tx *bbolt.Tx) error {
		// For children (files or subfolders)
		b := tx.Bucket([]byte(p.Prefix))
		if b == nil {
			return notFound("prefix bucket", p.String())
		}

		// Try the key as provided
		data := b.Get([]byte(p.Key))
		if data == nil {
			// If not found and doesn't have slash, try with slash
			if !strings.HasSuffix(p.Key, "/") {
				data = b.Get([]byte(p.Key + "/"))
			} else {
				// If has slash, try without slash
				data = b.Get([]byte(strings.TrimSuffix(p.Key, "/")))
			}
		}

		if data == nil {
			return notFound("entry", p.String())
		}

		attr = &meta.FileAttr{}
		return attr.UnmarshalBinary(data)
	})
	if err != nil {
		return nil, err
	}
	return attr, nil
}

func (s *BoltDBBlobStore) GetMeta(p meta.Path) (*meta.FileAttr, error) {
	switch p.Kind {
	case meta.PathIsRootFolder:
		return s.getRootFolderMeta()
	case meta.PathIsPrefixFolder:
		return s.getPrefixFolderMeta(p)
	default:
		return s.getChildMeta(p)
	}
}

func (s *BoltDBBlobStore) updateRootFolderMeta(_ meta.Path, _ *meta.FileAttr) error {
	return nil
}

func (s *BoltDBBlobStore) updatePrefixFolderMeta(_ meta.Path, _ *meta.FileAttr) error {
	return nil
}

func (s *BoltDBBlobStore) updateChildMeta(p meta.Path, attr *meta.FileAttr) error {
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
		err = b.Put([]byte(p.Key), data)
		if err != nil {
			return err
		}
		return nil
	})
}

func (s *BoltDBBlobStore) UpdateMeta(p meta.Path, attr *meta.FileAttr) error {
	switch p.Kind {
	case meta.PathIsRootFolder:
		return s.updateRootFolderMeta(p, attr)
	case meta.PathIsPrefixFolder:
		return s.updatePrefixFolderMeta(p, attr)
	default:
		return s.updateChildMeta(p, attr)
	}
}

func (s *BoltDBBlobStore) Truncate(p meta.Path, newSize uint64) error {
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

	now := time.Now().UnixNano()
	attr.Length = newSize
	attr.Mtime = now
	attr.Ctime = now
	return s.UpdateMeta(p, attr)
}

func (s *BoltDBBlobStore) List(p meta.Path) ([]string, error) {
	switch p.Kind {
	case meta.PathIsRootFolder:
		return s.listRootFolder(p)
	case meta.PathIsPrefixFolder:
		return s.listPrefixFolder(p)
	case meta.PathIsSubFolder:
		return s.listSubFolder(p)
	default:
		return []string{}, nil
	}
}

func (s *BoltDBBlobStore) listRootFolder(p meta.Path) ([]string, error) {
	var list []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
		if b == nil {
			return notFound("blob metadata bucket", p.String())
		}
		return b.ForEach(func(k, v []byte) error {
			list = append(list, string(k))
			return nil
		})
	})
	return list, err
}

func (s *BoltDBBlobStore) listPrefixFolder(p meta.Path) ([]string, error) {
	var list []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Prefix))
		if b == nil {
			return notFound("prefix", p.String())
		}
		return b.ForEach(func(k, v []byte) error {
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
	})
	return list, err
}

func (s *BoltDBBlobStore) listSubFolder(p meta.Path) ([]string, error) {
	var list []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(p.Prefix))
		if b == nil {
			return notFound("prefix", p.String())
		}
		return b.ForEach(func(k, v []byte) error {
			keyStr := string(k)
			if strings.HasPrefix(keyStr, p.Key) {
				remainder := keyStr[len(p.Key):]
				if len(remainder) > 0 {
					count := strings.Count(remainder, "/")
					if count == 0 || (count == 1 && strings.HasSuffix(remainder, "/")) {
						list = append(list, keyStr)
					}
				}
			}
			return nil
		})
	})
	return list, err
}

func (s *BoltDBBlobStore) Create(p meta.Path) error {
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

func (s *BoltDBBlobStore) Exists(p meta.Path) (bool, error) {
	attr, err := s.GetMeta(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return attr != nil, nil
}

func (s *BoltDBBlobStore) Rename(oldPath, newPath meta.Path) error {
	if oldPath.Prefix != newPath.Prefix {
		return s.renameAcrossPrefixes(oldPath, newPath)
	}
	return s.renameInPrefixes(oldPath, newPath)
}

func (s *BoltDBBlobStore) renameInPrefixes(oldPath, newPath meta.Path) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(oldPath.Prefix))
		if b == nil {
			return notFound("prefix bucket", oldPath.String())
		}

		// Detect if it's a directory
		isDir := strings.HasSuffix(oldPath.Key, "/")
		data := b.Get([]byte(oldPath.Key))
		if data != nil {
			attr := new(meta.FileAttr)
			if err := attr.UnmarshalBinary(data); err == nil && attr.IsDir() {
				isDir = true
			}
		}

		if !isDir {
			if data == nil {
				return notFound("key", oldPath.String())
			}

			// In-prefix file move: Put new, delete old
			if err := b.Put([]byte(newPath.Key), data); err != nil {
				return err
			}
			return b.Delete([]byte(oldPath.Key))
		}

		// Directory move: recursive key migration
		cursor := b.Cursor()
		oldKey := oldPath.Key
		if !strings.HasSuffix(oldKey, "/") {
			oldKey += "/"
		}
		oldKeyBytes := []byte(oldKey)

		// Move the directory entry itself
		dirKey := []byte(strings.TrimSuffix(oldPath.Key, "/"))
		if v := b.Get(dirKey); v != nil {
			newDirKey := []byte(strings.TrimSuffix(newPath.Key, "/"))
			if err := b.Put(newDirKey, v); err != nil {
				return err
			}
			_ = b.Delete(dirKey)
		}

		// Move all children. Collect first to avoid cursor instability during modifications.
		type entry struct {
			k, v []byte
		}
		var children []entry
		for k, v := cursor.Seek(oldKeyBytes); k != nil && bytes.HasPrefix(k, oldKeyBytes); k, v = cursor.Next() {
			kCopy := make([]byte, len(k))
			copy(kCopy, k)
			vCopy := make([]byte, len(v))
			copy(vCopy, v)
			children = append(children, entry{k: kCopy, v: vCopy})
		}

		newKeyPrefix := newPath.Key
		if !strings.HasSuffix(newKeyPrefix, "/") {
			newKeyPrefix += "/"
		}

		for _, e := range children {
			newKey := append([]byte(newKeyPrefix), e.k[len(oldKeyBytes):]...)
			if err := b.Put(newKey, e.v); err != nil {
				return err
			}
			if err := b.Delete(e.k); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *BoltDBBlobStore) renameAcrossPrefixes(oldPath, newPath meta.Path) error {
	attr, err := s.GetMeta(oldPath)
	if err != nil {
		return err
	}

	if attr.IsDir() {
		return s.renameDirAcrossPrefixes(oldPath, newPath)
	}

	// Rename file across prefixes: Read, Write, Delete
	data, _, err := s.Read(oldPath)
	if err != nil {
		return err
	}

	if err := s.Write(newPath, data, attr.Mode); err != nil {
		return err
	}

	// Preserve metadata if possible (times etc)
	_ = s.UpdateMeta(newPath, attr)

	return s.Delete(oldPath)
}

func (s *BoltDBBlobStore) renameDirAcrossPrefixes(oldPath, newPath meta.Path) error {
	// 1. Create target directory
	if err := s.Create(newPath); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}

	// 2. List all immediate children
	children, err := s.List(oldPath)
	if err != nil {
		return err
	}

	// 3. Move each child
	for _, childKey := range children {
		// childKey is already the full key in the old prefix bucket
		// We need to extract the relative part
		oldKeyPrefix := oldPath.Key
		if !strings.HasSuffix(oldKeyPrefix, "/") {
			oldKeyPrefix += "/"
		}

		relPath := strings.TrimPrefix(childKey, oldKeyPrefix)
		name := strings.TrimSuffix(relPath, "/")

		childOldPath := oldPath
		childOldPath.Key = childKey
		if strings.HasSuffix(childKey, "/") {
			childOldPath.Kind = meta.PathIsSubFolder
		} else {
			childOldPath.Kind = meta.PathIsFile
		}

		childNewPath := newPath
		childNewPath.Key = meta.ChildKey(newPath, name, childOldPath.Kind == meta.PathIsSubFolder)

		if err := s.Rename(childOldPath, childNewPath); err != nil {
			return err
		}
	}

	// 4. Delete old directory
	return s.Delete(oldPath)
}

func newBoltBlobStore(db *bbolt.DB) (*BoltDBBlobStore, error) {
	if db == nil {
		return nil, errors.New("store: nil db")
	}

	dir := filepath.Dir(db.Path())
	if dir == "" {
		return nil, fmt.Errorf("store: unable to determine blob directory from %q", db.Path())
	}

	db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(blob.BlobMetadataBucketName))
		return err
	})

	bm := blob.NewBlobManager(dir, db)

	return &BoltDBBlobStore{
		db:    db,
		blobs: bm,
		dir:   dir,
	}, nil
}

func (s *BoltDBBlobStore) Close() error {
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

func (s *BoltDBBlobStore) allocateBlocks(prefix string, needed int) ([]uint64, error) {
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
		nextBlock := bm.AllocatedBlocks
		for len(blocks) < needed {
			blocks = append(blocks, nextBlock)
			nextBlock++
		}
		bm.AllocatedBlocks = nextBlock
	}

	err := bm.Save(prefix, s.db)
	return blocks, err
}

func (s *BoltDBBlobStore) releaseBlocks(prefix string, blocks []uint64) error {
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

type BoltDBBlobStoreStat struct {
	Prefix string
	ItemN  int
	blob.BlobFileMeta
}

func (s *BoltDBBlobStore) GetStoreStat() (m map[string]BoltDBBlobStoreStat) {
	bfm := make(map[string]blob.BlobFileMeta)
	if s != nil && s.blobs != nil {
		s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(blob.BlobMetadataBucketName))
			if b == nil {
				return ErrStoreCorrupt
			}
			b.ForEach(func(k, v []byte) error {
				var meta blob.BlobFileMeta
				meta.UnmarshalBinary(v)
				prefix := string(k)
				bfm[prefix] = meta
				return nil
			})
			return nil
		})
	}
	m = map[string]BoltDBBlobStoreStat{}
	for prefix, meta := range bfm {
		itemN := 0
		s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(prefix))
			if b == nil {
				return ErrStoreCorrupt
			}
			itemN = b.Inspect().KeyN
			return nil
		})
		m[prefix] = BoltDBBlobStoreStat{
			Prefix:       prefix,
			ItemN:        itemN,
			BlobFileMeta: meta,
		}
	}
	return
}

func (s *BoltDBBlobStore) Stats() (stats StoreStats) {
	for _, stat := range s.GetStoreStat() {
		stats.Items += uint64(stat.ItemN)
		stats.AllocatedBlocks += stat.AllocatedBlocks
		stats.FreeBlocks += uint64(len(stat.RecycledBlocks))
	}
	return
}
