package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lch/cachefs/blob"
	"github.com/lch/cachefs/internal/meta"
	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

const freelistBucketName = "_freelist"

// bboltBlobStore stores file metadata in bbolt and content in blob files.
type bboltBlobStore struct {
	db        *bbolt.DB
	blobs     *blob.BlobManager
	dir       string
	freelists map[string]*blob.FreeList
	flMu      sync.Mutex
}

var _ Store = (*bboltBlobStore)(nil)

// NewStore opens the database and blob directory rooted at dir.
func NewStore(dir string) (Store, error) {
	if dir == "" {
		return nil, errors.New("store: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db, err := bbolt.Open(filepath.Join(dir, "cache.db"), 0o600, nil)
	if err != nil {
		return nil, err
	}

	s, err := newBboltBlobStoreFromExistingDB(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func newBboltBlobStoreFromExistingDB(db *bbolt.DB) (*bboltBlobStore, error) {
	if db == nil {
		return nil, errors.New("store: nil db")
	}

	dir := filepath.Dir(db.Path())
	if dir == "" {
		return nil, fmt.Errorf("store: unable to determine blob directory from %q", db.Path())
	}

	freelists, err := loadFreeLists(db)
	if err != nil {
		return nil, err
	}

	return &bboltBlobStore{
		db:        db,
		blobs:     blob.NewBlobManager(dir),
		dir:       dir,
		freelists: freelists,
	}, nil
}

func loadFreeLists(db *bbolt.DB) (map[string]*blob.FreeList, error) {
	freelists := make(map[string]*blob.FreeList)
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(freelistBucketName))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			fl := blob.NewFreeList()
			if err := fl.Unmarshal(v); err != nil {
				return fmt.Errorf("store: load freelist %q: %w", string(k), err)
			}
			freelists[string(k)] = fl
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return freelists, nil
}

func (s *bboltBlobStore) Close() error {
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

func (s *bboltBlobStore) readFileByKey(prefix, key string) ([]byte, *meta.FileAttr, error) {
	attr, err := s.getMetaByKey(prefix, key)
	if err != nil {
		return nil, nil, err
	}
	if attr.Length == 0 {
		return []byte{}, attr, nil
	}

	data, err := s.blobs.Read(prefix, attr.Offset, attr.Length)
	if err != nil {
		return nil, nil, err
	}
	return data, attr, nil
}

func (s *bboltBlobStore) writeFileByKey(prefix, key string, data []byte, mode uint32) (err error) {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	s.flMu.Lock()
	defer s.flMu.Unlock()

	var existingAttr *meta.FileAttr
	var existingData []byte
	if existingAttr, err = s.getMetaByKey(prefix, key); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		existingAttr = nil
		err = nil
	} else if existingAttr.Length > 0 {
		existingData, err = s.blobs.Read(prefix, existingAttr.Offset, existingAttr.Length)
		if err != nil {
			return err
		}
	}

	localFl, err := s.cloneFreeList(prefix)
	if err != nil {
		return err
	}
	if existingAttr != nil && existingAttr.Length > 0 {
		localFl.Free(existingAttr.Offset, existingAttr.Length)
	}

	return s.replaceFileLocked(prefix, key, data, mode, existingAttr, existingData, localFl)
}

func (s *bboltBlobStore) deleteFileByKey(prefix, key string) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	s.flMu.Lock()
	defer s.flMu.Unlock()

	attr, err := s.getMetaByKey(prefix, key)
	if err != nil {
		return err
	}

	localFl, err := s.cloneFreeList(prefix)
	if err != nil {
		return err
	}
	if attr.Length > 0 {
		localFl.Free(attr.Offset, attr.Length)
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return notFound("file", prefix, key)
		}
		k := []byte(key)
		if !hasKey(bucket, k) {
			return notFound("file", prefix, key)
		}
		if err := bucket.Delete(k); err != nil {
			return err
		}
		return persistFreeList(tx, prefix, localFl)
	})
	if err == nil {
		s.freelists[prefix] = localFl
	}
	return err
}

func (s *bboltBlobStore) getMetaByKey(prefix, key string) (*meta.FileAttr, error) {
	var out *meta.FileAttr
	if err := s.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return notFound("file", prefix, key)
		}
		data := bucket.Get([]byte(key))
		if data == nil {
			return notFound("file", prefix, key)
		}
		attr, err := meta.Unmarshal(copyBytes(data))
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

func (s *bboltBlobStore) updateMetaByKey(prefix, key string, attr *meta.FileAttr) error {
	if attr == nil {
		return errors.New("store: nil meta attr")
	}
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return notFound("file", prefix, key)
		}
		k := []byte(key)
		if !hasKey(bucket, k) {
			return notFound("file", prefix, key)
		}
		return bucket.Put(k, meta.Marshal(attr))
	})
}

func (s *bboltBlobStore) truncateFileByKey(prefix, key string, newSize uint64) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	s.flMu.Lock()
	defer s.flMu.Unlock()

	currentData, currentAttr, err := s.readFileByKey(prefix, key)
	if err != nil {
		return err
	}

	resized := resizeBytes(currentData, newSize)
	localFl, err := s.cloneFreeList(prefix)
	if err != nil {
		return err
	}
	if currentAttr.Length > 0 {
		localFl.Free(currentAttr.Offset, currentAttr.Length)
	}

	return s.replaceFileLocked(prefix, key, resized, currentAttr.Mode, currentAttr, currentData, localFl)
}

func (s *bboltBlobStore) createSubdir(prefix, dirname string) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	markerKey := []byte(meta.SubdirMarkerKey(dirname))
	attr := buildFileAttr(nil, syscall.S_IFDIR|meta.DefaultDirMode, 0, 0)

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(prefix))
		if err != nil {
			return err
		}
		if hasKey(bucket, markerKey) {
			return fmt.Errorf("store: subdir %q/%q: %w", prefix, dirname, os.ErrExist)
		}
		return bucket.Put(markerKey, meta.Marshal(attr))
	})
}

func (s *bboltBlobStore) removeSubdir(prefix, dirname string) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	markerKey := []byte(meta.SubdirMarkerKey(dirname))
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return notFound("subdir", prefix, dirname)
		}
		if !hasKey(bucket, markerKey) {
			return notFound("subdir", prefix, dirname)
		}
		cursor := bucket.Cursor()
		for k, _ := cursor.Seek(markerKey); k != nil && bytes.HasPrefix(k, markerKey); k, _ = cursor.Next() {
			if !bytes.Equal(k, markerKey) {
				return syscall.ENOTEMPTY
			}
		}
		return bucket.Delete(markerKey)
	})
}

func (s *bboltBlobStore) subdirExists(prefix, dirname string) (bool, error) {
	var exists bool
	if err := s.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			exists = false
			return nil
		}
		exists = hasKey(bucket, []byte(meta.SubdirMarkerKey(dirname)))
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *bboltBlobStore) listSubdirEntries(prefix, dirname string) ([]string, error) {
	entries := make([]string, 0)
	if err := s.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return notFound("subdir", prefix, dirname)
		}
		markerKey := []byte(meta.SubdirMarkerKey(dirname))
		if !hasKey(bucket, markerKey) {
			return notFound("subdir", prefix, dirname)
		}
		cursor := bucket.Cursor()
		for k, _ := cursor.Seek(markerKey); k != nil && bytes.HasPrefix(k, markerKey); k, _ = cursor.Next() {
			suffix := string(k[len(markerKey):])
			if suffix == "" || strings.Contains(suffix, "/") {
				continue
			}
			entries = append(entries, suffix)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(entries)
	return entries, nil
}

func (s *bboltBlobStore) listSubdirs(prefix string) ([]string, error) {
	subdirs := make([]string, 0)
	if err := s.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return notFound("prefix", prefix, "")
		}
		return bucket.ForEach(func(name, _ []byte) error {
			if meta.IsSubdirMarker(string(name)) {
				subdirs = append(subdirs, strings.TrimSuffix(string(name), "/"))
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	sort.Strings(subdirs)
	return subdirs, nil
}

func (s *bboltBlobStore) ReadFile(prefix, filename string) ([]byte, *meta.FileAttr, error) {
	return s.readFileByKey(prefix, filename)
}

func (s *bboltBlobStore) WriteFile(prefix, filename string, data []byte, mode uint32) (err error) {
	return s.writeFileByKey(prefix, filename, data, mode)
}

func (s *bboltBlobStore) DeleteFile(prefix, filename string) (err error) {
	return s.deleteFileByKey(prefix, filename)
}

func (s *bboltBlobStore) GetMeta(prefix, filename string) (*meta.FileAttr, error) {
	return s.getMetaByKey(prefix, filename)
}

func (s *bboltBlobStore) UpdateMeta(prefix, filename string, attr *meta.FileAttr) error {
	return s.updateMetaByKey(prefix, filename, attr)
}

func (s *bboltBlobStore) Truncate(prefix, filename string, newSize uint64) error {
	return s.truncateFileByKey(prefix, filename, newSize)
}

func (s *bboltBlobStore) ListPrefixes() ([]string, error) {
	prefixes := make([]string, 0)
	if err := s.view(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if b == nil {
				return nil
			}
			if string(name) == freelistBucketName {
				return nil
			}
			if !isHexPrefix(string(name)) {
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

func (s *bboltBlobStore) ListFiles(prefix string) ([]string, error) {
	files := make([]string, 0)
	if err := s.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return notFound("prefix", prefix, "")
		}
		return bucket.ForEach(func(name, _ []byte) error {
			if !strings.Contains(string(name), "/") {
				files = append(files, string(name))
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func (s *bboltBlobStore) ListSubdirs(prefix string) ([]string, error) {
	return s.listSubdirs(prefix)
}

func (s *bboltBlobStore) CreatePrefix(prefix string) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucket([]byte(prefix)); err != nil {
			if errors.Is(err, berrors.ErrBucketExists) {
				return fmt.Errorf("store: prefix %q: %w", prefix, os.ErrExist)
			}
			return err
		}
		return nil
	})
}

func (s *bboltBlobStore) RemovePrefix(prefix string) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}

	s.flMu.Lock()
	defer s.flMu.Unlock()

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(prefix))
		if bucket == nil {
			return fmt.Errorf("store: prefix %q: %w", prefix, os.ErrNotExist)
		}
		if k, _ := bucket.Cursor().First(); k != nil {
			return fmt.Errorf("store: prefix %q: %w", prefix, ErrPrefixNotEmpty)
		}
		if err := tx.DeleteBucket([]byte(prefix)); err != nil {
			if errors.Is(err, berrors.ErrBucketNotFound) {
				return fmt.Errorf("store: prefix %q: %w", prefix, os.ErrNotExist)
			}
			return err
		}
		if b := tx.Bucket([]byte(freelistBucketName)); b != nil {
			_ = b.Delete([]byte(prefix))
		}
		return nil
	}); err != nil {
		return err
	}

	delete(s.freelists, prefix)
	return s.blobs.RemoveBlob(prefix)
}

func (s *bboltBlobStore) PrefixExists(prefix string) (bool, error) {
	var exists bool
	if err := s.view(func(tx *bbolt.Tx) error {
		exists = tx.Bucket([]byte(prefix)) != nil
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *bboltBlobStore) CreateSubdir(prefix, dirname string) error {
	return s.createSubdir(prefix, dirname)
}

func (s *bboltBlobStore) RemoveSubdir(prefix, dirname string) error {
	return s.removeSubdir(prefix, dirname)
}

func (s *bboltBlobStore) SubdirExists(prefix, dirname string) (bool, error) {
	return s.subdirExists(prefix, dirname)
}

func (s *bboltBlobStore) ListSubdirEntries(prefix, dirname string) ([]string, error) {
	return s.listSubdirEntries(prefix, dirname)
}

func (s *bboltBlobStore) ReadSubdirFile(prefix, dirname, filename string) ([]byte, *meta.FileAttr, error) {
	return s.readFileByKey(prefix, meta.SubdirFileKey(dirname, filename))
}

func (s *bboltBlobStore) WriteSubdirFile(prefix, dirname, filename string, data []byte, mode uint32) error {
	return s.writeFileByKey(prefix, meta.SubdirFileKey(dirname, filename), data, mode)
}

func (s *bboltBlobStore) DeleteSubdirFile(prefix, dirname, filename string) error {
	return s.deleteFileByKey(prefix, meta.SubdirFileKey(dirname, filename))
}

func (s *bboltBlobStore) GetSubdirFileMeta(prefix, dirname, filename string) (*meta.FileAttr, error) {
	return s.getMetaByKey(prefix, meta.SubdirFileKey(dirname, filename))
}

func (s *bboltBlobStore) UpdateSubdirFileMeta(prefix, dirname, filename string, attr *meta.FileAttr) error {
	return s.updateMetaByKey(prefix, meta.SubdirFileKey(dirname, filename), attr)
}

func (s *bboltBlobStore) TruncateSubdirFile(prefix, dirname, filename string, newSize uint64) error {
	return s.truncateFileByKey(prefix, meta.SubdirFileKey(dirname, filename), newSize)
}

func (s *bboltBlobStore) cloneFreeList(prefix string) (*blob.FreeList, error) {
	current := s.freelists[prefix]
	clone := blob.NewFreeList()
	if current == nil {
		return clone, nil
	}
	if err := clone.Unmarshal(current.Marshal()); err != nil {
		return nil, err
	}
	return clone, nil
}

func (s *bboltBlobStore) replaceFileLocked(prefix, filename string, data []byte, mode uint32, existingAttr *meta.FileAttr, existingData []byte, freelist *blob.FreeList) (err error) {
	if freelist == nil {
		freelist = blob.NewFreeList()
	}

	var restoreNeeded bool
	var restoreOffset uint64
	var restoreData []byte
	if existingAttr != nil && existingAttr.Length > 0 {
		restoreOffset = existingAttr.Offset
	}

	writeOffset := uint64(0)
	usedAppend := false

	if len(data) > 0 {
		if off, ok := freelist.Allocate(uint64(len(data))); ok {
			writeOffset = off
		} else {
			writeOffset, err = s.blobs.Append(prefix, data)
			if err != nil {
				return err
			}
			usedAppend = true
		}

		if existingAttr != nil && existingAttr.Length > 0 && writeOffset == existingAttr.Offset {
			restoreNeeded = true
			restoreData = append([]byte(nil), existingData...)
		}

		if !usedAppend {
			if err = s.blobs.Write(prefix, data, writeOffset); err != nil {
				if restoreNeeded && len(restoreData) != 0 {
					if restoreErr := s.blobs.Write(prefix, restoreData, restoreOffset); restoreErr != nil {
						err = errors.Join(err, restoreErr)
					}
				}
				return err
			}
		}
	}

	attr := buildFileAttr(existingAttr, mode, writeOffset, uint64(len(data)))

	err = s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(prefix))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(filename), meta.Marshal(attr)); err != nil {
			return err
		}
		return persistFreeList(tx, prefix, freelist)
	})
	if err != nil {
		if restoreNeeded && len(restoreData) != 0 {
			if restoreErr := s.blobs.Write(prefix, restoreData, restoreOffset); restoreErr != nil {
				err = errors.Join(err, restoreErr)
			}
		}
		return err
	}

	s.freelists[prefix] = freelist
	return nil
}

func persistFreeList(tx *bbolt.Tx, prefix string, freelist *blob.FreeList) error {
	b, err := tx.CreateBucketIfNotExists([]byte(freelistBucketName))
	if err != nil {
		return err
	}
	if freelist == nil {
		freelist = blob.NewFreeList()
	}
	return b.Put([]byte(prefix), freelist.Marshal())
}

func (s *bboltBlobStore) view(fn func(*bbolt.Tx) error) error {
	if s == nil || s.db == nil {
		return errors.New("store: nil db")
	}
	return s.db.View(fn)
}

func isHexPrefix(name string) bool {
	if len(name) != 2 {
		return false
	}
	for i := range 2 {
		c := name[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

func buildFileAttr(existing *meta.FileAttr, mode uint32, offset, length uint64) *meta.FileAttr {
	if mode == 0 {
		mode = meta.DefaultFileMode
	}
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	if existing != nil {
		uid = existing.Uid
		gid = existing.Gid
	}
	now := time.Now().Unix()
	return &meta.FileAttr{
		Offset: offset,
		Length: length,
		Mode:   mode,
		Uid:    uid,
		Gid:    gid,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
	}
}

func resizeBytes(data []byte, newSize uint64) []byte {
	if uint64(len(data)) == newSize {
		return append([]byte(nil), data...)
	}
	if uint64(len(data)) > newSize {
		return append([]byte(nil), data[:int(newSize)]...)
	}
	buf := make([]byte, int(newSize))
	copy(buf, data)
	return buf
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

func notFound(kind, prefix, name string) error {
	if name == "" {
		return fmt.Errorf("store: %s %q: %w", kind, prefix, os.ErrNotExist)
	}
	return fmt.Errorf("store: %s %q/%q: %w", kind, prefix, name, os.ErrNotExist)
}
