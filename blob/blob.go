package blob

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/lch/cachefs/internal/meta"
	"go.etcd.io/bbolt"
)

const BlobMetadataBucketName = "_blob"

// BlobManager manages per-prefix blob files.
type BlobManager struct {
	dir   string
	db    *bbolt.DB
	mu    sync.RWMutex
	files map[string]*BlobFile
}

// BlobFile wraps an open blob file and its logical size.
type BlobFile struct {
	mu   sync.Mutex
	f    *os.File
	size uint64
	BlobFileMeta
}

type BlobFileMeta struct {
	AllocatedBlocks uint64
	RecycledBlocks  []uint64
}

// NewBlobManager returns a manager rooted at dir.
func NewBlobManager(dir string, db *bbolt.DB) *BlobManager {
	return &BlobManager{
		dir:   dir,
		db:    db,
		files: make(map[string]*BlobFile),
	}
}

func (m *BlobManager) openOrCreateLocked(prefix string) (*BlobFile, error) {
	path := filepath.Join(m.dir, prefix)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	var bfm BlobFileMeta
	err = bfm.Load(prefix, m.db)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if bfm.AllocatedBlocks == 0 && info.Size() > 0 {
		bfm.AllocatedBlocks = uint64(info.Size() / meta.DefaultBlockSize)
		if info.Size()%int64(meta.DefaultBlockSize) != 0 {
			bfm.AllocatedBlocks++
		}
	}
	return &BlobFile{f: f, size: uint64(info.Size()), BlobFileMeta: bfm}, nil
}

func (m *BlobManager) withBlobFile(prefix string, fn func(*BlobFile) error) error {
	m.mu.RLock()
	if bf, ok := m.files[prefix]; ok {
		defer m.mu.RUnlock()
		return fn(bf)
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	bf, ok := m.files[prefix]
	if !ok {
		var err error
		bf, err = m.openOrCreateLocked(prefix)
		if err != nil {
			return err
		}
		m.files[prefix] = bf
	}
	return fn(bf)
}

// Read returns length bytes from the blob identified by a list of block indices.
func (m *BlobManager) Read(prefix string, blocks []uint64) ([]byte, error) {
	data := make([]byte, len(blocks)*meta.DefaultBlockSize)
	err := m.withBlobFile(prefix, func(bf *BlobFile) error {
		for i, blockIndex := range blocks {
			bufStart := i * meta.DefaultBlockSize
			bufEnd := (i + 1) * meta.DefaultBlockSize
			buf := data[bufStart:bufEnd]
			fileOffset := blockIndex * meta.DefaultBlockSize
			n, readErr := bf.f.ReadAt(buf, int64(fileOffset))
			if readErr != nil {
				if errors.Is(readErr, io.EOF) && n == len(buf) {
					readErr = nil
				} else if errors.Is(readErr, io.EOF) {
					readErr = io.ErrUnexpectedEOF
				}
				return readErr
			}
		}
		return nil
	})
	return data, err
}

// Write writes data to the given blocks in the blob identified by prefix and block indices.
func (m *BlobManager) Write(prefix string, blocks []uint64, data []byte) error {
	err := m.withBlobFile(prefix, func(bf *BlobFile) error {
		bf.mu.Lock()
		defer bf.mu.Unlock()

		for i, blockIndex := range blocks {
			bufStart := i * meta.DefaultBlockSize
			bufEnd := (i + 1) * meta.DefaultBlockSize
			fileOffset := blockIndex * meta.DefaultBlockSize
			n, err := bf.f.WriteAt(data[bufStart:bufEnd], int64(fileOffset))
			if err != nil {
				if errors.Is(err, io.EOF) && n == meta.DefaultBlockSize {
					err = nil
				} else if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				return err
			}
		}
		return nil
	})
	return err
}

// RemoveBlob closes and deletes the blob file for prefix.
func (m *BlobManager) RemoveBlob(prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if bf, ok := m.files[prefix]; ok {
		delete(m.files, prefix)
		if bf != nil && bf.f != nil {
			if err := bf.f.Close(); err != nil {
				return err
			}
		}
	}

	err := os.Remove(filepath.Join(m.dir, prefix))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Close closes all open blob file handles.
func (m *BlobManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for prefix, bf := range m.files {
		if bf != nil && bf.f != nil {
			err := bf.Save(prefix, m.db)
			if err != nil {
				errs = append(errs, err)
			}
			if err := bf.f.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		delete(m.files, prefix)
	}
	return errors.Join(errs...)
}

func (m *BlobFileMeta) Load(prefix string, db *bbolt.DB) (err error) {
	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BlobMetadataBucketName))
		if b == nil {
			return errors.New("BlobFileMeta: no metadata bucket found")
		}
		data := b.Get([]byte(prefix))
		if data != nil {
			err = m.UnmarshalBinary(data)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return
}

func (m *BlobFileMeta) Save(prefix string, db *bbolt.DB) (err error) {
	err = db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(BlobMetadataBucketName))
		if err != nil {
			return err
		}
		data, err := m.MarshalBinary()
		if err != nil {
			return err
		}
		b.Put([]byte(prefix), data)
		return nil
	})
	return
}

func (m *BlobFileMeta) MarshalBinary() (data []byte, err error) {
	data = binary.LittleEndian.AppendUint64(data, m.AllocatedBlocks)
	data = binary.LittleEndian.AppendUint64(data, uint64(len(m.RecycledBlocks)))
	for _, blockInd := range m.RecycledBlocks {
		data = binary.LittleEndian.AppendUint64(data, blockInd)
	}
	return
}

func (m *BlobFileMeta) UnmarshalBinary(data []byte) (err error) {
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, &m.AllocatedBlocks); err != nil {
		return err
	}
	var length uint64
	err = binary.Read(buf, binary.LittleEndian, &length)
	if err != nil {
		return err
	}
	m.RecycledBlocks = make([]uint64, 0, length)
	for i := 0; i < int(length); i++ {
		var blockInd uint64
		err = binary.Read(buf, binary.LittleEndian, &blockInd)
		if err != nil {
			return err
		}
		m.RecycledBlocks = append(m.RecycledBlocks, blockInd)
	}
	return
}
