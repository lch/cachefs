package blob

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// BlobManager manages per-prefix blob files.
type BlobManager struct {
	dir   string
	mu    sync.RWMutex
	files map[string]*BlobFile
}

// BlobFile wraps an open blob file and its logical size.
type BlobFile struct {
	mu   sync.Mutex
	f    *os.File
	size int64
}

// NewBlobManager returns a manager rooted at dir.
func NewBlobManager(dir string) *BlobManager {
	return &BlobManager{
		dir:   dir,
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

	return &BlobFile{f: f, size: info.Size()}, nil
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

// Read returns length bytes from the blob identified by prefix.
func (m *BlobManager) Read(prefix string, offset, length uint64) ([]byte, error) {
	if err := m.withBlobFile(prefix, func(bf *BlobFile) error { return nil }); err != nil {
		return nil, err
	}

	if length == 0 {
		return []byte{}, nil
	}
	if length > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("blob: read length too large: %d", length)
	}
	if offset > uint64(int64(^uint64(0)>>1)) {
		return nil, fmt.Errorf("blob: read offset too large: %d", offset)
	}

	var data []byte
	err := m.withBlobFile(prefix, func(bf *BlobFile) error {
		buf := make([]byte, int(length))
		n, readErr := bf.f.ReadAt(buf, int64(offset))
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && n == len(buf) {
				readErr = nil
			} else if errors.Is(readErr, io.EOF) {
				readErr = io.ErrUnexpectedEOF
			}
		}
		data = buf[:n]
		return readErr
	})
	return data, err
}

// Write writes data to the given offset in the blob identified by prefix.
func (m *BlobManager) Write(prefix string, data []byte, offset uint64) error {
	if len(data) == 0 {
		return nil
	}
	if offset > uint64(int64(^uint64(0)>>1)) {
		return fmt.Errorf("blob: write offset too large: %d", offset)
	}
	if len(data) > int(^uint(0)>>1) {
		return fmt.Errorf("blob: write length too large: %d", len(data))
	}

	return m.withBlobFile(prefix, func(bf *BlobFile) error {
		bf.mu.Lock()
		defer bf.mu.Unlock()

		n, err := bf.f.WriteAt(data, int64(offset))
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}

		end := int64(offset) + int64(len(data))
		if end > bf.size {
			bf.size = end
		}
		return nil
	})
}

// Append writes data at the end of the blob and returns the offset used.
func (m *BlobManager) Append(prefix string, data []byte) (offset uint64, err error) {
	if len(data) == 0 {
		var bf *BlobFile
		err = m.withBlobFile(prefix, func(file *BlobFile) error {
			bf = file
			return nil
		})
		if err != nil {
			return 0, err
		}
		return uint64(bf.size), nil
	}
	if len(data) > int(^uint(0)>>1) {
		return 0, fmt.Errorf("blob: append length too large: %d", len(data))
	}

	err = m.withBlobFile(prefix, func(bf *BlobFile) error {
		bf.mu.Lock()
		defer bf.mu.Unlock()

		offset = uint64(bf.size)
		n, writeErr := bf.f.WriteAt(data, bf.size)
		if writeErr != nil {
			return writeErr
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
		bf.size += int64(len(data))
		return nil
	})
	return offset, err
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

	var firstErr error
	for prefix, bf := range m.files {
		if bf != nil && bf.f != nil {
			if err := bf.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(m.files, prefix)
	}
	return firstErr
}
