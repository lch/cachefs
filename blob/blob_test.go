package blob

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lch/cachefs/internal/meta"
	"go.etcd.io/bbolt"
)

func setupTestBlobManager(t *testing.T) (*BlobManager, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "blob-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(dir, "metadata.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to open bbolt db: %v", err)
	}

	// Initialize the metadata bucket
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BlobMetadataBucketName))
		return err
	})
	if err != nil {
		db.Close()
		os.RemoveAll(dir)
		t.Fatalf("failed to create metadata bucket: %v", err)
	}

	bm := NewBlobManager(dir, db)

	cleanup := func() {
		bm.Close()
		db.Close()
		os.RemoveAll(dir)
	}

	return bm, cleanup
}

func TestAppendAndReadRoundTrip(t *testing.T) {
	bm, cleanup := setupTestBlobManager(t)
	defer cleanup()

	prefix := "test-prefix"
	data := bytes.Repeat([]byte("a"), meta.DefaultBlockSize)
	blocks := []uint64{0}

	if err := bm.Write(prefix, blocks, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readData, err := bm.Read(prefix, blocks)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(data, readData) {
		t.Errorf("Read data does not match written data")
	}
}

func TestWriteAtSpecificOffset(t *testing.T) {
	bm, cleanup := setupTestBlobManager(t)
	defer cleanup()

	prefix := "test-prefix"
	data1 := bytes.Repeat([]byte("1"), meta.DefaultBlockSize)
	data2 := bytes.Repeat([]byte("2"), meta.DefaultBlockSize)

	// Write block 1 first
	if err := bm.Write(prefix, []uint64{1}, data1); err != nil {
		t.Fatalf("Write block 1 failed: %v", err)
	}

	// Write block 0
	if err := bm.Write(prefix, []uint64{0}, data2); err != nil {
		t.Fatalf("Write block 0 failed: %v", err)
	}

	// Read both
	readData, err := bm.Read(prefix, []uint64{0, 1})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	expected := append(data2, data1...)
	if !bytes.Equal(expected, readData) {
		t.Errorf("Read data does not match expected concatenation")
	}
}

func TestReadCreatesBlobFile(t *testing.T) {
	bm, cleanup := setupTestBlobManager(t)
	defer cleanup()

	prefix := "new-blob"
	// Reading from a non-existent file should create it if we use withBlobFile
	// But ReadAt on a new empty file at offset 0 will likely return EOF if we try to read DefaultBlockSize.
	// Actually, bm.Read will try to read meta.DefaultBlockSize.
	_, err := bm.Read(prefix, []uint64{0})
	if err == nil {
		t.Errorf("Expected error reading from empty non-existent file, got nil")
	}

	path := filepath.Join(bm.dir, prefix)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("Blob file was not created on Read")
	}
}

func TestRemoveBlobDeletesFile(t *testing.T) {
	bm, cleanup := setupTestBlobManager(t)
	defer cleanup()

	prefix := "delete-me"
	data := bytes.Repeat([]byte("x"), meta.DefaultBlockSize)
	if err := bm.Write(prefix, []uint64{0}, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	path := filepath.Join(bm.dir, prefix)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("File should exist before removal: %v", err)
	}

	if err := bm.RemoveBlob(prefix); err != nil {
		t.Fatalf("RemoveBlob failed: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("File should be deleted after RemoveBlob, but stat err: %v", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	bm, cleanup := setupTestBlobManager(t)
	defer cleanup()

	prefix := "concurrent-read"
	data := bytes.Repeat([]byte("c"), meta.DefaultBlockSize)
	if err := bm.Write(prefix, []uint64{0}, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readData, err := bm.Read(prefix, []uint64{0})
			if err != nil {
				t.Errorf("Concurrent read failed: %v", err)
				return
			}
			if !bytes.Equal(data, readData) {
				t.Errorf("Concurrent read data mismatch")
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentAppendAndRead(t *testing.T) {
	bm, cleanup := setupTestBlobManager(t)
	defer cleanup()

	prefix := "concurrent-mix"
	numIterations := 50
	var wg sync.WaitGroup

	// Start a writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			data := []byte(fmt.Sprintf("%04096d", i)) // 4096 bytes
			if err := bm.Write(prefix, []uint64{uint64(i)}, data); err != nil {
				t.Errorf("Concurrent write failed at iteration %d: %v", i, err)
				return
			}
		}
	}()

	// Start a reader that reads what's available
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			// Busy wait or small sleep to let writer proceed
			for {
				readData, err := bm.Read(prefix, []uint64{uint64(i)})
				if err == nil {
					expected := []byte(fmt.Sprintf("%04096d", i))
					if !bytes.Equal(expected, readData) {
						t.Errorf("Concurrent read mismatch at iteration %d", i)
					}
					break
				}
				// If error is EOF/UnexpectedEOF, writer might not have written it yet
			}
		}
	}()

	wg.Wait()
}

