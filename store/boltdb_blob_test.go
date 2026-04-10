package store

import (
	"bytes"
	"errors"
	"testing"
)

func newStoreForTest(t *testing.T) (Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	return s, dir
}

func TestWriteFileReadFileRoundTrip(t *testing.T) {
	s, _ := newStoreForTest(t)
	path := "aa/testfile"
	data := []byte("hello world")
	mode := uint32(0644)

	err := s.Write(path, data, mode)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	exists, err := s.Exists(path)
	if err != nil || !exists {
		t.Fatalf("Exists failed: %v, exists: %v", err, exists)
	}

	readData, attr, err := s.Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(readData, data) {
		t.Errorf("Read data mismatch: got %q, want %q", readData, data)
	}

	if attr.Length != uint64(len(data)) {
		t.Errorf("Attr length mismatch: got %d, want %d", attr.Length, len(data))
	}
}

func TestDeleteFile(t *testing.T) {
	s, _ := newStoreForTest(t)
	path := "aa/testfile"
	data := []byte("hello world")

	_ = s.Write(path, data, 0644)
	err := s.Delete(path)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	exists, _ := s.Exists(path)
	if exists {
		t.Error("File still exists after Delete")
	}
}

func TestTruncate(t *testing.T) {
	s, _ := newStoreForTest(t)
	path := "aa/testfile"
	data := []byte("hello world") // 11 bytes

	_ = s.Write(path, data, 0644)

	// Shrink
	err := s.Truncate(path, 5)
	if err != nil {
		t.Fatalf("Truncate shrink failed: %v", err)
	}
	readData, attr, _ := s.Read(path)
	if len(readData) != 5 || attr.Length != 5 {
		t.Errorf("Truncate shrink size mismatch: got %d", len(readData))
	}
	if !bytes.Equal(readData, data[:5]) {
		t.Errorf("Truncate shrink data mismatch: got %q", readData)
	}

	// Grow
	err = s.Truncate(path, 20)
	if err != nil {
		t.Fatalf("Truncate grow failed: %v", err)
	}
	readData, attr, _ = s.Read(path)
	if len(readData) != 20 || attr.Length != 20 {
		t.Errorf("Truncate grow size mismatch: got %d", len(readData))
	}
}

func TestList(t *testing.T) {
	s, _ := newStoreForTest(t)
	_ = s.Write("aa/f1", []byte("1"), 0644)
	_ = s.Write("aa/f2", []byte("2"), 0644)
	_ = s.Write("bb/f1", []byte("3"), 0644)

	// List root
	prefixes, err := s.List("")
	if err != nil {
		t.Fatalf("List root failed: %v", err)
	}
	// Note: BlobMetadataBucketName might be excluded or included depending on implementation
	// But List root usually lists prefix buckets from _blob
	if len(prefixes) < 2 {
		t.Errorf("Prefixes count mismatch: %v", prefixes)
	}
	foundAA := false
	foundBB := false
	for _, p := range prefixes {
		if p == "aa" {
			foundAA = true
		}
		if p == "bb" {
			foundBB = true
		}
	}
	if !foundAA || !foundBB {
		t.Errorf("Missing prefixes: aa=%v, bb=%v", foundAA, foundBB)
	}

	// List prefix
	keys, err := s.List("aa/")
	if err != nil {
		t.Fatalf("List aa/ failed: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("Keys count mismatch: %v", keys)
	}
}

func TestMetadata(t *testing.T) {
	s, _ := newStoreForTest(t)
	path := "aa/f1"
	_ = s.Write(path, []byte("data"), 0644)

	attr, _ := s.GetMeta(path)
	attr.Uid = 1000
	attr.Gid = 1000

	err := s.UpdateMeta(path, attr)
	if err != nil {
		t.Fatalf("UpdateMeta failed: %v", err)
	}

	newAttr, _ := s.GetMeta(path)
	if newAttr.Uid != 1000 || newAttr.Gid != 1000 {
		t.Errorf("Metadata update not reflected: %+v", newAttr)
	}
}

func TestDirectoryOperations(t *testing.T) {
	s, _ := newStoreForTest(t)

	// Create prefix
	err := s.Create("cc/")
	if err != nil {
		t.Fatalf("Create prefix failed: %v", err)
	}
	exists, _ := s.Exists("cc/")
	if !exists {
		t.Error("Prefix does not exist after Create")
	}

	// Create subfolder
	err = s.Create("cc/sub/")
	if err != nil {
		t.Fatalf("Create subfolder failed: %v", err)
	}
	exists, _ = s.Exists("cc/sub/")
	if !exists {
		t.Error("Subfolder does not exist after Create")
	}

	// Remove empty subfolder
	err = s.Remove("cc/sub/")
	if err != nil {
		t.Fatalf("Remove subfolder failed: %v", err)
	}
	exists, _ = s.Exists("cc/sub/")
	if exists {
		t.Error("Subfolder still exists after Remove")
	}

	// Remove prefix
	err = s.Remove("cc/")
	if err != nil {
		t.Fatalf("Remove prefix failed: %v", err)
	}
	exists, _ = s.Exists("cc/")
	if exists {
		t.Error("Prefix still exists after Remove")
	}
}

func TestErrPrefixNotEmpty(t *testing.T) {
	s, _ := newStoreForTest(t)
	_ = s.Write("aa/f1", []byte("data"), 0644)

	err := s.Remove("aa/")
	if !errors.Is(err, ErrPrefixNotEmpty) {
		t.Errorf("Expected ErrPrefixNotEmpty, got %v", err)
	}
}

func TestBlockRecycling(t *testing.T) {
	s, _ := newStoreForTest(t)
	// Write enough data to use a few blocks (BlockSize is 4096)
	data := make([]byte, 10000) // ~2.5 blocks
	_ = s.Write("aa/f1", data, 0644)

	attr1, _ := s.GetMeta("aa/f1")
	blocks1 := make([]uint64, len(attr1.BlockIndices))
	copy(blocks1, attr1.BlockIndices)

	_ = s.Delete("aa/f1")

	// Write again, should reuse some blocks
	_ = s.Write("aa/f2", data, 0644)
	attr2, _ := s.GetMeta("aa/f2")
	blocks2 := attr2.BlockIndices

	// Check if any block is reused
	found := false
	for _, b1 := range blocks1 {
		for _, b2 := range blocks2 {
			if b1 == b2 {
				found = true
				break
			}
		}
	}
	if !found {
		t.Errorf("No blocks were recycled. Blocks1: %v, Blocks2: %v", blocks1, blocks2)
	}
}
