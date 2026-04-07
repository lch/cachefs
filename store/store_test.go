package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lch/cachefs/internal/meta"
)

func TestWriteFileReadFileRoundTrip(t *testing.T) {
	s, _ := newStoreForTest(t)

	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	data := []byte("hello world")
	if err := s.WriteFile("aa", "alpha", data, meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, attr, err := s.ReadFile("aa", "alpha")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ReadFile data = %q, want %q", got, data)
	}
	if attr == nil {
		t.Fatal("ReadFile returned nil attr")
	}
	if attr.Length != uint64(len(data)) {
		t.Fatalf("attr.Length = %d, want %d", attr.Length, len(data))
	}
	if attr.Offset != 0 {
		t.Fatalf("attr.Offset = %d, want 0", attr.Offset)
	}

	metaAttr, err := s.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !reflect.DeepEqual(attr, metaAttr) {
		t.Fatalf("ReadFile attr = %#v, want %#v", attr, metaAttr)
	}
}

func TestDeleteFileReusesFreedOffset(t *testing.T) {
	s, _ := newStoreForTest(t)

	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	first := []byte("abcde")
	if err := s.WriteFile("aa", "alpha", first, meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile alpha: %v", err)
	}
	alphaMeta, err := s.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta alpha: %v", err)
	}

	if err := s.DeleteFile("aa", "alpha"); err != nil {
		t.Fatalf("DeleteFile alpha: %v", err)
	}

	second := []byte("vwxyz")
	if err := s.WriteFile("aa", "beta", second, meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile beta: %v", err)
	}
	betaMeta, err := s.GetMeta("aa", "beta")
	if err != nil {
		t.Fatalf("GetMeta beta: %v", err)
	}
	if betaMeta.Offset != alphaMeta.Offset {
		t.Fatalf("beta offset = %d, want %d", betaMeta.Offset, alphaMeta.Offset)
	}
	if betaMeta.Length != uint64(len(second)) {
		t.Fatalf("beta length = %d, want %d", betaMeta.Length, len(second))
	}
}

func TestTruncateShorterAndLonger(t *testing.T) {
	s, _ := newStoreForTest(t)

	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	if err := s.WriteFile("aa", "alpha", []byte("hello world"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := s.Truncate("aa", "alpha", 5); err != nil {
		t.Fatalf("Truncate shorter: %v", err)
	}
	got, attr, err := s.ReadFile("aa", "alpha")
	if err != nil {
		t.Fatalf("ReadFile after truncate shorter: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("truncate shorter data = %q, want %q", got, []byte("hello"))
	}
	if attr.Length != 5 {
		t.Fatalf("truncate shorter length = %d, want 5", attr.Length)
	}

	if err := s.Truncate("aa", "alpha", 8); err != nil {
		t.Fatalf("Truncate longer: %v", err)
	}
	got, attr, err = s.ReadFile("aa", "alpha")
	if err != nil {
		t.Fatalf("ReadFile after truncate longer: %v", err)
	}
	want := []byte{'h', 'e', 'l', 'l', 'o', 0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("truncate longer data = %v, want %v", got, want)
	}
	if attr.Length != 8 {
		t.Fatalf("truncate longer length = %d, want 8", attr.Length)
	}
}

func TestListPrefixesSkipsFreelistBucket(t *testing.T) {
	s, _ := newStoreForTest(t)

	for _, prefix := range []string{"bb", "aa"} {
		if err := s.CreatePrefix(prefix); err != nil {
			t.Fatalf("CreatePrefix(%s): %v", prefix, err)
		}
	}
	if err := s.WriteFile("aa", "alpha", []byte("x"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile alpha: %v", err)
	}
	if err := s.DeleteFile("aa", "alpha"); err != nil {
		t.Fatalf("DeleteFile alpha: %v", err)
	}

	prefixes, err := s.ListPrefixes()
	if err != nil {
		t.Fatalf("ListPrefixes: %v", err)
	}
	if !reflect.DeepEqual(prefixes, []string{"aa", "bb"}) {
		t.Fatalf("ListPrefixes = %v, want [aa bb]", prefixes)
	}
}

func TestRemovePrefixDeletesBlobFile(t *testing.T) {
	s, dir := newStoreForTest(t)

	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	if err := s.WriteFile("aa", "alpha", []byte("abc"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	blobPath := filepath.Join(dir, "aa")
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("blob file missing before remove: %v", err)
	}
	if err := s.DeleteFile("aa", "alpha"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if err := s.RemovePrefix("aa"); err != nil {
		t.Fatalf("RemovePrefix: %v", err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("blob file still exists after RemovePrefix: %v", err)
	}
	if exists, err := s.PrefixExists("aa"); err != nil {
		t.Fatalf("PrefixExists: %v", err)
	} else if exists {
		t.Fatal("PrefixExists returned true after RemovePrefix")
	}
}

func TestFreeListPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	if err := s.WriteFile("aa", "alpha", []byte("persist"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile alpha: %v", err)
	}
	alphaMeta, err := s.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta alpha: %v", err)
	}
	if err := s.DeleteFile("aa", "alpha"); err != nil {
		t.Fatalf("DeleteFile alpha: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}

	reopened, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close reopened store: %v", err)
		}
	})

	if err := reopened.WriteFile("aa", "beta", []byte("persist"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile beta: %v", err)
	}
	betaMeta, err := reopened.GetMeta("aa", "beta")
	if err != nil {
		t.Fatalf("GetMeta beta: %v", err)
	}
	if betaMeta.Offset != alphaMeta.Offset {
		t.Fatalf("beta offset after restart = %d, want %d", betaMeta.Offset, alphaMeta.Offset)
	}
}

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

func TestDeleteMissingFileReturnsNotExist(t *testing.T) {
	s, _ := newStoreForTest(t)

	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	if _, err := s.GetMeta("aa", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GetMeta missing error = %v, want os.ErrNotExist", err)
	}
	if _, _, err := s.ReadFile("aa", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadFile missing error = %v, want os.ErrNotExist", err)
	}
	if err := s.DeleteFile("aa", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DeleteFile missing error = %v, want os.ErrNotExist", err)
	}
	if err := s.RemovePrefix("aa"); err != nil {
		t.Fatalf("RemovePrefix empty prefix: %v", err)
	}
	if err := s.RemovePrefix("aa"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RemovePrefix missing error = %v, want os.ErrNotExist", err)
	}
}
