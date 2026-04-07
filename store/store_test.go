package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
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

func TestSubdirOperations(t *testing.T) {
	s, _ := newStoreForTest(t)

	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	if err := s.CreateSubdir("aa", "foo"); err != nil {
		t.Fatalf("CreateSubdir: %v", err)
	}
	if exists, err := s.SubdirExists("aa", "foo"); err != nil {
		t.Fatalf("SubdirExists: %v", err)
	} else if !exists {
		t.Fatal("SubdirExists returned false for created subdir")
	}

	directFoo := []byte("direct")
	if err := s.WriteFile("aa", "foo", directFoo, meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile foo: %v", err)
	}

	rootData := []byte("abcde")
	if err := s.WriteFile("aa", "root.txt", rootData, meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteFile root.txt: %v", err)
	}
	rootMeta, err := s.GetMeta("aa", "root.txt")
	if err != nil {
		t.Fatalf("GetMeta root.txt: %v", err)
	}

	if err := s.WriteSubdirFile("aa", "foo", "bar", []byte("bar"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteSubdirFile bar: %v", err)
	}
	if err := s.WriteSubdirFile("aa", "foo", "baz", []byte("baz"), meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteSubdirFile baz: %v", err)
	}

	if got, err := s.ListFiles("aa"); err != nil {
		t.Fatalf("ListFiles: %v", err)
	} else if !reflect.DeepEqual(got, []string{"foo", "root.txt"}) {
		t.Fatalf("ListFiles = %v, want [foo root.txt]", got)
	}
	if got, err := s.ListSubdirs("aa"); err != nil {
		t.Fatalf("ListSubdirs: %v", err)
	} else if !reflect.DeepEqual(got, []string{"foo"}) {
		t.Fatalf("ListSubdirs = %v, want [foo]", got)
	}
	if got, err := s.ListSubdirEntries("aa", "foo"); err != nil {
		t.Fatalf("ListSubdirEntries: %v", err)
	} else if !reflect.DeepEqual(got, []string{"bar", "baz"}) {
		t.Fatalf("ListSubdirEntries = %v, want [bar baz]", got)
	}

	if got, _, err := s.ReadFile("aa", "foo"); err != nil {
		t.Fatalf("ReadFile foo: %v", err)
	} else if !bytes.Equal(got, directFoo) {
		t.Fatalf("ReadFile foo = %q, want %q", got, directFoo)
	}
	if got, _, err := s.ReadSubdirFile("aa", "foo", "bar"); err != nil {
		t.Fatalf("ReadSubdirFile bar: %v", err)
	} else if !bytes.Equal(got, []byte("bar")) {
		t.Fatalf("ReadSubdirFile bar = %q, want %q", got, []byte("bar"))
	}

	if err := s.DeleteFile("aa", "root.txt"); err != nil {
		t.Fatalf("DeleteFile root.txt: %v", err)
	}
	if err := s.WriteSubdirFile("aa", "foo", "reuse.bin", rootData, meta.DefaultFileMode); err != nil {
		t.Fatalf("WriteSubdirFile reuse.bin: %v", err)
	}
	reuseMeta, err := s.GetSubdirFileMeta("aa", "foo", "reuse.bin")
	if err != nil {
		t.Fatalf("GetSubdirFileMeta reuse.bin: %v", err)
	}
	if reuseMeta.Offset != rootMeta.Offset {
		t.Fatalf("reuse.bin offset = %d, want %d", reuseMeta.Offset, rootMeta.Offset)
	}
	if got, err := s.ListSubdirEntries("aa", "foo"); err != nil {
		t.Fatalf("ListSubdirEntries after reuse: %v", err)
	} else if !reflect.DeepEqual(got, []string{"bar", "baz", "reuse.bin"}) {
		t.Fatalf("ListSubdirEntries after reuse = %v, want [bar baz reuse.bin]", got)
	}

	if err := s.RemoveSubdir("aa", "foo"); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("RemoveSubdir non-empty error = %v, want ENOTEMPTY", err)
	}
	if err := s.DeleteSubdirFile("aa", "foo", "bar"); err != nil {
		t.Fatalf("DeleteSubdirFile bar: %v", err)
	}
	if err := s.DeleteSubdirFile("aa", "foo", "baz"); err != nil {
		t.Fatalf("DeleteSubdirFile baz: %v", err)
	}
	if err := s.DeleteSubdirFile("aa", "foo", "reuse.bin"); err != nil {
		t.Fatalf("DeleteSubdirFile reuse.bin: %v", err)
	}
	if err := s.DeleteFile("aa", "foo"); err != nil {
		t.Fatalf("DeleteFile foo: %v", err)
	}

	if err := s.RemovePrefix("aa"); !errors.Is(err, ErrPrefixNotEmpty) {
		t.Fatalf("RemovePrefix with subdir marker error = %v, want ErrPrefixNotEmpty", err)
	}
	if err := s.RemoveSubdir("aa", "foo"); err != nil {
		t.Fatalf("RemoveSubdir empty: %v", err)
	}
	if exists, err := s.SubdirExists("aa", "foo"); err != nil {
		t.Fatalf("SubdirExists after remove: %v", err)
	} else if exists {
		t.Fatal("SubdirExists returned true after RemoveSubdir")
	}
	if err := s.RemovePrefix("aa"); err != nil {
		t.Fatalf("RemovePrefix after subdir removal: %v", err)
	}
}
