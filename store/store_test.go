package store

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/lch/cachefs/internal/meta"
	"go.etcd.io/bbolt"
)

func TestContentCRUD(t *testing.T) {
	s := newTestStore(t)

	data := []byte("hello world")
	if err := s.PutContent("aa", "alpha", data); err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	got, err := s.GetContent("aa", "alpha")
	if err != nil {
		t.Fatalf("GetContent: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("GetContent = %q, want %q", got, data)
	}

	got[0] = 'H'
	again, err := s.GetContent("aa", "alpha")
	if err != nil {
		t.Fatalf("GetContent after mutation: %v", err)
	}
	if string(again) != string(data) {
		t.Fatalf("stored data changed after caller mutation: %q", again)
	}

	if err := s.DeleteContent("aa", "alpha"); err != nil {
		t.Fatalf("DeleteContent: %v", err)
	}
	if _, err := s.GetContent("aa", "alpha"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GetContent after delete error = %v, want os.ErrNotExist", err)
	}
}

func TestMetaCRUD(t *testing.T) {
	s := newTestStore(t)

	attr := &meta.FileAttr{
		Size:  42,
		Mode:  meta.DefaultFileMode,
		Uid:   1000,
		Gid:   1001,
		Atime: 11,
		Mtime: 12,
		Ctime: 13,
	}
	if err := s.PutMeta("aa", "alpha", attr); err != nil {
		t.Fatalf("PutMeta: %v", err)
	}

	got, err := s.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !reflect.DeepEqual(got, attr) {
		t.Fatalf("GetMeta = %#v, want %#v", got, attr)
	}

	if err := s.DeleteMeta("aa", "alpha"); err != nil {
		t.Fatalf("DeleteMeta: %v", err)
	}
	if _, err := s.GetMeta("aa", "alpha"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GetMeta after delete error = %v, want os.ErrNotExist", err)
	}
}

func TestPutFileAndDeleteFileAtomicity(t *testing.T) {
	s := newTestStore(t)
	attr := &meta.FileAttr{Size: 1, Mode: meta.DefaultFileMode}

	afterPutFileContentHook = func() error { return errors.New("boom") }
	t.Cleanup(func() { afterPutFileContentHook = nil })
	if err := s.PutFile("aa", "alpha", []byte("x"), attr); err == nil {
		t.Fatal("PutFile succeeded despite injected failure")
	}
	if _, err := s.GetContent("aa", "alpha"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("content persisted after failed PutFile: %v", err)
	}
	if _, err := s.GetMeta("aa", "alpha"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("meta persisted after failed PutFile: %v", err)
	}

	afterPutFileContentHook = nil
	if err := s.PutFile("aa", "alpha", []byte("x"), attr); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	afterDeleteFileContentHook = func() error { return errors.New("boom") }
	t.Cleanup(func() { afterDeleteFileContentHook = nil })
	if err := s.DeleteFile("aa", "alpha"); err == nil {
		t.Fatal("DeleteFile succeeded despite injected failure")
	}
	if _, err := s.GetContent("aa", "alpha"); err != nil {
		t.Fatalf("content missing after failed DeleteFile: %v", err)
	}
	if _, err := s.GetMeta("aa", "alpha"); err != nil {
		t.Fatalf("meta missing after failed DeleteFile: %v", err)
	}
}

func TestListPrefixesAndFiles(t *testing.T) {
	s := newTestStore(t)

	if got, err := s.ListPrefixes(); err != nil || len(got) != 0 {
		t.Fatalf("ListPrefixes on empty store = %v, %v", got, err)
	}

	for _, tc := range []struct {
		prefix string
		name   string
	}{
		{"ff", "gamma"},
		{"aa", "beta"},
		{"aa", "alpha"},
		{"bb", "delta"},
	} {
		if err := s.PutFile(tc.prefix, tc.name, []byte(tc.name), &meta.FileAttr{Size: uint64(len(tc.name)), Mode: meta.DefaultFileMode}); err != nil {
			t.Fatalf("PutFile(%s,%s): %v", tc.prefix, tc.name, err)
		}
	}

	prefixes, err := s.ListPrefixes()
	if err != nil {
		t.Fatalf("ListPrefixes: %v", err)
	}
	if !reflect.DeepEqual(prefixes, []string{"aa", "bb", "ff"}) {
		t.Fatalf("ListPrefixes = %v, want [aa bb ff]", prefixes)
	}

	files, err := s.ListFiles("aa")
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if !reflect.DeepEqual(files, []string{"alpha", "beta"}) {
		t.Fatalf("ListFiles = %v, want [alpha beta]", files)
	}

	if err := s.DeleteFile("aa", "alpha"); err != nil {
		t.Fatalf("DeleteFile alpha: %v", err)
	}
	if err := s.DeleteFile("aa", "beta"); err != nil {
		t.Fatalf("DeleteFile beta: %v", err)
	}

	prefixes, err = s.ListPrefixes()
	if err != nil {
		t.Fatalf("ListPrefixes after deletes: %v", err)
	}
	if !reflect.DeepEqual(prefixes, []string{"bb", "ff"}) {
		t.Fatalf("ListPrefixes after deletes = %v, want [bb ff]", prefixes)
	}
}

func TestPrefixCRUD(t *testing.T) {
	s := newTestStore(t)

	if err := s.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	prefixes, err := s.ListPrefixes()
	if err != nil {
		t.Fatalf("ListPrefixes: %v", err)
	}
	if !reflect.DeepEqual(prefixes, []string{"aa"}) {
		t.Fatalf("ListPrefixes = %v, want [aa]", prefixes)
	}

	if err := s.CreatePrefix("aa"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("CreatePrefix duplicate error = %v, want os.ErrExist", err)
	}

	if err := s.PutContent("aa", "alpha", []byte("x")); err != nil {
		t.Fatalf("PutContent: %v", err)
	}
	if err := s.DeletePrefix("aa"); !errors.Is(err, ErrPrefixNotEmpty) {
		t.Fatalf("DeletePrefix non-empty error = %v, want ErrPrefixNotEmpty", err)
	}
	if err := s.DeleteContent("aa", "alpha"); err != nil {
		t.Fatalf("DeleteContent: %v", err)
	}
	if err := s.DeletePrefix("aa"); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}

	prefixes, err = s.ListPrefixes()
	if err != nil {
		t.Fatalf("ListPrefixes after delete: %v", err)
	}
	if len(prefixes) != 0 {
		t.Fatalf("ListPrefixes after delete = %v, want empty", prefixes)
	}

	if err := s.DeletePrefix("aa"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DeletePrefix missing error = %v, want os.ErrNotExist", err)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := newTestStore(t)

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			prefix := "aa"
			name := filepath.Base(filepath.Join("file", string(rune('a'+i))))
			payload := []byte(name)
			attr := &meta.FileAttr{Size: uint64(len(payload)), Mode: meta.DefaultFileMode}
			if err := s.PutFile(prefix, name, payload, attr); err != nil {
				errCh <- err
				return
			}
			got, err := s.GetContent(prefix, name)
			if err != nil {
				errCh <- err
				return
			}
			if string(got) != string(payload) {
				errCh <- errors.New("content mismatch")
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent operation failed: %v", err)
		}
	}

	files, err := s.ListFiles("aa")
	if err != nil {
		t.Fatalf("ListFiles after concurrency test: %v", err)
	}
	if len(files) != workers {
		t.Fatalf("ListFiles returned %d files, want %d", len(files), workers)
	}
}

func TestEdgeCases(t *testing.T) {
	s := newTestStore(t)

	if _, err := s.GetContent("aa", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GetContent missing error = %v, want os.ErrNotExist", err)
	}
	if err := s.DeleteContent("aa", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DeleteContent missing error = %v, want os.ErrNotExist", err)
	}
	if _, err := s.GetMeta("aa", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GetMeta missing error = %v, want os.ErrNotExist", err)
	}
	if err := s.DeleteMeta("aa", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DeleteMeta missing error = %v, want os.ErrNotExist", err)
	}
	if _, err := s.ListFiles("aa"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ListFiles missing prefix error = %v, want os.ErrNotExist", err)
	}
}

func newTestStore(t *testing.T) Store {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open bbolt db: %v", err)
	}
	s := New(db)
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}
