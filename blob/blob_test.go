package blob

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAppendAndReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mgr := NewBlobManager(dir)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	first := []byte("hello")
	second := []byte(" world")

	off1, err := mgr.Append("aa", first)
	if err != nil {
		t.Fatalf("Append first: %v", err)
	}
	off2, err := mgr.Append("aa", second)
	if err != nil {
		t.Fatalf("Append second: %v", err)
	}
	if off1 != 0 {
		t.Fatalf("first offset = %d, want 0", off1)
	}
	if off2 != uint64(len(first)) {
		t.Fatalf("second offset = %d, want %d", off2, len(first))
	}

	got1, err := mgr.Read("aa", off1, uint64(len(first)))
	if err != nil {
		t.Fatalf("Read first: %v", err)
	}
	if !bytes.Equal(got1, first) {
		t.Fatalf("Read first = %q, want %q", got1, first)
	}

	got2, err := mgr.Read("aa", off2, uint64(len(second)))
	if err != nil {
		t.Fatalf("Read second: %v", err)
	}
	if !bytes.Equal(got2, second) {
		t.Fatalf("Read second = %q, want %q", got2, second)
	}
}

func TestWriteAtSpecificOffset(t *testing.T) {
	dir := t.TempDir()
	mgr := NewBlobManager(dir)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	if err := mgr.Write("aa", []byte("abc"), 5); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := mgr.Read("aa", 5, 3)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, []byte("abc")) {
		t.Fatalf("Read = %q, want %q", got, []byte("abc"))
	}
}

func TestReadCreatesBlobFile(t *testing.T) {
	dir := t.TempDir()
	mgr := NewBlobManager(dir)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	if _, err := mgr.Read("aa", 0, 0); err != nil {
		t.Fatalf("Read: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "aa")); err != nil {
		t.Fatalf("blob file missing after Read: %v", err)
	}
}

func TestRemoveBlobDeletesFile(t *testing.T) {
	dir := t.TempDir()
	mgr := NewBlobManager(dir)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	if err := mgr.Write("aa", []byte("abc"), 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := mgr.RemoveBlob("aa"); err != nil {
		t.Fatalf("RemoveBlob: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "aa")); !os.IsNotExist(err) {
		t.Fatalf("blob file still exists after RemoveBlob: %v", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	mgr := NewBlobManager(dir)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	payload := bytes.Repeat([]byte("x"), 1024)
	if err := mgr.Write("aa", payload, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}

	const readers = 32
	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				got, err := mgr.Read("aa", 0, uint64(len(payload)))
				if err != nil {
					errCh <- err
					return
				}
				if !bytes.Equal(got, payload) {
					errCh <- os.ErrInvalid
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent read failed: %v", err)
		}
	}
}

func TestConcurrentAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	mgr := NewBlobManager(dir)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	if err := mgr.Write("aa", []byte("seed"), 0); err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	const iterations = 100
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if _, err := mgr.Append("aa", []byte("x")); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			got, err := mgr.Read("aa", 0, 4)
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(got, []byte("seed")) {
				errCh <- os.ErrInvalid
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent append/read failed: %v", err)
		}
	}
}
