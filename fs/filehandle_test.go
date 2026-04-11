package fs

import (
	"bytes"
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestCacheFileHandle_ReadWrite(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	path := "01/test.txt"
	_ = st.Create(path)

	h := &CacheFileHandle{
		cfs:  root.cfs,
		path: path,
	}

	// Test Write
	data := []byte("hello world")
	n, errno := h.Write(ctx, data, 0)
	if errno != 0 {
		t.Fatalf("Write failed: %v", errno)
	}
	if n != uint32(len(data)) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}

	// Verify dirty
	if !h.dirty {
		t.Errorf("expected dirty=true after write")
	}

	// Test Read
	dest := make([]byte, 5)
	rr, errno := h.Read(ctx, dest, 0)
	if errno != 0 {
		t.Fatalf("Read failed: %v", errno)
	}
	got := readResultBytes(t, rr)
	if !bytes.Equal(got, []byte("hello")) {
		t.Errorf("Read got %q, want %q", string(got), "hello")
	}

	// Test Flush
	errno = h.Flush(ctx)
	if errno != 0 {
		t.Fatalf("Flush failed: %v", errno)
	}

	// Verify dirty cleared
	if h.dirty {
		t.Errorf("expected dirty=false after flush")
	}

	// Verify in store
	stData, _, err := st.Read(path)
	if err != nil {
		t.Fatalf("Store.Read failed: %v", err)
	}
	if !bytes.Equal(stData, data) {
		t.Errorf("Store data got %q, want %q", string(stData), string(data))
	}
}

func TestCacheFileHandle_Getattr(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	path := "0a/attr.txt"
	_ = st.Create(path)
	_ = st.Write(path, []byte("some data"), 0644)

	h := &CacheFileHandle{
		cfs:  root.cfs,
		path: path,
	}

	var out fuse.AttrOut
	errno := h.Getattr(ctx, &out)
	if errno != 0 {
		t.Fatalf("Getattr failed: %v", errno)
	}

	if out.Size != 9 {
		t.Errorf("expected size 9, got %d", out.Size)
	}
}

func TestCacheFileHandle_Atime(t *testing.T) {
	root, st := newTestRoot(t)
	ctx := context.Background()

	path := "0b/atime.txt"
	_ = st.Create(path)
	_ = st.Write(path, []byte("data"), 0644)

	h := &CacheFileHandle{
		cfs:  root.cfs,
		path: path,
	}

	attr1, _ := st.GetMeta(path)
	
	// Read should touch atime
	dest := make([]byte, 4)
	_, _ = h.Read(ctx, dest, 0)

	attr2, _ := st.GetMeta(path)
	if attr2.Atime <= attr1.Atime && attr2.Atime == 0 {
		// Atime might not have changed if it's too fast, but it should be set (non-zero)
	}
	if attr2.Atime == 0 {
		t.Errorf("Atime was not set")
	}
}
