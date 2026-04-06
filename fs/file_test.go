package fs

import (
	"context"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/internal/meta"
)

func TestFileOpenAndGetattr(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}

	attr := &meta.FileAttr{
		Size:  5,
		Mode:  0o640,
		Uid:   123,
		Gid:   456,
		Atime: 111,
		Mtime: 222,
		Ctime: 333,
	}
	if err := st.PutFile("aa", "alpha", []byte("hello"), attr); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	node := &FileNode{cfs: root.cfs, prefix: "aa", filename: "alpha"}
	fh, flags, errno := node.Open(context.Background(), 0)
	if errno != 0 || flags != fuse.FOPEN_KEEP_CACHE {
		t.Fatalf("Open = (%v, %v, %v), want handle, KEEP_CACHE, OK", fh, flags, errno)
	}
	handle, ok := fh.(*CacheFileHandle)
	if !ok {
		t.Fatalf("Open handle type = %T, want *CacheFileHandle", fh)
	}
	if string(handle.buf) != "hello" || handle.dirty {
		t.Fatalf("Open handle = %#v, want buf=hello dirty=false", handle)
	}

	var out fuse.AttrOut
	if errno := node.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("Getattr errno = %v", errno)
	}
	if out.Mode&syscall.S_IFREG == 0 || out.Mode&0o777 != 0o640 {
		t.Fatalf("Getattr mode = %#o, want regular 0640", out.Mode)
	}
	if out.Size != 5 || out.Uid != 123 || out.Gid != 456 {
		t.Fatalf("Getattr attr = %#v, want size=5 uid=123 gid=456", out)
	}
	if out.Atime == 0 || out.Mtime == 0 || out.Ctime == 0 {
		t.Fatalf("Getattr times unexpectedly zero: %#v", out)
	}
}

func TestFileSetattrResizeAndMetadata(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	if err := st.PutFile("aa", "alpha", []byte("hello"), &meta.FileAttr{
		Size:  5,
		Mode:  0o640,
		Uid:   123,
		Gid:   456,
		Atime: 111,
		Mtime: 222,
		Ctime: 333,
	}); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	node := &FileNode{cfs: root.cfs, prefix: "aa", filename: "alpha"}

	var out fuse.AttrOut
	shrink := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{Valid: fuse.FATTR_SIZE, Size: 2}}
	if errno := node.Setattr(context.Background(), nil, shrink, &out); errno != 0 {
		t.Fatalf("Setattr shrink errno = %v", errno)
	}
	got, err := st.GetContent("aa", "alpha")
	if err != nil {
		t.Fatalf("GetContent after shrink: %v", err)
	}
	if string(got) != "he" {
		t.Fatalf("content after shrink = %q, want %q", got, "he")
	}
	metaAfterShrink, err := st.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta after shrink: %v", err)
	}
	if metaAfterShrink.Size != 2 {
		t.Fatalf("size after shrink = %d, want 2", metaAfterShrink.Size)
	}

	grow := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{Valid: fuse.FATTR_SIZE, Size: 6}}
	if errno := node.Setattr(context.Background(), nil, grow, &out); errno != 0 {
		t.Fatalf("Setattr grow errno = %v", errno)
	}
	got, err = st.GetContent("aa", "alpha")
	if err != nil {
		t.Fatalf("GetContent after grow: %v", err)
	}
	if len(got) != 6 || string(got[:2]) != "he" || !reflect.DeepEqual(got[2:], []byte{0, 0, 0, 0}) {
		t.Fatalf("content after grow = %v, want [104 101 0 0 0 0]", got)
	}
	metaAfterGrow, err := st.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta after grow: %v", err)
	}
	if metaAfterGrow.Size != 6 {
		t.Fatalf("size after grow = %d, want 6", metaAfterGrow.Size)
	}
	if out.Size != 6 {
		t.Fatalf("Setattr out size = %d, want 6", out.Size)
	}

	update := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
		Valid:     fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_ATIME | fuse.FATTR_MTIME | fuse.FATTR_CTIME,
		Mode:      0o600,
		Owner:     fuse.Owner{Uid: 777, Gid: 888},
		Atime:     444,
		Mtime:     555,
		Ctime:     666,
		Atimensec: 0,
		Mtimensec: 0,
		Ctimensec: 0,
	}}
	if errno := node.Setattr(context.Background(), nil, update, &out); errno != 0 {
		t.Fatalf("Setattr metadata errno = %v", errno)
	}
	metaAfterUpdate, err := st.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta after metadata update: %v", err)
	}
	if metaAfterUpdate.Mode != 0o600 || metaAfterUpdate.Uid != 777 || metaAfterUpdate.Gid != 888 {
		t.Fatalf("metadata after update = %#v, want mode 0600 uid 777 gid 888", metaAfterUpdate)
	}
	if metaAfterUpdate.Atime != 444 || metaAfterUpdate.Mtime != 555 || metaAfterUpdate.Ctime != 666 {
		t.Fatalf("times after update = %#v, want 444/555/666", metaAfterUpdate)
	}
	if out.Mode&0o777 != 0o600 || out.Uid != 777 || out.Gid != 888 {
		t.Fatalf("AttrOut after update = %#v, want mode 0600 uid 777 gid 888", out)
	}
	if time.Unix(metaAfterUpdate.Atime, 0).IsZero() {
		t.Fatal("expected non-zero atime")
	}
}

func TestCacheFileHandleReadWriteFlushRelease(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	if err := st.PutFile("aa", "alpha", []byte("hello"), &meta.FileAttr{
		Size:  5,
		Mode:  0o644,
		Uid:   123,
		Gid:   456,
		Atime: 111,
		Mtime: 222,
		Ctime: 333,
	}); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	node := &FileNode{cfs: root.cfs, prefix: "aa", filename: "alpha"}
	fh, _, errno := node.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open errno = %v", errno)
	}
	handle, ok := fh.(*CacheFileHandle)
	if !ok {
		t.Fatalf("Open handle type = %T, want *CacheFileHandle", fh)
	}

	rr, errno := handle.Read(context.Background(), make([]byte, 3), 1)
	if errno != 0 {
		t.Fatalf("Read errno = %v", errno)
	}
	if got := readResultBytes(t, rr); string(got) != "ell" {
		t.Fatalf("Read = %q, want %q", got, "ell")
	}
	metaAfterRead, err := st.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta after read: %v", err)
	}
	if metaAfterRead.Atime <= 111 {
		t.Fatalf("atime after read = %d, want greater than 111", metaAfterRead.Atime)
	}

	if n, errno := handle.Write(context.Background(), []byte("Z"), 7); errno != 0 || n != 1 {
		t.Fatalf("Write = (%d, %v), want (1, OK)", n, errno)
	}
	want := []byte{'h', 'e', 'l', 'l', 'o', 0, 0, 'Z'}
	if got, err := st.GetContent("aa", "alpha"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("store content after write = %v, %v; want %v", got, err, want)
	}
	if handle.dirty {
		t.Fatal("handle should be clean after write-through write")
	}

	var out fuse.AttrOut
	if errno := handle.Getattr(context.Background(), &out); errno != 0 {
		t.Fatalf("Handle getattr errno = %v", errno)
	}
	if out.Size != uint64(len(want)) {
		t.Fatalf("Handle getattr size = %d, want %d", out.Size, len(want))
	}

	rr, errno = handle.Read(context.Background(), make([]byte, 8), 0)
	if errno != 0 {
		t.Fatalf("Read after write errno = %v", errno)
	}
	if got := readResultBytes(t, rr); !reflect.DeepEqual(got, want) {
		t.Fatalf("Read after write = %v, want %v", got, want)
	}

	if errno := handle.Flush(context.Background()); errno != 0 {
		t.Fatalf("Flush errno = %v", errno)
	}
	if got, err := st.GetContent("aa", "alpha"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("store content after flush = %v, %v; want %v", got, err, want)
	}
	metaAfterFlush, err := st.GetMeta("aa", "alpha")
	if err != nil {
		t.Fatalf("GetMeta after flush: %v", err)
	}
	if metaAfterFlush.Size != uint64(len(want)) {
		t.Fatalf("meta size after flush = %d, want %d", metaAfterFlush.Size, len(want))
	}

	if n, errno := handle.Write(context.Background(), []byte("Q"), 0); errno != 0 || n != 1 {
		t.Fatalf("second Write = (%d, %v), want (1, OK)", n, errno)
	}
	want[0] = 'Q'
	if got, err := st.GetContent("aa", "alpha"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("store content after second write = %v, %v; want %v", got, err, want)
	}
	if errno := handle.Release(context.Background()); errno != 0 {
		t.Fatalf("Release errno = %v", errno)
	}

	reopened, _, errno := node.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("re-open errno = %v", errno)
	}
	newHandle := reopened.(*CacheFileHandle)
	rr, errno = newHandle.Read(context.Background(), make([]byte, 8), 0)
	if errno != 0 {
		t.Fatalf("re-open read errno = %v", errno)
	}
	if got := readResultBytes(t, rr); got[0] != 'Q' {
		t.Fatalf("re-open read = %v, want first byte Q", got)
	}
}

func TestCacheFileHandleReadAfterUnlink(t *testing.T) {
	root, st := newTestRoot(t)
	if err := st.CreatePrefix("aa"); err != nil {
		t.Fatalf("CreatePrefix: %v", err)
	}
	if err := st.PutFile("aa", "alpha", []byte("hello"), &meta.FileAttr{
		Size:  5,
		Mode:  0o644,
		Uid:   123,
		Gid:   456,
		Atime: 111,
		Mtime: 222,
		Ctime: 333,
	}); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	node := &FileNode{cfs: root.cfs, prefix: "aa", filename: "alpha"}
	fh, _, errno := node.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open errno = %v", errno)
	}
	handle := fh.(*CacheFileHandle)

	if err := st.DeleteFile("aa", "alpha"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	var out fuse.AttrOut
	if errno := handle.Getattr(context.Background(), &out); errno != 0 {
		t.Fatalf("Handle getattr after unlink errno = %v", errno)
	}
	if out.Size != 5 {
		t.Fatalf("Handle getattr after unlink size = %d, want 5", out.Size)
	}

	rr, errno := handle.Read(context.Background(), make([]byte, 5), 0)
	if errno != 0 {
		t.Fatalf("Read after unlink errno = %v", errno)
	}
	if got := readResultBytes(t, rr); string(got) != "hello" {
		t.Fatalf("Read after unlink = %q, want hello", got)
	}

	if n, errno := handle.Write(context.Background(), []byte("!"), 5); errno != syscall.ENOENT || n != 0 {
		t.Fatalf("Write after unlink = (%d, %v), want (0, ENOENT)", n, errno)
	}
}

func readResultBytes(t *testing.T, rr fuse.ReadResult) []byte {
	t.Helper()
	buf := make([]byte, rr.Size())
	got, status := rr.Bytes(buf)
	if status != fuse.OK {
		t.Fatalf("ReadResult.Bytes status = %v, want OK", status)
	}
	return append([]byte(nil), got...)
}
