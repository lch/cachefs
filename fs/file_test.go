package fs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func readResultBytes(t *testing.T, rr fuse.ReadResult) []byte {
	t.Helper()
	buf := make([]byte, rr.Size())
	got, status := rr.Bytes(buf)
	if status != fuse.OK {
		t.Fatalf("ReadResult.Bytes status = %v, want OK", status)
	}
	return append([]byte(nil), got...)
}
