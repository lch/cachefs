package meta

import (
	"syscall"
	"testing"
)

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	original := &FileAttr{
		Mode:         DefaultFileMode,
		Uid:          1000,
		Gid:          1001,
		Atime:        1712345678,
		Mtime:        1712345679,
		Ctime:        1712345680,
		Length:       123456789,
		BlockIndices: []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		XAttrs: map[string]string{
			"test": "test",
		},
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary error: %v", err)
	}

	got := &FileAttr{}
	err = got.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if !got.Equal(original) {
		t.Fatalf("round-trip mismatch:\n got: %#v\nwant: %#v", got, original)
	}
}

func TestMarshalUnmarshalDirectoryRoundTrip(t *testing.T) {
	original := &FileAttr{
		Mode: syscall.S_IFDIR | DefaultDirMode,
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary error: %v", err)
	}

	got := &FileAttr{}
	err = got.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if !got.Equal(original) {
		t.Fatalf("round-trip mismatch:\n got: %#v\nwant: %#v", got, original)
	}

	if !got.IsDir() {
		t.Fatal("IsDir returned false for directory attr")
	}
}
