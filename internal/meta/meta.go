package meta

import (
	"fmt"
	"syscall"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	DefaultBlockSize        = 4096
	DefaultFileMode  uint32 = 0o644
	DefaultDirMode   uint32 = 0o755
)

// FileAttr stores the serializable metadata for a cached file or directory.
type FileAttr struct {
	Mode         uint32
	Uid          uint32
	Gid          uint32
	Atime        int64
	Mtime        int64
	Ctime        int64
	Length       uint64
	BlockIndices []uint64
	XAttrs       map[string]string
}

// Fix stack overflow according to
// https://web.archive.org/web/20210511104619/https://msgpack.uptrace.dev/faq/#fatal-error-stack-overflow
type rawFileAttr FileAttr

// IsDir reports whether the attr represents a directory entry.
func (a *FileAttr) IsDir() bool {
	if a == nil {
		return false
	}
	return a.Mode&syscall.S_IFMT == syscall.S_IFDIR
}

// MarshalBinary encodes attr into a MsgPack binary format.
func (a *FileAttr) MarshalBinary() ([]byte, error) {
	if a == nil {
		return nil, fmt.Errorf("meta: uninitialized FileAttr")
	}
	data, err := msgpack.Marshal((rawFileAttr)(*a))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// UnmarshalBinary decodes a MsgPack binary format.
func (a *FileAttr) UnmarshalBinary(data []byte) error {
	if a == nil {
		return fmt.Errorf("meta: uninitialized FileAttr")
	}
	return msgpack.Unmarshal(data, (*rawFileAttr)(a))
}

func (a *FileAttr) Equal(b *FileAttr) bool {
	if a.Mode != b.Mode {
		return false
	}
	if a.Uid != b.Uid {
		return false
	}
	if a.Gid != b.Gid {
		return false
	}
	if a.Atime != b.Atime {
		return false
	}
	if a.Mtime != b.Mtime {
		return false
	}
	if a.Ctime != b.Ctime {
		return false
	}
	if a.Length != b.Length {
		return false
	}
	if len(a.BlockIndices) != len(b.BlockIndices) {
		return false
	}
	for i, v := range a.BlockIndices {
		if v != b.BlockIndices[i] {
			return false
		}
	}
	if len(a.XAttrs) != len(b.XAttrs) {
		return false
	}
	for i, v := range a.XAttrs {
		if v != b.XAttrs[i] {
			return false
		}
	}
	return true
}
