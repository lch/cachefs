package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
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
	Blocks       uint64
	BlockIndices []uint64
}

// IsDir reports whether the attr represents a directory entry.
func (a *FileAttr) IsDir() bool {
	if a == nil {
		return false
	}
	return a.Mode&syscall.S_IFMT == syscall.S_IFDIR
}

// MarshalBinary encodes attr into a little-endian layout.
func (a *FileAttr) MarshalBinary() (data []byte, err error) {
	if a == nil {
		return nil, fmt.Errorf("meta: uninitialized FileAttr")
	}
	data = binary.LittleEndian.AppendUint32(data, a.Mode)
	data = binary.LittleEndian.AppendUint32(data, a.Uid)
	data = binary.LittleEndian.AppendUint32(data, a.Gid)
	data = binary.LittleEndian.AppendUint64(data, uint64(a.Atime))
	data = binary.LittleEndian.AppendUint64(data, uint64(a.Mtime))
	data = binary.LittleEndian.AppendUint64(data, uint64(a.Ctime))
	data = binary.LittleEndian.AppendUint64(data, a.Length)
	data = binary.LittleEndian.AppendUint64(data, a.Blocks)
	for _, v := range a.BlockIndices {
		data = binary.LittleEndian.AppendUint64(data, v)
	}
	return
}

// UnmarshalBinary decodes a little-endian FileAttr value.
func (a *FileAttr) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.LittleEndian, &a.Mode); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &a.Uid); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &a.Gid); err != nil {
		return err
	}
	var atime uint64
	if err := binary.Read(buf, binary.LittleEndian, &atime); err != nil {
		return err
	}
	a.Atime = int64(atime)
	var mtime uint64
	if err := binary.Read(buf, binary.LittleEndian, &mtime); err != nil {
		return err
	}
	a.Mtime = int64(mtime)
	var ctime uint64
	if err := binary.Read(buf, binary.LittleEndian, &ctime); err != nil {
		return err
	}
	a.Ctime = int64(ctime)
	if err := binary.Read(buf, binary.LittleEndian, &a.Length); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &a.Blocks); err != nil {
		return err
	}
	var blockIndices []uint64
	for range a.Blocks {
		var blockIndex uint64
		if err := binary.Read(buf, binary.LittleEndian, &blockIndex); err != nil {
			return err
		}
		blockIndices = append(blockIndices, blockIndex)
	}
	a.BlockIndices = blockIndices
	return nil
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
	if a.Blocks != b.Blocks {
		return false
	}
	for i := range a.Blocks {
		if a.BlockIndices[i] != b.BlockIndices[i] {
			return false
		}
	}
	return true
}
