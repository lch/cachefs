package meta

import (
	"encoding/binary"
	"fmt"
	"syscall"
)

const SerializedSize = 52

const (
	DefaultFileMode uint32 = 0o644
	DefaultDirMode  uint32 = 0o755
)

// FileAttr stores the serializable metadata for a cached file or directory.
type FileAttr struct {
	Offset uint64
	Length uint64
	Mode   uint32
	Uid    uint32
	Gid    uint32
	Atime  int64
	Mtime  int64
	Ctime  int64
}

// IsDir reports whether the attr represents a directory entry.
func (a *FileAttr) IsDir() bool {
	if a == nil {
		return false
	}
	return a.Mode&syscall.S_IFMT == syscall.S_IFDIR
}

// Marshal encodes attr into the fixed 52-byte little-endian layout.
func Marshal(attr *FileAttr) []byte {
	if attr == nil {
		return nil
	}

	buf := make([]byte, SerializedSize)
	binary.LittleEndian.PutUint64(buf[0:8], attr.Offset)
	binary.LittleEndian.PutUint64(buf[8:16], attr.Length)
	binary.LittleEndian.PutUint32(buf[16:20], attr.Mode)
	binary.LittleEndian.PutUint32(buf[20:24], attr.Uid)
	binary.LittleEndian.PutUint32(buf[24:28], attr.Gid)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(attr.Atime))
	binary.LittleEndian.PutUint64(buf[36:44], uint64(attr.Mtime))
	binary.LittleEndian.PutUint64(buf[44:52], uint64(attr.Ctime))
	return buf
}

// Unmarshal decodes a 52-byte little-endian FileAttr value.
func Unmarshal(data []byte) (*FileAttr, error) {
	if len(data) != SerializedSize {
		return nil, fmt.Errorf("meta: expected %d bytes, got %d", SerializedSize, len(data))
	}

	return &FileAttr{
		Offset: binary.LittleEndian.Uint64(data[0:8]),
		Length: binary.LittleEndian.Uint64(data[8:16]),
		Mode:   binary.LittleEndian.Uint32(data[16:20]),
		Uid:    binary.LittleEndian.Uint32(data[20:24]),
		Gid:    binary.LittleEndian.Uint32(data[24:28]),
		Atime:  int64(binary.LittleEndian.Uint64(data[28:36])),
		Mtime:  int64(binary.LittleEndian.Uint64(data[36:44])),
		Ctime:  int64(binary.LittleEndian.Uint64(data[44:52])),
	}, nil
}
