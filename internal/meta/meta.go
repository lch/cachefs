package meta

import (
	"encoding/binary"
	"fmt"
)

const SerializedSize = 44

const (
	DefaultFileMode uint32 = 0o644
	DefaultDirMode  uint32 = 0o755
)

// FileAttr stores the serializable metadata for a cached file or directory.
type FileAttr struct {
	Size  uint64
	Mode  uint32
	Uid   uint32
	Gid   uint32
	Atime int64
	Mtime int64
	Ctime int64
}

// Marshal encodes attr into the fixed 44-byte little-endian layout.
func Marshal(attr *FileAttr) []byte {
	if attr == nil {
		return nil
	}

	buf := make([]byte, SerializedSize)
	binary.LittleEndian.PutUint64(buf[0:8], attr.Size)
	binary.LittleEndian.PutUint32(buf[8:12], attr.Mode)
	binary.LittleEndian.PutUint32(buf[12:16], attr.Uid)
	binary.LittleEndian.PutUint32(buf[16:20], attr.Gid)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(attr.Atime))
	binary.LittleEndian.PutUint64(buf[28:36], uint64(attr.Mtime))
	binary.LittleEndian.PutUint64(buf[36:44], uint64(attr.Ctime))
	return buf
}

// Unmarshal decodes a 44-byte little-endian FileAttr value.
func Unmarshal(data []byte) (*FileAttr, error) {
	if len(data) != SerializedSize {
		return nil, fmt.Errorf("meta: expected %d bytes, got %d", SerializedSize, len(data))
	}

	return &FileAttr{
		Size:  binary.LittleEndian.Uint64(data[0:8]),
		Mode:  binary.LittleEndian.Uint32(data[8:12]),
		Uid:   binary.LittleEndian.Uint32(data[12:16]),
		Gid:   binary.LittleEndian.Uint32(data[16:20]),
		Atime: int64(binary.LittleEndian.Uint64(data[20:28])),
		Mtime: int64(binary.LittleEndian.Uint64(data[28:36])),
		Ctime: int64(binary.LittleEndian.Uint64(data[36:44])),
	}, nil
}
