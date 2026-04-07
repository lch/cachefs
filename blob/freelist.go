package blob

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// FreeRegion describes a reusable range inside a blob file.
type FreeRegion struct {
	Offset uint64
	Length uint64
}

// FreeList tracks reusable ranges in a blob file.
type FreeList struct {
	regions []FreeRegion
}

// NewFreeList returns an empty free list.
func NewFreeList() *FreeList {
	return &FreeList{}
}

// Allocate finds the first region large enough for size bytes.
func (fl *FreeList) Allocate(size uint64) (offset uint64, ok bool) {
	if size == 0 {
		return 0, false
	}

	for i, region := range fl.regions {
		if region.Length < size {
			continue
		}

		offset = region.Offset
		if region.Length == size {
			copy(fl.regions[i:], fl.regions[i+1:])
			fl.regions = fl.regions[:len(fl.regions)-1]
			return offset, true
		}

		fl.regions[i].Offset += size
		fl.regions[i].Length -= size
		return offset, true
	}

	return 0, false
}

// Free marks a range as reusable and merges overlapping or adjacent regions.
func (fl *FreeList) Free(offset, length uint64) {
	if length == 0 {
		return
	}

	fl.regions = append(fl.regions, FreeRegion{Offset: offset, Length: length})
	sort.Slice(fl.regions, func(i, j int) bool {
		return fl.regions[i].Offset < fl.regions[j].Offset
	})

	normalized := fl.regions[:0]
	for _, region := range fl.regions {
		if region.Length == 0 {
			continue
		}

		if len(normalized) == 0 {
			normalized = append(normalized, region)
			continue
		}

		last := &normalized[len(normalized)-1]
		lastEnd := last.Offset + last.Length
		regionEnd := region.Offset + region.Length
		if lastEnd >= region.Offset {
			if regionEnd > lastEnd {
				last.Length = regionEnd - last.Offset
			}
			continue
		}

		normalized = append(normalized, region)
	}

	fl.regions = normalized
}

// Marshal encodes the free list as [N uint32][Offset0 uint64][Length0 uint64]...
func (fl *FreeList) Marshal() []byte {
	buf := make([]byte, 4+len(fl.regions)*16)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(fl.regions)))
	for i, region := range fl.regions {
		base := 4 + i*16
		binary.LittleEndian.PutUint64(buf[base:base+8], region.Offset)
		binary.LittleEndian.PutUint64(buf[base+8:base+16], region.Length)
	}
	return buf
}

// Unmarshal decodes a serialized free list.
func (fl *FreeList) Unmarshal(data []byte) error {
	if len(data) == 0 {
		fl.regions = nil
		return nil
	}
	if len(data) < 4 {
		return fmt.Errorf("blob: free list data too short: %d", len(data))
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	expected := 4 + int(count)*16
	if len(data) != expected {
		return fmt.Errorf("blob: free list size mismatch: got %d, want %d", len(data), expected)
	}

	regions := make([]FreeRegion, 0, count)
	var prevEnd uint64
	for i := range count {
		base := 4 + int(i)*16
		offset := binary.LittleEndian.Uint64(data[base : base+8])
		length := binary.LittleEndian.Uint64(data[base+8 : base+16])
		if length == 0 {
			return fmt.Errorf("blob: free list region %d has zero length", i)
		}
		if offset < prevEnd {
			return fmt.Errorf("blob: free list regions overlap or are out of order")
		}
		end := offset + length
		if end < offset {
			return fmt.Errorf("blob: free list region %d overflow", i)
		}
		if i > 0 && prevEnd >= offset {
			return fmt.Errorf("blob: free list regions overlap or are adjacent")
		}
		regions = append(regions, FreeRegion{Offset: offset, Length: length})
		prevEnd = end
	}

	fl.regions = regions
	return nil
}

// TotalFree returns the total number of free bytes tracked by the list.
func (fl *FreeList) TotalFree() uint64 {
	var total uint64
	for _, region := range fl.regions {
		total += region.Length
	}
	return total
}
