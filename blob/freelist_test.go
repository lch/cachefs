package blob

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestAllocateEmptyList(t *testing.T) {
	fl := NewFreeList()
	if _, ok := fl.Allocate(1); ok {
		t.Fatal("Allocate succeeded on empty free list")
	}
}

func TestFreeAndAllocateReusesOffset(t *testing.T) {
	fl := NewFreeList()
	fl.Free(100, 20)

	offset, ok := fl.Allocate(20)
	if !ok {
		t.Fatal("Allocate failed after Free")
	}
	if offset != 100 {
		t.Fatalf("Allocate offset = %d, want 100", offset)
	}
	if got := fl.TotalFree(); got != 0 {
		t.Fatalf("TotalFree = %d, want 0", got)
	}
}

func TestFreeMergesAdjacentRegions(t *testing.T) {
	fl := NewFreeList()
	fl.Free(0, 10)
	fl.Free(10, 5)

	want := []FreeRegion{{Offset: 0, Length: 15}}
	if !reflect.DeepEqual(fl.regions, want) {
		t.Fatalf("regions = %#v, want %#v", fl.regions, want)
	}
}

func TestFreeBridgingGapMergesAll(t *testing.T) {
	fl := NewFreeList()
	fl.Free(0, 10)
	fl.Free(20, 10)
	fl.Free(10, 10)

	want := []FreeRegion{{Offset: 0, Length: 30}}
	if !reflect.DeepEqual(fl.regions, want) {
		t.Fatalf("regions = %#v, want %#v", fl.regions, want)
	}
}

func TestAllocateShrinksRegion(t *testing.T) {
	fl := NewFreeList()
	fl.Free(50, 30)

	offset, ok := fl.Allocate(10)
	if !ok {
		t.Fatal("Allocate failed")
	}
	if offset != 50 {
		t.Fatalf("Allocate offset = %d, want 50", offset)
	}

	want := []FreeRegion{{Offset: 60, Length: 20}}
	if !reflect.DeepEqual(fl.regions, want) {
		t.Fatalf("regions = %#v, want %#v", fl.regions, want)
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	original := NewFreeList()
	original.Free(0, 10)
	original.Free(100, 20)
	original.Free(200, 30)

	data := original.Marshal()
	var got FreeList
	if err := got.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !reflect.DeepEqual(got.regions, original.regions) {
		t.Fatalf("regions = %#v, want %#v", got.regions, original.regions)
	}
	if got.TotalFree() != original.TotalFree() {
		t.Fatalf("TotalFree = %d, want %d", got.TotalFree(), original.TotalFree())
	}
}

func TestUnmarshalEmptyData(t *testing.T) {
	var fl FreeList
	if err := fl.Unmarshal(nil); err != nil {
		t.Fatalf("Unmarshal(nil): %v", err)
	}
	if len(fl.regions) != 0 {
		t.Fatalf("Unmarshal(nil) regions = %#v, want empty", fl.regions)
	}

	if err := fl.Unmarshal([]byte{}); err != nil {
		t.Fatalf("Unmarshal(empty): %v", err)
	}
	if len(fl.regions) != 0 {
		t.Fatalf("Unmarshal(empty) regions = %#v, want empty", fl.regions)
	}
}

func TestUnmarshalMalformedData(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 1)
	var fl FreeList
	if err := fl.Unmarshal(data); err == nil {
		t.Fatal("Unmarshal succeeded for malformed input")
	}
}
