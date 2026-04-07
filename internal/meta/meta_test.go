package meta

import "testing"

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	original := &FileAttr{
		Offset: 1234,
		Length: 123456789,
		Mode:   DefaultFileMode,
		Uid:    1000,
		Gid:    1001,
		Atime:  1712345678,
		Mtime:  1712345679,
		Ctime:  1712345680,
	}

	data := Marshal(original)
	if len(data) != SerializedSize {
		t.Fatalf("Marshal length = %d, want %d", len(data), SerializedSize)
	}

	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if *got != *original {
		t.Fatalf("round-trip mismatch:\n got: %#v\nwant: %#v", got, original)
	}
}

func TestUnmarshalTruncatedInput(t *testing.T) {
	data := make([]byte, SerializedSize-1)
	if _, err := Unmarshal(data); err == nil {
		t.Fatal("Unmarshal succeeded for truncated input")
	}
}
