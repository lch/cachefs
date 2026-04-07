package meta

import "testing"

func TestSubdirMarkerKey(t *testing.T) {
	if got, want := SubdirMarkerKey("abc"), "abc/"; got != want {
		t.Fatalf("SubdirMarkerKey() = %q, want %q", got, want)
	}
}

func TestSubdirFileKey(t *testing.T) {
	if got, want := SubdirFileKey("abc", "f.txt"), "abc/f.txt"; got != want {
		t.Fatalf("SubdirFileKey() = %q, want %q", got, want)
	}
}

func TestParseKey(t *testing.T) {
	tests := []struct {
		name            string
		key             string
		wantDirname     string
		wantBasename    string
		wantSubdirEntry bool
	}{
		{name: "direct file", key: "filename", wantDirname: "", wantBasename: "filename", wantSubdirEntry: false},
		{name: "marker", key: "dir/", wantDirname: "dir", wantBasename: "", wantSubdirEntry: true},
		{name: "subdir file", key: "dir/file.txt", wantDirname: "dir", wantBasename: "file.txt", wantSubdirEntry: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDirname, gotBasename, gotSubdirEntry := ParseKey(tt.key)
			if gotDirname != tt.wantDirname || gotBasename != tt.wantBasename || gotSubdirEntry != tt.wantSubdirEntry {
				t.Fatalf("ParseKey(%q) = (%q, %q, %v), want (%q, %q, %v)", tt.key, gotDirname, gotBasename, gotSubdirEntry, tt.wantDirname, tt.wantBasename, tt.wantSubdirEntry)
			}
		})
	}
}

func TestIsSubdirMarker(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{key: "dir/", want: true},
		{key: "dir/file", want: false},
		{key: "file", want: false},
	}

	for _, tt := range tests {
		if got := IsSubdirMarker(tt.key); got != tt.want {
			t.Fatalf("IsSubdirMarker(%q) = %v, want %v", tt.key, got, tt.want)
		}
	}
}
