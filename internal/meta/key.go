package meta

import "strings"

// SubdirMarkerKey returns the bbolt key for a subdirectory marker.
// e.g., SubdirMarkerKey("mydir") = "mydir/"
func SubdirMarkerKey(dirname string) string {
	return dirname + "/"
}

// SubdirFileKey returns the bbolt key for a file inside a subdirectory.
// e.g., SubdirFileKey("mydir", "foo.txt") = "mydir/foo.txt"
func SubdirFileKey(dirname, filename string) string {
	return dirname + "/" + filename
}

// ParseKey splits a composite key into (dirname, basename, isSubdirEntry).
// - "filename"         -> ("", "filename", false)       - direct file
// - "dirname/"         -> ("dirname", "", true)        - subdir marker
// - "dirname/file.txt" -> ("dirname", "file.txt", true) - file in subdir
func ParseKey(key string) (dirname, basename string, isSubdirEntry bool) {
	idx := strings.IndexByte(key, '/')
	if idx < 0 {
		return "", key, false
	}
	return key[:idx], key[idx+1:], true
}

// IsSubdirMarker returns true if the key is a subdirectory marker (ends with "/").
func IsSubdirMarker(key string) bool {
	return len(key) > 0 && key[len(key)-1] == '/'
}
