package store

import (
	"testing"
)

func TestWriteFileReadFileRoundTrip(t *testing.T) {
}

func TestDeleteFileReusesFreedOffset(t *testing.T) {
}

func TestTruncateShorterAndLonger(t *testing.T) {
}

func TestListPrefixesSkipsFreelistBucket(t *testing.T) {
}

func TestRemovePrefixDeletesBlobFile(t *testing.T) {
}

func TestFreeListPersistenceAcrossRestart(t *testing.T) {
}

func newStoreForTest(t *testing.T) (Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	return s, dir
}

func TestDeleteMissingFileReturnsNotExist(t *testing.T) {
}

func TestSubdirOperations(t *testing.T) {
}
