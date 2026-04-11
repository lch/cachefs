package fs

import (
	"context"
	"testing"

	"github.com/lch/cachefs/store"
)

func readDirEntries(root *RootNode) ([]string, error) {
	ds, errno := root.Readdir(context.Background())
	if errno != 0 {
		return nil, errno
	}
	defer ds.Close()

	var names []string
	for ds.HasNext() {
		entry, errno := ds.Next()
		if errno != 0 {
			return nil, errno
		}
		names = append(names, entry.Name)
	}
	return names, nil
}

func newTestRoot(t *testing.T) (*RootNode, store.Store) {
	t.Helper()
	st, err := store.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	return NewRootNode(NewCacheFS(st, 1000, 1001)), st
}
