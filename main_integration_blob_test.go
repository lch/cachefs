package main_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	cachefsfs "github.com/lch/cachefs/fs"
	"github.com/lch/cachefs/store"
)

func TestIntegrationBlobBackedArchitecture(t *testing.T) {
	requireIntegration(t)

	t.Run("blob-file-verification", func(t *testing.T) {
		withMountedFSAndStore(t, func(mount, backendDir string, st store.Store) {
			prefix := filepath.Join(mount, "aa")
			file := filepath.Join(prefix, "blob.txt")
			payload := []byte("blob-backed payload")

			mustMkdir(t, prefix)
			mustWriteFile(t, file, payload)

			attr, err := st.GetMeta("aa", "blob.txt")
			if err != nil {
				t.Fatalf("GetMeta: %v", err)
			}

			raw, err := os.ReadFile(filepath.Join(backendDir, "aa"))
			if err != nil {
				t.Fatalf("ReadFile backend blob: %v", err)
			}
			start := int(attr.Offset)
			end := start + int(attr.Length)
			if end > len(raw) {
				t.Fatalf("blob file too short: end=%d len=%d", end, len(raw))
			}
			if got := raw[start:end]; !bytes.Equal(got, payload) {
				t.Fatalf("blob file content = %q, want %q", got, payload)
			}
		})
	})

	t.Run("free-list-reuse", func(t *testing.T) {
		withMountedFSAndStore(t, func(mount, _ string, st store.Store) {
			prefix := filepath.Join(mount, "aa")
			first := filepath.Join(prefix, "first.bin")
			second := filepath.Join(prefix, "second.bin")
			payload := bytes.Repeat([]byte("x"), 16)

			mustMkdir(t, prefix)
			mustWriteFile(t, first, payload)
			firstMeta, err := st.GetMeta("aa", "first.bin")
			if err != nil {
				t.Fatalf("GetMeta first: %v", err)
			}
			mustRemove(t, first)
			mustWriteFile(t, second, payload)
			secondMeta, err := st.GetMeta("aa", "second.bin")
			if err != nil {
				t.Fatalf("GetMeta second: %v", err)
			}
			if secondMeta.Offset != firstMeta.Offset {
				t.Fatalf("second offset = %d, want %d", secondMeta.Offset, firstMeta.Offset)
			}
		})
	})

	t.Run("free-list-merge", func(t *testing.T) {
		withMountedFSAndStore(t, func(mount, _ string, st store.Store) {
			prefix := filepath.Join(mount, "aa")
			payloadA := []byte("aaaa")
			payloadB := []byte("bbbb")
			payloadC := []byte("cccc")
			payloadD := bytes.Repeat([]byte("d"), len(payloadA)+len(payloadB))

			mustMkdir(t, prefix)
			mustWriteFile(t, filepath.Join(prefix, "a.bin"), payloadA)
			mustWriteFile(t, filepath.Join(prefix, "b.bin"), payloadB)
			mustWriteFile(t, filepath.Join(prefix, "c.bin"), payloadC)

			aMeta, err := st.GetMeta("aa", "a.bin")
			if err != nil {
				t.Fatalf("GetMeta a: %v", err)
			}
			bMeta, err := st.GetMeta("aa", "b.bin")
			if err != nil {
				t.Fatalf("GetMeta b: %v", err)
			}
			cMeta, err := st.GetMeta("aa", "c.bin")
			if err != nil {
				t.Fatalf("GetMeta c: %v", err)
			}
			if aMeta.Offset+uint64(len(payloadA)) != bMeta.Offset || bMeta.Offset+uint64(len(payloadB)) != cMeta.Offset {
				t.Fatalf("expected contiguous blob layout, got a=%#v b=%#v c=%#v", aMeta, bMeta, cMeta)
			}

			mustRemove(t, filepath.Join(prefix, "b.bin"))
			mustRemove(t, filepath.Join(prefix, "a.bin"))

			mustWriteFile(t, filepath.Join(prefix, "d.bin"), payloadD)
			dMeta, err := st.GetMeta("aa", "d.bin")
			if err != nil {
				t.Fatalf("GetMeta d: %v", err)
			}
			if dMeta.Offset != aMeta.Offset {
				t.Fatalf("merged allocation offset = %d, want %d", dMeta.Offset, aMeta.Offset)
			}
		})
	})

	t.Run("prefix-removal-cleanup", func(t *testing.T) {
		withMountedFSAndStore(t, func(mount, backendDir string, st store.Store) {
			prefix := filepath.Join(mount, "aa")
			blobPath := filepath.Join(backendDir, "aa")

			mustMkdir(t, prefix)
			mustWriteFile(t, filepath.Join(prefix, "cleanup.bin"), []byte("cleanup"))
			if _, err := st.GetMeta("aa", "cleanup.bin"); err != nil {
				t.Fatalf("GetMeta cleanup: %v", err)
			}

			mustRemove(t, filepath.Join(prefix, "cleanup.bin"))
			mustRemove(t, prefix)

			if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
				t.Fatalf("blob file still exists after prefix removal: %v", err)
			}
		})
	})

	t.Run("persistence-across-restart", func(t *testing.T) {
		backendDir := t.TempDir()
		var freedOffset uint64

		func() {
			mount, st, cleanup := mountIntegrationFSWithStore(t, backendDir)
			defer cleanup()

			prefix := filepath.Join(mount, "aa")
			mustMkdir(t, prefix)
			mustWriteFile(t, filepath.Join(prefix, "persist.txt"), []byte("persist me"))
			mustWriteFile(t, filepath.Join(prefix, "freed.bin"), bytes.Repeat([]byte("f"), 8))
			freedMeta, err := st.GetMeta("aa", "freed.bin")
			if err != nil {
				t.Fatalf("GetMeta freed: %v", err)
			}
			freedOffset = freedMeta.Offset
			mustRemove(t, filepath.Join(prefix, "freed.bin"))
		}()

		func() {
			mount, st, cleanup := mountIntegrationFSWithStore(t, backendDir)
			defer cleanup()

			if got := mustReadFile(t, filepath.Join(mount, "aa", "persist.txt")); string(got) != "persist me" {
				t.Fatalf("ReadFile after restart = %q, want persist me", got)
			}

			mustWriteFile(t, filepath.Join(mount, "aa", "reused.bin"), bytes.Repeat([]byte("r"), 8))
			reusedMeta, err := st.GetMeta("aa", "reused.bin")
			if err != nil {
				t.Fatalf("GetMeta reused: %v", err)
			}
			if reusedMeta.Offset != freedOffset {
				t.Fatalf("reused offset after restart = %d, want %d", reusedMeta.Offset, freedOffset)
			}
		}()
	})
}

func withMountedFSAndStore(t *testing.T, fn func(mount, backendDir string, st store.Store)) {
	t.Helper()
	backendDir := t.TempDir()
	mount, st, cleanup := mountIntegrationFSWithStore(t, backendDir)
	defer cleanup()
	fn(mount, backendDir, st)
}

func mountIntegrationFSWithStore(t *testing.T, backendDir string) (string, store.Store, func()) {
	t.Helper()

	mountDir := t.TempDir()
	st, err := store.NewStore(backendDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	shared := cachefsfs.NewCacheFS(st, uint32(os.Getuid()), uint32(os.Getgid()))
	root := cachefsfs.NewRootNode(shared)
	sec := time.Second
	server, err := gfs.Mount(mountDir, root, &gfs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      false,
			FsName:     "cachefs",
			Name:       "cachefs",
		},
		AttrTimeout:       &sec,
		EntryTimeout:      &sec,
		RootStableAttr:    &gfs.StableAttr{Ino: 1},
		UID:               uint32(os.Getuid()),
		GID:               uint32(os.Getgid()),
		FirstAutomaticIno: 2,
	})
	if err != nil {
		_ = st.Close()
		t.Skipf("FUSE mount unavailable: %v", err)
	}

	cleanup := func() {
		if err := server.Unmount(); err != nil {
			_ = err
		}
		_ = st.Close()
	}
	return mountDir, st, cleanup
}
