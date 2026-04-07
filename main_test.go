package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/fs"
	"github.com/lch/cachefs/store"
)

func TestIntegrationFilesystem(t *testing.T) {
	requireIntegration(t)

	t.Run("basic-lifecycle", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			file := filepath.Join(prefix, "alpha.txt")

			mustMkdir(t, prefix)
			mustWriteFile(t, file, []byte("hello"))
			if got := mustReadFile(t, file); string(got) != "hello" {
				t.Fatalf("ReadFile = %q, want hello", got)
			}
			mustRemove(t, file)
			mustRemove(t, prefix)
			assertNames(t, mount, nil)
		})
	})

	t.Run("write-read-back", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			file := filepath.Join(prefix, "payload.bin")
			mustMkdir(t, prefix)
			want := []byte("cachefs payload")
			mustWriteFile(t, file, want)
			if got := mustReadFile(t, file); !reflect.DeepEqual(got, want) {
				t.Fatalf("ReadFile = %q, want %q", got, want)
			}
		})
	})

	t.Run("overwrite", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			file := filepath.Join(prefix, "payload.bin")
			mustMkdir(t, prefix)
			mustWriteFile(t, file, []byte("hello"))
			mustWriteFile(t, file, []byte("world"))
			if got := mustReadFile(t, file); string(got) != "world" {
				t.Fatalf("ReadFile after overwrite = %q, want world", got)
			}
		})
	})

	t.Run("truncate", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			file := filepath.Join(prefix, "payload.bin")
			mustMkdir(t, prefix)
			mustWriteFile(t, file, []byte("hello"))
			mustTruncate(t, file, 2)
			if got := mustReadFile(t, file); string(got) != "he" {
				t.Fatalf("ReadFile after truncate = %q, want he", got)
			}
		})
	})

	t.Run("directory-listing", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			mustMkdir(t, prefix)
			for _, name := range []string{"beta", "alpha", "delta"} {
				mustWriteFile(t, filepath.Join(prefix, name), []byte(name))
			}
			assertNames(t, prefix, []string{"alpha", "beta", "delta"})
		})
	})

	t.Run("multiple-prefixes", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			for _, prefix := range []string{"aa", "bb", "ff"} {
				mustMkdir(t, filepath.Join(mount, prefix))
				mustWriteFile(t, filepath.Join(mount, prefix, prefix+".txt"), []byte(prefix))
			}
			assertNames(t, mount, []string{"aa", "bb", "ff"})
		})
	})

	t.Run("invalid-prefix-rejection", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			for _, name := range []string{"zz", "a", "abc", "AA"} {
				if err := os.Mkdir(filepath.Join(mount, name), 0o755); err == nil {
					t.Fatalf("Mkdir(%q) succeeded, want error", name)
				}
			}
		})
	})

	t.Run("delete-non-empty-dir", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			mustMkdir(t, prefix)
			mustWriteFile(t, filepath.Join(prefix, "alpha.txt"), []byte("hello"))
			if err := os.Remove(prefix); !errors.Is(err, syscall.ENOTEMPTY) {
				t.Fatalf("Remove(non-empty dir) = %v, want ENOTEMPTY", err)
			}
		})
	})

	t.Run("rename-within-prefix", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			mustMkdir(t, prefix)
			oldPath := filepath.Join(prefix, "old.txt")
			newPath := filepath.Join(prefix, "new.txt")
			mustWriteFile(t, oldPath, []byte("hello"))
			mustRename(t, oldPath, newPath)
			if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old path stat error = %v, want os.ErrNotExist", err)
			}
			if got := mustReadFile(t, newPath); string(got) != "hello" {
				t.Fatalf("ReadFile after rename = %q, want hello", got)
			}
		})
	})

	t.Run("rename-across-prefixes", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			from := filepath.Join(mount, "aa")
			to := filepath.Join(mount, "bb")
			mustMkdir(t, from)
			mustMkdir(t, to)
			src := filepath.Join(from, "alpha.txt")
			dst := filepath.Join(to, "beta.txt")
			mustWriteFile(t, src, []byte("hello"))
			mustRename(t, src, dst)
			if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source stat error = %v, want os.ErrNotExist", err)
			}
			if got := mustReadFile(t, dst); string(got) != "hello" {
				t.Fatalf("ReadFile after cross-prefix rename = %q, want hello", got)
			}
		})
	})

	t.Run("concurrent-access", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			for _, prefix := range []string{"aa", "bb"} {
				mustMkdir(t, filepath.Join(mount, prefix))
			}
			const workers = 16
			var wg sync.WaitGroup
			errCh := make(chan error, workers)
			for i := range workers {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					prefix := "aa"
					if i%2 == 1 {
						prefix = "bb"
					}
					path := filepath.Join(mount, prefix, fmt.Sprintf("file-%02d.txt", i))
					payload := fmt.Appendf(nil, "payload-%02d", i)
					if err := os.WriteFile(path, payload, 0o644); err != nil {
						errCh <- err
						return
					}
					got, err := os.ReadFile(path)
					if err != nil {
						errCh <- err
						return
					}
					if string(got) != string(payload) {
						errCh <- fmt.Errorf("read back %q, want %q", got, payload)
					}
				}(i)
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				if err != nil {
					t.Fatalf("concurrent access failed: %v", err)
				}
			}
		})
	})

	t.Run("persistence", func(t *testing.T) {
		backendDir := t.TempDir()

		func() {
			mount1, cleanup1 := mountIntegrationFS(t, backendDir)
			defer cleanup1()
			mustMkdir(t, filepath.Join(mount1, "aa"))
			mustWriteFile(t, filepath.Join(mount1, "aa", "persist.txt"), []byte("persist me"))
		}()

		func() {
			mount2, cleanup2 := mountIntegrationFS(t, backendDir)
			defer cleanup2()
			if got := mustReadFile(t, filepath.Join(mount2, "aa", "persist.txt")); string(got) != "persist me" {
				t.Fatalf("ReadFile after remount = %q, want persist me", got)
			}
		}()
	})
}

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

func TestIntegrationSubdirectories(t *testing.T) {
	requireIntegration(t)

	t.Run("create-read-list-delete-rmdir", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			subdir := filepath.Join(prefix, "mysubdir")
			fileA := filepath.Join(subdir, "foo.txt")
			fileB := filepath.Join(subdir, "bar.dat")

			mustMkdir(t, prefix)
			mustMkdir(t, subdir)
			mustWriteFile(t, fileA, []byte("hello"))
			mustWriteFile(t, fileB, []byte("world"))

			if got := mustReadFile(t, fileA); string(got) != "hello" {
				t.Fatalf("ReadFile subdir file = %q, want hello", got)
			}
			assertNames(t, subdir, []string{"bar.dat", "foo.txt"})

			mustRemove(t, fileA)
			mustRemove(t, fileB)
			mustRemove(t, subdir)
			assertNames(t, prefix, nil)
		})
	})

	t.Run("mixed-prefix-listing", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			direct := filepath.Join(prefix, "alpha.txt")
			subdir := filepath.Join(prefix, "mysubdir")

			mustMkdir(t, prefix)
			mustMkdir(t, subdir)
			mustWriteFile(t, direct, []byte("alpha"))
			mustWriteFile(t, filepath.Join(subdir, "beta.txt"), []byte("beta"))

			entries, err := os.ReadDir(prefix)
			if err != nil {
				t.Fatalf("ReadDir(%q): %v", prefix, err)
			}
			if len(entries) != 2 {
				t.Fatalf("ReadDir(%q) len = %d, want 2", prefix, len(entries))
			}
			if entries[0].Name() != "alpha.txt" || entries[0].IsDir() {
				t.Fatalf("ReadDir file entry = %q dir=%v, want alpha.txt file", entries[0].Name(), entries[0].IsDir())
			}
			if entries[1].Name() != "mysubdir" || !entries[1].IsDir() {
				t.Fatalf("ReadDir dir entry = %q dir=%v, want mysubdir directory", entries[1].Name(), entries[1].IsDir())
			}
		})
	})

	t.Run("no-nested-subdirs", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			subdir := filepath.Join(prefix, "mysubdir")

			mustMkdir(t, prefix)
			mustMkdir(t, subdir)
			if err := os.Mkdir(filepath.Join(subdir, "nested"), 0o755); err == nil {
				t.Fatal("Mkdir nested subdir succeeded, want error")
			} else if !errors.Is(err, syscall.ENOTSUP) && !errors.Is(err, syscall.EPERM) {
				t.Fatalf("Mkdir nested subdir error = %v, want ENOTSUP or EPERM", err)
			}
		})
	})

	t.Run("blob-sharing-and-freelist", func(t *testing.T) {
		withMountedFSAndStore(t, func(mount, backendDir string, st store.Store) {
			prefix := filepath.Join(mount, "aa")
			subdir := filepath.Join(prefix, "mysubdir")
			direct := filepath.Join(prefix, "direct.bin")
			subFile := filepath.Join(subdir, "sub.bin")

			mustMkdir(t, prefix)
			mustMkdir(t, subdir)

			directPayload := []byte("direct-payload")
			subPayload := []byte("subdir-payload")
			mustWriteFile(t, direct, directPayload)
			mustWriteFile(t, subFile, subPayload)

			directMeta, err := st.GetMeta("aa", "direct.bin")
			if err != nil {
				t.Fatalf("GetMeta direct.bin: %v", err)
			}
			subMeta, err := st.GetSubdirFileMeta("aa", "mysubdir", "sub.bin")
			if err != nil {
				t.Fatalf("GetSubdirFileMeta sub.bin: %v", err)
			}

			raw, err := os.ReadFile(filepath.Join(backendDir, "aa"))
			if err != nil {
				t.Fatalf("ReadFile backend blob: %v", err)
			}
			if got := raw[directMeta.Offset : directMeta.Offset+directMeta.Length]; !bytes.Equal(got, directPayload) {
				t.Fatalf("backend bytes for direct file = %q, want %q", got, directPayload)
			}
			if got := raw[subMeta.Offset : subMeta.Offset+subMeta.Length]; !bytes.Equal(got, subPayload) {
				t.Fatalf("backend bytes for subdir file = %q, want %q", got, subPayload)
			}

			mustRemove(t, direct)
			reused := filepath.Join(subdir, "reused.bin")
			mustWriteFile(t, reused, directPayload)
			reusedMeta, err := st.GetSubdirFileMeta("aa", "mysubdir", "reused.bin")
			if err != nil {
				t.Fatalf("GetSubdirFileMeta reused.bin: %v", err)
			}
			if reusedMeta.Offset != directMeta.Offset {
				t.Fatalf("reused offset = %d, want %d", reusedMeta.Offset, directMeta.Offset)
			}
		})
	})

	t.Run("overwrite-frees-old-space", func(t *testing.T) {
		withMountedFSAndStore(t, func(mount, _ string, st store.Store) {
			prefix := filepath.Join(mount, "aa")
			subdir := filepath.Join(prefix, "mysubdir")
			overwrite := filepath.Join(subdir, "overwrite.bin")
			reuse := filepath.Join(subdir, "reuse.bin")

			mustMkdir(t, prefix)
			mustMkdir(t, subdir)

			original := bytes.Repeat([]byte("o"), 8)
			replacement := bytes.Repeat([]byte("r"), 16)
			mustWriteFile(t, overwrite, original)
			oldMeta, err := st.GetSubdirFileMeta("aa", "mysubdir", "overwrite.bin")
			if err != nil {
				t.Fatalf("GetSubdirFileMeta overwrite.bin: %v", err)
			}

			mustWriteFile(t, overwrite, replacement)
			newMeta, err := st.GetSubdirFileMeta("aa", "mysubdir", "overwrite.bin")
			if err != nil {
				t.Fatalf("GetSubdirFileMeta overwrite.bin after replace: %v", err)
			}
			if newMeta.Offset == oldMeta.Offset {
				t.Fatalf("overwrite offset = %d, want different from %d", newMeta.Offset, oldMeta.Offset)
			}
			if got := mustReadFile(t, overwrite); !bytes.Equal(got, replacement) {
				t.Fatalf("overwrite content = %q, want %q", got, replacement)
			}

			mustWriteFile(t, reuse, original)
			reuseMeta, err := st.GetSubdirFileMeta("aa", "mysubdir", "reuse.bin")
			if err != nil {
				t.Fatalf("GetSubdirFileMeta reuse.bin: %v", err)
			}
			if reuseMeta.Offset != oldMeta.Offset {
				t.Fatalf("reuse offset = %d, want %d", reuseMeta.Offset, oldMeta.Offset)
			}
		})
	})

	t.Run("persistence", func(t *testing.T) {
		backendDir := t.TempDir()

		func() {
			mount, cleanup := mountIntegrationFS(t, backendDir)
			defer cleanup()

			prefix := filepath.Join(mount, "aa")
			subdir := filepath.Join(prefix, "persisted")
			mustMkdir(t, prefix)
			mustMkdir(t, subdir)
			mustWriteFile(t, filepath.Join(subdir, "kept.txt"), []byte("kept"))
			mustWriteFile(t, filepath.Join(prefix, "direct.txt"), []byte("direct"))
		}()

		func() {
			mount, cleanup := mountIntegrationFS(t, backendDir)
			defer cleanup()

			prefix := filepath.Join(mount, "aa")
			subdir := filepath.Join(prefix, "persisted")
			if got := mustReadFile(t, filepath.Join(subdir, "kept.txt")); string(got) != "kept" {
				t.Fatalf("ReadFile persisted subdir file = %q, want kept", got)
			}
			if got := mustReadFile(t, filepath.Join(prefix, "direct.txt")); string(got) != "direct" {
				t.Fatalf("ReadFile persisted direct file = %q, want direct", got)
			}
			assertNames(t, prefix, []string{"direct.txt", "persisted"})
			if info, err := os.Stat(subdir); err != nil {
				t.Fatalf("Stat persisted subdir: %v", err)
			} else if !info.IsDir() {
				t.Fatalf("Stat persisted subdir reported non-dir: %#v", info.Mode())
			}
		}()
	})

	t.Run("stat-on-subdir", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			prefix := filepath.Join(mount, "aa")
			subdir := filepath.Join(prefix, "mysubdir")

			mustMkdir(t, prefix)
			mustMkdir(t, subdir)

			info, err := os.Stat(subdir)
			if err != nil {
				t.Fatalf("Stat subdir: %v", err)
			}
			if !info.Mode().IsDir() {
				t.Fatalf("Stat subdir mode = %v, want directory", info.Mode())
			}
			if info.Mode().Perm() != 0o755 {
				t.Fatalf("Stat subdir perm = %#o, want 0755", info.Mode().Perm())
			}
		})
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

	shared := fs.NewCacheFS(st, uint32(os.Getuid()), uint32(os.Getgid()))
	root := fs.NewRootNode(shared)
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

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CACHEFS_INTEGRATION") == "" {
		t.Skip("set CACHEFS_INTEGRATION=1 to run FUSE integration tests")
	}
}

func withMountedFS(t *testing.T, fn func(mount string)) {
	t.Helper()
	backendDir := t.TempDir()
	mount, cleanup := mountIntegrationFS(t, backendDir)
	defer cleanup()
	fn(mount)
}

func mountIntegrationFS(t *testing.T, backendDir string) (string, func()) {
	t.Helper()

	mountDir := t.TempDir()
	st, err := store.NewStore(backendDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	shared := fs.NewCacheFS(st, uint32(os.Getuid()), uint32(os.Getgid()))
	root := fs.NewRootNode(shared)
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
	return mountDir, cleanup
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("Mkdir(%q): %v", path, err)
	}
}

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	return data
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove(%q): %v", path, err)
	}
}

func mustRename(t *testing.T, oldPath, newPath string) {
	t.Helper()
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename(%q, %q): %v", oldPath, newPath, err)
	}
}

func mustTruncate(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("Truncate(%q, %d): %v", path, size, err)
	}
}

func assertNames(t *testing.T, path string, want []string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", path, err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	if got == nil {
		got = []string{}
	}
	if want == nil {
		want = []string{}
	}
	sort.Strings(got)
	expected := append([]string{}, want...)
	sort.Strings(expected)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("ReadDir(%q) = %v, want %v", path, got, expected)
	}
}
