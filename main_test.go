package main_test

import (
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

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/store"

	cfs "github.com/lch/cachefs/fs"
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
			for _, name := range []string{"zz", "a", "abc"} {
				if err := os.Mkdir(filepath.Join(mount, name), 0o755); err == nil {
					t.Fatalf("Mkdir(%q) succeeded, want error", name)
				}
			}
		})
	})

	t.Run("prefix-with-uppercase", func(t *testing.T) {
		withMountedFS(t, func(mount string) {
			for _, name := range []string{"AA", "BB", "CC"} {
				if err := os.Mkdir(filepath.Join(mount, name), 0o755); err != nil {
					t.Fatalf("Mkdir(%q) = %v, want succeed", name, err)
				}
			}
			for _, name := range []string{"aa", "bb", "cc"} {
				if err := os.Mkdir(filepath.Join(mount, name), 0o755); !errors.Is(err, os.ErrExist) {
					t.Fatalf("Mkdir(%q) = %v, want os.ErrExist", name, err)
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

	shared := cfs.NewCacheFS(st, uint32(os.Getuid()), uint32(os.Getgid()))
	root := cfs.NewRootNode(shared)
	sec := time.Second
	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      false,
			FsName:     "cachefs",
			Name:       "cachefs",
		},
		AttrTimeout:       &sec,
		EntryTimeout:      &sec,
		RootStableAttr:    &fs.StableAttr{Ino: 1},
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

	shared := cfs.NewCacheFS(st, uint32(os.Getuid()), uint32(os.Getgid()))
	root := cfs.NewRootNode(shared)
	sec := time.Second
	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      false,
			FsName:     "cachefs",
			Name:       "cachefs",
		},
		AttrTimeout:       &sec,
		EntryTimeout:      &sec,
		RootStableAttr:    &fs.StableAttr{Ino: 1},
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
