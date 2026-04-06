# CacheFS Implementation Plan

## Overview

**CacheFS** is a FUSE filesystem that presents a flat-prefix directory structure
(`/mountpoint/aa/aabb1122...`) to consumers, while packing all files sharing a
hex prefix into a single bbolt database on the underlying filesystem. This
dramatically reduces inode pressure when storing large numbers of small cache
files.

```
User sees (virtual):              Backend (real):
/mountpoint/                      /backend/
├── aa/                           └── cache.db  (single bbolt DB)
│   ├── aabb1122...ff                  ├── bucket "aa"
│   └── aa991234...00                  │   ├── key "aabb1122...ff" → file data
├── bb/                                │   └── key "aa991234...00" → file data
│   └── bb001122...ee                  ├── bucket "bb"
└── ff/                                │   └── key "bb001122...ee" → file data
    └── ff112233...aa                  ├── bucket "ff"
                                       │   └── key "ff112233...aa" → file data
                                       └── bucket "_meta" → file attributes
```

## Technology Stack

| Component       | Choice                                                       |
| --------------- | ------------------------------------------------------------ |
| Language         | Go 1.23+                                                     |
| FUSE library     | `github.com/hanwen/go-fuse/v2` (fs package, node-based API) |
| Storage backend  | `go.etcd.io/bbolt` (single DB file)                         |
| Build system     | Go modules                                                   |

## Architecture

### Node Types

Three node types, each embedding `fs.Inode`:

1. **`RootNode`** — the mountpoint root directory. Lists prefix directories.
2. **`PrefixDirNode`** — a virtual directory for a 2-char hex prefix (e.g., `aa`). Lists files within that prefix.
3. **`FileNode`** — a virtual file. Reads/writes data from/to the bbolt bucket.

### Storage Layout (bbolt)

- **One global bbolt DB file** at `<backend_dir>/cache.db`
- **One bucket per prefix** (e.g., bucket `"aa"`, `"bb"`, ..., `"ff"`)
  - Key = filename (e.g., `"aabb1122...ff"`)
  - Value = file content bytes
- **One metadata bucket** (`"_meta"`) storing file attributes as binary-encoded values:
  - Key = `"<prefix>/<filename>"` (e.g., `"aa/aabb1122ff"`)
  - Value = binary-encoded `FileAttr` struct (using `encoding/binary`, fixed-size layout)

### Concurrency Model

- bbolt provides single-writer / multiple-reader MVCC.
- Each FUSE operation opens its own bbolt transaction (read or read-write as needed).
- Node-level `sync.RWMutex` for in-memory state (inode children cache, etc.).
- Write-through: every `Write` / `Create` / `Unlink` immediately commits to bbolt.

### Prefix Validation

- Only valid 2-character lowercase hexadecimal strings are accepted as directory names (`00`–`ff`, 256 possible prefixes).
- `Mkdir` and `Lookup` on `RootNode` reject any name that does not match `^[0-9a-f]{2}$`.

---

## Phases

### Phase 1: Project Scaffolding

1. Initialize Go module (`go mod init github.com/lch/cachefs`).
2. Set up directory structure:
   ```
   cachefs/
   ├── main.go              # CLI entry point, mount/unmount
   ├── go.mod
   ├── go.sum
   ├── fs/
   │   ├── root.go          # RootNode implementation
   │   ├── prefixdir.go     # PrefixDirNode implementation
   │   ├── file.go          # FileNode implementation
   │   ├── filehandle.go    # FileHandle implementation (per-fd state)
   │   └── fs.go            # CacheFS struct (shared state, DB handle)
   ├── store/
   │   ├── store.go         # Storage interface + bbolt implementation
   │   └── store_test.go    # Storage layer unit tests
   └── internal/
       └── meta/
           ├── meta.go      # FileAttr struct, binary serialization
           └── meta_test.go
   ```
3. Add dependencies: `go-fuse/v2`, `bbolt`.

### Phase 2: Metadata Model (`internal/meta` package)

Define the file attribute structure and its binary serialization.

```go
type FileAttr struct {
    Size  uint64
    Mode  uint32    // e.g., 0644
    Uid   uint32
    Gid   uint32
    Atime int64     // Unix timestamp (seconds)
    Mtime int64
    Ctime int64
}
```

- Use `encoding/binary` with `binary.LittleEndian` and fixed-size struct layout.
- Helper functions: `Marshal(*FileAttr) []byte`, `Unmarshal([]byte) (*FileAttr, error)`.
- Default mode: `0644` for files, `0755` for directories.
- Fixed serialized size: 44 bytes (8+4+4+4+8+8+8).
- Unit tests: round-trip serialization, error on truncated input.

### Phase 3: Storage Layer (`store` package)

Define a clean storage interface so the FUSE layer does not directly depend on bbolt.

```go
type Store interface {
    // File content operations
    GetContent(prefix, filename string) ([]byte, error)
    PutContent(prefix, filename string, data []byte) error
    DeleteContent(prefix, filename string) error

    // Metadata operations
    GetMeta(prefix, filename string) (*meta.FileAttr, error)
    PutMeta(prefix, filename string, attr *meta.FileAttr) error
    DeleteMeta(prefix, filename string) error

    // Atomic content + metadata write
    PutFile(prefix, filename string, data []byte, attr *meta.FileAttr) error
    DeleteFile(prefix, filename string) error

    // Directory listing
    ListPrefixes() ([]string, error)
    ListFiles(prefix string) ([]string, error)

    // Lifecycle
    Close() error
}
```

**bbolt implementation details:**

- `bboltStore` struct holds `*bbolt.DB`.
- Prefix buckets created lazily on first write via `CreateBucketIfNotExists`.
- Metadata stored in a dedicated `_meta` bucket with composite key `"<prefix>/<filename>"`.
- `PutFile` and `DeleteFile` perform content + metadata operations in a single `db.Update()` transaction for atomicity.
- All content-modifying operations use `db.Update()` (write-through).
- All reads use `db.View()`.
- **Critical:** values are always copied out of bbolt transactions before returning.
- Empty prefix buckets are removed when the last file in them is deleted.

**Unit tests:**

- CRUD operations for content and metadata.
- `PutFile` / `DeleteFile` atomicity.
- Listing prefixes and files.
- Concurrent read/write from multiple goroutines.
- Edge cases: get non-existent key, delete non-existent key, empty prefix list.

### Phase 4: FUSE — CacheFS Shared State (`fs/fs.go`)

**`CacheFS`** struct holds shared state passed to all nodes:

```go
type CacheFS struct {
    Store    store.Store
    Uid      uint32
    Gid      uint32
}
```

- Initialized in `main.go` and passed by pointer to all nodes.
- Provides default UID/GID for file ownership.

### Phase 5: FUSE — RootNode (`fs/root.go`)

**`RootNode`** represents the mountpoint `/`.

| Interface       | Behavior                                                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `NodeLookuper`  | Validates name is a 2-char hex string. Checks if the prefix bucket exists in the store. If yes, returns a `PrefixDirNode`.     |
| `NodeReaddirer` | Calls `store.ListPrefixes()`, returns all known prefixes as directory entries.                                                  |
| `NodeMkdirer`   | Validates name is 2-char hex. Creates the prefix bucket in the store. Returns a `PrefixDirNode`.                               |
| `NodeRmdirer`   | Checks prefix bucket is empty via `store.ListFiles()`. If empty, removes the bucket. Otherwise returns `ENOTEMPTY`.            |
| `NodeGetattrer` | Returns directory attributes (mode `0755`, current time).                                                                      |
| `NodeStatfser`  | Returns filesystem stats (optional, useful for `df`).                                                                          |

**Prefix validation helper:**

```go
func isValidPrefix(name string) bool {
    if len(name) != 2 { return false }
    _, err := hex.DecodeString(name)
    return err == nil
}
```

### Phase 6: FUSE — PrefixDirNode (`fs/prefixdir.go`)

**`PrefixDirNode`** represents a prefix directory like `/aa/`.

Fields: `cfs *CacheFS`, `prefix string`

| Interface       | Behavior                                                                                                                       |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `NodeLookuper`  | Calls `store.GetMeta(prefix, name)`. If found, returns a `FileNode` with attributes populated.                                |
| `NodeReaddirer` | Calls `store.ListFiles(prefix)`, returns all filenames as regular file entries.                                                |
| `NodeCreater`   | Creates a new file: initializes default `FileAttr` + empty content via `store.PutFile()`. Returns `FileNode` + `CacheFileHandle`. |
| `NodeUnlinker`  | Calls `store.DeleteFile(prefix, name)`. Removes child inode.                                                                  |
| `NodeGetattrer` | Returns directory attributes (mode `0755`).                                                                                    |
| `NodeRenamer`   | Handles rename within same prefix (move metadata + content keys) or across prefixes (copy to new prefix, delete from old).     |

### Phase 7: FUSE — FileNode (`fs/file.go`)

**`FileNode`** represents a single cached file like `/aa/aabb1122...ff`.

Fields: `cfs *CacheFS`, `prefix string`, `filename string`

| Interface         | Behavior                                                                                                                   |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `NodeOpener`      | Loads file content from `store.GetContent()` into a new `CacheFileHandle`. Returns the handle.                            |
| `NodeGetattrer`   | Reads metadata from `store.GetMeta()`, fills `AttrOut` (size, mode, timestamps, uid, gid).                                |
| `NodeSetattrer`   | Updates metadata fields (mode, timestamps, size/truncate). For truncate (`FATTR_SIZE`), also truncates/extends content.   |

### Phase 8: FUSE — FileHandle (`fs/filehandle.go`)

**`CacheFileHandle`** provides per-fd state for open files.

Fields:
- `cfs *CacheFS` — shared state
- `prefix string`, `filename string` — file identity
- `mu sync.Mutex` — protects buffer
- `buf []byte` — in-memory copy of file content
- `dirty bool` — whether buffer has been modified

| Interface       | Behavior                                                                                          |
| --------------- | ------------------------------------------------------------------------------------------------- |
| `FileReader`    | Reads from in-memory `buf` at the requested offset. Returns `fuse.ReadResultData`.               |
| `FileWriter`    | Writes to in-memory `buf` (growing if needed). Marks dirty. Immediately flushes to store.        |
| `FileFlusher`   | If dirty, writes `buf` to store via `store.PutContent()` and updates size in metadata.           |
| `FileReleaser`  | Final flush if needed. Clears buffer reference.                                                  |
| `FileGetattrer` | Returns attributes using in-memory buffer size + stored metadata.                                |

**Write-through strategy:**

Each `Write()` call:
1. Updates the in-memory buffer.
2. Immediately calls `store.PutFile(prefix, filename, buf, updatedAttr)`.
3. This ensures durability at the cost of per-write latency (acceptable for small files).

### Phase 9: CLI Entry Point (`main.go`)

```
Usage: cachefs [options] <backend_dir> <mountpoint>

Options:
  -debug          Enable FUSE debug logging
  -allow-other    Allow other users to access the mount
  -uid <uid>      Default file owner UID (default: current user)
  -gid <gid>      Default file owner GID (default: current group)
```

Implementation steps:
1. Parse command-line flags.
2. Validate that `<backend_dir>` exists (create if not).
3. Validate that `<mountpoint>` exists.
4. Open bbolt database at `<backend_dir>/cache.db`.
5. Create `Store` instance wrapping the bbolt DB.
6. Create `CacheFS` shared state.
7. Create `RootNode`.
8. Call `fs.Mount(mountpoint, rootNode, options)` with:
   - `EntryTimeout` and `AttrTimeout` set to 1 second.
   - `MountOptions.Name = "cachefs"`.
   - Debug flag forwarded.
9. Set up signal handler for `SIGINT` / `SIGTERM`:
   - Call `fuse.Unmount(mountpoint)` on signal.
10. Call `server.Wait()`.
11. Close store / bbolt DB on exit.

### Phase 10: Integration Tests

Test the full stack by mounting the filesystem in a temporary directory and performing real file operations.

**Test cases:**

1. **Basic lifecycle:** create prefix dir -> create file -> read file -> delete file -> delete dir.
2. **Write and read back:** write content via `os.WriteFile`, read back via `os.ReadFile`, verify match.
3. **Overwrite:** write file, overwrite with new content, verify new content.
4. **Truncate:** write file, truncate to shorter length, verify.
5. **Directory listing:** create multiple files in a prefix, verify `os.ReadDir` returns all.
6. **Multiple prefixes:** create files across `aa`, `bb`, `ff`, list root, verify all prefix dirs.
7. **Invalid prefix rejection:** attempt to `mkdir` with invalid names (`"zz"`, `"a"`, `"abc"`), verify error.
8. **Delete non-empty dir:** attempt to `rmdir` a prefix with files, verify `ENOTEMPTY`.
9. **Rename within prefix:** rename a file within the same prefix dir.
10. **Rename across prefixes:** move a file from one prefix dir to another.
11. **Concurrent access:** multiple goroutines reading/writing different files simultaneously.
12. **Persistence:** mount, write files, unmount, remount, verify files persist.

### Phase 11: Edge Cases & Polish

- **Error mapping:** Map bbolt and OS errors to appropriate `syscall.Errno` values using `fs.ToErrno()`.
- **Graceful shutdown:** Ensure all in-flight FUSE operations complete before closing the store.
- **Empty prefix cleanup:** When the last file in a prefix bucket is deleted, optionally remove the bucket and forget the prefix dir inode.
- **Statfs implementation:** Report total DB file size, number of files, etc.
- **File size consistency:** Ensure `Getattr` always reflects the current size, even during writes.
- **Timestamp updates:** Update `mtime` on write, `atime` on read (or mount with `noatime` semantics for performance).

---

## Key Design Decisions

| Decision                            | Rationale                                                                                              |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Single bbolt DB file                | Simpler management, single file to back up. Acceptable concurrency for <1M files.                     |
| Bucket-per-prefix in bbolt          | Logical isolation. Prefix listing = bucket enumeration. Prefix deletion = delete bucket.               |
| Binary-encoded metadata             | Compact (44 bytes fixed), fast serialize/deserialize, no parsing overhead.                             |
| Separate `_meta` bucket             | Allows reading file attributes without loading file content (important for `ls -l`, `stat`).          |
| In-memory buffer in FileHandle      | Small files fit easily in memory. Avoids repeated bbolt reads during sequential read/write.           |
| Write-through on every Write()      | Ensures durability. For small files, the per-write overhead is minimal.                                |
| Storage interface abstraction       | Enables future backend swaps and easier unit testing with mocks.                                      |
| 2-char hex prefix enforcement       | Prevents misuse; ensures predictable directory structure matching the hash-prefix convention.          |
| `PutFile` / `DeleteFile` atomicity  | Content and metadata always stay in sync within a single bbolt transaction.                           |

## Dependencies

```
github.com/hanwen/go-fuse/v2   — FUSE bindings (node-based fs package)
go.etcd.io/bbolt                — embedded B+tree key-value store
```

No other external dependencies required.
