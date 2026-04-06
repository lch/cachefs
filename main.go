package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	cachefsfs "github.com/lch/cachefs/fs"
	"github.com/lch/cachefs/store"
	"go.etcd.io/bbolt"
)

const (
	defaultTimeout = time.Second
	dbFileName     = "cache.db"
	fsName         = "cachefs"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fsFlags := flag.NewFlagSet(fsName, flag.ContinueOnError)
	fsFlags.SetOutput(os.Stderr)

	debug := fsFlags.Bool("debug", false, "enable FUSE debug logging")
	allowOther := fsFlags.Bool("allow-other", false, "allow other users to access the mount")
	uid := fsFlags.Uint("uid", uint(os.Getuid()), "default file owner UID")
	gid := fsFlags.Uint("gid", uint(os.Getgid()), "default file owner GID")

	if err := fsFlags.Parse(args); err != nil {
		return err
	}
	if fsFlags.NArg() != 2 {
		return errors.New("usage: cachefs [options] <backend_dir> <mountpoint>")
	}

	backendDir := fsFlags.Arg(0)
	mountPoint := fsFlags.Arg(1)
	if err := os.MkdirAll(backendDir, 0o755); err != nil {
		return fmt.Errorf("create backend dir %q: %w", backendDir, err)
	}
	if info, err := os.Stat(mountPoint); err != nil {
		return fmt.Errorf("stat mountpoint %q: %w", mountPoint, err)
	} else if !info.IsDir() {
		return fmt.Errorf("mountpoint %q is not a directory", mountPoint)
	}

	db, err := bbolt.Open(filepath.Join(backendDir, dbFileName), 0o600, &bbolt.Options{Timeout: defaultTimeout})
	if err != nil {
		return fmt.Errorf("open bbolt database: %w", err)
	}
	st := store.New(db)
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Printf("closing store: %v", cerr)
		}
	}()

	shared := cachefsfs.NewCacheFS(st, uint32(*uid), uint32(*gid))
	root := cachefsfs.NewRootNode(shared)
	sec := defaultTimeout
	server, err := gfs.Mount(mountPoint, root, &gfs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: *allowOther,
			Debug:      *debug,
			FsName:     fsName,
			Name:       fsName,
		},
		AttrTimeout:       &sec,
		EntryTimeout:      &sec,
		RootStableAttr:    &gfs.StableAttr{Ino: 1},
		UID:               uint32(*uid),
		GID:               uint32(*gid),
		FirstAutomaticIno: 2,
	})
	if err != nil {
		return fmt.Errorf("mount filesystem: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		<-sigCh
		if err := server.Unmount(); err != nil {
			log.Printf("unmount: %v", err)
		}
	}()

	server.Wait()
	return nil
}
