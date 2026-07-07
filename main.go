package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	gfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lch/cachefs/fs"
	"github.com/lch/cachefs/store"
)

const (
	defaultTimeout = time.Second
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
	allowOther := fsFlags.Bool("allow_other", false, "allow other users to access the mount")
	uid := fsFlags.Uint("uid", uint(os.Getuid()), "default file owner UID")
	gid := fsFlags.Uint("gid", uint(os.Getgid()), "default file owner GID")
	fg := fsFlags.Bool("fg", false, "run program in foreground")
	isDaemon := fsFlags.Bool("_daemon", false, "internal use only")

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

	if !*fg && !*isDaemon {
		r, w, err := os.Pipe()
		if err != nil {
			return fmt.Errorf("create signal pipe: %w", err)
		}
		defer r.Close()

		binPath, err := os.Executable()
		if err != nil {
			binPath = os.Args[0]
		}

		childArgs := make([]string, 0, len(args)+1)
		childArgs = append(childArgs, "-_daemon")
		childArgs = append(childArgs, args...)

		cmd := exec.Command(binPath, childArgs...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setsid: true,
		}
		cmd.ExtraFiles = []*os.File{w}

		cmd.Stdin = nil
		cmd.Stdout = nil
		cmd.Stderr = nil

		if err := cmd.Start(); err != nil {
			w.Close()
			return fmt.Errorf("start daemon: %w", err)
		}

		w.Close()

		buf := make([]byte, 1024)
		n, readErr := r.Read(buf)
		if readErr != nil {
			if waitErr := cmd.Wait(); waitErr != nil {
				return fmt.Errorf("daemon failed to start: %w", waitErr)
			}
			return errors.New("daemon exited prematurely without signaling success")
		}

		sigStr := string(buf[:n])
		if sigStr != "ok" {
			return fmt.Errorf("daemon initialization failed: %s", sigStr)
		}

		os.Exit(0)
	}

	var signalPipe *os.File
	if *isDaemon {
		signalPipe = os.NewFile(3, "signal-pipe")
	}

	signalSuccess := func() {
		if signalPipe != nil {
			_, _ = signalPipe.Write([]byte("ok"))
			_ = signalPipe.Close()
		}
	}

	signalFailure := func(err error) {
		if signalPipe != nil {
			_, _ = signalPipe.Write([]byte(err.Error()))
			_ = signalPipe.Close()
		}
	}

	st, err := store.NewStore(backendDir)
	if err != nil {
		err = fmt.Errorf("open store: %w", err)
		signalFailure(err)
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Printf("closing store: %v", cerr)
		}
	}()

	shared := fs.NewCacheFS(st, uint32(*uid), uint32(*gid))
	root := fs.NewRootNode(shared)
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
		err = fmt.Errorf("mount filesystem: %w", err)
		signalFailure(err)
		return err
	}

	signalSuccess()

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
