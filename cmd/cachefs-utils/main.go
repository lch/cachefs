package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jedib0t/go-pretty/table"
	"github.com/lch/cachefs/internal/meta"
	"github.com/lch/cachefs/store"
)

func main() {
	inspectCmd := flag.NewFlagSet("inspect", flag.ContinueOnError)
	defragCmd := flag.NewFlagSet("defragment", flag.ContinueOnError)
	if len(os.Args) < 2 {
		log.Fatalln("expected 'inspect' or 'defragment' subcommands")
	}
	switch os.Args[1] {
	case "inspect":
		err := runInspect(inspectCmd)
		if err != nil {
			log.Println(err)
		}
	case "defrag":
		runDefragment(defragCmd)
	default:
		log.Fatalf("unknow subcommand %q", os.Args[1])
	}
}

func runInspect(fl *flag.FlagSet) (err error) {
	fl.Parse(os.Args[2:])
	switch fl.NArg() {
	case 2:
		// list specific blob
		realFsPath := fl.Arg(0)
		s, err := store.NewStoreForRead(realFsPath)
		if err != nil {
			return err
		}
		defer s.Close()
		cacheFsPath, err := meta.NewPathFromString(fl.Arg(1))
		if err != nil {
			return err
		}
		switch cacheFsPath.Kind {
		case meta.PathIsFile, meta.PathIsSubFolder:
			fmt.Printf("File: %s\n", cacheFsPath)
			m, err := s.GetMeta(cacheFsPath)
			if err != nil {
				return err
			}
			fmt.Printf("%s", m)

		case meta.PathIsPrefixFolder:
			prefix := cacheFsPath.Prefix
			if s, ok := s.(*store.BoltDBBlobStore); ok {
				t := table.NewWriter()
				t.SetStyle(table.StyleColoredBright)
				t.SetTitle("Blob Storage Metadata")
				t.AppendHeader(table.Row{"Prefix", "Files", "Allocated", "Recycled"})
				m := s.GetStoreStat()
				t.AppendRow([]any{m[prefix].Prefix, m[prefix].ItemN, m[prefix].AllocatedBlocks, len(m[prefix].RecycledBlocks)})
				fmt.Println(t.Render())
			}
		default:
			return errors.New("not implemented")
		}
	case 1:
		// list all blobs from realFsPath
		realFsPath := fl.Arg(0)
		s, err := store.NewStoreForRead(realFsPath)
		if err != nil {
			return err
		}
		defer s.Close()
		if s, ok := s.(*store.BoltDBBlobStore); ok {
			t := table.NewWriter()
			t.SetStyle(table.StyleColoredBright)
			t.SetTitle("Blob Storage Metadata")
			t.AppendHeader(table.Row{"Prefix", "Files", "Allocated", "Recycled"})
			m := s.GetStoreStat()
			for _, info := range m {
				t.AppendRow([]any{info.Prefix, info.ItemN, info.AllocatedBlocks, len(info.RecycledBlocks)})
			}
			fmt.Println(t.Render())
		}
	case 0:
		err = errors.New("no filesystem path specified")
		return
	}
	return
}

func runDefragment(fl *flag.FlagSet) {
	fl.Parse(os.Args[2:])
	switch fl.NArg() {
	case 2:
		// list specific blob
		_ = fl.Arg(0)
		_ = fl.Arg(1)
	case 1:
		// list all blobs from path
		_ = fl.Arg(0)
	case 0:
		log.Fatalf("no filesystem path specified")
	}
}
