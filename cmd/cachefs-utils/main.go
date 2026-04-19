package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jedib0t/go-pretty/table"
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
		runInspect(inspectCmd)
	case "defrag":
		runDefragment(defragCmd)
	default:
		log.Fatalf("unknow subcommand %q", os.Args[1])
	}
}

func runInspect(fl *flag.FlagSet) {
	fl.Parse(os.Args[2:])
	switch fl.NArg() {
	case 2:
		// list specific blob
	case 1:
		// list all blobs from path
		path := fl.Arg(0)
		s, err := store.NewStore(path)
		defer s.Close()
		if err != nil {
			log.Fatalln(err)
		}
		if s, ok := s.(*store.BoltDBBlobStore); ok {
			t := table.NewWriter()
			t.SetStyle(table.StyleColoredBright)
			t.SetTitle("Blob Storage Metadata")
			t.AppendHeader(table.Row{"Prefix", "ItemN", "Allocated", "Recycled"})
			m := s.PrefixMeta()
			for prefix, info := range m {
				t.AppendRow([]any{prefix, info.ItemN, info.AllocatedBlocks, len(info.RecycledBlocks)})
			}
			fmt.Println(t.Render())
		}
	case 0:
		log.Fatalf("no filesystem path specified")
	}
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
