package meta_test

import (
	"testing"

	"github.com/lch/cachefs/internal/meta"
)

func TestPathCreationFromString(t *testing.T) {
	tests := map[string]meta.Path{
		"aa/aa.txt":         {Kind: meta.PathIsFile, Prefix: "aa", Key: "aa.txt"},
		"11/11.txt":         {Kind: meta.PathIsFile, Prefix: "11", Key: "11.txt"},
		"11/":               {Kind: meta.PathIsPrefixFolder, Prefix: "11", Key: ""},
		"11/1122/":          {Kind: meta.PathIsSubFolder, Prefix: "11", Key: "1122/"},
		"11/1122/test.txt":  {Kind: meta.PathIsFile, Prefix: "11", Key: "1122/test.txt"},
		"11/1122/test.txt/": {Kind: meta.PathIsSubFolder, Prefix: "11", Key: "1122/test.txt/"},
		"":                  {Kind: meta.PathIsRootFolder, Prefix: "", Key: ""},
	}

	for input, expected := range tests {
		output, err := meta.NewPathFromString(input)
		if err != nil {
			t.Fatalf("NewPathFromString() error %q", err)
		}
		if output != expected {
			t.Fatalf("NewPathFromString() = %v, expected %v", output, expected)
		}
	}
}
