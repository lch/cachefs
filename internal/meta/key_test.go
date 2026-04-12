package meta

import "testing"

func TestChildKey(t *testing.T) {
	inputPath := []Path{
		{Kind: PathIsSubFolder, Prefix: "00", Key: "ab/"},
		{Kind: PathIsSubFolder, Prefix: "00", Key: "ab/"},
		{Kind: PathIsSubFolder, Prefix: "00", Key: "ab/aa/"},
	}
	inputName := []string{
		"f.txt",
		"f",
		"f",
	}
	output := []string{
		"ab/f.txt",
		"ab/f",
		"ab/aa/f",
	}

	for k, v := range inputPath {
		got := ChildKey(v, inputName[k])
		want := output[k]
		if got != want {
			t.Fatalf("SubdirFileKey() = %q, want %q", got, want)
		}
	}
}
