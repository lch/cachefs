package meta

import "testing"

func TestChildKey(t *testing.T) {
	inputPath := []Path{
		{Kind: PathIsSubFolder, Prefix: "00", Key: "ab/"},
		{Kind: PathIsSubFolder, Prefix: "00", Key: "ab/"},
		{Kind: PathIsSubFolder, Prefix: "00", Key: "ab/aa/"},
		{Kind: PathIsSubFolder, Prefix: "00", Key: "ab/aa/"},
	}
	inputName := []string{
		"f.txt",
		"f",
		"f",
		"f",
	}
	inputIsDir := []bool{
		false,
		false,
		false,
		true,
	}
	output := []string{
		"ab/f.txt",
		"ab/f",
		"ab/aa/f",
		"ab/aa/f/",
	}

	for k, v := range inputPath {
		got := ChildKey(v, inputName[k], inputIsDir[k])
		want := output[k]
		if got != want {
			t.Fatalf("SubdirFileKey() = %q, want %q", got, want)
		}
	}
}
