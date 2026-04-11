package meta

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidPath error = errors.New("invalid path string provided")

const (
	PathInvalid = iota
	PathIsRootFolder
	PathIsPrefixFolder
	PathIsSubFolder
	PathIsFile
)

type Path struct {
	Prefix string
	Key    string
	Kind   int
}

func NewPathFromString(path string) (Path, error) {
	if path == "" {
		return Path{Kind: PathIsRootFolder}, nil
	} else {
		prefix, key, _ := strings.Cut(path, "/")
		if prefix != "" && IsHexPrefix(prefix) {
			kind := PathIsPrefixFolder
			if key != "" {
				if strings.HasSuffix(key, "/") {
					kind = PathIsSubFolder
				} else {
					kind = PathIsFile
				}
			}
			return Path{Prefix: prefix, Key: key, Kind: kind}, nil
		}
	}
	return Path{Kind: PathInvalid}, ErrInvalidPath
}

func (p Path) String() string {
	return fmt.Sprintf("%v/%v", p.Prefix, p.Key)
}

func IsHexPrefix(prefix string) bool {
	if len(prefix) != 2 {
		return false
	}
	for i := range 2 {
		c := prefix[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
