package meta

import (
	"fmt"
	"strings"
)

// ChildKey returns the bbolt key for a child item under meta.Path with oldKey.
// e.g., ChildKey(Path{Prefix: "aa", Key: "aa/"}, "mydir", true) = "aa/mydir/"
func ChildKey(path Path, childname string, isDir bool) string {
	if isDir {
		childname = childname + "/"
	}
	if path.Key == "" {
		return childname
	}
	if strings.HasSuffix(path.Key, "/") {
		return fmt.Sprintf("%s%s", path.Key, childname)
	}
	return fmt.Sprintf("%s/%s", path.Key, childname)
}
