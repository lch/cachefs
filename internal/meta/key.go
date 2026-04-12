package meta

import (
	"fmt"
	"strings"
)

// ChildKey returns the bbolt key for a child item under meta.Path with oldKey.
// e.g., ChildKey("") = "mydir/"
func ChildKey(path Path, childname string) string {
	if strings.HasSuffix(path.Key, "/") {
		return fmt.Sprintf("%s%s", path.Key, childname)
	} else {
		return fmt.Sprintf("%s/%s", path.Key, childname)
	}
}
