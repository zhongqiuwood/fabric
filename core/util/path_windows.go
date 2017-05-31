package util

import "strings"
import "golang.org/x/sys/windows/registry"

func CanonicalizeFilePath(path string) string {

	pathexpanded, err := registry.ExpandString(path)
	if err == nil {
		path = pathexpanded
	} 

	if !strings.HasSuffix(path, "\\") {
		path = path + "\\"
	}

	return path
	
}
