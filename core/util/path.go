// +build !windows

package util

import "strings"

func CanonicalizeFilePath(path string) string {

	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return path
	
}
