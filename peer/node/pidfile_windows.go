
package node

import (
	"golang.org/x/sys/windows"
	"path/filepath"
)

func writePid(fileName string, _ int) error {
	
	err := windows.Mkdir(filepath.Dir(fileName), 0755)
	if err != nil {
		return err
	}	
	
	return nil
}

func killbyPidfile(_ string) error{
	return nil
}
