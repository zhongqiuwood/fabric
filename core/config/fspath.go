package config

import (
	"github.com/abchain/fabric/core/util"
	"github.com/spf13/viper"
	"sync"
)

var testMode = false
var fsPath = ""
var obtainfsPath sync.Once

func GlobalFileSystemPath() string {

	path := GlobalFileSystemPathRaw()
	if path == "" {
		return ""
	}

	return util.CanonicalizePath(path)
}

func GlobalFileSystemPathRaw() string {

	setup := func() {

		fsPath = viper.GetString("node.fileSystemPath")
		if fsPath == "" {
			fsPath = viper.GetString("peer.fileSystemPath")
			logger.Warningf("filesystem path has been set by deprecated configuration to [%s]", fsPath)
		}
	}

	if testMode {
		setup()
	} else {
		obtainfsPath.Do(setup)
	}

	return fsPath

}
