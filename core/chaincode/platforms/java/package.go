/*
Copyright DTCC 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package java

import (
	"archive/tar"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	"bytes"
	"os"
	"os/exec"

	cutil "github.com/abchain/fabric/core/chaincode/util"
	pb "github.com/abchain/fabric/protos"
	_ "github.com/spf13/viper"
)

// var buildCmd = map[string]string{
// 	"build.gradle": "gradle -b build.gradle clean && gradle -b build.gradle build",
// 	"pom.xml":      "mvn -f pom.xml clean && mvn -f pom.xml package",
// }

// TODO: we change getBuildCmd into a script and execute it in run-time env.
// this default script is not verified yet
var buildCmd = `
#!/bin/bash
set -e

function getdir() {
	for element in $(ls $1); do
		dir_or_file=$1"/"$element
		if [ -d $dir_or_file ]; then
			getFile $dir_or_file
		else
			getFile $1
		fi
	done
}

function getFile() {
	execPath=$1
	for element in $(ls $1); do
		filePath=$1"/"$element
		if [ -f $filePath ]; then
			for key in ${!map[@]}; do
				if [ "$element" == "$key" ]; then
					cd $execPath
					echo "find file: $key ,path is :$filePath"
					echo "执行命令：${map[$key]} ......"
					${map[$key]}
					exit 0
				fi
			done
		fi
	done
}

function parseArgs() {
	index=0
	arr[0]=a

	for a in "$@"; do
		arr[$index]=$a
		let index+=1
	done

	for ((i = 1; i < $#; i = i + 2)); do
		key=${arr[$i]}
		value=${arr[$(($i + 1))]}
		map["$key"]="$value"
		echo "文件——>命令：$key——>${map[$key]}"
	done
}

function printHelp() {
	echo "-----------------------------------------------------------------"
	echo "用法示例： ./findFile.sh  [path] [fileName] [command]   ...."
	echo "-----------------------------------------------------------------"
}

declare -A map=()

#  至少3个参数
if [ $# -lt 3 ]; then
	echo "参数个数太少"
	printHelp
	exit 1
else
	parseArgs "$@"

	getdir $1
fi
`
var zeroTime time.Time

func (javaPlatform *Platform) WriteDockerRunTime(spec *pb.ChaincodeSpec, tw *tar.Writer) (dockertemplate string, err error) {

	//write buildcmd script
	tw.WriteHeader(&tar.Header{Name: "buildcmd",
		Mode: 0777,
		Size: int64(len(buildCmd)), ModTime: zeroTime,
		AccessTime: zeroTime, ChangeTime: zeroTime})
	tw.Write([]byte(buildCmd))

	var buf []string
	buf = append(buf, cutil.GetDockerfileFromConfig("chaincode.java.Dockerfile"))
	buf = append(buf, "COPY ccfile/ /root/chaincode")
	buf = append(buf, "COPY buildcmd /root/chaincode")
	buf = append(buf, "RUN  cd /root/chaincode && buildcmd")
	buf = append(buf, "RUN  cp /root/chaincode/build/chaincode.jar /root")
	buf = append(buf, "RUN  cp /root/chaincode/build/libs/* /root/libs")
	dockertemplate = strings.Join(buf, "\n")

	return
}

func getCodeFromHTTP(path string) (codegopath string, err error) {

	var tmp string
	tmp, err = ioutil.TempDir("", "javachaincode")

	if err != nil {
		return "", fmt.Errorf("Error creating temporary file: %s", err)
	}
	var out bytes.Buffer

	cmd := exec.Command("git", "clone", path, tmp)
	cmd.Stderr = &out
	cmderr := cmd.Run()
	if cmderr != nil {
		return "", fmt.Errorf("Error cloning git repository %s", cmderr)
	}

	return tmp, nil

}

// WritePackage writes the java chaincode package
func (javaPlatform *Platform) GetCodePath(path string) (rootpath string, packpath string, shouldclean bool, err error) {

	if strings.HasPrefix(path, "http://") ||
		strings.HasPrefix(path, "https://") {
		shouldclean = true
		rootpath, err = getCodeFromHTTP(path)
	} else if !strings.HasPrefix(path, "/") {
		wd := ""
		wd, err = os.Getwd()
		path = filepath.Join(wd, path)
		path = strings.TrimSuffix(path, "/")
		rootpath, packpath = filepath.Split(path)
	}

	return
}
