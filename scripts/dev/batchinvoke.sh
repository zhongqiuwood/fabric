#!/bin/bash

FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric

${FABRIC_TOP}/scripts/dev/invoke.sh 0 mycc
${FABRIC_TOP}/scripts/dev/rinvoke.sh 0 mycc