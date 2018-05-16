#!/bin/bash

EXECUTION=wkdir

BUILD_BIN=${FABRIC_TOP}/build/bin

#EXECUTION=${BUILD_BIN}/dbupgrade

./killbyname.sh peer_fabric


rm ./${EXECUTION}

go build

#GOBIN=${GOPATH}/src/github.com/abchain/fabric/build/bin go install github.com/abchain/fabric/tools/dbupgrade

./${EXECUTION} -dbpath /var/hyperledger/production$1

#ls -l /var/hyperledger/production$1/checkpoint/db
