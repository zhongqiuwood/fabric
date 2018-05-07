#!/bin/bash

EXECUTION=wkdir

BUILD_BIN=${FABRIC_TOP}/build/bin

./killbyname.sh peer_fabric

rm ./${EXECUTION}
go build
./${EXECUTION} -dbpath /var/hyperledger/production$1
