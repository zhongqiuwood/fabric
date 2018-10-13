#!/bin/bash

export FABRIC_PATH=github.com/abchain/fabric
export FABRIC_TOP=${GOPATH}/src/${FABRIC_PATH}
export BUILD_BIN=${FABRIC_TOP}/build/bin
export PEER_CLIENT_BINARY=peer
export FABRIC_DEV_SCRIPT_TOP=$FABRIC_TOP/scripts/dev


export PEER_BINARY=embedded
#export PEER_BINARY=testsync

export BUILD_PEER_SCRIPT=build_${PEER_BINARY}








