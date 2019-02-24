#!/bin/bash

export FABRIC_PATH=github.com/abchain/fabric
export FABRIC_TOP=${GOPATH}/src/${FABRIC_PATH}
export BUILD_BIN=${FABRIC_TOP}/build/bin

export PEER_CLIENT=peer
export PEER_CLIENT_PATH=peer

export PEER_DAEMON=testsync
export PEER_DAEMON_PATH=core/statesync/testsync

export FABRIC_DEV_SCRIPT_TOP=$FABRIC_TOP/scripts/dev
export CORE_PEER_DISCOVERY_PERSIST=false
export CORE_PEER_LOGPATH=${FABRIC_DEV_SCRIPT_TOP}




