#!/bin/bash

FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric

BUILD_BIN=${FABRIC_TOP}/build/bin

if [ ! -f ${BUILD_BIN}/peer ]; then
    ${FABRIC_TOP}/scripts/dev/buildpeer.sh
fi

CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/peer chaincode query -n $1 -c '{"Args":["query", "a"]}'


