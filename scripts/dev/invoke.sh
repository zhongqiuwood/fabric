#!/bin/bash


FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric
BUILD_BIN=${FABRIC_TOP}/build/bin

if [ $# -lt 2 ]; then
    echo "invoke.sh <peer id> <chaincode name1> <chaincode name2> ..."
    exit
fi


if [ ! -f ${BUILD_BIN}/peer ]; then
    ${FABRIC_TOP}/scripts/dev/buildpeer.sh
fi


index=0

for i in $@; do
    if [ $index -eq 0 ]; then
        let index=index+1
    else
        CORE_PEER_LOCALADDR=127.0.0.1:2$156 ${FABRIC_TOP}/build/bin/peer chaincode invoke -l golang -n $i \
            -c '{"Args":["invoke", "a", "b", "1"]}'
    fi
done