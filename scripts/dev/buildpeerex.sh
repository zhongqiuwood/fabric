#!/bin/bash


FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric
BUILD_BIN=${FABRIC_TOP}/build/bin
PEER_BINARY=embedded

if [ ! -d ${BUILD_BIN} ]; then
    mkdir -p ${BUILD_BIN}
fi

if [ -f ${BUILD_BIN}/${PEER_BINARY} ]; then
    rm ${BUILD_BIN}/${PEER_BINARY}
fi

CGO_CFLAGS=" " CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" \
    GOBIN=${GOPATH}/src/github.com/abchain/fabric/build/bin go install github.com/abchain/fabric/examples/chaincode/go/embedded


if [ ! -f ${BUILD_BIN}/${PEER_BINARY} ]; then
    echo 'Failed to build '${BUILD_BIN}/${PEER_BINARY}
    exit -1
fi


if [ ! -L ${BUILD_BIN}/peerex ]; then
    cd ${BUILD_BIN}
    ln -s ${PEER_BINARY} peerex
fi
