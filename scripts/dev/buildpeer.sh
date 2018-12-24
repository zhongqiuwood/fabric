#!/bin/bash

source fabric.profile

if [ ! -d ${BUILD_BIN} ]; then
    mkdir -p ${BUILD_BIN}
fi

if [ -f ${BUILD_BIN}/${PEER_CLIENT} ]; then
    rm ${BUILD_BIN}/${PEER_CLIENT}
fi

CGO_CFLAGS=" " CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" GOBIN=$BUILD_BIN go install $FABRIC_PATH/${PEER_CLIENT}


if [ ! -f ${BUILD_BIN}/${PEER_BINARY} ]; then
    echo "Failed to build ${PEER_CLIENT}!"
    exit -1
fi