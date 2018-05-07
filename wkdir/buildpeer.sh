#!/bin/bash

FABRIC_PATH=github.com/abchain/wood/fabric
FABRIC_TOP=${GOPATH}/src/$FABRIC_PATH
BUILD_BIN=${FABRIC_TOP}/build/bin


if [ ! -d ${BUILD_BIN} ]; then
    mkdir -p ${BUILD_BIN}
fi

if [ -f ${BUILD_BIN}/peer ]; then
    rm ${BUILD_BIN}/peer
fi

CGO_CFLAGS=" " CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" \
    GOBIN=${GOPATH}/src/$FABRIC_PATH/build/bin go install $FABRIC_PATH/peer


if [ ! -f ${BUILD_BIN}/peer ]; then
    echo 'Failed to build peer!'
    exit -1
fi
