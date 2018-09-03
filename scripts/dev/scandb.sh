#!/bin/bash


export FABRIC_PATH=github.com/abchain/fabric
export FABRIC_TOP=${GOPATH}/src/${FABRIC_PATH}

EXECUTION=$FABRIC_TOP/tools/dbutility/dbscan/dbscan

./killbyname.sh peer_fabric
rm ${EXECUTION}

cd $FABRIC_TOP/tools/dbutility/dbscan
go build
${EXECUTION} $@

