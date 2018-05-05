#!/bin/bash


FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric
BUILD_BIN=${FABRIC_TOP}/build/bin

ACTION=$1
CCNAME=$2
PEER_ID=0

function deployAndInvoke {
    ./deploy.sh $CCNAME
    sleep 1
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/peer chaincode invoke -l golang -n $CCNAME \
            -c '{"Args":["invoke", "a", "b", "1"]}'
}

if [ $# -eq 0 ]; then
    ACTION=i
    CCNAME=mycc
fi

if [ $# -eq 1 ]; then
    CCNAME=mycc
fi

if [ ! -f ${BUILD_BIN}/peer ]; then
    ${FABRIC_TOP}/scripts/dev/buildpeer.sh
fi

if [ "$ACTION" = "d" ];then
    ./deploy.sh $CCNAME
    exit
fi

if [ "$ACTION" = "i" ];then
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/peer chaincode invoke -l golang -n $CCNAME \
            -c '{"Args":["invoke", "a", "b", "1"]}'
    exit
fi

if [ "$ACTION" = "q" ];then
    ./querya.sh $CCNAME
    exit
fi

if [ "$ACTION" = "di" ];then
    deployAndInvoke
    exit
fi

if [ "$ACTION" = "id" ];then
    deployAndInvoke
    exit
fi


