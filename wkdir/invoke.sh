#!/bin/bash


FABRIC_PATH=github.com/abchain/wood/fabric
FABRIC_TOP=${GOPATH}/src/$FABRIC_PATH
BUILD_BIN=${FABRIC_TOP}/build/bin

ACTION=$1
CCNAME=$2
PEER_ID=$3

if [ "$PEER_ID" = "" ];then
    PEER_ID=0
fi

if [ "$CCNAME" = "" ];then
    CCNAME=mycc
fi

if [ "$ACTION" = "" ];then
    ACTION=i
fi

function deployAndInvoke {
    ./deploy.sh $CCNAME
    sleep 1
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/peer chaincode invoke -l golang -n $CCNAME \
            -c '{"Args":["invoke", "a", "b", "1"]}'
}


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
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/peer chaincode query -n $CCNAME -c '{"Args":["query", "a"]}'
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/peer chaincode query -n $CCNAME -c '{"Args":["query", "b"]}'
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


