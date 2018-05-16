#!/bin/bash


source fabric.profile


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
    deploy $1
    sleep 1
    invoke $1
}


function invoke {
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode invoke -l golang -n $1 \
            -c '{"Args":["invoke", "a", "b", "1"]}'
}

function query {
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 -c '{"Args":["query", "a"]}'
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 -c '{"Args":["query", "b"]}'
}

function deploy {
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode deploy -n $1 -p \
        $FABRIC_PATH/examples/chaincode/go/chaincode_example02 -c '{"Function":"init", "Args": ["a","1000", "b", "2000"]}'
}


if [ ! -f ${BUILD_BIN}/$PEER_CLIENT_BINARY ]; then
    $FABRIC_DEV_SCRIPT_TOP/buildpeer.sh
fi

if [ "$ACTION" = "d" ];then
    deploy $CCNAME
    exit
fi

if [ "$ACTION" = "i" ];then
    invoke $CCNAME
    exit
fi

if [ "$ACTION" = "q" ];then
    query $CCNAME
    exit
fi

if [ "$ACTION" = "di" ];then
    deploy $CCNAME
    sleep 1
    invoke $CCNAME
    exit
fi

if [ "$ACTION" = "id" ];then
    deploy $CCNAME
    sleep 1
    invoke $CCNAME
    exit
fi


