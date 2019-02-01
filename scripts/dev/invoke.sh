#!/bin/bash

source fabric.profile

ACTION=$1
AMOUNT=$2
CCNAME=$3
PEER_ID=$4

if [ "$ACTION" = "" ];then
    ACTION=i
fi

if [ "$AMOUNT" = "" ];then
    AMOUNT=1
fi

if [ "$CCNAME" = "" ];then
    CCNAME=example02
fi

if [ "$PEER_ID" = "" ];then
    PEER_ID=0
fi

function deployAndInvoke {
    deploy $1
    sleep 1
    invoke $1 1
}

PEER_PORT=2055


function invoke {
    CORE_SERVICE_CLIADDRESS=127.0.0.1:${PEER_PORT} ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode invoke -l golang -n $1 -c "{\"Args\":[\"invoke\", \"a\", \"b\", \"$2\"]}"
}

function query {
    CORE_SERVICE_CLIADDRESS=127.0.0.1:${PEER_PORT} ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 \
        -c '{"Args":["query", "a"]}'
    CORE_SERVICE_CLIADDRESS=127.0.0.1:${PEER_PORT} ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 \
        -c '{"Args":["query", "b"]}'
}


function deploy {
    CORE_SERVICE_CLIADDRESS=127.0.0.1:${PEER_PORT} ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode deploy -n $1 \
        -c '{"Function":"init", "Args": ["a","100000", "b", "200000"]}'
}

function main {

    if [ ! -f ${BUILD_BIN}/$PEER_CLIENT_BINARY ]; then
        $FABRIC_DEV_SCRIPT_TOP/buildpeer.sh
    fi

    if [ "$ACTION" = "d" ];then
        deploy ${CCNAME}
        exit
    fi

    if [ "$ACTION" = "i" ];then
        invoke ${CCNAME} ${AMOUNT}
        exit
    fi


    if [ "$ACTION" = "q" ];then
        query ${CCNAME}
        exit
    fi


    if [ "$ACTION" = "di" ];then
        deploy ${CCNAME}
        sleep 1
        invoke ${CCNAME}
        exit
    fi

    if [ "$ACTION" = "id" ];then
        deploy ${CCNAME}
        sleep 1
        invoke ${CCNAME}
        exit
    fi

}

main