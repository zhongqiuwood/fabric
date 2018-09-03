#!/bin/bash


source fabric.profile


ACTION=$1
CCNAME=$2
PEER_ID=$3

if [ "$PEER_ID" = "" ];then
    PEER_ID=0
fi

if [ "$CCNAME" = "" ];then
    CCNAME=example02
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

function invoke10 {
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode invoke -l golang -n $1 \
            -c '{"Args":["invoke", "a", "b", "10"]}'
}

function invoke100 {
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode invoke -l golang -n $1 \
            -c '{"Args":["invoke", "a", "b", "100"]}'
}

function open {
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode invoke -l golang -n $1 \
            -c '{"Args":["open", "a", "100000"]}'
    CORE_PEER_LOCALADDR=127.0.0.1:2${PEER_ID}56 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode invoke -l golang -n $1 \
            -c '{"Args":["open", "b", "200000"]}'
}

function query {
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 -c '{"Args":["query", "a"]}'
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 -c '{"Args":["query", "b"]}'
}

function querytx {
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 -c '{"Args":["querytx", "5beb88174d5b323161719fd833e70eb88afc915a312ef1ad77d94b2ec6c00a6c"]}'
}

#//f8378d8c20c419488aa8ec01ce3fb0732a862d28ce9efeda70cbd7811f606b76

function hastx {
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode query -n $1 -c '{"Args":["hastx", "f878d8c20c419488aa8ec01ce3fb0732a862d28ce9efeda70cbd7811f606b76"]}'
}

function deploy {
    CORE_PEER_LOCALADDR=127.0.0.1:2056 ${FABRIC_TOP}/build/bin/$PEER_CLIENT_BINARY chaincode deploy -n $1 -c '{"Function":"init", "Args": ["a","100000", "b", "200000"]}'
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
if [ "$ACTION" = "i10" ];then
    invoke10 $CCNAME
    exit
fi

if [ "$ACTION" = "i100" ];then
    invoke100 $CCNAME
    exit
fi

if [ "$ACTION" = "o" ];then
    open $CCNAME
    exit
fi

if [ "$ACTION" = "q" ];then
    query $CCNAME
    exit
fi

if [ "$ACTION" = "qtx" ];then
    querytx $CCNAME
    exit
fi

if [ "$ACTION" = "htx" ];then
    hastx $CCNAME
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


