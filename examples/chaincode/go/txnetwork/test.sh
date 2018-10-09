#!/usr/bin/env bash

PEERADDRBASE=7055
PEERLOCALADDRBASE=7051
EVENTADDRBASE=7053
FILEPATHBASE=/var/hyperledger

function invokebody {
    ./peer chaincode invoke -n txnetwork -c "{\"Function\": \"invoke\", \"Args\": [\"aa\",\"$1\"]}"
}

function testbody {
    PEERLOCALADDRBASE=7051
    let LOCADDRPORT=${PEERLOCALADDRBASE}+$1*100
    export CORE_PEER_LOCALADDR=127.0.0.1:${LOCADDRPORT}

    ./peer network status
    ./peer chaincode query -n txnetwork -c "{\"Function\": \"count\", \"Args\": []}"
    ./peer chaincode query -n txnetwork -c "{\"Function\": \"status\", \"Args\": []}"
}

function main {

    for ((index=0; index<${1}; index++)) do
        invokebody ${index}
    done

    sleep 1
    ./peer network status
    ./peer chaincode query -n txnetwork -c "{\"Function\": \"count\", \"Args\": []}"

    for ((index=0; index<${2}; index++)) do
        invokebody ${index}
    done
}

main $1 $2


