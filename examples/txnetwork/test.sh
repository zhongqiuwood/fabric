#!/usr/bin/env bash

PEERLOCALADDRBASE=7055

PEER_BINARY=../../peer/peer

function invokebody {

    ((LOCADDRPORT = PEERLOCALADDRBASE + $1 * 100))
    export CORE_SERVICE_CLIADDRESS=127.0.0.1:${LOCADDRPORT}
    ${PEER_BINARY} chaincode invoke -n txnetwork -c "{\"Function\": \"invoke\", \"Args\": [\"aa\",\"$1\"]}"
}

function testbody {

    ((LOCADDRPORT = PEERLOCALADDRBASE + $1 * 100))
    export CORE_SERVICE_CLIADDRESS=127.0.0.1:${LOCADDRPORT}

    ${PEER_BINARY} network status
    ${PEER_BINARY} chaincode query -n txnetwork -c "{\"Function\": \"count\", \"Args\": []}"
    ${PEER_BINARY} chaincode query -n txnetwork -c "{\"Function\": \"status\", \"Args\": []}"
}

function main {

    for ((index=0; index<$1; index++)) do
        invokebody ${index}
    done

    sleep 1

    export CORE_SERVICE_CLIADDRESS=127.0.0.1:${PEERLOCALADDRBASE}
    ${PEER_BINARY} network status
    ${PEER_BINARY} chaincode query -n txnetwork -c "{\"Function\": \"count\", \"Args\": []}"

    for ((index=0; index<$2; index++)) do
        testbody ${index}
    done
}

function build {
    cd ../../peer
    go build
    cd ../examples/txnetwork
    go build
}

build
main $1 $2




