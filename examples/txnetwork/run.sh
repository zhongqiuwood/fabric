#!/usr/bin/env bash

PEERADDRBASE=7055
PEERLOCALADDRBASE=7051
EVENTADDRBASE=7053
FILEPATHBASE=/var/hyperledger

function startpeer {

    index=$1
    ((ADDRPORT = PEERADDRBASE + index * 100))
    ((LOCADDRPORT = PEERLOCALADDRBASE + index * 100))
    ((EVENTADDRPORT = EVENTADDRBASE + index * 100))

    export CORE_PEER_LISTENADDRESS=127.0.0.1:${ADDRPORT}
    export CORE_PEER_ADDRESS=${CORE_PEER_LISTENADDRESS}
    export CORE_PEER_LOCALADDR=127.0.0.1:${LOCADDRPORT}
    export CORE_PEER_ID=billgates_${index}
    export CORE_PEER_VALIDATOR_EVENTS_ADDRESS=127.0.0.1:${EVENTADDRPORT}
    export CORE_PEER_FILESYSTEMPATH=${FILEPATHBASE}/txnet${index}

    export LOG_STDOUT_FILE=_stdout_${CORE_PEER_ID}.json
    ./txnetwork >> ${LOG_STDOUT_FILE} 2>>${LOG_STDOUT_FILE} &
}


function main {
    ./killbyname.sh txnetwork
    rm *_stdout_billgates_*.json
    for ((index=0; index<$1; index++))
    do
        startpeer ${index}
    done
}

main $1
