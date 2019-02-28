#!/bin/bash

# PEER_NUM - 1 is the number of concurrent sync
PEER_NUM=2
BACKUP_TOP=/var/hyperledger/dev_backup
DB_TOP=/var/hyperledger

function sleep_and_invoke {
    sleep 2
    ./invoke.sh $@
}

function start_peers {
    sleep 3
    ./killbyname.sh peer_fabric
    ./runyafabric.sh -f $1 -c n -r $2
}

function start_peers_and_sync {
    sleep 3
    ./killbyname.sh peer_fabric
    ./runyafabric.sh -f $1 -c n -s vp0
}

function backup_ledger {
    DIR_NAME=$1
    if [ ! -z "${DIR_NAME}" ]; then
        mkdir -p ${BACKUP_TOP}/${DIR_NAME}
        rm -rf ${BACKUP_TOP}/${DIR_NAME}/*
        cp -rf ${DB_TOP}/production* ${BACKUP_TOP}/${DIR_NAME}
    fi
}

function copy_diff_from_disk {
    rm -rf ${DB_TOP}/production*
    rm -rf _stdout*.json
    cp -rf ${BACKUP_TOP}/$1/production* ${DB_TOP}
}

function sync_from_disk {

    export CORE_PEER_SYNCTYPE=$2
    copy_diff_from_disk $1
    start_peers_and_sync ${PEER_NUM}
}

function make_huge_diff {
    start_peers 1 clearall
    sleep_and_invoke -a d -n 500 -c example01_
    sleep_and_invoke -a d -n 500 -c example02_
    sleep_and_invoke -a d -n 500 -c example03_
    for ((index=0; index<10; index++))
    do
        sleep_and_invoke -a i -c example01_ -n 1000
        sleep_and_invoke -a i -c example02_ -n 1000
        sleep_and_invoke -a i -c example03_ -n 1000
    done
    sleep 20
    backup_ledger $1
}

function make_diff {
    start_peers ${PEER_NUM} clearall
    sleep_and_invoke -a d -n 10 -c example01_
    sleep_and_invoke -a d -n 10 -c example02_
    sleep_and_invoke -a i -c example01_ -n 20
    sleep_and_invoke -a i -c example02_ -n 20
    start_peers 1 none
    sleep_and_invoke -a i -c example01_ -n 20
    sleep_and_invoke -a i -c example02_ -n 20
    sleep_and_invoke -a i -c example01_ -n 20
    sleep_and_invoke -a i -c example02_ -n 20
    sleep 10
    backup_ledger $1
}

function sync_breakpoint_test {

    $1 $1

    export CORE_PEER_BREAKPOINT=true
    start_peers_and_sync ${PEER_NUM}

    sleep 20

    export CORE_PEER_BREAKPOINT=false
    start_peers_and_sync ${PEER_NUM}
}


function sync_test {

    export CORE_PEER_SYNCTYPE=$2
    $1 $1
    start_peers_and_sync ${PEER_NUM}
}

#sync_test make_diff true
#sync_test make_diff false

#sync_test make_huge_diff true
#sync_test make_huge_diff false


function loadenv {

    export CORE_LOGGING_NODE=debug:statesync=debug:state=info:buckettree=debug:peer=info:statesyncstub=debug:ledger=debug
#    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_NUMBUCKETS=1000003
#    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_MAXGROUPINGATEACHLEVEL=5
#    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_SYNCDELTA=100
#
    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_NUMBUCKETS=32
    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_MAXGROUPINGATEACHLEVEL=3
    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_SYNCDELTA=1
}

loadenv
#sync_breakpoint_test make_diff
#sync_test make_diff state

sync_from_disk make_diff state


