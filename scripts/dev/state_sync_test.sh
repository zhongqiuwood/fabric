#!/bin/bash

# PEER_NUM - 1 is the number of concurrent sync
PEER_NUM=4

function sleep_and_invoke {
    sleep 2
    ./invoke.sh $1
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


function sync_test {

    echo '=============================================='
    echo '============= sync_test started =============='

    start_peers ${PEER_NUM} clearall
    sleep_and_invoke di

    start_peers 1 none
    sleep_and_invoke i
    sleep_and_invoke i
    sleep_and_invoke i

    start_peers_and_sync ${PEER_NUM}

    sleep 20

    echo '=========================================================================='
    echo '============= sync_test: dump vp0 and vp1 db compare result =============='
    ./cmpscandb.sh


    echo '============= sync_test finished =============='
    echo '==============================================='

}


function sync_from_genesis_test {

    echo '==========================================================='
    echo '============= sync_from_genesis_test started =============='
    start_peers 1 clearall
    sleep_and_invoke di
    sleep_and_invoke i
    sleep_and_invoke i
    sleep_and_invoke i

    start_peers_and_sync ${PEER_NUM}

    sleep 20

    echo '======================================================================================='
    echo '============= sync_from_genesis_test: dump vp0 and vp1 db compare result =============='
    ./cmpscandb.sh


    echo '============= sync_from_genesis_test finished =============='
    echo '============================================================'
}


sync_from_genesis_test

sync_test