#!/bin/bash

# PEER_NUM-1 is the number of concurrent sync
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

start_peers ${PEER_NUM} clearall
sleep_and_invoke di

start_peers 1 none
sleep_and_invoke i
sleep_and_invoke i
sleep_and_invoke i

start_peers_and_sync ${PEER_NUM}

sleep 20
./cmpscandb.sh
