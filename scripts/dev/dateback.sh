#!/bin/bash

PRODUCTION_TOP=/var/hyperledger/production
PEER_ID=$1
CHECKPOINT_NAME=$2

if [ $# -eq 1 ]; then
    ls -l ${PRODUCTION_TOP}${PEER_ID}/checkpoint/db
    exit
fi

if [ $# -lt 2 ]; then
    echo "past.sh <peer id> <statehash>"
    exit
fi

./killbyname.sh peer_fabric

rm -rf ${PRODUCTION_TOP}${PEER_ID}/db

cp -rf ${PRODUCTION_TOP}${PEER_ID}/checkpoint/db/${CHECKPOINT_NAME} ${PRODUCTION_TOP}${PEER_ID}/db
