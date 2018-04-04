#!/bin/bash

NUM_F=$1
TAG_CLEAR=$2

FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric

if [ $# -eq 0 ]; then
    echo "runpbft.sh <f> <clear>"
    exit
fi

CONSENSUS=pbft
let NUM_N=$NUM_F*3+1

${FABRIC_TOP}/scripts/dev/runpeers.sh $NUM_N 0 0 $CONSENSUS $NUM_F $TAG_CLEAR
