#!/bin/bash


NUM_F=$1
TAG_CLEAR=$2

CONSENSUS=pbft
let NUM_N=$NUM_F*3+1

if [ $# -eq 0 ]; then
    echo "runpeers.sh <#vp> <#lvp> <#nvp> <pbft | noops> <N> <f> <clear>"
    exit
fi

./runpeers.sh $NUM_N 0 0 $CONSENSUS $NUM_F $TAG_CLEAR
