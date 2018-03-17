#!/bin/bash

##################################################################
# lvp start from 4
##################################################################

NUM_VP=$1
NUM_LVP=$2
NUM_NVP=$3
CONSENSUS=$4
CORE_PBFT_GENERAL_F=$5
ACTION_CLEAR=$6
export CORE_PEER_VALIDATOR_TRUSTEDVALIDATORS=$8

TAG_CLEAR="clear"
if [ $# -eq 0 ]; then
    echo "runpeers.sh <#vp> <#lvp> <#nvp> <pbft | noops> <f> <clear>"
    exit
fi


if [ "$ACTION_CLEAR" = "clear" ];then
    echo '' > vp0.json
    rm -rf /var/hyperledger/*
fi

./buildpeer.sh

BUILD_BIN=build/bin

if [ ! -f ${BUILD_BIN}/peer ]; then
    echo 'No such a file: '${BUILD_BIN}'/peer'
    exit -1
fi

./killbyname.sh peer_fabric_

index=0
let allpeer=$NUM_VP
while [ $index -lt $allpeer ]; do
    ./startpeer.sh vp $index $CONSENSUS $CORE_PBFT_GENERAL_F
    let index=index+1
done  

allpeer=0
let allpeer=index+$2
while [  $index -lt $allpeer ]; do
    ./startpeer.sh lvp $index $CONSENSUS $CORE_PBFT_GENERAL_F
    let index=index+1
done  

let allpeer=index+$3
while [  $index -lt $allpeer ]; do
    ./startpeer.sh nvp $index $CONSENSUS $CORE_PBFT_GENERAL_F
    let index=index+1
done  

ps -ef|grep "peer_fabric_"|grep -v grep |awk '{print "New processid: "$2 ", " $8}'
