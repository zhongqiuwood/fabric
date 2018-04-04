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

FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric
BUILD_BIN=${FABRIC_TOP}/build/bin


TAG_CLEAR="clear"
if [ $# -eq 0 ]; then
    echo "runpeers.sh <#vp> <#lvp> <#nvp> <pbft | noops> <f> <clear>"
    exit
fi


if [ "$ACTION_CLEAR" = "clearall" ];then
    echo 'a' > peer0.json
    echo 'a' > peer1.json
    echo 'a' > peer2.json
    echo 'a' > peer4.json
    rm -rf /var/hyperledger/*
fi


if [ "$ACTION_CLEAR" = "clearlog" ];then
    echo 'a' > peer0.json
    echo 'a' > peer1.json
    echo 'a' > peer2.json
    echo 'a' > peer4.json
fi


if [ "$ACTION_CLEAR" = "cleardb" ];then
    rm -rf /var/hyperledger/*
fi


${FABRIC_TOP}/scripts/dev/buildpeerex.sh


if [ ! -f ${BUILD_BIN}/embedded ]; then
    echo 'No such a file: '${BUILD_BIN}'/embedded'
    exit -1
fi

${FABRIC_TOP}/scripts/dev/killbyname.sh peer_fabric_

index=0
let allpeer=$NUM_VP
while [ $index -lt $allpeer ]; do
    ${FABRIC_TOP}/scripts/dev/startpeer.sh vp $index $CONSENSUS $CORE_PBFT_GENERAL_F
    let index=index+1
done  

allpeer=0
let allpeer=index+$2
while [  $index -lt $allpeer ]; do
    ${FABRIC_TOP}/scripts/dev/startpeer.sh lvp $index $CONSENSUS $CORE_PBFT_GENERAL_F
    let index=index+1
done  

let allpeer=index+$3
while [  $index -lt $allpeer ]; do
    ${FABRIC_TOP}/scripts/dev/startpeer.sh nvp $index $CONSENSUS $CORE_PBFT_GENERAL_F
    let index=index+1
done  

ps -ef|grep "peer_fabric_"|grep -v grep |awk '{print "New processid: "$2 ", " $8}'
