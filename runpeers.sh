#!/bin/bash

##################################################################
# lvp start from 4
##################################################################

NUM_VP=$1
NUM_LVP=$2
NUM_NVP=$3
CONSENSUS=$4
CORE_PBFT_GENERAL_N=$5
CORE_PBFT_GENERAL_F=$6
ACTION_CLEAR=$7
export CORE_PEER_VALIDATOR_TRUSTEDVALIDATORS=$8

TAG_CLEAR="clear"
if [ $# -eq 0 ]; then
    echo "runpeers.sh <#vp> <#lvp> <#nvp> <pbft | noops> <N> <f> <clear>"
    exit
fi


if [ "$ACTION_CLEAR" = "clear" ];then
echo "aa" > vp0.log
echo "aa" > vp1.log
echo "aa" > vp2.log
echo "aa" > vp3.log
echo "aa" > lvp4.log
echo "aa" > lvp5.log
echo "aa" > lvp6.log
echo "aa" > lvp7.log
echo "aa" > lvp10000.log
echo "aa" > lvp10001.log
echo "aa" > lvp10002.log

echo "aa" > peer1.log
echo "aa" > peer2.log
echo "aa" > peer3.log
echo "aa" > peer0.log
echo "aa" > peer4.log
echo "aa" > peer5.log
echo "aa" > peer6.log
echo "aa" > peer7.log
echo "aa" > peer8.log
echo "aa" > peer9.log
rm -rf /var/hyperledger/*
fi

./makepeer.sh
./killbyname.sh peer_fabric_

index=0
let allpeer=$NUM_VP
while [ $index -lt $allpeer ]; do  
    #echo "run ./startpeer.sh  vp" $index  
    ./startpeer.sh vp $index $CONSENSUS $CORE_PBFT_GENERAL_N $CORE_PBFT_GENERAL_F
    let index=index+1   
done  

allpeer=0
let allpeer=index+$2
while [  $index -lt $allpeer ]; do  
    #echo "run ./startpeer.sh lvp" $index 
    ./startpeer.sh lvp $index $CONSENSUS $CORE_PBFT_GENERAL_N $CORE_PBFT_GENERAL_F
    let index=index+1   
done  

let allpeer=index+$3
while [  $index -lt $allpeer ]; do  
    #echo "run ./startpeer.sh lvp" $index 
    ./startpeer.sh nvp $index $CONSENSUS $CORE_PBFT_GENERAL_N $CORE_PBFT_GENERAL_F
    let index=index+1   
done  

ps -ef|grep "peer_fabric_"|grep -v grep |awk '{print "New processid: "$2 ", " $8}'
#ps -ef|grep "peer_fabric_"|grep -v grep
