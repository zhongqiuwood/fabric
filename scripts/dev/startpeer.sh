#!/bin/bash

PEER_MODE="$1"
PEER_ID="$2"
CONSENSUS=$3

TAG_LVP="lvp"
TAG_PBFT="pbft"
PORT_PREFIX=2

NUM_F=$4
let NUM_N=$NUM_F*3+1


FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric
BUILD_BIN=${FABRIC_TOP}/build/bin

if [ $# -eq 0 ]; then
    echo "startpeer.sh <PEER_MODE> <#PEER_ID> <pbft | noops> <f>"
    exit
fi

echo "======================================================"

FULL_PEER_ID=${PEER_MODE}${PEER_ID}

if [ "$PEER_MODE" = "$TAG_LVP" ];then
    FULL_PEER_ID=${PEER_MODE}${PEER_ID}
fi

if [ $PEER_ID -gt 0 ];then
    export CORE_PEER_DISCOVERY_ROOTNODE=127.0.0.1:${PORT_PREFIX}055
    echo "The <$FULL_PEER_ID> started up with <$CONSENSUS> consensus. CORE_PEER_DISCOVERY_ROOTNODE=$CORE_PEER_DISCOVERY_ROOTNODE"
else
    echo "The lead peer <$FULL_PEER_ID> started up with <$CONSENSUS> consensus"
fi


if [ "$CONSENSUS" = "$TAG_PBFT" ];then
    export CORE_PBFT_GENERAL_N=$NUM_N
    export CORE_PBFT_GENERAL_F=$NUM_F
    #echo "CORE_PBFT_GENERAL_N=$CORE_PBFT_GENERAL_N, CORE_PBFT_GENERAL_F=$CORE_PBFT_GENERAL_F"
fi

echo "======================================================"
export CORE_PEER_VALIDATOR_CONSENSUS_PLUGIN=$CONSENSUS

export CORE_LOGGING_OUTPUT_FILE=peer${PEER_ID}.json
#export CORE_LOGGING_OUTPUTFILE=${FULL_PEER_ID}.json

export CORE_CLI_ADDRESS=127.0.0.1:${PORT_PREFIX}${PEER_ID}52
export CORE_REST_ADDRESS=127.0.0.1:${PORT_PREFIX}${PEER_ID}50

export CORE_SERVICE_ADDRESS=127.0.0.1:${PORT_PREFIX}${PEER_ID}51
export CORE_SERVICE_CLIADDRESS=127.0.0.1:${PORT_PREFIX}${PEER_ID}51

export CORE_PEER_ID=${FULL_PEER_ID}
export CORE_PEER_LOCALADDR=127.0.0.1:${PORT_PREFIX}${PEER_ID}56
export CORE_PEER_LISTENADDRESS=127.0.0.1:${PORT_PREFIX}${PEER_ID}55
export CORE_PEER_ADDRESS=127.0.0.1:${PORT_PREFIX}${PEER_ID}55
export CORE_PEER_CLIADDRESS=127.0.0.1:${PORT_PREFIX}${PEER_ID}51

export CORE_PEER_VALIDATOR_EVENTS_ADDRESS=0.0.0.0:${PORT_PREFIX}${PEER_ID}53
export CORE_PEER_VALIDATOR_MODE=${PEER_MODE}

export CORE_PEER_VALIDATOR_EVENTS_ADDRESS=0.0.0.0:${PORT_PREFIX}${PEER_ID}53
export CORE_PEER_PKI_ECA_PADDR=localhost:${PORT_PREFIX}${PEER_ID}54
export CORE_PEER_PKI_TCA_PADDR=localhost:${PORT_PREFIX}${PEER_ID}54
export CORE_PEER_PKI_TLSCA_PADDR=localhost:${PORT_PREFIX}${PEER_ID}54
export CORE_PEER_PROFILE_LISTENADDRESS=0.0.0.0:${PORT_PREFIX}${PEER_ID}60

export CORE_PEER_FILESYSTEMPATH=/var/hyperledger/production${PEER_ID}

if [ ! -f ${BUILD_BIN}/peer_fabric_${PEER_ID} ]; then
    cd ${BUILD_BIN}
    ln -s peerex peer_fabric_${PEER_ID}
    cd ../..
fi

if [ ! -d ${FABRIC_TOP}/stderrdir ]; then
    mkdir ${FABRIC_TOP}/stderrdir
fi

# build/bin/peer_fabric_${PEER_ID} node start
#${BUILD_BIN}/peer_fabric_${PEER_ID} node start
#nohup ${BUILD_BIN}/peer_fabric_${PEER_ID} node start > /dev/null 2>${FABRIC_TOP}/stderrdir/err_nohup_peer${PEER_ID}.json &
nohup ${BUILD_BIN}/peer_fabric_${PEER_ID} node start > ${FABRIC_TOP}/nohup_peer${PEER_ID}.log 2>&1 &
