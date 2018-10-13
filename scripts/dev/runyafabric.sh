#!/bin/bash
source fabric.profile

while getopts "i:k:c:f:r:b:s:" opt; do
  case $opt in
    b)
      echo "BUILD_VERSION = $OPTARG"
      BUILD_VERSION=$OPTARG
      ;;
    i)
      echo "TARGET_PEER_ID = $OPTARG"
      TARGET_PEER_ID=$OPTARG
      ;;
    k)
      echo "TARGET_STOPPED_PEER_ID = $OPTARG"
      TARGET_STOPPED_PEER_ID=$OPTARG
      ;;
    c)
      echo "CONSENSUS_INPUT = $OPTARG"
      CONSENSUS_INPUT=$OPTARG
      ;;
    r)
      echo "TAG_CLEAR = $OPTARG"
      TAG_CLEAR=$OPTARG
      ;;
    f)
      echo "NUM_F = $OPTARG"
      NUM_F=$OPTARG
      ;;
    s)
      echo "SYNC_TARGET = $OPTARG"
      SYNC_TARGET=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      ;;
  esac
done

echo 'FABRIC_PATH = '${FABRIC_PATH}
echo "FABRIC_TOP = $FABRIC_TOP"


function init {

    DB_VERSION=0
    CONSENSUS=pbft

    if [ "$NUM_F" = "" ]; then
        echo "runyafabric.sh  -c <p|n> -f NUM_F -r -i TARGET_PEER_ID"
        exit
    fi

    let NUM_N=$NUM_F*3+1
    if [ "$CONSENSUS_INPUT" = "n" ];then
        CONSENSUS=noops
        let NUM_N=$NUM_F
    fi

    FABRIC_DB_PATH=/var/hyperledger/production

}

function clearLog {
    rm peer*.json
    rm err_nohup_peer*.json
}

function clearBin {
    rm -rf $FABRIC_TOP/build/bin/*
}

function runpeers {
    NUM_VP=$1
    NUM_LVP=$2
    NUM_NVP=$3
    CONSENSUS=$4
    CORE_PBFT_GENERAL_F=$5
    ACTION_CLEAR=$6
    export CORE_PEER_VALIDATOR_TRUSTEDVALIDATORS=$8

    if [ $# -eq 0 ]; then
        echo "runpeers.sh <#vp> <#lvp> <#nvp> <pbft | noops> <f> <clear>"
        exit
    fi

    if [ "$ACTION_CLEAR" = "clearall" ];then
        clearLog
        clearBin
        rm -rf ${FABRIC_DB_PATH}*
    fi


    if [ "$ACTION_CLEAR" = "clearlog" ];then
        clearBin
        clearLog
    fi

    
    if [ "$ACTION_CLEAR" = "cleardb" ];then
        clearBin
        rm -rf ${FABRIC_DB_PATH}*
    fi

    killbyname peer_fabric_

    build_peer_process ${BUILD_PEER_SCRIPT}


    index=0
    let allpeer=$NUM_VP
    while [ $index -lt $allpeer ]; do
        startpeer vp $index $CONSENSUS $CORE_PBFT_GENERAL_F
        let index=index+1
    done

    allpeer=0
    let allpeer=index+$2
    while [  $index -lt $allpeer ]; do
        startpeer lvp $index $CONSENSUS $CORE_PBFT_GENERAL_F
        let index=index+1
    done

    let allpeer=index+$3
    while [  $index -lt $allpeer ]; do
        startpeer nvp $index $CONSENSUS $CORE_PBFT_GENERAL_F
        let index=index+1
    done

    ps -ef|grep "peer_fabric_"|grep -v grep |awk '{print "New processid: "$2 ", " $8}'
}


function build_peer {
    CGO_CFLAGS=" " CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" \
        GOBIN=${GOPATH}/src/$FABRIC_PATH/build/bin go install $FABRIC_PATH/peer
}

function build_embedded {
    CGO_CFLAGS=" " CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" \
        GOBIN=${GOPATH}/src/$FABRIC_PATH/build/bin go install $FABRIC_PATH/examples/chaincode/go/embedded
}

function build_testsync {
    CGO_CFLAGS=" " CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" \
        GOBIN=${GOPATH}/src/$FABRIC_PATH/build/bin go install $FABRIC_PATH/core/statesync/testsync
}

function build_peer_process {

    clearBin

    if [ ! -d ${BUILD_BIN} ]; then
        mkdir -p ${BUILD_BIN}
    fi

    if [ -f ${BUILD_BIN}/${PEER_BINARY} ]; then
        rm ${BUILD_BIN}/${PEER_BINARY}
    fi

    if [ -L ${BUILD_BIN}/peerex ]; then
        rm ${BUILD_BIN}/peerex
    fi

    $1

    if [ ! -f ${BUILD_BIN}/${PEER_BINARY} ]; then
        echo 'Failed to build '${BUILD_BIN}/${PEER_BINARY}
        exit
    fi

    if [ ! -L ${BUILD_BIN}/peerex ]; then
        cd ${BUILD_BIN}
        ln -fnsv ${PEER_BINARY} peerex
    fi
}


function killbyname {
    NAME=$1
    MYNAME="runpbft.sh"

    if [ $# -eq 0 ]; then
        echo "$MYNAME <process name>"
        exit
    fi

    ps -ef|grep "$NAME"|grep -v grep |grep -v $MYNAME |awk '{print "kill -9 "$2", "$8}'
    ps -ef|grep "$NAME"|grep -v grep |grep -v $MYNAME |awk '{print "kill -9 "$2}' | sh
    echo "All <$NAME*> killed!"
}

function startpeer {

    PEER_MODE=$1
    PEER_ID=$2
    CONSENSUS=$3
    NUM_F=$4
    TAG_LVP="lvp"
    TAG_PBFT="pbft"
    PORT_PREFIX=2
    let NUM_N=$NUM_F*3+1

    if [ $# -eq 0 ]; then
        echo "startpeer.sh <PEER_MODE> <#PEER_ID> <pbft | noops> <f>"
        exit
    fi

    echo "======================================================"

    FULL_PEER_ID=${PEER_MODE}${PEER_ID}


    if [ $PEER_ID -gt 0 ];then
        export CORE_PEER_DISCOVERY_ROOTNODE=127.0.0.1:${PORT_PREFIX}055
        echo "The <$FULL_PEER_ID> started up with <$CONSENSUS> consensus. CORE_PEER_DISCOVERY_ROOTNODE=$CORE_PEER_DISCOVERY_ROOTNODE"
    else
        echo "The lead peer <$FULL_PEER_ID> started up with <$CONSENSUS> consensus"
    fi

    if [ "$CONSENSUS" = "$TAG_PBFT" ];then
        export CORE_PBFT_GENERAL_N=$NUM_N
        export CORE_PBFT_GENERAL_F=$NUM_F
    fi
    echo "======================================================"

    export CORE_PEER_TXNETWORK_ENABLE=false

    if [ ! "${FULL_PEER_ID}" = "${SYNC_TARGET}" ];then
        export CORE_PEER_SYNCTARGET=${SYNC_TARGET}
        export CORE_PEER_MYID=${FULL_PEER_ID}
        export CORE_PEER_ENABLESTATESYNCTEST=true
    fi

    export CORE_PEER_DB_VERSION=${DB_VERSION}
    export CORE_PEER_VALIDATOR_CONSENSUS_PLUGIN=$CONSENSUS

    export CORE_LOGGING_OUTPUT_FILE=peer${PEER_ID}.json
    export CORE_PEER_LOGPATH=${FABRIC_DEV_SCRIPT_TOP}

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

    export CORE_PEER_FILESYSTEMPATH=${FABRIC_DB_PATH}${PEER_ID}

    export LOG_STDOUT_FILE=$CORE_PEER_LOGPATH/_stdout_$CORE_LOGGING_OUTPUT_FILE

    export CORE_PEER_FILESYSTEMPATH=${FABRIC_DB_PATH}${PEER_ID}

#    export CORE_LOGGING_NODE=info:ledger=debug:db=debug:consensus/noops=debug:consensus/executor=debug:buckettree2=debug:statemgmt=debug
#    export CORE_LOGGING_NODE=info:statesync=debug:ledger=debug:nodeCmd=debug:peer=debug
    export CORE_LOGGING_NODE=info:statesync=debug:ledger=debug


    if [ ! -f ${BUILD_BIN}/peer_fabric_${PEER_ID} ]; then
        cd ${BUILD_BIN}
        ln -s peerex peer_fabric_${PEER_ID}
        cd ../..
    fi


    if [ "$ACTION_CLEAR" = "clearall" ] || [ "$ACTION_CLEAR" = "clearlog" ] || [ "$ACTION_CLEAR" = "cleardb" ];then
        echo '' > ${LOG_STDOUT_FILE}
    fi

    nohup ${BUILD_BIN}/peer_fabric_${PEER_ID} node start >> ${LOG_STDOUT_FILE} 2>>${LOG_STDOUT_FILE} &
#    ${BUILD_BIN}/peer_fabric_${PEER_ID} node start
}

function main {

     if [ "${BUILD_VERSION}" = "6" ]; then
          build_peer_process build_peer
          exit
     fi

     if [ "${BUILD_VERSION}" = "8" ]; then
          build_peer_process build_embedded
          exit
     fi

     if [ "$TARGET_STOPPED_PEER_ID" != "" ];then
        killbyname peer_fabric_$TARGET_STOPPED_PEER_ID
        exit
     fi

     init

     if [ "$TARGET_PEER_ID" = "" ];then
        runpeers $NUM_N 0 0 $CONSENSUS $NUM_F $TAG_CLEAR
     else
        startpeer vp $TARGET_PEER_ID $CONSENSUS $NUM_F $TAG_CLEAR
        ps -ef|grep "peer_fabric_"|grep -v grep |awk '{print "New processid: "$2 ", " $8}'
     fi
}

main
