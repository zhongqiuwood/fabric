#!/bin/bash

source fabric.profile

ACTION=i
CCNAME=example02_
VALUE=10
PEER_ID=0
KEY=a
PEER_URL=127.0.0.1:2055
EXEC=${FABRIC_TOP}/build/bin/${PEER_CLIENT}

while getopts "a:c:n:k:" opt; do
  case $opt in
    a)
      echo "ACTION = $OPTARG"
      ACTION=$OPTARG
      ;;
    c)
      echo "CCNAME = $OPTARG"
      CCNAME=$OPTARG
      ;;
    n)
      echo "VALUE = $OPTARG"
      VALUE=$OPTARG
      ;;
    v)
      echo "VALUE = $OPTARG"
      VALUE=$OPTARG
      ;;
    k)
      echo "KEY = $OPTARG"
      KEY=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      ;;
  esac
done

function invoke {
    CORE_SERVICE_CLIADDRESS=${PEER_URL} ${EXEC} chaincode invoke -l golang -n $1 \
            -c "{\"Args\":[\"invoke\", \"a\", \"b\", \"${VALUE}\"]}"
}

function deploy {
    CORE_SERVICE_CLIADDRESS=${PEER_URL} ${EXEC} chaincode deploy -n $1 -c \
        "{\"Function\":\"init\", \"Args\": [\"a\",\"$2\", \"b\", \"$3\"]}"
}

function queryAll {
    query  $CCNAME a
    query  $CCNAME b
    query  $CCNAME a1
    query  $CCNAME b1
    query  $CCNAME a2
    query  $CCNAME b2
}

function query {
    CORE_SERVICE_CLIADDRESS=${PEER_URL} ${EXEC} chaincode query -n $1 -c "{\"Args\":[\"query\", \"$2\"]}"
}

function main {

    if [ ! -f ${BUILD_BIN}/${PEER_CLIENT} ]; then
        $FABRIC_DEV_SCRIPT_TOP/buildpeer.sh
    fi

    if [ "$ACTION" = "d" ];then
        deploy ${CCNAME} ${VALUE} ${VALUE}
    exit
    fi

    if [ "$ACTION" = "i" ];then
        invoke  ${CCNAME} ${VALUE}
    exit
    fi

    if [ "$ACTION" = "q" ];then
        query ${CCNAME} ${KEY}
    exit
    fi

    if [ "$ACTION" = "qa" ];then
        queryAll
    exit
    fi

    if [ "$ACTION" = "di" or "$ACTION" = "id" ];then
        deploy  ${CCNAME} ${VALUE}
        sleep 1
        invoke  ${CCNAME} ${VALUE}
    exit
    fi

}

main
