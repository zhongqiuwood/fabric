#!/bin/bash

CCNAME=mycc
FAILEDPEER=3
LIVEDPEER=0
TXPERBLOCK=5

function invoke {
    sleep 2
    let limit=$1
    let index=0
    while [  $index -lt $limit ]; do
        ./invoke.sh i $CCNAME $LIVEDPEER
        let index=index+1
    done
}

./runyafabric.sh -f 1 -c p -r clearall
./invoke.sh d
invoke 1
invoke 1
invoke 1

echo "======================================================"
echo "============ kill peer$FAILEDPEER"
echo "======================================================"
sleep 5
./runyafabric.sh -k $FAILEDPEER


invoke $TXPERBLOCK $CCNAME $LIVEDPEER
invoke $TXPERBLOCK $CCNAME $LIVEDPEER
invoke $TXPERBLOCK $CCNAME $LIVEDPEER
invoke $TXPERBLOCK $CCNAME $LIVEDPEER
invoke $TXPERBLOCK $CCNAME $LIVEDPEER

invoke $TXPERBLOCK $CCNAME $LIVEDPEER
invoke $TXPERBLOCK $CCNAME $LIVEDPEER
invoke $TXPERBLOCK $CCNAME $LIVEDPEER
invoke $TXPERBLOCK $CCNAME $LIVEDPEER


echo "======================================================"
echo "============ start up peer$FAILEDPEER"
echo "======================================================"

sleep 5
./runyafabric.sh -f 1 -c p -i $FAILEDPEER


invoke 1


exit

invoke 5
invoke 5
invoke 5
invoke 5
invoke 5
invoke 5