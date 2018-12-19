#!/usr/bin/env bash

CWD=`dirname $0`
cd ${CWD}

echo Build txnetwork ...
go build

cd ../../peer
echo Build peer ...
go build

mv peer ../examples/txnetwork/peer

echo Done
