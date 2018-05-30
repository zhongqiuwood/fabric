#!/bin/bash

EXECUTION=reorganizedb

./killbyname.sh peer_fabric

rm -rf /var/hyperledger/test
cp -rf ~/org.production0 /var/hyperledger/test

rm ./${EXECUTION}
go build
./${EXECUTION} -dbpath /var/hyperledger/test -mode r

exit
./${EXECUTION} -dbpath /var/hyperledger/test -mode q





