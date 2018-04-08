#!/bin/bash

EXECUTION=dbupgrade

./killbyname.sh peer_fabric


rm ./${EXECUTION}

go build
./${EXECUTION} -dbpath /var/hyperledger/production$1 -mode q

exit
./${EXECUTION} -dbpath /var/hyperledger/test -mode q


