#!/bin/bash


./killbyname.sh txnetwork
./killbyname.sh peer
ln -s ../../core.yaml core.yaml

./scandb.sh -dbpath /var/hyperledger/production0 -block y > production0.json
./scandb.sh -dbpath /var/hyperledger/production1 -block y > production1.json


#./scandb.sh -dbpath /var/hyperledger/txnet0 -dump y > production0.json
#./scandb.sh -dbpath /var/hyperledger/txnet1 -dump y > production1.json
#./scandb.sh -dbpath /var/hyperledger/txnet2 -dump y > production2.json
#./scandb.sh -dbpath /var/hyperledger/txnet3 -dump y > production3.json
#diff production0.json production1.json

