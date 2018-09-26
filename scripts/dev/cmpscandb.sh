#!/bin/bash


./scandb.sh -dbpath /var/hyperledger/production0 -dump y > production0.json
./scandb.sh -dbpath /var/hyperledger/production1 -dump y > production1.json
diff production0.json production1.json

