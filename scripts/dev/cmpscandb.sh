#!/bin/bash

function loadenv {

    export CORE_LOGGING_NODE=debug:statesync=debug:state=info:buckettree=info:peer=info:statesyncstub=debug:ledger=info
    #export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_NUMBUCKETS=1000003
    #export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_MAXGROUPINGATEACHLEVEL=5
#    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_NUMBUCKETS=32
#    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_MAXGROUPINGATEACHLEVEL=3
#    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_SYNCDELTA=2


    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_NUMBUCKETS=100
    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_MAXGROUPINGATEACHLEVEL=2
    export CORE_LEDGER_STATE_DATASTRUCTURE_CONFIGS_SYNCDELTA=1
}


./killbyname.sh txnetwork
./killbyname.sh peer
ln -s ../../core.yaml core.yaml
loadenv
./scandb.sh -dbpath /var/hyperledger/production0 -block y > production0.json
./scandb.sh -dbpath /var/hyperledger/production1 -block y > production1.json


#./scandb.sh -dbpath /var/hyperledger/txnet0 -dump y > production0.json
#./scandb.sh -dbpath /var/hyperledger/txnet1 -dump y > production1.json
#./scandb.sh -dbpath /var/hyperledger/txnet2 -dump y > production2.json
#./scandb.sh -dbpath /var/hyperledger/txnet3 -dump y > production3.json
#diff production0.json production1.json

