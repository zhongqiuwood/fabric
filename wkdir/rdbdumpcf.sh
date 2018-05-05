#!/bin/bash

rocksdb_ldb list_column_families /var/hyperledger/production$1

echo 'dump column_family: '$2
rocksdb_ldb --db=/var/hyperledger/production$1 --column_family=$2 dump


