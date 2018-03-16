#!/bin/bash

rm /Users/zhongqiu/goworkdir/src/github.com/abchain/fabric/build/bin/peer

CGO_CFLAGS=" " CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" \
GOBIN=/Users/zhongqiu/goworkdir/src/github.com/abchain/fabric/build/bin go install -ldflags \
"-X github.com/abchain/fabric/metadata.Version=0.6.2-preview-snapshot-7b14b750" github.com/abchain/fabric/peer

