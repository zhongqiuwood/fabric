#!/bin/bash

index=0
while [ $index -lt $1 ]; do
    ./invoke.sh mycc
    ./invoke.sh mycc
    ./invoke.sh mycc
    sleep $2
    let index=index+1   
done  

