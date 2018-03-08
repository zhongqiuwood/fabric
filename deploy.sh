build/bin/peerorg chaincode deploy -n $1 -p github.com/abchain/fabric/examples/chaincode/go/chaincode_example02 -c '{"Function":"init", "Args": ["a","100", "b", "200"]}' 

