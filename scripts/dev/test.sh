#!/bin/bash

FABRIC_TOP=${GOPATH}/src/github.com/abchain/fabric
BUILD_BIN=${FABRIC_TOP}/build/bin


./runyafabric.sh n 1 1 clearall

if [ ! -f ${BUILD_BIN}/embedded ]; then
        echo 'No such a file: '${BUILD_BIN}'/embedded'
        exit -1
fi

sleep 1
./invoke.sh id

sleep 1
./invoke.sh i
sleep 1
./invoke.sh i

sleep 1
./invoke.sh i
sleep 1
./invoke.sh i


sleep 2
./dateback.sh 0

./dateback.sh 0 6762a39d88ab1c7ba694601cfadff1d80c13ef12238837fc2366cd2e0549da5c62ee89b8e60ad31fef684b703025a2c725baef3fb666cc7e3f8a5cc62c7c8c51
#./dateback.sh 0 2-6762a39d88ab1c7ba694601cfadff1d80c13ef12238837fc2366cd2e0549da5c62ee89b8e60ad31fef684b703025a2c725baef3fb666cc7e3f8a5cc62c7c8c51

./runyafabric.sh n 1 1

./invoke10.sh i

#sleep 3
#cd ${FABRIC_TOP}/tools/dbupgrade
#./scandb.sh 0



function t1 {
    sleep 3
    #./dateback.sh 0 3-ba0465e5850b1b15177b95cbbeb11088d7f42801ada897ba068f939ad4053a446a5d292baa7bff91ec53f5424aba59477b88f5ee1111516a15834609b6f01ff0
    ./dateback.sh 0 ba0465e5850b1b15177b95cbbeb11088d7f42801ada897ba068f939ad4053a446a5d292baa7bff91ec53f5424aba59477b88f5ee1111516a15834609b6f01ff0
    ./runyafabric.sh n 1
    ./invoke.sh i
}


function t2 {
    sleep 3
    #./dateback.sh 0 2-6762a39d88ab1c7ba694601cfadff1d80c13ef12238837fc2366cd2e0549da5c62ee89b8e60ad31fef684b703025a2c725baef3fb666cc7e3f8a5cc62c7c8c51
    ./dateback.sh 0 6762a39d88ab1c7ba694601cfadff1d80c13ef12238837fc2366cd2e0549da5c62ee89b8e60ad31fef684b703025a2c725baef3fb666cc7e3f8a5cc62c7c8c51
    ./runyafabric.sh n 1 1
    ./invoke35.sh i
    sleep 1
   ./invoke.sh i
    sleep 1
   ./invoke.sh i
}

t2