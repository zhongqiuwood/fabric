## YA-fabric

This is purposed to be another future of fabric 0.6, keeping the original architecture and continue it with our improvments.

The original readme can be found [here](https://github.com/hyperledger/fabric/blob/v0.6/README.md)

## Our plans

* A "real" impelemnt for production, including some security and applicating consideration

* Completing some features and making improvments for the chaincode part

* Easy to be packed as a "standalone" blockchain, that is, the integration of chaincodes and fabric peer

* Our implement for the concurreny handling of transactions

* More consensus modules

## Building

You need to build rocksdb 5.10

* For windows, build with following FLAGS:
```
    set CGO_CFLAGS "-g -O2 -I<Your rocksdb project path>\\include"
```
```
    set CGO_LDFLAGS "<The fullpath of librocksdb.a> -lrpcrt4"
```

* For other OS you should make and "install-shared", and make sure following libs availiable
```
    -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
```

## License <a name="license"></a>
The Hyperledger Project uses the [Apache License Version 2.0](LICENSE) software
license.
