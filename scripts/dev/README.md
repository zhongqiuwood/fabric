## Startup 4 peers with pbft consensus
```
cd ${GOPATH}/src/github.com/abchain/fabric
./scripts/dev/runpbft.sh 1

```

## Deploy example02
```
cd ${GOPATH}/src/github.com/abchain/fabric
./scripts/dev/deploy.sh example02
```

## Invoke example02
```
cd ${GOPATH}/src/github.com/abchain/fabric
./scripts/dev/invoke.sh example02
```

## Query example02
```
cd ${GOPATH}/src/github.com/abchain/fabric
./scripts/dev/querya.sh example02
./scripts/dev/queryb.sh example02
```
