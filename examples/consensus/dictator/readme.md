## Example for consensus framework: Dictator 

The most simple consensus mode. One node with special credentials is specfied as the miner, which will generate and broadcast all the block and the other nodes will just accept them

The miner node work like the legacy NOOPS consensus, which generate a block in a specified interval or transactions limit.

The broadcasted blocks will be encoded into a transaction, in which the first parameter of chaincode spec. is used for the payload of block