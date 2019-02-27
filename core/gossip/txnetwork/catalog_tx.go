package txnetwork

import (
	"fmt"
	"github.com/abchain/fabric/core/gossip"
	model "github.com/abchain/fabric/core/gossip/model"
	"github.com/abchain/fabric/core/gossip/stub"
	pb "github.com/abchain/fabric/protos"
	proto "github.com/golang/protobuf/proto"
)

const (
	syncTxCatName = "synctx"
)

type txsyncCat struct {
	policy   gossip.CatalogPolicies
	tasklist map[string]bool
	network  *txNetworkGlobal
}

type txUpdate struct {
	*pb.HotTransactionBlock
}

func (txUpdate) Gossip_IsUpdateIn() bool { return true }

type taskList []string

func (taskList) Gossip_IsUpdateIn() bool { return true }

func init() {
	stub.RegisterCat = append(stub.RegisterCat, initTxSync)
}

func initTxSync(stub *gossip.GossipStub) {

	policy := gossip.NewCatalogPolicyDefault()
	policy.SetPullOnly()

	syncCore := &txsyncCat{
		tasklist: make(map[string]bool),
		policy:   policy,
		network:  getTxNetwork(stub),
	}
	m := model.NewGossipModel(syncCore)

	stub.AddCatalogHandler(gossip.NewCatalogHandlerImpl(stub.GetSStub(),
		stub.GetStubContext(), syncCore, m))

}

//Implement for CatalogHelper
func (c *txsyncCat) Name() string                        { return syncTxCatName }
func (c *txsyncCat) GetPolicies() gossip.CatalogPolicies { return c.policy }

func (c *txsyncCat) GenDigest() model.Digest {
	return c.tasklist
}

func (c *txsyncCat) Update(u model.Update) error {
	switch ut := u.(type) {
	case txUpdate:
		logger.Debugf("receive %d txs synced", len(ut.Transactions))
		for _, tx := range ut.Transactions {
			delete(txsyncCat.tasklist, tx.GetTxid())
		}
		l := c.network.txPool.ledger.PoolTransactions(ut.Transactions)
	case taskList:
		for _, id := range ut {
			txsyncCat.tasklist[id] = true
		}
		logger.Debugf("add %d tx tasks, now <%d>", len(ut), len(txsyncCat.tasklist))
	default:
		return fmt.Errorf("Not recognized type of update: %v", u)
	}

	return nil
}

func (c *txsyncCat) MakeUpdate(d model.Digest) model.Update {
	ld, ok := d.(taskList)
	if !ok {
		panic(fmt.Errorf("Not recognized type of digest: %v", d))
	}

	txblk := new(pb.HotTransactionBlock)

	//sanity check
	if l := c.network.txPool.ledger; l == nil {
		panic(fmt.Errorf("Ledger is not exist"))
	} else {
		for _, txid := range ld {
			if tx, err := l.GetTransactionByID(txid); err != nil {
				logger.Debugf("can not query tx <%s>: %s", txid, err)
				continue
			} else {
				txblk.Transactions = append(txblk.Transactions, tx)
			}

		}
	}

	logger.Debugf("response %d txs from %d request", len(txblk.Transactions), len(ld))

	return txUpdate{txblk}
}

func (c *txsyncCat) TransDigestToPb(d_in model.Digest) *pb.GossipMsg_Digest {
	return nil
}

func (c *txsyncCat) TransPbToDigest(msg *pb.GossipMsg_Digest) model.Digest {
	return nil
}

func (c *txsyncCat) UpdateMessage() proto.Message { return new(pb.HotTransactionBlock) }

func (c *txsyncCat) EncodeUpdate(cpo gossip.CatalogPeerPolicies, u_in model.Update, msg_in proto.Message) proto.Message {

	u, ok := u_in.(txUpdate)

	if !ok {
		panic("Type error, not txUpdate")
	}

	msg, ok := msg_in.(*pb.HotTransactionBlock)
	if !ok {
		panic("Type error, not HotTransactionBlock")
	}

	msg.Transactions = u.Transactions
	return msg
}

func (c *txsyncCat) DecodeUpdate(cpo gossip.CatalogPeerPolicies, msg_in proto.Message) (model.Update, error) {

	m, ok := msg_in.(*pb.HotTransactionBlock)
	if !ok {
		panic("Type error, not HotTransactionBlock")
	}

	for _, tx := range m.Transactions {
		if !tx.IsValid() {
			return nil, fmt.Errorf("receive invalid tx <%v>", tx)
		}
	}

	return txUpdate{m}, nil
}
