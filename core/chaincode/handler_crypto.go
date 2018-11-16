//the cryptology part of chaincode handler...

package chaincode

import (
	"fmt"

	cred "github.com/abchain/fabric/core/cred"
	prim "github.com/abchain/fabric/core/crypto/primitives"
	pb "github.com/abchain/fabric/protos"
)

func (handler *Handler) encryptOrDecrypt(encrypt bool, txctx *transactionContext, payload []byte) ([]byte, error) {
	txHandler := handler.chaincodeSupport.getTxHandler()
	if txHandler == nil {
		return payload, nil
	}

	var enc cred.DataEncryptor
	if txctx.encryptor == nil {
		//create encryptor and cache it
		txid := txctx.transactionSecContext.GetTxid()
		var err error
		if txctx.transactionSecContext.Type == pb.Transaction_CHAINCODE_DEPLOY {
			if enc, err = txHandler.GenDataEncryptor(handler.deployTXSecContext, handler.deployTXSecContext); err != nil {
				return nil, fmt.Errorf("error getting crypto encryptor for deploy tx :%s", err)
			}
		} else if txctx.transactionSecContext.Type == pb.Transaction_CHAINCODE_INVOKE || txctx.transactionSecContext.Type == pb.Transaction_CHAINCODE_QUERY {
			if enc, err = txHandler.GenDataEncryptor(handler.deployTXSecContext, txctx.transactionSecContext); err != nil {
				return nil, fmt.Errorf("error getting crypto encryptor %s", err)
			}
		} else {
			return nil, fmt.Errorf("invalid transaction type %s", txctx.transactionSecContext.Type.String())
		}
		if enc == nil {
			return nil, fmt.Errorf("secure context returns nil encryptor for tx %s", txid)
		}

		txctx.encryptor = enc
		chaincodeLogger.Debugf("[%s]Payload before encrypt/decrypt: %v", shorttxid(txid), payload)
	} else {
		enc = txctx.encryptor.(cred.DataEncryptor)
	}

	if encrypt {
		payload, err = enc.Encrypt(payload)
	} else {
		payload, err = enc.Decrypt(payload)
	}

	chaincodeLogger.Debugf("[%s]Payload after encrypt/decrypt: %v", shorttxid(txid), payload)

	return payload, err
}

func (handler *Handler) decrypt(tctx *transactionContext, payload []byte) ([]byte, error) {
	return handler.encryptOrDecrypt(false, tctx, payload)
}

func (handler *Handler) encrypt(tctx *transactionContext, payload []byte) ([]byte, error) {
	return handler.encryptOrDecrypt(true, tctx, payload)
}

//with txnetwork we should not need binding (which is designed for resisting copy attacking)
//anymore, we just add one for compatible
func (handler *Handler) getSecurityBinding(tx *pb.Transaction) ([]byte, error) {
	return prim.Hash(append(tx.Cert, tx.Nonce...))
}
