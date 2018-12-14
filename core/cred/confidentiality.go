package credentials

import (
	"errors"
	pb "github.com/abchain/fabric/protos"
)

var confidentialityHandlerTag = "_confidentialy_"

//extract a DataEncryptor linked to pair defined by the deploy transaction and the execute transaction.
//the context must firstly be handled by TxConfidentialityHandler, or it just return nil
func GenDataEncryptor(trippeddeployTx *pb.Transaction, txe *pb.TransactionHandlingContext) (DataEncryptor, error) {

	var h TxConfidentialityHandler

	if d, ok := txe.CustomFields[confidentialityHandlerTag]; !ok {
		return nil, nil
	} else if h, ok = d.(TxConfidentialityHandler); !ok {
		return nil, errors.New("Invalid field for the confidentiality handler")
	}

	return h.GetStateEncryptor(trippeddeployTx, txe.Transaction)
}
