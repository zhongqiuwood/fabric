package credentials

import (
	pb "github.com/abchain/fabric/protos"
)

type mutiTxPreHandler []TxPreHandler
type mutiTxHandlerFactory []TxHandlerFactory

type interruptErr struct {
	err error
}

func (e interruptErr) Error() string {
	return e.Error()
}

func InterruptHandling(err error) error {
	return interruptErr{err}
}

func MutipleTxHandler(m ...TxHandlerFactory) TxHandlerFactory {
	return mutiTxHandlerFactory(m)
}

func (m mutiTxHandlerFactory) ValidatePeerStatus(id string, status *pb.PeerTxState) error {
	for _, h := range m {
		err := h.ValidatePeerStatus(id, status)
		if err != nil {
			if ierr, ok := err.(interruptErr); ok {
				return ierr.err
			}
			return err
		}
	}
	return nil
}

//could not be interrupt, but a nil can be return to incidate not care about doing prehandling
func (m mutiTxHandlerFactory) GetPreHandler(id string) (TxPreHandler, error) {

	var hs []TxPreHandler
	for _, h := range m {
		hh, err := h.GetPreHandler(id)
		if err != nil {
			return nil, err
		} else if hh != nil {
			hs = append(hs, hh)
		}
	}
	return mutiTxPreHandler(hs), nil
}

func (m mutiTxHandlerFactory) RemovePreHandler(id string) {
	for _, h := range m {
		h.RemovePreHandler(id)
	}
}

func (m mutiTxPreHandler) TransactionPreValidation(tx *pb.Transaction) (*pb.Transaction, error) {
	var err error
	for _, h := range m {
		tx, err = h.TransactionPreValidation(tx)
		if err != nil {
			if ierr, ok := err.(interruptErr); ok {
				return tx, ierr.err
			}
			return tx, err
		}
	}
	return tx, nil
}

func (m mutiTxPreHandler) Release() {
	for _, h := range m {
		h.Release()
	}
}
