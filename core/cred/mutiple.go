package credentials

import (
	pb "github.com/abchain/fabric/protos"
)

type mutiTxPreHandler []TxPreHandler
type mutiTxHandlerFactory []TxHandlerFactory

type interruptErr struct{}

func (interruptErr) Error() string {
	return "User interrupted"
}

var ValidateInterrupt = interruptErr{}

func MutipleTxHandler(m ...TxHandlerFactory) TxHandlerFactory {
	var flattedM []TxHandlerFactory
	//"flat" the recursive mutiple txhandler
	for _, mh := range m {
		if mmh, ok := mh.(mutiTxHandlerFactory); ok {
			flattedM = append(flattedM, mmh...)
		} else {
			flattedM = append(flattedM, mh)
		}
	}

	switch len(flattedM) {
	case 0:
		return nil
	case 1:
		return flattedM[0]
	default:
		return mutiTxHandlerFactory(flattedM)
	}
}

func MutipleTxPreHandler(m ...TxPreHandler) TxPreHandler {
	var flattedM []TxPreHandler
	//also "flat" the recursive mutiple txhandler (we are pain for lacking of generics )
	for _, mh := range m {
		if mmh, ok := mh.(mutiTxPreHandler); ok {
			flattedM = append(flattedM, mmh...)
		} else {
			flattedM = append(flattedM, mh)
		}
	}

	switch len(flattedM) {
	case 0:
		return nil
	case 1:
		return flattedM[0]
	default:
		return mutiTxPreHandler(flattedM)
	}
}

func (m mutiTxHandlerFactory) ValidatePeerStatus(id string, status *pb.PeerTxState) error {
	for _, h := range m {
		err := h.ValidatePeerStatus(id, status)
		if err == ValidateInterrupt {
			return nil
		} else if err != nil {
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
			if mhh, ok := hh.(mutiTxPreHandler); ok {
				hs = append(hs, mhh...)
			} else {
				hs = append(hs, hh)
			}
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
		if err == ValidateInterrupt {
			return tx, nil
		} else if err != nil {
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

//an TxPreHandler can act as a "dummy" HandlerFactory and be integrated into a mutiple handlerfactory
type dummyTxHandlerFactory struct {
	TxPreHandler
}

func (dummyTxHandlerFactory) ValidatePeerStatus(string, *pb.PeerTxState) error { return nil }
func (dummyTxHandlerFactory) RemovePreHandler(string)                          {}
func (m dummyTxHandlerFactory) GetPreHandler(string) (TxPreHandler, error)     { return m.TxPreHandler, nil }

func EscalateToTxHandler(h TxPreHandler) TxHandlerFactory {
	return dummyTxHandlerFactory{h}
}
