package peer

import (
	pb "github.com/abchain/fabric/protos"
)

//used in legacy mode to connect with the old consensus module from fabric 0.6

type LegacyMessageHandler interface {
	HandleMessage(msg *pb.Message) error
}

type TransactionProccesor interface {
	ProcessTransactionMsg(*pb.Message, *pb.Transaction) *pb.Response
}

type Engine interface {
	TransactionProccesor
	HandlerFactory(MessageHandler) (LegacyMessageHandler, error)
}

var legacy_Engine Engine

func SetLegacyEngine(e Engine) { legacy_Engine = e }
