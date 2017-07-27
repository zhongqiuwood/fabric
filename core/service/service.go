package service

import (
	
	"errors"
	"fmt"

	"golang.org/x/net/context"
	"github.com/op/go-logging"	
	
	pb "github.com/hyperledger/fabric/protos"
)

var restLogger = logging.MustGetLogger("service")

func StartFabricService(server *ServerOpenchain, devops *core.Devops) error{
	return nil
}