package peerex

import (
	_ "github.com/spf13/viper"
	"github.com/spf13/cobra"
	
	"github.com/hyperledger/fabric/peer/chaincode"
	"github.com/hyperledger/fabric/peer/network"
	"github.com/hyperledger/fabric/peer/node"
	"github.com/hyperledger/fabric/peer/version"	
)

type  consoleForPeer struct{
	cobra.Command
}

const DefaultLeaderCmd = "peer"

var console = consoleForPeer{cobra.Command{
	Use: DefaultLeaderCmd,
	Run: func(cmd *cobra.Command, args []string) {		
		cmd.HelpFunc()(cmd, args)
	},
}}

func GetConsolePeer(config *GlobalConfig) *consoleForPeer{
	
	if !config.InitFinished(){
		err := config.InitGlobal()
		if err != nil{
			return nil
		}
	}
	
	console.AddCommand(version.Cmd())
	console.AddCommand(node.Cmd())
	console.AddCommand(network.Cmd())
	console.AddCommand(chaincode.Cmd())		
	return &console
}

