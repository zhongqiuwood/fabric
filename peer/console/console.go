package console

import (
	_ "github.com/spf13/viper"
	"github.com/spf13/cobra"
	
	"github.com/abchain/fabric/peer/chaincode"
	"github.com/abchain/fabric/peer/network"
	"github.com/abchain/fabric/peer/node"
	"github.com/abchain/fabric/peer/version"
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

func GetConsolePeer() *consoleForPeer{
	
	console.AddCommand(version.Cmd())
	console.AddCommand(node.Cmd())
	console.AddCommand(network.Cmd())
	console.AddCommand(chaincode.Cmd())		
	return &console
}

