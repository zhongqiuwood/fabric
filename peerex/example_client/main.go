package main

import (
	"bufio"
	"fmt"
	"os"

	arghelper "github.com/mattn/go-shellwords"

	fabricpeer_comm "github.com/abchain/fabric/peer/common"
	"github.com/abchain/fabric/peer/console"
	"github.com/abchain/fabric/peerex"
	fabric_pb "github.com/abchain/fabric/protos"
)

var default_conn peerex.ClientConn
var logger = peerex.InitLogger("main")

func genDevopsClientKeepAlive() (fabric_pb.DevopsClient, error) {
	logger.Debug("Generate new DevopsClient")
	devopsClient := fabric_pb.NewDevopsClient(default_conn.C)
	return devopsClient, nil
}

func main() {

	config := peerex.GlobalConfig{}
	err := config.InitGlobal(true)

	if err != nil {
		panic(err)
	}

	err = default_conn.Dialdefault()
	if err != nil {
		fmt.Println("Dial to peer fail:", err)
		os.Exit(1)
	}

	defer default_conn.C.Close()

	cmd := console.GetConsolePeer()
	if cmd == nil {
		fmt.Println("Can't not make command handler")
		os.Exit(1)
	}

	fmt.Print("Starting .... ")
	cmd.SetArgs([]string{"--help"})
	err = cmd.Execute()
	if err != nil {
		fmt.Println("Command handler error:", err)
		os.Exit(1)
	}

	fabricpeer_comm.GenDevopsClient = genDevopsClientKeepAlive

	reader := bufio.NewReader(os.Stdin)
	parser := arghelper.NewParser()

	var ln string = ""

	for {
		retbyte, notfinished, err := reader.ReadLine()
		if err != nil {
			break
		}

		ln += string(retbyte)
		if !notfinished {
			//handle read command line
			//fmt.Println("We get:", ln)
			args, err := parser.Parse(ln)
			if err != nil {
				fmt.Println("Input parse error:", err)
			} else {
				logger.Debug(len(args), "items:", args)
				cmd.SetArgs(args)
				err = cmd.Execute()
				if err != nil {
					fmt.Println("Command error:", err)
				}
			}

			ln = ""
			fmt.Println("Continue to next command:")
		}
	}

	fmt.Println("Exiting ...")

}
