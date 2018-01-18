/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package node

import (
	_ "bytes"
	"fmt"
	_ "os"
	_ "strconv"
	"github.com/golang/protobuf/ptypes/empty"
	_ "github.com/abchain/fabric/core/db"
	"github.com/abchain/fabric/core/peer"
	pb "github.com/abchain/fabric/protos"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

func stopCmd() *cobra.Command {
	nodeStopCmd.Flags().StringVar(&stopPidFile, "stop-peer-pid-file",
		viper.GetString("peer.fileSystemPath"),
		"Location of peer pid local file, for forces kill")

	return nodeStopCmd
}

var nodeStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stops the running node.",
	Long:  `Stops the running node, disconnecting from the network.`,
	Run: func(cmd *cobra.Command, args []string) {
		stop()
	},
}

func stop() (err error) {
	clientConn, err := peer.NewPeerClientConnection()
	if err != nil {
		return killbyPidfile(stopPidFile + "/peer.pid");
	}
	logger.Info("Stopping peer using grpc")
	serverClient := pb.NewAdminClient(clientConn)

	status, err := serverClient.StopServer(context.Background(), &empty.Empty{})
//	db.Stop()
	if err != nil {
		fmt.Println(&pb.ServerStatus{Status: pb.ServerStatus_STOPPED})
		return nil
	}

	err = fmt.Errorf("Connection remain opened, peer process doesn't exit")
	fmt.Println(status)
	return err
}

