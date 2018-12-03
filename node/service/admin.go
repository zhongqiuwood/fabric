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

package service

import (
	"errors"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"os"
	"runtime"

	"github.com/abchain/fabric/core/util"
	pb "github.com/abchain/fabric/protos"
	"github.com/golang/protobuf/ptypes/empty"
)

// NewAdminServer creates and returns a Admin service instance.
func NewAdminServer() *ServerAdmin {
	s := new(ServerAdmin)
	return s
}

// ServerAdmin implementation of the Admin service for the Peer
type ServerAdmin struct {
}

func worker(id int, die chan struct{}) {
	for {
		select {
		case <-die:
			clisrvLogger.Debugf("worker %d terminating", id)
			return
		default:
			clisrvLogger.Debugf("%d is working...", id)
			runtime.Gosched()
		}
	}
}

// GetStatus reports the status of the server
func (*ServerAdmin) GetStatus(context.Context, *empty.Empty) (*pb.ServerStatus, error) {
	status := &pb.ServerStatus{Status: pb.ServerStatus_STARTED}
	clisrvLogger.Debugf("returning status: %s", status)
	return status, nil
}

// StartServer starts the server
func (*ServerAdmin) StartServer(context.Context, *empty.Empty) (*pb.ServerStatus, error) {
	status := &pb.ServerStatus{Status: pb.ServerStatus_STARTED}
	clisrvLogger.Debugf("returning status: %s", status)
	return status, nil
}

// StopServer stops the server
func (*ServerAdmin) StopServer(context.Context, *empty.Empty) (*pb.ServerStatus, error) {
	return nil, errors.New("Not allowed")

	// status := &pb.ServerStatus{Status: pb.ServerStatus_STOPPED}
	// clisrvLogger.Debugf("returning status: %s", status)

	// pidFile := util.CanonicalizePath(viper.GetString("peer.fileSystemPath")) + "peer.pid"
	// clisrvLogger.Debugf("Remove pid file  %s", pidFile)
	// os.Remove(pidFile)
	// defer os.Exit(0)
	// return status, nil
}
