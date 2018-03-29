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

package ca

import (
	"crypto/x509"
	"os"

	// "github.com/abchain/fabric/flogging"
	"github.com/op/go-logging"
	pb "github.com/abchain/fabric/membersrvc/protos"
	"google.golang.org/grpc"
	"github.com/spf13/viper"
)

var acaLogger = logging.MustGetLogger("aca")

// ACA is the attribute certificate authority.
type ACA struct {
	*CA
	gRPCServer *grpc.Server
}

// ACAA serves the administrator GRPC interface of the ACA.
//
type ACAA struct {
	aca *ACA
}

// NewACA sets up a new ACA.
func NewACA() *ACA {
	aca := &ACA{CA: NewCA("aca", initializeACATables)}
	
	aca.init();
	// flogging.LoggingInit("aca")
	return aca
}

func (aca *ACA) init() {
	if viper.GetBool("aca.enabled") && viper.GetBool("aca.initData") {
		if _, err := os.Stat(aca.path + "/aca.db"); err != nil {
			// aca.db is use by old ca version for db
			oldAcaDb := NewCADB(aca.path + "/aca.db", initNoneTable)
			attrs, err := oldAcaDb.fetchAttributes("")
			if err != nil {
				panic("found old aca.db but read old aca db error")
			}
			acaLogger.Error("found old aca.db and has attrs ", len(attrs))
			aca.cadb.InsertAttributes(attrs)
	
		} else {
			// init attributes from yaml file to db
			aca.initAllAttributesFromFile()
		}
	}
}

// is it usefull ?
func (aca *ACA) getECACertificate() (*x509.Certificate, error) {
	raw, err := aca.readCACertificate("eca") // inherit from CA
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(raw)
}

func (aca *ACA) getTCACertificate() (*x509.Certificate, error) {
	raw, err := aca.readCACertificate("tca") // // inherit from CA
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(raw)
}

func (aca *ACA) startACAP(srv *grpc.Server) {
	pb.RegisterACAPServer(srv, &ACAP{aca})
	acaLogger.Info("ACA PUBLIC gRPC API server started")
}

// Start starts the ACA.
func (aca *ACA) Start(srv *grpc.Server) {
	acaLogger.Info("Staring ACA services...")
	aca.startACAP(srv)
	aca.gRPCServer = srv
	acaLogger.Info("ACA services started")
}

// Stop stops the ACA
func (aca *ACA) Stop() error {
	acaLogger.Info("Stopping the ACA services...")
	if aca.gRPCServer != nil {
		aca.gRPCServer.Stop()
	}
	err := aca.CA.Stop()
	if err != nil {
		acaLogger.Errorf("Error stopping the ACA services: %s ", err)
	} else {
		acaLogger.Info("ACA services stopped")
	}
	return err
}
