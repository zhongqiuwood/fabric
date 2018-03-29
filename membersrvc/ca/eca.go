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
	"fmt"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/elliptic"
	"crypto/x509"
	"database/sql"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"io/ioutil"
	"strconv"
	"strings"
	"errors"
	
	// "github.com/abchain/fabric/flogging"
	pb "github.com/abchain/fabric/membersrvc/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

var ecaLogger = logging.MustGetLogger("eca")

var (
	// ECertSubjectRole is the ASN1 object identifier of the subject's role.
	//
	ECertSubjectRole = asn1.ObjectIdentifier{2, 1, 3, 4, 5, 6, 7}
)

// ECA is the enrollment certificate authority.
//
type ECA struct {
	*CA
	// aca             *ACA
	obcKey          []byte
	obcPriv, obcPub []byte
	gRPCServer      *grpc.Server
}

func initializeECATables(db *sql.DB) error {
	return initializeCommonTables(db)
}

// NewECA sets up a new ECA.
// remove aca *ACA param from NewECA
func NewECA() *ECA {
	eca := &ECA{CA: NewCA("eca", initializeECATables)}
	// flogging.LoggingInit("eca")

	{
		// read or create global symmetric encryption key
		var cooked string
		var l = logging.MustGetLogger("ECA")

		raw, err := ioutil.ReadFile(eca.path + "/obc.aes")
		if err != nil {
			rand := rand.Reader
			key := make([]byte, 32) // AES-256
			rand.Read(key)
			cooked = base64.StdEncoding.EncodeToString(key)

			err = ioutil.WriteFile(eca.path+"/obc.aes", []byte(cooked), 0644)
			if err != nil {
				l.Panic(err)
			}
		} else {
			cooked = string(raw)
		}

		eca.obcKey, err = base64.StdEncoding.DecodeString(cooked)
		if err != nil {
			l.Panic(err)
		}
	}

	{
		// read or create global ECDSA key pair for ECIES
		var priv *ecdsa.PrivateKey
		cooked, err := ioutil.ReadFile(eca.path + "/obc.ecies")
		if err == nil {
			block, _ := pem.Decode(cooked)
			priv, err = x509.ParseECPrivateKey(block.Bytes)
			if err != nil {
				ecaLogger.Panic(err)
			}
		} else {
			priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				ecaLogger.Panic(err)
			}

			raw, _ := x509.MarshalECPrivateKey(priv)
			cooked = pem.EncodeToMemory(
				&pem.Block{
					Type:  "ECDSA PRIVATE KEY",
					Bytes: raw,
				})
			err := ioutil.WriteFile(eca.path+"/obc.ecies", cooked, 0644)
			if err != nil {
				ecaLogger.Panic(err)
			}
		}

		eca.obcPriv = cooked
		raw, _ := x509.MarshalPKIXPublicKey(&priv.PublicKey)
		eca.obcPub = pem.EncodeToMemory(
			&pem.Block{
				Type:  "ECDSA PUBLIC KEY",
				Bytes: raw,
			})
	}

	eca.populateAffiliationGroupsTable()
	eca.populateUsersTable()
	return eca
}

// *** populate action read object from yml file and write to db

// populateUsersTable populates the users table.
func (eca *ECA) populateUsersTable() {
	// populate user table, read from config yml file and  wrtie to ca db
	users := viper.GetStringMapString("eca.users")
	for id, flds := range users {
		vals := strings.Fields(flds)
		role, err := strconv.Atoi(vals[0])
		if err != nil {
			ecaLogger.Panic(err)
		}
		var affiliation, memberMetadata, registrar string
		if len(vals) >= 3 {
			affiliation = vals[2]
			if len(vals) >= 4 {
				memberMetadata = vals[3]
				if len(vals) >= 5 {
					registrar = vals[4]
				}
			}
		}
		_, err = eca.registerUser(id, affiliation, pb.Role(role), nil, registrar, memberMetadata, vals[1])
		if err != nil {
			fmt.Errorf("error when registerUser: %s %s \n", id, err)
		}
	}
}

// populateAffiliationGroup populates the affiliation groups table.
func (eca *ECA) _populateAffiliationGroup(name, parent, key string, level int) {
	eca.registerAffiliationGroup(name, parent)
	newKey := key + "." + name

	if level == 0 {
		affiliationGroups := viper.GetStringSlice(newKey)
		for ci := range affiliationGroups {
			eca.registerAffiliationGroup(affiliationGroups[ci], name)
		}
	} else {
		affiliationGroups := viper.GetStringMapString(newKey)
		for childName := range affiliationGroups {
			eca._populateAffiliationGroup(childName, name, newKey, level-1)
		}
	}
}

// populateAffiliationGroupsTable populates affiliation groups table.
func (eca *ECA) populateAffiliationGroupsTable() {
	key := "eca.affiliations"
	affiliationGroups := viper.GetStringMapString(key)
	for name := range affiliationGroups {
		eca._populateAffiliationGroup(name, "", key, 1)
	}
}

// Start starts the ECA.
func (eca *ECA) Start(srv *grpc.Server) {
	ecaLogger.Info("Starting ECA...")

	eca.startECAP(srv)
	eca.startECAA(srv)
	eca.gRPCServer = srv

	ecaLogger.Info("ECA started.")
}

// Stop stops the ECA services.
func (eca *ECA) Stop() {
	ecaLogger.Info("Stopping ECA services...")
	if eca.gRPCServer != nil {
		eca.gRPCServer.Stop()
	}
	err := eca.CA.Stop()
	if err != nil {
		ecaLogger.Errorf("ECA Error stopping services: %s", err)
	} else {
		ecaLogger.Info("ECA stopped")
	}
}

func (eca *ECA) startECAP(srv *grpc.Server) {
	pb.RegisterECAPServer(srv, &ECAP{eca})
	ecaLogger.Info("ECA PUBLIC gRPC API server started")
}

func (eca *ECA) startECAA(srv *grpc.Server) {
	pb.RegisterECAAServer(srv, &ECAA{eca})
	ecaLogger.Info("ECA ADMIN gRPC API server started")
}

// Return an error if all strings in 'strs1' are not contained in 'strs2'
func checkDelegateRoles(strs1 []string, strs2 []string, registrar string) error {
	caLogger.Debugf("CA.checkDelegateRoles: registrar=%s, strs1=%+v, strs2=%+v\n", registrar, strs1, strs2)
	for _, s := range strs1 {
		if !strContained(s, strs2) {
			caLogger.Debugf("CA.checkDelegateRoles: no: %s not in %+v\n", s, strs2)
			return errors.New("user " + registrar + " may not register delegateRoles " + s)
		}
	}
	caLogger.Debug("CA.checkDelegateRoles: ok")
	return nil
}