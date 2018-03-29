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
	"crypto/hmac"
	"crypto/x509"
	"database/sql"
	"encoding/asn1"
	"errors"

	"github.com/abchain/fabric/core/crypto/primitives"
	// "github.com/abchain/fabric/flogging"
	pb "github.com/abchain/fabric/membersrvc/protos"
	"github.com/op/go-logging"
	"google.golang.org/grpc"
)

var tcaLogger = logging.MustGetLogger("tca")

var (
	// TCertEncTCertIndex is the ASN1 object identifier of the TCert index.
	TCertEncTCertIndex = asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 7}

	// TCertEncEnrollmentID is the ASN1 object identifier of the enrollment id.
	TCertEncEnrollmentID = asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 8}

	// TCertAttributesHeaders is the ASN1 object identifier of attributes header.
	TCertAttributesHeaders = asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 9}

	// Padding for encryption.
	Padding = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}

	// RootPreKeySize for attribute encryption keys derivation
	RootPreKeySize = 48
)

// TCA is the transaction certificate authority.
type TCA struct {
	*CA
	eca        *ECA
	hmacKey    []byte
	rootPreKey []byte
	preKeys    map[string][]byte
	gRPCServer *grpc.Server
}

// TCertSet contains relevant information of a set of tcerts
type TCertSet struct {
	Ts           int64
	EnrollmentID string
	Nonce        []byte
	Key          []byte
}


// NewTCA sets up a new TCA.
func NewTCA(eca *ECA) *TCA {
	tca := &TCA{NewCA("tca", initializeTCATables, true), eca, nil, nil, nil, nil}
	// flogging.LoggingInit("tca")
	
	var err error
	tca.hmacKey, err = readHmacKey(tca.path)
	if err != nil {
		tcaLogger.Panic(err)
	}

	tca.rootPreKey, err = readRootPreKey(tca.path)
	if err != nil {
		tcaLogger.Panic(err)
	}

	err = tca.initializePreKeyTree()
	if err != nil {
		tcaLogger.Panic(err)
	}
	return tca
}


func (tca *TCA) calculatePreKey(variant []byte, preKey []byte) ([]byte, error) {
	mac := hmac.New(primitives.GetDefaultHash(), preKey)
	_, err := mac.Write(variant)
	if err != nil {
		return nil, err
	}
	return mac.Sum(nil), nil
}

func (tca *TCA) initializePreKeyNonRootGroup(group *AffiliationGroup) error {
	if group.parent.preKey == nil {
		//Initialize parent if it is not initialized yet.
		tca.initializePreKeyGroup(group.parent)
	}
	var err error
	group.preKey, err = tca.calculatePreKey([]byte(group.name), group.parent.preKey)
	return err
}

func (tca *TCA) initializePreKeyGroup(group *AffiliationGroup) error {
	if group.parentID == 0 {
		//This group is root
		group.preKey = tca.rootPreKey
		return nil
	}
	return tca.initializePreKeyNonRootGroup(group)
}

func (tca *TCA) initializePreKeyTree() error {
	tcaLogger.Debug("Initializing PreKeys.")
	groups, err := tca.eca.cadb.ReadAffiliationGroups()
	if err != nil {
		return err
	}
	tca.preKeys = make(map[string][]byte)
	for _, group := range groups {
		if group.preKey == nil {
			err = tca.initializePreKeyGroup(group)
			if err != nil {
				return err
			}
		}
		tcaLogger.Debug("Initializing PK group ", group.name)
		tca.preKeys[group.name] = group.preKey
	}

	return nil
}

func (tca *TCA) getPreKFrom(enrollmentCertificate *x509.Certificate) ([]byte, error) {
	_, affiliation, err := tca.eca.parseEnrollID(enrollmentCertificate.Subject.CommonName)
	if err != nil {
		return nil, err
	}
	preK := tca.preKeys[affiliation]
	if preK == nil {
		return nil, errors.New("Could not be found a pre-k to the affiliation group " + affiliation + ".")
	}
	return preK, nil
}

// Start starts the TCA.
func (tca *TCA) Start(srv *grpc.Server) {
	tcaLogger.Info("Staring TCA services...")
	tca.startTCAP(srv)
	tca.startTCAA(srv)
	tca.gRPCServer = srv
	tcaLogger.Info("TCA started.")
}

// Stop stops the TCA services.
func (tca *TCA) Stop() error {
	tcaLogger.Info("Stopping the TCA services...")
	if tca.gRPCServer != nil {
		tca.gRPCServer.Stop()
	}
	err := tca.CA.Stop()
	if err != nil {
		tcaLogger.Errorf("Error stopping TCA services: %s", err)
	} else {
		tcaLogger.Info("TCA services stopped")
	}
	return err
}

func (tca *TCA) startTCAP(srv *grpc.Server) {
	pb.RegisterTCAPServer(srv, &TCAP{tca})
	tcaLogger.Info("TCA PUBLIC gRPC API server started")
}

func (tca *TCA) startTCAA(srv *grpc.Server) {
	pb.RegisterTCAAServer(srv, &TCAA{tca})
	tcaLogger.Info("TCA ADMIN gRPC API server started")
}

func (tca *TCA) getCertificateSets(enrollmentID string) ([]*TCertSet, error) {
	mutex.RLock()
	defer mutex.RUnlock()

	var sets = []*TCertSet{}
	var err error

	var rows *sql.Rows
	rows, err = tca.retrieveCertificateSets(enrollmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var enrollID string
	var timestamp int64
	var nonce []byte
	var kdfKey []byte

	for rows.Next() {
		if err = rows.Scan(&enrollID, &timestamp, &nonce, &kdfKey); err != nil {
			return nil, err
		}
		sets = append(sets, &TCertSet{Ts: timestamp, EnrollmentID: enrollID, Key: kdfKey})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return sets, nil
}

func (tca *TCA) persistCertificateSet(enrollmentID string, timestamp int64, nonce []byte, kdfKey []byte) error {
	return tca.cadb.persistCertificateSet(enrollmentID, timestamp, nonce, kdfKey)
}

func (tca *TCA) retrieveCertificateSets(enrollmentID string) (*sql.Rows, error) {
	return tca.cadb.retrieveCertificateSets(enrollmentID)
}
