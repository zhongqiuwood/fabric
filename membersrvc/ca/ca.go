package ca

import (
	"crypto/ecdsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/abchain/fabric/core/util"
	"github.com/op/go-logging"
	// "github.com/abchain/fabric/flogging"
	pb "github.com/abchain/fabric/membersrvc/protos"
	"github.com/spf13/viper" // TODO del it
)

var (
	mutex          = &sync.RWMutex{}
	caOrganization string
	caCountry      string
	rootPath       string
	caDir          string
)

// CacheConfiguration caches the viper configuration
func CacheConfiguration() {
	caOrganization = viper.GetString("pki.ca.subject.organization")
	caCountry = viper.GetString("pki.ca.subject.country")
	rootPath = util.CanonicalizePath(viper.GetString("server.rootpath"))
	caDir = viper.GetString("server.cadir")
}

var caLogger = logging.MustGetLogger("ca")

// CA is the base certificate authority.
type CA struct {
	cadb *CADB

	path string

	priv *ecdsa.PrivateKey
	cert *x509.Certificate
	raw  []byte
}

// NewCA sets up a new CA.
// @parameter name is name of this CA instance
// @parameter initTables create table when it not exist
func NewCA(name string, initTables TableInitializer, isNewDB bool) *CA {
	ca := new(CA)
	// flogging.LoggingInit("ca")
	ca.path = filepath.Join(rootPath, caDir)
	if _, err := os.Stat(ca.path); err != nil {
		caLogger.Info("Fresh start; creating databases, key pairs, and certificates.")

		if err := os.MkdirAll(ca.path, 0755); err != nil {
			caLogger.Panic(err)
		}
	}
	if isNewDB {
		ca.cadb = NewCADB(fmt.Sprintf("%s/%s.db", ca.path, name), initTables)
	} else {
		ca.cadb = NewCADB(ca.path+"/ca.db", initTables)
	}
	
	// read or create signing key pair
	priv, err := readCAPrivateKeyFile(ca.path, name)
	if err != nil {
		priv = createCAKeyPair(ca.path, name)
	}
	ca.priv = priv

	// read CA certificate, or create a self-signed CA certificate
	raw, err := readCACertificateFile(ca.path, name)
	if err != nil {
		raw = ca._createCACertificateWriteFile(name, &ca.priv.PublicKey)
	}
	cert, err := x509.ParseCertificate(raw)
	if err != nil {
		caLogger.Panic(err)
	}

	ca.raw = raw
	ca.cert = cert

	return ca
}

// Stop Close closes down the CA.
func (ca *CA) Stop() error {
	err := ca.cadb.close()
	if err == nil {
		caLogger.Debug("Shutting down CA - Successfully")
	} else {
		caLogger.Debug(fmt.Sprintf("Shutting down CA - Error closing DB [%s]", err))
	}
	return err
}


func (ca *CA) _createCACertificateWriteFile(name string, pub *ecdsa.PublicKey) []byte {
	caLogger.Debug("func createCACertificate Creating CA certificate.")

	raw, err := ca.newCertificate(name, pub, x509.KeyUsageDigitalSignature|x509.KeyUsageCertSign, nil)
	if err != nil {
		caLogger.Panic(err)
	}

	err = createCACertificateFIle(ca.path, name, raw)
	if err != nil {
		caLogger.Panic(err)
	}

	return raw
}


// registerUser registers a new member with the CA
func (ca *CA) registerUser(id, affiliation string, role pb.Role, attrs []*pb.Attribute, registrar, memberMetadata string, opt ...string) (string, error) {
	memberMetadata = removeQuotes(memberMetadata)
	roleStr, _ := MemberRoleToString(role)
	caLogger.Debugf("Received request to register user with id: %s, affiliation: %s, role: %s, attrs: %+v, registrar: %s, memberMetadata: %s\n",
		id, affiliation, roleStr, attrs, registrar, memberMetadata)
	// fmt.Printf("registerUser user %s affiliation %s \n", id, affiliation)	
	var enrollID, tok string
	var err error

	// There are two ways that registerUser can be called:
	// 1) At initialization time from eca.users in the YAML file
	//    In this case, 'registrar' may be nil but we still register the users from the YAML file
	// 2) At runtime via the GRPC ECA.RegisterUser handler (see RegisterUser in eca.go)
	//    In this case, 'registrar' must never be nil and furthermore the caller must have been authenticated
	//    to actually be the 'registrar' identity
	// This means we trust what is in the YAML file but not what comes over the network
	if registrar != "" {
		// Check the permission of member named 'registrar' to perform this registration
		err = ca._canRegister(registrar, role2String(int(role)), memberMetadata)
		if err != nil {
			caLogger.Error(err)
			return "", err
		}
	}
	enrollID, err = ca._validateAndGenerateEnrollID(id, affiliation, role)
	if err != nil {
		// caLogger.Error(err)
		return "", err
	}
	tok, err = ca._registerUserWithEnrollID(id, enrollID, role, memberMetadata, opt...)
	if err != nil {
		// caLogger.Error(err)
		return "", err
	}
	if attrs != nil {
		var pairs []*AttributePair
		pairs, err = toAttributePairs(id, affiliation, attrs)
		if err == nil {
			err = ca.cadb.InsertAttributes(pairs)
		}
	}
	return tok, err
}


func (ca *CA) initAllAttributesFromFile() {
	// TODO this attributes should be readed from the outside world in place of configuration file.
	var attributes = make([]*AttributePair, 0)
	attrs := viper.GetStringMapString("aca.attributes")

	for _, flds := range attrs {
		vals := strings.Fields(flds)
		if len(vals) >= 1 {
			val := ""
			for _, eachVal := range vals {
				val = val + " " + eachVal
			}
			attributeVals := strings.Split(val, ";")
			if len(attributeVals) >= 6 {
				attrPair, err := NewAttributePair(attributeVals, nil)
				if err != nil {
					cadbLogger.Errorf("Invalid attribute entry " + val + " " + err.Error())
				}
				attributes = append(attributes, attrPair)
				
			} else {
				cadbLogger.Errorf("Invalid attribute entry '%v'", vals[0])
			}
		}
	}
	ca.cadb.InsertAttributes(attributes)
}

func (ca *CA) readAttributesFromFile(id, affiliation string) ([]*AttributePair, error) {
	// TODO this attributes should be readed from the outside world in place of configuration file.
	var attributes = make([]*AttributePair, 0)
	attrs := viper.GetStringMapString("aca.attributes")

	for _, flds := range attrs {
		vals := strings.Fields(flds)
		if len(vals) >= 1 {
			val := ""
			for _, eachVal := range vals {
				val = val + " " + eachVal
			}
			attributeVals := strings.Split(val, ";")
			if len(attributeVals) >= 6 {
				attrPair, err := NewAttributePair(attributeVals, nil)
				if err != nil {
					return nil, errors.New("Invalid attribute entry " + val + " " + err.Error())
				}
				if attrPair.GetID() != id || attrPair.GetAffiliation() != affiliation {
					continue
				}
				attributes = append(attributes, attrPair)
			} else {
				cadbLogger.Errorf("Invalid attribute entry '%v'", vals[0])
			}
		}
	}

	cadbLogger.Debugf("%v %v", id, attributes)

	return attributes, nil
}

func (ca *CA) initAttributes(id, affiliation string) error {
	var attrs []*AttributePair
	attrs, err := ca.readAttributesFromFile(id, affiliation)
	if err != nil {
		return err
	}
	err = ca.cadb.InsertAttributes(attrs)
	if err != nil {
		return err
	}
	return nil
}

// registerUser registers a new member with the CA
func (ca *CA) registerUserWithoutRegistar(id, affiliation string, role pb.Role, memberMetadata string, pwd string) (string, error) {
	memberMetadata = removeQuotes(memberMetadata)
	// roleStr, _ := MemberRoleToString(role)

	// fmt.Printf("registerUser id: %s affiliation: %s role: %s \n", id, affiliation, roleStr)	
	var enrollID, tok string
	var err error
	enrollID, err = ca._validateAndGenerateEnrollID(id, affiliation, role)
	if err != nil {
		caLogger.Error(err)
		return "", err
	}
	tok, err = ca._registerUserWithEnrollID(id, enrollID, role, memberMetadata, pwd)
	if err != nil {
		caLogger.Error(err)
		return "", err
	}
	return tok, err
}

// registerUserWithEnrollID registers a new user and its enrollmentID, role and state
func (ca *CA) _registerUserWithEnrollID(id string, enrollID string, role pb.Role, memberMetadata string, opt ...string) (string, error) {
	mutex.Lock()
	defer mutex.Unlock()

	roleStr, _ := MemberRoleToString(role)
	caLogger.Infof("Registering user %s as %s with memberMetadata %s\n", id, roleStr, memberMetadata)

	var tok string
	if len(opt) > 0 && len(opt[0]) > 0 {
		tok = opt[0]
	} else {
		tok = randomString(12)
	}

	err := ca.cadb.checkAndAddUser(id, enrollID, tok, role, memberMetadata)

	return tok, err
}

// registerAffiliationGroup registers a new affiliation group
func (ca *CA) registerAffiliationGroup(name string, parentName string) error {
	mutex.Lock()
	defer mutex.Unlock()

	caLogger.Debug("Registering affiliation group " + name + " parent " + parentName + ".")
	var err error
	if err = ca.cadb.CheckAndAddAffiliationGroup(name, parentName); err != nil {
		caLogger.Error(err)
	}

	return err
}

func _generateEnrollID(id string, affiliation string) (string, error) {
	if id == "" || affiliation == "" {
		return "", errors.New("Please provide all the input parameters, id and role")
	}

	if strings.Contains(id, "\\") || strings.Contains(affiliation, "\\") {
		return "", errors.New("Do not include the escape character \\ as part of the values")
	}

	return id + "\\" + affiliation, nil
}

// validateAndGenerateEnrollID validates the affiliation subject
func (ca *CA) _validateAndGenerateEnrollID(id, affiliation string, role pb.Role) (string, error) {
	roleStr, _ := MemberRoleToString(role)
	caLogger.Debug("Validating and generating enrollID for user id: " + id + ", affiliation: " + affiliation + ", role: " + roleStr + ".")

	// Check whether the affiliation is required for the current user.
	//
	// Affiliation is required if the role is client or peer.
	// Affiliation is not required if the role is validator or auditor.
	if requireAffiliation(role) {
		valid, err := ca.cadb.isValidAffiliation(affiliation)
		if err != nil {
			return "", err
		}

		if !valid {
			caLogger.Debug("Invalid affiliation group: ")
			return "", errors.New("Invalid affiliation group " + affiliation)
		}

		return _generateEnrollID(id, affiliation)
	}

	return "", nil
}

// Determine if affiliation is required for a given registration request.
//
// Affiliation is required if the role is client or peer.
// Affiliation is not required if the role is validator or auditor.
// 1: client, 2: peer, 4: validator, 8: auditor
func requireAffiliation(role pb.Role) bool {
	roleStr, _ := MemberRoleToString(role)
	caLogger.Debug("Assigned role is: " + roleStr + ".")

	return role != pb.Role_VALIDATOR && role != pb.Role_AUDITOR
}

func (ca *CA) parseEnrollID(enrollID string) (id string, affiliation string, err error) {

	if enrollID == "" {
		return "", "", errors.New("Input parameter missing")
	}

	enrollIDSections := strings.Split(enrollID, "\\")

	if len(enrollIDSections) != 2 {
		return "", "", errors.New("Either the userId or affiliation is missing from the enrollmentID. EnrollID was " + enrollID)
	}

	id = enrollIDSections[0]
	affiliation = enrollIDSections[1]
	err = nil
	return
}

// Check to see if member 'registrar' can register a new member of type 'newMemberRole'
// and with metadata associated with 'newMemberMetadataStr'
// Return nil if allowed, or an error if not allowed
// MemberMetadata ex: {"registrar": {"roles": [], "delegateRoles": []}}
func (ca *CA) _canRegister(registrar string, newMemberRole string, newMemberMetadataStr string) error {
	mutex.RLock()
	defer mutex.RUnlock()

	// Read the user metadata associated with 'registrar'
	var registrarMetadataStr string
	var err error
	if registrarMetadataStr, err = ca.cadb.checkMetadata(registrar); err != nil {
		caLogger.Debugf("CA.canRegister: db error: %s\n", err.Error())
		return err
	}
	caLogger.Debugf("CA.canRegister: registrar=%s, registrarMD=%s, newMemberRole=%s, newMemberMD=%s",
		registrar, registrarMetadataStr, newMemberRole, newMemberMetadataStr)
	// If isn't a registrar at all, then error
	if registrarMetadataStr == "" {
		caLogger.Debug("canRegister: member " + registrar + " is not a registrar")
		return errors.New("member " + registrar + " is not a registrar")
	}
	// Get the registrar's metadata
	caLogger.Debug("CA.canRegister: parsing registrar's metadata")
	registrarMetadata, err := newMemberMetadata(registrarMetadataStr)
	if err != nil {
		return err
	}
	// Convert the user's meta to an object
	caLogger.Debug("CA.canRegister: parsing new member's metadata")
	newMemberMetadata, err := newMemberMetadata(newMemberMetadataStr)
	if err != nil {
		return err
	}
	// See if the metadata to be registered is acceptable for the registrar
	return registrarMetadata.canRegister(registrar, newMemberRole, newMemberMetadata)
}

func (ca *CA) newCertificate(id string, pub interface{}, usage x509.KeyUsage, ext []pkix.Extension) ([]byte, error) {
	spec := NewDefaultCertificateSpec(id, pub, usage, ext...)
	return ca.newCertificateFromSpec(spec)
}

func (ca *CA) newCertificateFromSpec(spec *CertificateSpec) ([]byte, error) {
	return newCertificateFromSpec(spec, ca.cert, ca.priv)  // implementation in crypto.go
	// notBefore := spec.GetNotBefore()
	// notAfter := spec.GetNotAfter()

	// parent := ca.cert
	// isCA := parent == nil

	// tmpl := x509.Certificate{
	// 	SerialNumber: spec.GetSerialNumber(),
	// 	Subject: pkix.Name{
	// 		CommonName:   spec.GetCommonName(),
	// 		Organization: []string{spec.GetOrganization()},
	// 		Country:      []string{spec.GetCountry()},
	// 	},
	// 	NotBefore: *notBefore,
	// 	NotAfter:  *notAfter,

	// 	SubjectKeyId:       *spec.GetSubjectKeyID(),
	// 	SignatureAlgorithm: spec.GetSignatureAlgorithm(),
	// 	KeyUsage:           spec.GetUsage(),

	// 	BasicConstraintsValid: true,
	// 	IsCA: isCA,
	// }

	// if len(*spec.GetExtensions()) > 0 {
	// 	tmpl.Extensions = *spec.GetExtensions()
	// 	tmpl.ExtraExtensions = *spec.GetExtensions()
	// }
	// if isCA {
	// 	parent = &tmpl
	// }

	// raw, err := x509.CreateCertificate(
	// 	rand.Reader,
	// 	&tmpl,
	// 	parent,
	// 	spec.GetPublicKey(),
	// 	ca.priv,
	// )
	// if isCA && err != nil {
	// 	caLogger.Panic(err)
	// }

	// return raw, err
}

// create certificate and insert to db
func (ca *CA) createCertificate(id string, pub interface{}, usage x509.KeyUsage, timestamp int64, kdfKey []byte, opt ...pkix.Extension) ([]byte, error) {
	spec := NewDefaultCertificateSpec(id, pub, usage, opt...)
	return ca.createCertificateFromSpec(spec, timestamp, kdfKey, true)
}

func (ca *CA) createCertificateFromSpec(spec *CertificateSpec, timestamp int64, kdfKey []byte, persist bool) ([]byte, error) {
	caLogger.Debug("Creating certificate for " + spec.GetID() + ".")

	raw, err := ca.newCertificateFromSpec(spec)
	if err != nil {
		caLogger.Error(err)
		return nil, err
	}

	if persist {
		err = ca.cadb.persistCertificate(spec.GetID(), timestamp, spec.GetUsage(), raw, kdfKey)
	}

	return raw, err
}

func (ca *CA) readCACertificate(name string) ([]byte, error) {
	return readCACertificateFile(ca.path, name)
}

func MemberRoleToString(role pb.Role) (string, error) {
	roleMap := pb.Role_name

	roleStr := roleMap[int32(role)]
	if roleStr == "" {
		return "", errors.New("Undefined user role passed.")
	}

	return roleStr, nil
}
