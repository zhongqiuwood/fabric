package msp
import (
	"fmt"
	"path"
	"os"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
    "strings"
	"io/ioutil"
	"path/filepath"

	"github.com/cloudflare/cfssl/log"
	"github.com/hyperledger/fabric-ca/api"
	"github.com/hyperledger/fabric-ca/util"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/utils"
)
// StoreMyIdentity stores my identity to disk
func StoreIdentity(cert []byte, filePath string) error {
	// WriteFile writes a file
	err := ioutil.WriteFile(filePath, cert, 0644)
	if err != nil {
		return fmt.Errorf("Failed to store my certificate: %s", err)
	}
	return nil
}

// LoadCSRInfo reads CSR (Certificate Signing Request) from a file
// @parameter path The path to the file contains CSR info in JSON format
func LoadCSRInfo(path string) (*api.CSRInfo, error) {
	csrJSON, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var csrInfo api.CSRInfo
	err = util.Unmarshal(csrJSON, &csrInfo, "LoadCSRInfo")
	if err != nil {
		return nil, err
	}
	return &csrInfo, nil
}

// LoadIdentity loads an identity from disk
func LoadIdentity(csp bccsp.BCCSP, keyFile, certFile string) (*Identity, error) {
	log.Debug("Loading identity: keyFile=%s, certFile=%s", keyFile, certFile)
	cert, err := util.ReadFile(certFile)
	if err != nil {
		log.Debugf("No cert found at %s", certFile)
		return nil, err
	}
	key, _, _, err := util.GetSignerFromCertFile(certFile, csp)
	if err != nil {
		// Fallback: attempt to read out of keyFile and import
		log.Debugf("No key found in BCCSP keystore, attempting fallback")
		key, err = ImportBCCSPKeyFromPEM(keyFile, csp, true)
		if err != nil {
			return nil, fmt.Errorf("Could not find the private key in BCCSP keystore nor in keyfile %s: %s", keyFile, err)
		}
	}
	name, err := GetEnrollmentIDFromPEM(cert)
	if err != nil {
		return nil, err
	}
	return newIdentity(csp, name, key, cert), nil
}

// ImportBCCSPKeyFromPEM attempts to create a private BCCSP key from a pem file keyFile
func ImportBCCSPKeyFromPEM(keyFile string, myCSP bccsp.BCCSP, temporary bool) (bccsp.Key, error) {
	keyBuff, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	key, err := utils.PEMtoPrivateKey(keyBuff, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing private key from %s: %s", keyFile, err.Error())
	}
	switch key.(type) {
	case *ecdsa.PrivateKey:
		priv, err := utils.PrivateKeyToDER(key.(*ecdsa.PrivateKey))
		if err != nil {
			return nil, fmt.Errorf("Failed to convert ECDSA private key from %s: %s", keyFile, err.Error())
		}
		sk, err := myCSP.KeyImport(priv, &bccsp.ECDSAPrivateKeyImportOpts{Temporary: temporary})
		if err != nil {
			return nil, fmt.Errorf("Failed to import ECDSA private key from %s: %s", keyFile, err.Error())
		}
		return sk, nil
	case *rsa.PrivateKey:
		return nil, fmt.Errorf("Failed to import RSA key from %s; RSA private key import is not supported", keyFile)
	default:
		return nil, fmt.Errorf("Failed to import key from %s: invalid secret key type", keyFile)
	}
}

// GetEnrollmentIDFromPEM returns the EnrollmentID from a PEM buffer
func GetEnrollmentIDFromPEM(cert []byte) (string, error) {
	x509Cert, err := GetX509CertificateFromPEM(cert)
	if err != nil {
		return "", err
	}
	return GetEnrollmentIDFromX509Certificate(x509Cert), nil
}

// GetX509CertificateFromPEM get an X509 certificate from bytes in PEM format
func GetX509CertificateFromPEM(cert []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(cert)
	if block == nil {
		return nil, errors.New("Failed to PEM decode certificate")
	}
	x509Cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Error parsing certificate: %s", err)
	}
	return x509Cert, nil
}

// GetEnrollmentIDFromX509Certificate returns the EnrollmentID from the X509 certificate
func GetEnrollmentIDFromX509Certificate(cert *x509.Certificate) string {
	return cert.Subject.CommonName
}


func MakeFileAbs(file, dir string) (string, error) {
	if file == "" {
		return "", nil
	}
	if filepath.IsAbs(file) {
		return file, nil
	}
	path, err := filepath.Abs(filepath.Join(dir, file))
	if err != nil {
		return "", fmt.Errorf("Failed making '%s' absolute based on '%s'", file, dir)
	}
	return path, nil
}

// Store the CAChain in the CACerts folder of MSP (Membership Service Provider)
// The 1st cert in the chain goes into MSP 'cacerts' directory.
// The others (if any) go into the MSP 'intermediates' directory.
func storeCAChain(caName, serverHost, profile string, si *GetServerInfoResponse) error {
	mspDir := "msp"
	fname := serverHost
	if caName != "" {
		fname = fmt.Sprintf("%s-%s", fname, caName)
	}
	fname = strings.Replace(fname, ":", "-", -1)
	fname = strings.Replace(fname, ".", "-", -1) + ".pem"
	// Split the root and intermediate certs
	block, intermediateCerts := pem.Decode(si.CAChain)
	if block == nil {
		return errors.New("No root certificate was found")
	}
	rootCert := pem.EncodeToMemory(block)
	dirPrefix := dirPrefixByProfile(profile)
	// Store the root certificate in "cacerts"
	certsDir := fmt.Sprintf("%scacerts", dirPrefix)
	err := storeFile("CA root certificate", mspDir, certsDir, fname, rootCert)
	if err != nil {
		return err
	}
	// Store the intermediate certs if there are any
	if len(intermediateCerts) > 0 {
		certsDir = fmt.Sprintf("%sintermediatecerts", dirPrefix)
		err = storeFile("CA intermediate certificates", mspDir, certsDir, fname, intermediateCerts)
		if err != nil {
			return err
		}
	}
	return nil
}

func storeFile(what, mspDir, subDir, fname string, contents []byte) error {
	dir := path.Join(mspDir, subDir)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("Failed to create directory for %s at '%s': %s", what, dir, err)
	}
	fpath := path.Join(dir, fname)
	err = util.WriteFile(fpath, contents, 0644)
	if err != nil {
		return fmt.Errorf("Failed to store %s at '%s': %s", what, fpath, err)
	}
	log.Infof("Stored %s at %s", what, fpath)
	return nil
}

// Return the prefix to add to the "cacerts" and "intermediatecerts" directories
// based on the target profile.  If the profile is "tls", these directories become
// "tlscacerts" and "tlsintermediatecerts", respectively.  There is no prefix for
// any other profile.
func dirPrefixByProfile(profile string) string {
	if profile == "tls" {
		return "tls"
	}
	return ""
}