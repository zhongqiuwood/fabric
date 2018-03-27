package msp

import (
	// "net/url"
	// "github.com/hyperledger/fabric-ca/api"
	// "github.com/hyperledger/fabric/bccsp/sw"
	"errors"
	"fmt"
	"os"
	"path"
	"io/ioutil"
	"strings"

	"github.com/cloudflare/cfssl/log"
	"github.com/hyperledger/fabric-ca/util"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric-ca/api"
)


// Client is the fabric-ca client object
type Client struct {
	// The client's home directory
	HomeDir string `json:"homeDir,omitempty"`
	// File and directory paths
	keyFile, certFile, caCertsDir string
	cfgFileName string
	// The crypto service provider (BCCSP)
	CSP bccsp.BCCSP
	ServerHost, ServerPort string
	// Denotes if the client object is already initialized
	isInitialized bool
}

func newClient(csp bccsp.BCCSP, homeDir, keyFile, certFile, caCertsDir string) *Client {
	c := &Client{
		HomeDir: homeDir,
		keyFile: keyFile, 
		certFile: certFile, 
		caCertsDir: caCertsDir,
		CSP: csp,
	}
	return c
}


// Init initializes the client
func (c *Client) Init() error {
	// mkdir struct 
	// home
	//   msp 
	//     keystore
	//       key.pem
	//     signcerts
	//       cert.pem
	//     cacerts
	if !c.isInitialized {
		log.Debugf("Initializing client...")
		mspDir, err := MakeFileAbs("msp", c.HomeDir)
		if err != nil {
			return err
		}
		// Key directory and file
		keyDir := path.Join(mspDir, "keystore")
		err = os.MkdirAll(keyDir, 0700)
		if err != nil {
			return fmt.Errorf("Failed to create keystore directory: %s", err)
		}
		c.keyFile = path.Join(keyDir, "key.pem")
		// Cert directory and file
		certDir := path.Join(mspDir, "signcerts")
		err = os.MkdirAll(certDir, 0755)
		if err != nil {
			return fmt.Errorf("Failed to create signcerts directory: %s", err)
		}
		c.certFile = path.Join(certDir, "cert.pem")
		// CA certs directory
		c.caCertsDir = path.Join(mspDir, "cacerts")
		err = os.MkdirAll(c.caCertsDir, 0755)
		if err != nil {
			return fmt.Errorf("Failed to create cacerts directory: %s", err)
		}
		// Successfully initialized the client
		c.isInitialized = true
	}
	return nil
}

// CheckEnrollment returns an error if this client is not enrolled
func (c *Client) CheckEnrollment() error {
	
	keyFileExists := util.FileExists(c.keyFile)
	certFileExists := util.FileExists(c.certFile)
	if keyFileExists && certFileExists {
		return nil
	}
	// If key file does not exist, but certFile does, key file is probably
	// stored by bccsp, so check to see if this is the case
	if certFileExists {
		_, _, _, err := util.GetSignerFromCertFile(c.certFile, c.CSP)
		if err == nil {
			// Yes, the key is stored by BCCSP
			return nil
		}
	}
	return errors.New("Enrollment information does not exist. Please execute enroll command first. Example: fabric-ca-client enroll -u http://user:userpw@serverAddr:serverPort")
}

func getServerUrl(name, psw string) string {
	return ""
}

func (c *Client) Eroll(name, psw string) error {
	sUrl := getServerUrl(name, psw)
	enrollment := &api.EnrollmentRequest{
		Name: name,
		Secret: psw,
	}
	resp, err := Enroll(c.CSP, sUrl, enrollment)
	if err != nil {
		return err
	}

	ID := resp.Identity
    cfgFilepath := path.Join(c.HomeDir, c.cfgFileName)
	cfgFile, err := ioutil.ReadFile(cfgFilepath)
	if err != nil {
		return err
	}

	cfg := strings.Replace(string(cfgFile), "<<<ENROLLMENT_ID>>>", ID.GetName(), 1)

	err = ioutil.WriteFile(cfgFilepath, []byte(cfg), 0644)

	StoreIdentity(ID.ecert.cert, c.certFile);  // store cert file

	if err != nil {
		return err
	}
	return nil
}
// func doEnroll(rurl string) {
	
// 	purl, err := url.Parse(rurl)
// 	if err != nil || purl.User == nil {
// 		return
// 	}
// 	log.Debugf("%+v \n", purl.User)
// 	murl := purl.String()

// 	secret, _ := purl.User.Password()
// 	enrollment := &api.EnrollmentRequest{
// 		Name: purl.User.Username(),
// 		Secret: secret,
// 	}
// 	purl.User = nil
// 	csp, _ := sw.NewDefaultSecurityLevel("/Users/mr404/Workspace/go/src/github.com/abchain/fabric/msp/keystroe")
// 	log.Debugf(murl)
// 	Enroll(murl, enrollment, csp)
// }


