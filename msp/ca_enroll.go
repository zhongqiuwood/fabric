package msp

import (
	"fmt"
	"os"

	"github.com/cloudflare/cfssl/csr"
	"github.com/cloudflare/cfssl/log"
	"github.com/hyperledger/fabric-ca/api"
	"github.com/hyperledger/fabric-ca/util"
	"github.com/hyperledger/fabric/bccsp"
)


// GetServerInfoResponse is the response from the GetServerInfo call
type GetServerInfoResponse struct {
	// CAName is the name of the CA
	CAName string
	// CAChain is the PEM-encoded bytes of the fabric-ca-server's CA chain.
	// The 1st element of the chain is the root CA cert
	CAChain []byte
}

// EnrollmentResponse is the response from Client.Enroll and Identity.Reenroll
type EnrollmentResponse struct {
	Identity   *Identity
	ServerInfo GetServerInfoResponse
}

// The enrollment response from the server
type enrollmentResponseNet struct {
	// Base64 encoded PEM-encoded ECert
	Cert string
	// The server information
	ServerInfo serverInfoResponseNet
}

// The response to the GET /info request
type serverInfoResponseNet struct {
	// CAName is a unique name associated with fabric-ca-server's CA
	CAName string
	// Base64 encoding of PEM-encoded certificate chain
	CAChain string
}

// Enroll enrolls a new identity
// @param req The enrollment request
func Enroll(csp bccsp.BCCSP, caServerUrl string, req *api.EnrollmentRequest) (*EnrollmentResponse, error) {
	log.Debugf("Enrolling %+v", req)

	// Generate the CSR
	csrPEM, key, err := GenCSR(csp, req.CSR, req.Name)
	if err != nil {
		return nil, fmt.Errorf("Failure generating CSR: %s", err)
	}
	reqNet, _ := makeEnrollmentRequestNet(req, csrPEM)
	body, err := util.Marshal(reqNet, "SignRequest")
	if err != nil {
		return nil, err
	}
	// Send the CSR to the fabric-ca server with basic auth header
	post, err := newPost("enroll", caServerUrl, body)
	if err != nil {
		return nil, err
	}
	post.SetBasicAuth(req.Name, req.Secret)
	var result enrollmentResponseNet
	tlsConfig := &TLSConfig{
		enable: false,
	}
	err = SendReq(post, &result, tlsConfig) 
	if err != nil {
		return nil, err
	}
	fmt.Printf("%+v \n", result)
	return nil, nil

	// Create the enrollment response
	return newEnrollmentResponse(csp, &result, req.Name, key)
}

func makeEnrollmentRequestNet(req *api.EnrollmentRequest, csrPEM []byte) (*api.EnrollmentRequestNet, error) {
	reqNet := &api.EnrollmentRequestNet{
		CAName: req.CAName,
	}
	if req.CSR != nil {
		reqNet.SignRequest.Hosts = req.CSR.Hosts
	}
	reqNet.SignRequest.Request = string(csrPEM)
	reqNet.SignRequest.Profile = req.Profile
	reqNet.SignRequest.Label = req.Label
	return reqNet, nil
}

// GenCSR generates a CSR (Certificate Signing Request)
func GenCSR(csp bccsp.BCCSP, req *api.CSRInfo, id string) ([]byte, bccsp.Key, error) {
	log.Debugf("GenCSR %+v", req)

	cr := newCertificateRequest(req)
	cr.CN = id

	if cr.KeyRequest == nil {
		cr.KeyRequest = csr.NewBasicKeyRequest()
	}

	key, cspSigner, err := util.BCCSPKeyRequestGenerate(cr, csp)
	if err != nil {
		log.Debugf("failed generating BCCSP key: %s", err)
		return nil, nil, err
	}

	csrPEM, err := csr.Generate(cspSigner, cr)
	if err != nil {
		log.Debugf("failed generating CSR: %s", err)
		return nil, nil, err
	}

	return csrPEM, key, nil
}

// newCertificateRequest creates a certificate request which is used to generate
// a CSR (Certificate Signing Request)
func newCertificateRequest(req *api.CSRInfo) *csr.CertificateRequest {
	cr := csr.CertificateRequest{}
	if req != nil && req.Names != nil {
		cr.Names = req.Names
	}
	if req != nil && req.Hosts != nil {
		cr.Hosts = req.Hosts
	} else {
		// Default requested hosts are local hostname
		hostname, _ := os.Hostname()
		if hostname != "" {
			cr.Hosts = make([]string, 1)
			cr.Hosts[0] = hostname
		}
	}
	if req != nil && req.KeyRequest != nil {
		cr.KeyRequest = req.KeyRequest
	}
	if req != nil {
		cr.CA = req.CA
		cr.SerialNumber = req.SerialNumber
	}
	return &cr
}

// ********************** response ************************

// newEnrollmentResponse creates a client enrollment response from a network response
// @param result The result from server
// @param id Name of identity being enrolled or reenrolled
// @param key The private key which was used to sign the request
func newEnrollmentResponse(csp bccsp.BCCSP, result *enrollmentResponseNet, idName string, key bccsp.Key) (*EnrollmentResponse, error) {
	log.Debugf("newEnrollmentResponse %s", idName)
	certByte, err := util.B64Decode(result.Cert)
	if err != nil {
		return nil, fmt.Errorf("Invalid response format from server: %s", err)
	}
	resp := &EnrollmentResponse{
		Identity: newIdentity(csp, idName, key, certByte),
	}
	err = net2LocalServerInfo(&result.ServerInfo, &resp.ServerInfo)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Convert from network to local server information
func net2LocalServerInfo(srvInfoResp *serverInfoResponseNet, local *GetServerInfoResponse) error {
	caChain, err := util.B64Decode(srvInfoResp.CAChain)
	if err != nil {
		return err
	}
	local.CAName = srvInfoResp.CAName
	local.CAChain = caChain
	return nil
}

// GetCertFilePath returns the path to the certificate file for this client
func (c *Client) GetCertFilePath() string {
	return c.certFile
}


