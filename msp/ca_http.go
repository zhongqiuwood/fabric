package msp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	
	"strconv"
	"strings"

	cfsslapi "github.com/cloudflare/cfssl/api"
	"github.com/cloudflare/cfssl/log"
	"github.com/hyperledger/fabric-ca/lib/tls"
	"github.com/hyperledger/fabric-ca/util"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/mitchellh/mapstructure"
)

// NewGet create a new GET request
func newGet(endpoint, murl string) (*http.Request, error) {
	curl, err := getURL(endpoint, murl)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", curl, bytes.NewReader([]byte{}))
	if err != nil {
		return nil, fmt.Errorf("Failed creating GET request for %s: %s", curl, err)
	}
	return req, nil
}

// NewPost create a new post request
func newPost(endpoint, murl string, reqBody []byte) (*http.Request, error) {
	curl, err := getURL(endpoint, murl)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", curl, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("Failed posting to %s: %s", curl, err)
	}
	return req, nil
}


type TLSConfig struct {
	enable               bool
	clientTLSConfig      *tls.ClientTLSConfig 
	homeDir              string
	csp                  bccsp.BCCSP
}

// SendReq sends a request to the fabric-ca-server and fills in the result
func SendReq(req *http.Request, result interface{}, tlsConfig *TLSConfig) (err error) {

	reqStr := util.HTTPRequestToString(req)
	log.Debugf("Sending request\n%s", reqStr)

	var tr = new(http.Transport)

	if tlsConfig.enable {
		log.Info("TLS Enabled")

		err = tls.AbsTLSClient(tlsConfig.clientTLSConfig, tlsConfig.homeDir)
		if err != nil {
			return err
		}

		tlsConfig, err2 := tls.GetClientTLSConfig(tlsConfig.clientTLSConfig, tlsConfig.csp)
		if err2 != nil {
			return fmt.Errorf("Failed to get client TLS config: %s", err2)
		}

		tr.TLSClientConfig = tlsConfig
	}

	httpClient := &http.Client{Transport: tr}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST failure [%s]; not sending\n%s", err, reqStr)
	}
	var respBody []byte
	if resp.Body != nil {
		respBody, err = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		if err != nil {
			return fmt.Errorf("Failed to read response [%s] of request:\n%s", err, reqStr)
		}
		log.Debugf("Received response\n%s", util.HTTPResponseToString(resp))
	}
	var body *cfsslapi.Response
	if respBody != nil && len(respBody) > 0 {
		body = new(cfsslapi.Response)
		err = json.Unmarshal(respBody, body)
		if err != nil {
			return fmt.Errorf("Failed to parse response: %s\n%s", err, respBody)
		}
		if len(body.Errors) > 0 {
			msg := body.Errors[0].Message
			return fmt.Errorf("Error response from server was: %s", msg)
		}
	}
	scode := resp.StatusCode
	if scode >= 400 {
		return fmt.Errorf("Failed with server status code %d for request:\n%s", scode, reqStr)
	}
	if body == nil {
		return fmt.Errorf("Empty response body:\n%s", reqStr)
	}
	if !body.Success {
		return fmt.Errorf("Server returned failure for request:\n%s", reqStr)
	}
	log.Debugf("Response body result: %+v", body.Result)
	if result != nil {
		return mapstructure.Decode(body.Result, result)
	}
	return nil
}


func getURL(endpoint, url string) (string, error) {
	nurl, err := NormalizeURL(url)
	if err != nil {
		return "", err
	}
	rtn := fmt.Sprintf("%s/%s", nurl, endpoint)
	return rtn, nil
}


// NormalizeURL normalizes a URL (from cfssl)
func NormalizeURL(addr string) (*url.URL, error) {
	addr = strings.TrimSpace(addr)
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	if u.Opaque != "" {
		u.Host = net.JoinHostPort(u.Scheme, u.Opaque)
		u.Opaque = ""
	} else if u.Path != "" && !strings.Contains(u.Path, ":") {
		u.Host = net.JoinHostPort(u.Path, util.GetServerPort())
		u.Path = ""
	} else if u.Scheme == "" {
		u.Host = u.Path
		u.Path = ""
	}
	if u.Scheme != "https" {
		u.Scheme = "http"
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		_, port, err = net.SplitHostPort(u.Host + ":" + util.GetServerPort())
		if err != nil {
			return nil, err
		}
	}
	if port != "" {
		_, err = strconv.Atoi(port)
		if err != nil {
			return nil, err
		}
	}
	return u, nil
}
