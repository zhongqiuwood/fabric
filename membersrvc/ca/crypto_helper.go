package ca

import (
	_ "crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"

	// "github.com/abchain/fabric/core/crypto/primitives"
)


// GetDefaultCurve returns the default elliptic curve used by the crypto layer
func GetDefaultCurve() elliptic.Curve {
	return elliptic.P256()
}

func createCAKeyPair(path, name string) *ecdsa.PrivateKey {
	caLogger.Debug("function createCAKeyPair")

	curve := GetDefaultCurve() // primitives.GetDefaultCurve()

	priv, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err == nil {
		raw, _ := x509.MarshalECPrivateKey(priv)
		cooked := pem.EncodeToMemory(
			&pem.Block{
				Type:  "ECDSA PRIVATE KEY",
				Bytes: raw,
			})
		err = ioutil.WriteFile(path+"/"+name+".priv", cooked, 0644)
		if err != nil {
			caLogger.Panic(err)
		}

		raw, _ = x509.MarshalPKIXPublicKey(&priv.PublicKey)
		cooked = pem.EncodeToMemory(
			&pem.Block{
				Type:  "ECDSA PUBLIC KEY",
				Bytes: raw,
			})
		err = ioutil.WriteFile(path+"/"+name+".pub", cooked, 0644)
		if err != nil {
			caLogger.Panic(err)
		}
	}
	if err != nil {
		caLogger.Panic(err)
	}

	return priv
}

func readCAPrivateKeyFile(path, name string) (*ecdsa.PrivateKey, error) {
	caLogger.Debug("func readCAPrivateKey.")

	cooked, err := ioutil.ReadFile(path + "/" + name + ".priv")
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(cooked)
	return x509.ParseECPrivateKey(block.Bytes)
}

func createCACertificateFIle(path, name string, raw []byte) error {
	caLogger.Debug("func createCACertificate Creating CA certificate.")

	cooked := pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: raw,
		})
	err := ioutil.WriteFile(path+"/"+name+".cert", cooked, 0644)

	return err
}

func readCACertificateFile(path, name string) ([]byte, error) {
	caLogger.Debug("function readCACertificate, Reading CA certificate.")

	cooked, err := ioutil.ReadFile(path + "/" + name + ".cert")
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(cooked)
	return block.Bytes, nil
}


func newCertificateFromSpec(spec *CertificateSpec, parentCert *x509.Certificate, priv *ecdsa.PrivateKey) ([]byte, error) {
	notBefore := spec.GetNotBefore()
	notAfter := spec.GetNotAfter()

	parent := parentCert
	isCA := parent == nil

	tmpl := x509.Certificate{
		SerialNumber: spec.GetSerialNumber(),
		Subject: pkix.Name{
			CommonName:   spec.GetCommonName(),
			Organization: []string{spec.GetOrganization()},
			Country:      []string{spec.GetCountry()},
		},
		NotBefore: *notBefore,
		NotAfter:  *notAfter,

		SubjectKeyId:       *spec.GetSubjectKeyID(),
		SignatureAlgorithm: spec.GetSignatureAlgorithm(),
		KeyUsage:           spec.GetUsage(),

		BasicConstraintsValid: true,
		IsCA: isCA,
	}

	if len(*spec.GetExtensions()) > 0 {
		tmpl.Extensions = *spec.GetExtensions()
		tmpl.ExtraExtensions = *spec.GetExtensions()
	}
	if isCA {
		parent = &tmpl
	}

	raw, err := x509.CreateCertificate(
		rand.Reader,
		&tmpl,
		parent,
		spec.GetPublicKey(),
		priv,
	)
	if isCA && err != nil {
		caLogger.Panic(err)
	}

	return raw, err
}