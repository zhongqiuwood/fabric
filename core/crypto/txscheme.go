package crypto

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/hmac"
	"errors"
	"github.com/abchain/fabric/core/crypto/primitives"
	obc "github.com/abchain/fabric/protos"
	"math/big"
)

/*
	crypto schemes for transaction, including the key-derivation of tx signature and encryption of attribution
*/

func TxToSignatureMsg(tx *obc.Transaction) ([]byte, error) {

	digest, err := tx.Digest()
	if err != nil {
		return nil, err
	}

	return bytes.Join([][]byte{digest, tx.GetCert()}, nil), nil
}

// the secret algorithm with double HMAC
func derivedSecret(kdfkey []byte, tidx []byte) []byte {

	mac := hmac.New(primitives.GetDefaultHash(), kdfkey)
	mac.Write([]byte{2})
	mac = hmac.New(primitives.GetDefaultHash(), mac.Sum(nil))
	mac.Write(tidx)

	return mac.Sum(nil)
}

var bi_one = new(big.Int).SetInt64(1)

//ECDSA version
func TxCertKeyDerivationEC(pub *ecdsa.PublicKey, kdfkey []byte, tidx []byte) (*ecdsa.PublicKey, error) {

	k := new(big.Int).SetBytes(derivedSecret(kdfkey, tidx))
	k.Mod(k, new(big.Int).Sub(pub.Curve.Params().N, bi_one))
	k.Add(k, bi_one)

	tmpX, tmpY := pub.ScalarBaseMult(k.Bytes())
	txX, txY := pub.Curve.Add(pub.X, pub.Y, tmpX, tmpY)
	return &ecdsa.PublicKey{Curve: pub.Curve, X: txX, Y: txY}, nil
}

func TxCertPrivKDerivationEC(privk *ecdsa.PrivateKey, kdfkey []byte, tidx []byte) (*ecdsa.PrivateKey, error) {

	tempSK := &ecdsa.PrivateKey{
		PublicKey: ecdsa.PublicKey{
			Curve: privk.Curve,
			X:     new(big.Int),
			Y:     new(big.Int),
		},
		D: new(big.Int),
	}

	var k = new(big.Int).SetBytes(derivedSecret(kdfkey, tidx))
	n := new(big.Int).Sub(privk.Params().N, bi_one)
	k.Mod(k, n)
	k.Add(k, bi_one)

	tempSK.D.Add(privk.D, k)
	tempSK.D.Mod(tempSK.D, privk.Params().N)

	tempX, tempY := privk.PublicKey.ScalarBaseMult(k.Bytes())
	tempSK.PublicKey.X, tempSK.PublicKey.Y =
		tempSK.PublicKey.Add(
			privk.PublicKey.X, privk.PublicKey.Y,
			tempX, tempY,
		)

	// Verify temporary public key is a valid point on the reference curve
	if !tempSK.Curve.IsOnCurve(tempSK.PublicKey.X, tempSK.PublicKey.Y) {
		return nil, errors.New("Failed temporary public key IsOnCurve check.")
	}

	return tempSK, nil
}
