package util

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

const (
	chainedBytesLen = 16
)

func GenCryptoChainedBytes(src []byte, appended []byte) []byte {
	out := sha256.Sum256(src)
	return bytes.Join([][]byte{out[:chainedBytesLen], appended}, nil)
}

//we just take the first 16bytes in next for verify, so more message can be appended
//to the tail of bytes
func CheckCryptoChainedBytes(prev []byte, next []byte) error {

	if len(prev) < chainedBytesLen {
		return fmt.Errorf("Length is short for prev chained bytes: %d", len(prev))
	} else if len(next) < chainedBytesLen {
		return fmt.Errorf("Length is short for next chained bytes: %d", len(next))
	}

	expected := sha256.Sum256(prev)
	if bytes.Compare(next[:chainedBytesLen], expected[:chainedBytesLen]) != 0 {
		return fmt.Errorf("Fail chained bytes, expected %x but get %x", expected[:chainedBytesLen], next)
	}
	return nil
}

func InitCryptoChainedBytes(appended ...byte) []byte {

	ret := GenerateBytesUUID()
	if len(ret) < chainedBytesLen {
		return append(append(ret, make([]byte, chainedBytesLen-len(ret))...), appended...)
	}

	return append(ret[:chainedBytesLen], appended...)
}
