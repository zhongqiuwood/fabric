package util

import (
	"testing"
)

func TestChainedBytes(t *testing.T) {

	first1 := InitCryptoChainedBytes()
	if len(first1) != chainedBytesLen {
		t.Fatalf("Wrong size for first byte: %x", first1)
	}

	second1 := GenCryptoChainedBytes(first1, nil)

	if err := CheckCryptoChainedBytes(first1, second1); err != nil {
		t.Fatal("check fail:", err)
	}

	if err := CheckCryptoChainedBytes([]byte{2, 3, 3}, second1); err == nil {
		t.Fatal("Can not get failure")
	}

	if err := CheckCryptoChainedBytes(first1, []byte{2, 3, 3}); err == nil {
		t.Fatal("Can not get failure")
	}

	if err := CheckCryptoChainedBytes(append(first1, 'a'), second1); err == nil {
		t.Fatal("Can not get failure")
	}

	if err := CheckCryptoChainedBytes(first1, append(second1, 'a')); err != nil {
		t.Fatal("check fail:", err)
	}

	third1 := GenCryptoChainedBytes(second1, []byte{2, 3, 3})

	if err := CheckCryptoChainedBytes(second1, third1); err != nil {
		t.Fatal("check fail:", err)
	}

	fourth1 := GenCryptoChainedBytes(third1, nil)

	if err := CheckCryptoChainedBytes(third1, fourth1); err != nil {
		t.Fatal("check fail:", err)
	}
}
