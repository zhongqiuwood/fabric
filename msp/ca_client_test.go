package msp
import (
	"testing"
)


func TestEroll(t *testing.T) {
	doEnroll("http://peer1:peer1pw@localhost:7054")
}