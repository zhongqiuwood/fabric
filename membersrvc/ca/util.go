package ca

import (
	mrand "math/rand"
	"strings"
	"time"

	gp "google/protobuf"
	pb "github.com/abchain/fabric/membersrvc/protos"
)


// Return true if 'str' is in 'strs'; otherwise return false
func strContained(str string, strs []string) bool {
	for _, s := range strs {
		if s == str {
			return true
		}
	}
	return false
}

// Return true if 'str' is prefixed by any string in 'strs'; otherwise return false
func isPrefixed(str string, strs []string) bool {
	for _, s := range strs {
		if strings.HasPrefix(str, s) {
			return true
		}
	}
	return false
}

// convert a role to a string
func role2String(role int) string {
	if role == int(pb.Role_CLIENT) {
		return "client"
	} else if role == int(pb.Role_PEER) {
		return "peer"
	} else if role == int(pb.Role_VALIDATOR) {
		return "validator"
	} else if role == int(pb.Role_AUDITOR) {
		return "auditor"
	}
	return ""
}

// Remove outer quotes from a string if necessary
func removeQuotes(str string) string {
	if str == "" {
		return str
	}
	if (strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'")) ||
		(strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\"")) {
		str = str[1 : len(str)-1]
	}
	caLogger.Debugf("removeQuotes: %s\n", str)
	return str
}

func convertTime(ts *gp.Timestamp) time.Time {
	var t time.Time
	if ts == nil {
		t = time.Unix(0, 0).UTC()
	} else {
		t = time.Unix(ts.Seconds, int64(ts.Nanos)).UTC()
	}
	return t
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var rnd = mrand.NewSource(time.Now().UnixNano())

func randomString(n int) string {
	b := make([]byte, n)

	for i, cache, remain := n-1, rnd.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rnd.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
