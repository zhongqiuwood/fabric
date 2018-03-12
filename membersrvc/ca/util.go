package ca

import (
	"errors"
	mrand "math/rand"
	"strings"
	"time"

	gp "google/protobuf"
	"github.com/op/go-logging"
	pb "github.com/abchain/fabric/membersrvc/protos"
)

var caLogger = logging.MustGetLogger("ca")

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

// Convert the protobuf array of attributes to the AttributePair array format
// as required by the ACA code to populate the table
func toAttributePairs(id, affiliation string, attrs []*pb.Attribute) ([]*AttributePair, error) {
	var pairs = make([]*AttributePair, 0)
	for _, attr := range attrs {
		vals := []string{id, affiliation, attr.Name, attr.Value, attr.NotBefore, attr.NotAfter}
		pair, err := NewAttributePair(vals, nil)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, pair)
	}
	caLogger.Debugf("toAttributePairs: id=%s, affiliation=%s, attrs=%v, pairs=%v\n",
		id, affiliation, attrs, pairs)
	return pairs, nil
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

// Return an error if all strings in 'strs1' are not contained in 'strs2'
func checkDelegateRoles(strs1 []string, strs2 []string, registrar string) error {
	caLogger.Debugf("CA.checkDelegateRoles: registrar=%s, strs1=%+v, strs2=%+v\n", registrar, strs1, strs2)
	for _, s := range strs1 {
		if !strContained(s, strs2) {
			caLogger.Debugf("CA.checkDelegateRoles: no: %s not in %+v\n", s, strs2)
			return errors.New("user " + registrar + " may not register delegateRoles " + s)
		}
	}
	caLogger.Debug("CA.checkDelegateRoles: ok")
	return nil
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

//
// MemberRoleToString converts a member role representation from int32 to a string,
// according to the Role enum defined in ca.proto.
//
func MemberRoleToString(role pb.Role) (string, error) {
	roleMap := pb.Role_name

	roleStr := roleMap[int32(role)]
	if roleStr == "" {
		return "", errors.New("Undefined user role passed.")
	}

	return roleStr, nil
}
