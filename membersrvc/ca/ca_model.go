package ca

import (
	"fmt"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"

	pb "github.com/abchain/fabric/membersrvc/protos"
	"github.com/golang/protobuf/ptypes/timestamp"
)


// CertificateSpec defines the parameter used to create a new certificate.
type CertificateSpec struct {
	id           string
	commonName   string
	serialNumber *big.Int
	pub          interface{}
	usage        x509.KeyUsage
	NotBefore    *time.Time
	NotAfter     *time.Time
	ext          *[]pkix.Extension
}

// NewCertificateSpec creates a new certificate spec
func NewCertificateSpec(id string, commonName string, serialNumber *big.Int, pub interface{}, usage x509.KeyUsage, notBefore *time.Time, notAfter *time.Time, opt ...pkix.Extension) *CertificateSpec {
	spec := new(CertificateSpec)
	spec.id = id
	spec.commonName = commonName
	spec.serialNumber = serialNumber
	spec.pub = pub
	spec.usage = usage
	spec.NotBefore = notBefore
	spec.NotAfter = notAfter
	spec.ext = &opt
	return spec
}

// GetID returns the spec's ID field/value
//
func (spec *CertificateSpec) GetID() string {
	return spec.id
}

// GetCommonName returns the spec's Common Name field/value
//
func (spec *CertificateSpec) GetCommonName() string {
	return spec.commonName
}

// GetSerialNumber returns the spec's Serial Number field/value
//
func (spec *CertificateSpec) GetSerialNumber() *big.Int {
	return spec.serialNumber
}

// GetPublicKey returns the spec's Public Key field/value
//
func (spec *CertificateSpec) GetPublicKey() interface{} {
	return spec.pub
}

// GetUsage returns the spec's usage (which is the x509.KeyUsage) field/value
//
func (spec *CertificateSpec) GetUsage() x509.KeyUsage {
	return spec.usage
}

// GetNotBefore returns the spec NotBefore (time.Time) field/value
//
func (spec *CertificateSpec) GetNotBefore() *time.Time {
	return spec.NotBefore
}

// GetNotAfter returns the spec NotAfter (time.Time) field/value
//
func (spec *CertificateSpec) GetNotAfter() *time.Time {
	return spec.NotAfter
}

// GetOrganization returns the spec's Organization field/value
//
func (spec *CertificateSpec) GetOrganization() string {
	return caOrganization
}

// GetCountry returns the spec's Country field/value
//
func (spec *CertificateSpec) GetCountry() string {
	return caCountry
}

// GetSubjectKeyID returns the spec's subject KeyID
//
func (spec *CertificateSpec) GetSubjectKeyID() *[]byte {
	return &[]byte{1, 2, 3, 4}
}

// GetSignatureAlgorithm returns the X509.SignatureAlgorithm field/value
//
func (spec *CertificateSpec) GetSignatureAlgorithm() x509.SignatureAlgorithm {
	return x509.ECDSAWithSHA384
}

// GetExtensions returns the sepc's extensions
//
func (spec *CertificateSpec) GetExtensions() *[]pkix.Extension {
	return spec.ext
}

// Convert a string to a MemberMetadata
func newMemberMetadata(metadata string) (*MemberMetadata, error) {
	if metadata == "" {
		caLogger.Debug("newMemberMetadata: nil")
		return nil, nil
	}
	var mm MemberMetadata
	err := json.Unmarshal([]byte(metadata), &mm)
	if err != nil {
		caLogger.Debugf("newMemberMetadata: error: %s, metadata: %s\n", err.Error(), metadata)
	}
	caLogger.Debugf("newMemberMetadata: metadata=%s, object=%+v\n", metadata, mm)
	return &mm, err
}

// MemberMetadata Additional member metadata
// MemberMetadata ex: {"registrar": {"roles": ["client", "peer", "validator", "auditor"], "delegateRoles": ["client"]}}
type MemberMetadata struct {
	Registrar Registrar `json:"registrar"`
}

// Registrar metadata
type Registrar struct {
	Roles         []string `json:"roles"`
	DelegateRoles []string `json:"delegateRoles"`
}

// See if member 'registrar' can register a member of type 'newRole'
// with MemberMetadata of 'newMemberMetadata'
func (mm *MemberMetadata) canRegister(registrar string, newRole string, newMemberMetadata *MemberMetadata) error {
	// Can register a member of this type?
	caLogger.Debugf("MM.canRegister registrar=%s, newRole=%s\n", registrar, newRole)
	if !strContained(newRole, mm.Registrar.Roles) {
		caLogger.Debugf("MM.canRegister: role %s can't be registered by %s\n", newRole, registrar)
		return errors.New("member " + registrar + " may not register member of type " + newRole)
	}

	// The registrar privileges that are being registered must not be larger than the registrar's
	if newMemberMetadata == nil {
		// Not requesting registrar privileges for this member, so we are OK
		caLogger.Debug("MM.canRegister: not requesting registrar privileges")
		return nil
	}

	// Make sure this registrar is not delegating an invalid role
	err := checkDelegateRoles(newMemberMetadata.Registrar.Roles, mm.Registrar.DelegateRoles, registrar)
	if err != nil {
		caLogger.Debug("MM.canRegister: checkDelegateRoles failure")
		return err
	}

	// Can register OK
	caLogger.Debug("MM.canRegister: OK")
	return nil
}

// AffiliationGroup struct
type AffiliationGroup struct {
	name     string
	parentID int64
	parent   *AffiliationGroup
	preKey   []byte
}

type User struct {
	Id			   string
	EnrollmentID   string
	Role           int32
	Psw            string
	Affiliation    *AffiliationGroup
	Metadata       *MemberMetadata
}

// NewDefaultPeriodCertificateSpec creates a new certificate spec with notBefore a minute ago and not after 90 days from notBefore.
//
func NewDefaultPeriodCertificateSpec(id string, serialNumber *big.Int, pub interface{}, usage x509.KeyUsage, opt ...pkix.Extension) *CertificateSpec {
	return NewDefaultPeriodCertificateSpecWithCommonName(id, id, serialNumber, pub, usage, opt...)
}

// NewDefaultPeriodCertificateSpecWithCommonName creates a new certificate spec with notBefore a minute ago and not after 90 days from notBefore and a specifc commonName.
//
func NewDefaultPeriodCertificateSpecWithCommonName(id string, commonName string, serialNumber *big.Int, pub interface{}, usage x509.KeyUsage, opt ...pkix.Extension) *CertificateSpec {
	notBefore := time.Now().Add(-1 * time.Minute)
	notAfter := notBefore.Add(time.Hour * 24 * 90)
	return NewCertificateSpec(id, commonName, serialNumber, pub, usage, &notBefore, &notAfter, opt...)
}

// NewDefaultCertificateSpec creates a new certificate spec with serialNumber = 1, notBefore a minute ago and not after 90 days from notBefore.
//
func NewDefaultCertificateSpec(id string, pub interface{}, usage x509.KeyUsage, opt ...pkix.Extension) *CertificateSpec {
	serialNumber := big.NewInt(1)
	return NewDefaultPeriodCertificateSpec(id, serialNumber, pub, usage, opt...)
}

// NewDefaultCertificateSpecWithCommonName creates a new certificate spec with serialNumber = 1, notBefore a minute ago and not after 90 days from notBefore and a specific commonName.
//
func NewDefaultCertificateSpecWithCommonName(id string, commonName string, pub interface{}, usage x509.KeyUsage, opt ...pkix.Extension) *CertificateSpec {
	serialNumber := big.NewInt(1)
	return NewDefaultPeriodCertificateSpecWithCommonName(id, commonName, serialNumber, pub, usage, opt...)
}

/*************************************** AttributePair ****************************************/

//AttributeOwner is the struct that contains the data related with the user who owns the attribute.
type AttributeOwner struct {
	id          string
	affiliation string
}

//AttributePair is an struct that store the relation between an owner (user who owns the attribute), attributeName (name of the attribute), attributeValue (value of the attribute),
//validFrom (time since the attribute is valid) and validTo (time until the attribute will be valid).
type AttributePair struct {
	owner          *AttributeOwner
	attributeName  string
	attributeValue []byte
	validFrom      time.Time
	validTo        time.Time
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

//NewAttributePair creates a new attribute pair associated with <attrOwner>.
func NewAttributePair(attributeVals []string, attrOwner *AttributeOwner) (*AttributePair, error) {
	if len(attributeVals) < 6 {
		return nil, errors.New("Invalid attribute entry")
	}
	var attrPair = *new(AttributePair)
	if attrOwner != nil {
		attrPair.SetOwner(attrOwner)
	} else {
		attrPair.SetOwner(&AttributeOwner{strings.TrimSpace(attributeVals[0]), strings.TrimSpace(attributeVals[1])})
	}
	attrPair.SetAttributeName(strings.TrimSpace(attributeVals[2]))
	attrPair.SetAttributeValue([]byte(strings.TrimSpace(attributeVals[3])))
	//Reading validFrom date
	dateStr := strings.TrimSpace(attributeVals[4])
	if dateStr != "" {
		var t time.Time
		var err error
		if t, err = time.Parse(time.RFC3339, dateStr); err != nil {
			return nil, err
		}
		attrPair.SetValidFrom(t)
	}
	//Reading validTo date
	dateStr = strings.TrimSpace(attributeVals[5])
	if dateStr != "" {
		var t time.Time
		var err error
		if t, err = time.Parse(time.RFC3339, dateStr); err != nil {
			return nil, err
		}
		attrPair.SetValidTo(t)
	}
	return &attrPair, nil
}

//GetID returns the id of the attributeOwner.
func (attrOwner *AttributeOwner) GetID() string {
	return attrOwner.id
}

//GetAffiliation returns the affiliation related with the owner.
func (attrOwner *AttributeOwner) GetAffiliation() string {
	return attrOwner.affiliation
}

//GetOwner returns the owner of the attribute pair.
func (attrPair *AttributePair) GetOwner() *AttributeOwner {
	return attrPair.owner
}

//SetOwner sets the owner of the attributes.
func (attrPair *AttributePair) SetOwner(owner *AttributeOwner) {
	attrPair.owner = owner
}

//GetID returns the id of the attributePair.
func (attrPair *AttributePair) GetID() string {
	return attrPair.owner.GetID()
}

//GetAffiliation gets the affilition of the attribute pair.
func (attrPair *AttributePair) GetAffiliation() string {
	return attrPair.owner.GetAffiliation()
}

//GetAttributeName gets the attribute name related with the attribute pair.
func (attrPair *AttributePair) GetAttributeName() string {
	return attrPair.attributeName
}

//SetAttributeName sets the name related with the attribute pair.
func (attrPair *AttributePair) SetAttributeName(name string) {
	attrPair.attributeName = name
}

//GetAttributeValue returns the value of the pair.
func (attrPair *AttributePair) GetAttributeValue() []byte {
	return attrPair.attributeValue
}

//SetAttributeValue sets the value of the pair.
func (attrPair *AttributePair) SetAttributeValue(val []byte) {
	attrPair.attributeValue = val
}

//IsValidFor returns if the pair is valid for date.
func (attrPair *AttributePair) IsValidFor(date time.Time) bool {
	return (attrPair.validFrom.Before(date) || attrPair.validFrom.Equal(date)) && (attrPair.validTo.IsZero() || attrPair.validTo.After(date))
}

//GetValidFrom returns time which is valid from the pair.
func (attrPair *AttributePair) GetValidFrom() time.Time {
	return attrPair.validFrom
}

//SetValidFrom returns time which is valid from the pair.
func (attrPair *AttributePair) SetValidFrom(date time.Time) {
	attrPair.validFrom = date
}

//GetValidTo returns time which is valid to the pair.
func (attrPair *AttributePair) GetValidTo() time.Time {
	return attrPair.validTo
}

//SetValidTo returns time which is valid to the pair.
func (attrPair *AttributePair) SetValidTo(date time.Time) {
	attrPair.validTo = date
}

func (attrPair *AttributePair) ToString() string {
	formatStr := "ownerId: %s, ownerAffiliation: %s, attributeName: %s, attributeValue: %s,  validFrom %s, validTo %s"
	str := fmt.Sprintf(formatStr, attrPair.owner.id, attrPair.owner.affiliation, attrPair.attributeName, attrPair.attributeValue, attrPair.validFrom, attrPair.validTo)
	return str
}

//ToACAAttribute converts the receiver to the protobuf format.
func (attrPair *AttributePair) ToACAAttribute() *pb.ACAAttribute {
	var from, to *timestamp.Timestamp
	if attrPair.validFrom.IsZero() {
		from = nil
	} else {
		from = &timestamp.Timestamp{Seconds: attrPair.validFrom.Unix(), Nanos: int32(attrPair.validFrom.UnixNano())}
	}
	if attrPair.validTo.IsZero() {
		to = nil
	} else {
		to = &timestamp.Timestamp{Seconds: attrPair.validTo.Unix(), Nanos: int32(attrPair.validTo.UnixNano())}

	}
	return &pb.ACAAttribute{AttributeName: attrPair.attributeName, AttributeValue: attrPair.attributeValue, ValidFrom: from, ValidTo: to}
}

var (
	//ACAAttribute is the base OID to the attributes extensions.
	ACAAttribute = asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6, 10}
)

//IsAttributeOID returns if the oid passed as parameter is or not linked with an attribute
func IsAttributeOID(oid asn1.ObjectIdentifier) bool {
	l := len(oid)
	if len(ACAAttribute) != l {
		return false
	}
	for i := 0; i < l-1; i++ {
		if ACAAttribute[i] != oid[i] {
			return false
		}
	}

	return ACAAttribute[l-1] < oid[l-1]
}
