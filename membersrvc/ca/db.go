/*
    struct of database table.
	Certificate: {
		id               VARCHAR(64), 
		timestamp        INTEGER, 
		usage            INTEGER, 
		cert             BLOB, 
		hash             BLOB, 
		kdfkey           BLOB
	}
	User: {
		id               VARCHAR(64), 
		enrollmentId     VARCHAR(100), 
		role             INTEGER,    
		metadata         VARCHAR(256), 
		token            BLOB, 
		state            INTEGER, 
		key              BLOB
	}
	AffiliationGroup: {
		name             VARCHAR(64), 
		parent           INTEGER, FOREIGN KEY(parent) REFERENCES AffiliationGroups(row)
	}
	Attribute: {
		id               VARCHAR(64), 
		affiliation      VARCHAR(64), 
		attributeName    VARCHAR(64), 
		attributeValue   BLOB
		validFrom        DATETIME, 
		validTo          DATETIME, 
	}
	TCertificateSet: {
		enrollmentID     VARCHAR(64), 
		timestamp        INTEGER, 
		nonce            BLOB, 
		kdfkey           BLOB
	}
*/
package ca

import (
	"fmt"
	"crypto/x509"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
	"github.com/op/go-logging"
	"github.com/abchain/fabric/core/crypto/primitives"
	pb "github.com/abchain/fabric/membersrvc/protos"
	_ "github.com/mattn/go-sqlite3" // This blank import is required to load sqlite3 driver
)

const (
	initTableCertificateStr       = "CREATE TABLE IF NOT EXISTS Certificates (row INTEGER PRIMARY KEY, id VARCHAR(64), timestamp INTEGER, usage INTEGER, cert BLOB, hash BLOB, kdfkey BLOB)"
	initTableUsersStr             = "CREATE TABLE IF NOT EXISTS Users (row INTEGER PRIMARY KEY, id VARCHAR(64), enrollmentId VARCHAR(100), role INTEGER, metadata VARCHAR(256), token BLOB, state INTEGER, key BLOB)"
	initTableAffiliationGroupsStr = "CREATE TABLE IF NOT EXISTS AffiliationGroups (row INTEGER PRIMARY KEY, name VARCHAR(64), parent INTEGER, FOREIGN KEY(parent) REFERENCES AffiliationGroups(row))"
	// Attribute table
	initTableAttributesStr        = "CREATE TABLE IF NOT EXISTS Attributes (row INTEGER PRIMARY KEY, id VARCHAR(64), affiliation VARCHAR(64), attributeName VARCHAR(64), validFrom DATETIME, validTo DATETIME,  attributeValue BLOB)"
	// TCA Certificate table
	initTableTCertificateSets     = "CREATE TABLE IF NOT EXISTS TCertificateSets (row INTEGER PRIMARY KEY, enrollmentID VARCHAR(64), timestamp INTEGER, nonce BLOB, kdfkey BLOB)"
)

var (
	// mutex           = &sync.RWMutex{}
	cadbLogger = logging.MustGetLogger("cadb")
)

// TableInitializer is a function protocal for table initialization

type TableInitializer func(*sql.DB) error


// CADB is the database component for ca
type CADB struct {
	db *sql.DB
}

func (cadb *CADB) initAllTable() error {
	var err error
	// common tables : affiliation, user, certificate
	if err = initializeCommonTables(cadb.db); err != nil {
		return err;
	}
	// tcertificate
	if err = initializeTCATables(cadb.db); err != nil {
		return err;
	}
	// attribute
	if err = initializeACATables(cadb.db); err != nil {
		return err;
	}
	return nil
}

func NewCADB(dbpath string, initTables TableInitializer) *CADB {
	cadb := new(CADB)
	// open or create certificate database
	db, err := sql.Open("sqlite3", dbpath) // ca.path+"/"+name+".db"
	if err != nil {
		caLogger.Panic(err)
	}

	if err = db.Ping(); err != nil {
		caLogger.Panic(err)
	}

	if err = initTables(db); err != nil {
		caLogger.Panic(err)
	}
	cadb.db = db
	return cadb
}

func (cadb *CADB) close() error {
	return cadb.db.Close()
}

// init table affiliation, user and certficate
func initializeCommonTables(db *sql.DB) error {
	// affiliation
	if _, err := db.Exec(initTableAffiliationGroupsStr); err != nil {
		return err
	}
	// user
	if _, err := db.Exec(initTableUsersStr); err != nil {
		return err
	}
	// certificate
	if _, err := db.Exec(initTableCertificateStr); err != nil {
		return err
	}
	return nil
}

// ******************************** Certificate ***********************************
func (cadb *CADB) persistCertificate(id string, timestamp int64, usage x509.KeyUsage, certRaw []byte, kdfKey []byte) error {
	mutex.Lock()
	defer mutex.Unlock()
	
	hash := primitives.NewHash()
	hash.Write(certRaw)
	var err error
	sqlStr := "INSERT INTO Certificates (id, timestamp, usage, cert, hash, kdfkey) VALUES (?, ?, ?, ?, ?, ?)"
	if _, err = cadb.db.Exec(sqlStr, id, timestamp, usage, certRaw, hash.Sum(nil), kdfKey); err != nil {
		cadbLogger.Error(err)
	}
	return err
}

func (cadb *CADB) readCertificateByKeyUsage(id string, usage x509.KeyUsage) ([]byte, error) {
	cadbLogger.Debugf("Reading certificate for %s and usage %v", id, usage)

	mutex.RLock()
	defer mutex.RUnlock()

	var raw []byte
	sqlStr := "SELECT cert FROM Certificates WHERE id=? AND usage=?"
	err := cadb.db.QueryRow(sqlStr, id, usage).Scan(&raw)

	if err != nil {
		cadbLogger.Debugf("readCertificateByKeyUsage() Error: %v", err)
	}

	return raw, err
}

func (cadb *CADB) readCertificateByTimestamp(id string, ts int64) ([]byte, error) {
	cadbLogger.Debug("Reading certificate for " + id + ".")

	mutex.RLock()
	defer mutex.RUnlock()

	var raw []byte
	sqlStr := "SELECT cert FROM Certificates WHERE id=? AND timestamp=?"
	err := cadb.db.QueryRow(sqlStr, id, ts).Scan(&raw)

	return raw, err
}

func (cadb *CADB) readCertificates(id string, opt ...int64) (*sql.Rows, error) {
	cadbLogger.Debug("Reading certificatess for " + id + ".")

	mutex.RLock()
	defer mutex.RUnlock()
	sqlStr := "SELECT cert FROM Certificates WHERE id=?"
	if len(opt) > 0 && opt[0] != 0 {
		sqlStr = "SELECT cert FROM Certificates WHERE id=? AND timestamp=? ORDER BY usage"
		return cadb.db.Query(sqlStr, id, opt[0])
	}

	return cadb.db.Query(sqlStr, id)
}

func (cadb *CADB) readCertificateSets(id string, start, end int64) (*sql.Rows, error) {
	cadbLogger.Debug("Reading certificate sets for " + id + ".")

	mutex.RLock()
	defer mutex.RUnlock()
	sqlStr := "SELECT cert, timestamp FROM Certificates WHERE id=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp"
	return cadb.db.Query(sqlStr, id, start, end)
}

func (cadb *CADB) readCertificateByHash(hash []byte) ([]byte, error) {
	cadbLogger.Debug("Reading certificate for hash " + string(hash) + ".")

	mutex.RLock()
	defer mutex.RUnlock()

	var raw []byte
	sqlStr := "SELECT cert FROM Certificates WHERE hash=?"
	row := cadb.db.QueryRow(sqlStr, hash)
	err := row.Scan(&raw)

	return raw, err
}

// ******************************** Affiliation ***********************************

func (cadb *CADB) isValidAffiliation(affiliation string) (bool, error) {
	cadbLogger.Debug("Validating affiliation: " + affiliation)

	mutex.RLock()
	defer mutex.RUnlock()

	var count int
	var err error
	sqlStr := "SELECT count(row) FROM AffiliationGroups WHERE name=?"
	err = cadb.db.QueryRow(sqlStr, affiliation).Scan(&count)
	if err != nil {
		caLogger.Debug("Affiliation <" + affiliation + "> is INVALID.")

		return false, err
	}
	cadbLogger.Debug("Affiliation <" + affiliation + "> is VALID.")

	return count == 1, nil
}

func (cadb *CADB) CheckAndAddAffiliationGroup(name, parentName string) error {
	var parentID int
	var err error
	var count int
	err = cadb.db.QueryRow("SELECT count(row) FROM AffiliationGroups WHERE name=?", name).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return errors.New("Affiliation group " + name + " is already registered")
	}

	if strings.Compare(parentName, "") != 0 {
		err = cadb.db.QueryRow("SELECT row FROM AffiliationGroups WHERE name=?", parentName).Scan(&parentID)
		if err != nil {
			return err
		}
	}

	_, err = cadb.db.Exec("INSERT INTO AffiliationGroups (name, parent) VALUES (?, ?)", name, parentID)
	return err
}

func (cadb *CADB) ReadAffiliationGroups() ([]*AffiliationGroup, error) {
	cadbLogger.Debug("Reading affilition groups.")

	rows, err := cadb.db.Query("SELECT row, name, parent FROM AffiliationGroups")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := make(map[int64]*AffiliationGroup)

	for rows.Next() {
		group := new(AffiliationGroup)
		var id int64
		if e := rows.Scan(&id, &group.name, &group.parentID); e != nil {
			return nil, err
		}
		groups[id] = group
	}

	groupList := make([]*AffiliationGroup, len(groups))
	idx := 0
	for _, eachGroup := range groups {
		eachGroup.parent = groups[eachGroup.parentID]
		groupList[idx] = eachGroup
		idx++
	}

	return groupList, nil
}

func (cadb *CADB) deleteAffiliation(name string) error {
	fmt.Println("deleteAffiliation " + name)

	mutex.Lock()
	defer mutex.Unlock()
	// var count int
	// var err error
	// sqlStr := "SELECT count(row) FROM AffiliationGroups WHERE name=?"
	// err = cadb.db.QueryRow(sqlStr, name).Scan(&count)
	// if err != nil {
	// 	return  err
	// }
	// sqlStr := "SELECT count(row) FROM AffiliationGroups WHERE name=?"
	// err = cadb.db.QueryRow(sqlStr, name).Scan(&count)
	// if err != nil {
	// 	return  err
	// }
	
	v, err := cadb.db.Exec("DELETE FROM AffiliationGroups WHERE name=?", name)
	if err != nil {
		// fmt.Println(err)
		cadbLogger.Error(err)
	}
	fmt.Println(v)
	
	return err
}
// ******************************* User **********************************


// readUser reads a token given an id
func (cadb *CADB) readUser(id string) *sql.Row {
	cadbLogger.Debug("Reading token for " + id + ".")

	mutex.RLock()
	defer mutex.RUnlock()

	return cadb.db.QueryRow("SELECT role, token, state, key, enrollmentId FROM Users WHERE id=?", id)
}

// readUsers reads users of a given Role
func (cadb *CADB) readUsers(role int) (*sql.Rows, error) {
	cadbLogger.Debug("Reading users matching role " + strconv.FormatInt(int64(role), 2) + ".")

	return cadb.db.Query("SELECT id, role FROM Users WHERE role&?!=0", role)
}

func (cadb *CADB) fetchAllUsers(rule string) ([]*User, error) {
	cadbLogger.Debug("Reading all users")
	rows, err := cadb.db.Query(fmt.Sprintf("SELECT id, enrollmentId, role FROM Users %s", rule))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make([]*User, 0)

	for rows.Next() {
		user := new(User)
		if e := rows.Scan(&user.Id, &user.EnrollmentID, &user.Role); e != nil {
			return nil, err
		}
		users = append(users, user)
	}

	return users, nil
}

// checkMetadata query user that as the registrar metadata
func (cadb *CADB) checkMetadata(registrar string) (string, error) {
	var registrarMetadataStr string
	err := cadb.db.QueryRow("SELECT metadata FROM Users WHERE id=?", registrar).Scan(&registrarMetadataStr)
	if err != nil {
		return "", err
	}
	return registrarMetadataStr, nil
}

// readRole returns the user Role given a user id
func (cadb *CADB) readRole(id string) int {
	cadbLogger.Debug("Reading role for " + id + ".")

	mutex.RLock()
	defer mutex.RUnlock()

	var role int
	cadb.db.QueryRow("SELECT role FROM Users WHERE id=?", id).Scan(&role)

	return role
}

func (cadb *CADB) checkAndAddUser(id string, enrollID string, tok string, role pb.Role, memberMetadata string) error {
	var row int
	err := cadb.db.QueryRow("SELECT row FROM Users WHERE id=?", id).Scan(&row)
	if err == nil {
		return errors.New("User is already registered")
	}

	_, err = cadb.db.Exec("INSERT INTO Users (id, enrollmentId, token, role, metadata, state) VALUES (?, ?, ?, ?, ?, ?)", id, enrollID, tok, role, memberMetadata, 0)

	if err != nil {
		cadbLogger.Error(err)
	}
	cadbLogger.Info("user insert" + enrollID)
	return err
}

// deleteUser deletes a user given a name
func (cadb *CADB) deleteUser(id string) error {
	cadbLogger.Debug("Deleting user " + id + ".")

	mutex.Lock()
	defer mutex.Unlock()

	var row int
	err := cadb.db.QueryRow("SELECT row FROM Users WHERE id=?", id).Scan(&row)
	if err == nil {
		_, err = cadb.db.Exec("DELETE FROM Certificates Where id=?", id)
		if err != nil {
			cadbLogger.Error(err)
		}

		_, err = cadb.db.Exec("DELETE FROM Users WHERE row=?", row)
		if err != nil {
			cadbLogger.Error(err)
		}
	}

	return err
}


// *********************************** ACA attrbution *********************************

// init attrbutes table
func initializeACATables(db *sql.DB) error {
	if _, err := db.Exec(initTableAttributesStr); err != nil {
		return err
	}
	return nil
}

func (cadb *CADB) InsertAttributes(attrs []*AttributePair) error {

	cadbLogger.Debugf("PopulateAttributes: %+v", attrs)

	mutex.Lock()
	defer mutex.Unlock()

	tx, dberr := cadb.db.Begin()
	if dberr != nil {
		return dberr
	}
	for _, attr := range attrs {
		cadbLogger.Debugf("attr: %+v", attr)
		if err := cadb.insertAttribute(tx, attr); err != nil {
			dberr = tx.Rollback()
			if dberr != nil {
				return dberr
			}
			return err
		}
	}
	dberr = tx.Commit()
	if dberr != nil {
		return dberr
	}
	return nil
}

func (cadb *CADB) insertAttribute(tx *sql.Tx, attr *AttributePair) error {
	var count int
	err := tx.QueryRow("SELECT count(row) AS cant FROM Attributes WHERE id=? AND affiliation =? AND attributeName =?",
		attr.GetID(), attr.GetAffiliation(), attr.GetAttributeName()).Scan(&count)

	if err != nil {
		return err
	}

	if count > 0 {
		_, err = tx.Exec("UPDATE Attributes SET validFrom = ?, validTo = ?,  attributeValue = ? WHERE  id=? AND affiliation =? AND attributeName =? AND validFrom < ?",
			attr.GetValidFrom(), attr.GetValidTo(), attr.GetAttributeValue(), attr.GetID(), attr.GetAffiliation(), attr.GetAttributeName(), attr.GetValidFrom())
		if err != nil {
			return err
		}
	} else {
		_, err = tx.Exec("INSERT INTO Attributes (validFrom , validTo,  attributeValue, id, affiliation, attributeName) VALUES (?,?,?,?,?,?)",
			attr.GetValidFrom(), attr.GetValidTo(), attr.GetAttributeValue(), attr.GetID(), attr.GetAffiliation(), attr.GetAttributeName())
		if err != nil {
			return err
		}
	}
	return nil
}

func (cadb *CADB) addOrUpdateUserAttribute(attr *AttributePair) error {
	var count int
	selectSql := "SELECT count(row) AS cant FROM Attributes WHERE id=? AND affiliation =? AND attributeName =?"
	err := cadb.db.QueryRow(selectSql, attr.GetID(), attr.GetAffiliation(), attr.GetAttributeName()).Scan(&count)

	if err != nil {
		return err
	}

	if count > 0 {
		updateSql := "UPDATE Attributes SET validFrom = ?, validTo = ?,  attributeValue = ? WHERE  id=? AND affiliation =? AND attributeName =? AND validFrom < ?"
		_, err = cadb.db.Exec(updateSql, 
			attr.GetValidFrom(), 
			attr.GetValidTo(), 
			attr.GetAttributeValue(), 
			attr.GetID(), 
			attr.GetAffiliation(), 
			attr.GetAttributeName(), 
			attr.GetValidFrom(),
		)
		if err != nil {
			return err
		}
	} else {
		insertSql := "INSERT INTO Attributes (validFrom , validTo,  attributeValue, id, affiliation, attributeName) VALUES (?,?,?,?,?,?)"
		_, err = cadb.db.Exec(insertSql,
			attr.GetValidFrom(), 
			attr.GetValidTo(), 
			attr.GetAttributeValue(), 
			attr.GetID(), 
			attr.GetAffiliation(), 
			attr.GetAttributeName(),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cadb *CADB) fetchAttributes(id string) ([]*AttributePair, error) {
	
	rows, err := cadb.db.Query("SELECT id, affiliation, attributeName, attributeValue, validFrom, validTo FROM Attributes WHERE id=?", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	aps := make([]*AttributePair, 0)

	for rows.Next() {
		ap := &AttributePair{
			owner: &AttributeOwner{
			},
		}

		if e := rows.Scan(&ap.owner.id, &ap.owner.affiliation, 
			&ap.attributeName, &ap.attributeValue, &ap.validFrom, &ap.validTo); e != nil {
			return nil, err
		}
		aps = append(aps, ap)
	}

	return aps, nil
}

func (cadb *CADB) findAttribute(owner *AttributeOwner, attributeName string) (*AttributePair, error) {
	var count int

	mutex.RLock()
	defer mutex.RUnlock()

	err := cadb.db.QueryRow("SELECT count(row) AS cant FROM Attributes WHERE id=? AND affiliation =? AND attributeName =?",
		owner.GetID(), owner.GetAffiliation(), attributeName).Scan(&count)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	var attName string
	var attValue []byte
	var validFrom, validTo time.Time
	err = cadb.db.QueryRow("SELECT attributeName, attributeValue, validFrom, validTo AS cant FROM Attributes WHERE id=? AND affiliation =? AND attributeName =?",
		owner.GetID(), owner.GetAffiliation(), attributeName).Scan(&attName, &attValue, &validFrom, &validTo)
	if err != nil {
		return nil, err
	}

	return &AttributePair{owner, attName, attValue, validFrom, validTo}, nil
}

// deleteAttribute delete attribute
func (cadb *CADB) deleteAttributeOfUser(userId string) error {
	cadbLogger.Debug("Deleting user " + userId + ".")

	mutex.Lock()
	defer mutex.Unlock()

	_, err := cadb.db.Exec("DELETE FROM Attributes WHERE id=?", userId)
	if err != nil {
		cadbLogger.Error(err)
	}
	
	return err
}

// deleteAttribute delete attribute
func (cadb *CADB) deleteAttribute(userId, attrName string) error {
	fmt.Println("deleteAttribute " + userId + "." + attrName)

	mutex.Lock()
	defer mutex.Unlock()
	
	_, err := cadb.db.Exec("DELETE FROM Attributes WHERE id=? and attributeName=?", userId, attrName)
	if err != nil {
		fmt.Println(err)
		cadbLogger.Error(err)
	}
	return err
}

/******************************* TCA *********************************/

func initializeTCATables(db *sql.DB) error {
	var err error

	// err = initializeCommonTables(db)
	// if err != nil {
	// 	return err
	// }

	if _, err = db.Exec(initTableTCertificateSets); err != nil {
		return err
	}

	return err
}

func (cadb *CADB) persistCertificateSet(enrollmentID string, timestamp int64, nonce []byte, kdfKey []byte) error {
	mutex.Lock()
	defer mutex.Unlock()

	var err error

	if _, err = cadb.db.Exec("INSERT INTO TCertificateSets (enrollmentID, timestamp, nonce, kdfkey) VALUES (?, ?, ?, ?)", enrollmentID, timestamp, nonce, kdfKey); err != nil {
		tcaLogger.Error(err)
	}
	return err
}

func (cadb *CADB) retrieveCertificateSets(enrollmentID string) (*sql.Rows, error) {
	return cadb.db.Query("SELECT enrollmentID, timestamp, nonce, kdfkey FROM TCertificateSets WHERE enrollmentID=?", enrollmentID)
}
