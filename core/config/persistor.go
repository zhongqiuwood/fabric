package config

import (
	"github.com/abchain/fabric/core/db"
)

// Persistor enables module to persist and restore data to the database
type Persistor interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
}

type persistor struct {
	prefix string
}

func NewPersistor(p string) Persistor {

	return &persistor{p}
}

func (p *persistor) buildStoreKey(key string) []byte {
	return []byte(p.prefix + key)
}

// Store enables a peer to persist the given key,value pair to the database
func (p *persistor) Store(key string, value []byte) error {
	dbhandler := db.GetGlobalDBHandle()

	//dbg.Infof("add db.PersistCF: <%s> --> <%x>", key, value)
	return dbhandler.PutValue(db.PersistCF, p.buildStoreKey(key), value)
}

// Load enables a peer to read the value that corresponds to the given database key
func (p *persistor) Load(key string) ([]byte, error) {
	dbhandler := db.GetGlobalDBHandle()
	return dbhandler.GetValue(db.PersistCF, p.buildStoreKey(key))
}
