package memdb

import (
	"sync"
)

type (
	Database struct {
		name        string
		collections map[string]*Collection
		lock        sync.RWMutex
	}
)

func New(name string) *Database {
	return &Database{
		name:        name,
		collections: map[string]*Collection{},
		lock:        sync.RWMutex{},
	}
}

func (db *Database) Name() string {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.name
}

func (db *Database) Collection(name string) *Collection {
	db.lock.Lock()
	defer db.lock.Unlock()

	if coll, ok := db.collections[name]; ok {
		return coll
	}

	coll := newCollection(name)
	db.collections[name] = coll

	return coll
}

func (db *Database) Drop() {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, coll := range db.collections {
		coll.Drop()
	}

	db.collections = map[string]*Collection{}
}
