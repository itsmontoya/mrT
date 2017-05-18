package mapDB

import (
	"sync"

	"github.com/itsmontoya/middleware"
	"github.com/itsmontoya/mrT"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrKeyDoesNotExist is returned when a key does not exist within the map
	ErrKeyDoesNotExist = errors.Error("key does not exist")
)

// New will return a new map db
func New(dir, name string) (mp *MapDB, err error) {
	var m MapDB
	// Initialize map
	m.m = make(map[string]string)

	// Encryption middleware
	cmw := middleware.NewCryptyMW([]byte("         encryption key         "), make([]byte, 16))
	if cmw == nil {

	}

	// Create a new instance of mrT
	if m.mrT, err = mrT.New(dir, name); err != nil {
		return
	}

	if err = m.mrT.ForEach(m.load); err != nil {
		return
	}

	// Assign pointer to our MapDB
	mp = &m
	return
}

// MapDB is a simple map database
type MapDB struct {
	// Mutex for thread-safety
	mux sync.RWMutex
	// Internal map store
	m map[string]string
	// Our backend-storage
	mrT *mrT.MrT
	// Closed state
	closed bool
}

func (m *MapDB) load(lineType byte, key, value []byte) (end bool) {
	switch lineType {
	case mrT.PutLine:
		m.m[string(key)] = string(value)
	case mrT.DeleteLine:
		delete(m.m, string(key))
	}

	return
}

func (m *MapDB) populate(txn *mrT.Txn) (err error) {
	for key, value := range m.m {
		if err = txn.Put([]byte(key), []byte(value)); err != nil {
			return
		}
	}

	return
}

// Get will retrieve a value from the DB
func (m *MapDB) Get(key string) (value string, err error) {
	var ok bool
	m.mux.RLock()
	defer m.mux.RUnlock()

	if m.closed {
		err = errors.ErrIsClosed
		return
	}

	if value, ok = m.m[key]; !ok {
		err = ErrKeyDoesNotExist
		return
	}

	return
}

// Put will set a value for a given key within the DB
func (m *MapDB) Put(key, value string) (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.closed {
		err = errors.ErrIsClosed
		return
	}

	if err = m.mrT.Txn(func(txn *mrT.Txn) (err error) {
		txn.Put([]byte(key), []byte(value))
		return
	}); err != nil {
		return
	}

	m.m[key] = value
	return
}

// Delete will remove a value for a given key within the DB
func (m *MapDB) Delete(key string) (err error) {
	var ok bool
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.closed {
		err = errors.ErrIsClosed
		return
	}

	if _, ok = m.m[key]; !ok {
		return ErrKeyDoesNotExist
	}

	if err = m.mrT.Txn(func(txn *mrT.Txn) (err error) {
		txn.Delete([]byte(key))
		return
	}); err != nil {
		return
	}

	delete(m.m, key)
	return
}

// ForEach will iterate through all the values within the DB
func (m *MapDB) ForEach(fn ForEachFn) (ended bool) {
	m.mux.RLock()
	defer m.mux.RUnlock()
	// Iterate through all the map values
	for key, value := range m.m {
		if ended = fn(key, value); ended {
			return
		}
	}

	return
}

// Txn will begin a new transaction
func (m *MapDB) Txn(fn func(*Txn) error) (err error) {
	var txn Txn
	m.mux.Lock()
	defer m.mux.Unlock()

	return m.mrT.Txn(func(t *mrT.Txn) (err error) {
		txn.txn = t
		txn.m = m.m
		err = fn(&txn)
		txn.txn = nil
		txn.m = nil
		return
	})
}

// Close will close map db
func (m *MapDB) Close() (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.closed {
		err = errors.ErrIsClosed
		return
	}

	var errs errors.ErrorList
	errs.Push(m.mrT.Archive(m.populate))

	// Close underlying Mr.T
	errs.Push(m.mrT.Close())
	// Zero-out map db values
	m.closed = true
	m.m = nil
	m.mrT = nil
	return errs.Err()
}

// ForEachFn is used for iterating over map db items
type ForEachFn func(key, value string) (end bool)

// Txn is a MapDB transaction
type Txn struct {
	txn *mrT.Txn
	m   map[string]string
}

// Get will get a value
func (t *Txn) Get(key string) (value string, err error) {
	var ok bool
	if value, ok = t.m[key]; !ok {
		err = ErrKeyDoesNotExist
		return
	}

	return
}

// Put will put a value
func (t *Txn) Put(key, value string) (err error) {
	if err = t.txn.Put([]byte(key), []byte(value)); err != nil {
		return
	}

	t.m[key] = value
	return
}

// Delete will delete a value
func (t *Txn) Delete(key string) (err error) {
	var ok bool
	if _, ok = t.m[key]; !ok {
		return ErrKeyDoesNotExist
	}

	if err = t.txn.Delete([]byte(key)); err != nil {
		return
	}

	delete(t.m, key)
	return
}
