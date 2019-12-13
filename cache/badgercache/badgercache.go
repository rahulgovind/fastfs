package badgercache

import (
	"bytes"
	"github.com/dgraph-io/badger"
	log "github.com/sirupsen/logrus"
)

type BadgerCache struct {
	db *badger.DB
}

func NewBadgerCache() *BadgerCache {
	var err error
	bc := new(BadgerCache)
	bc.db, err = badger.Open(badger.DefaultOptions("/tmp/badger2"))
	if err != nil {
		log.Fatal(err)
	}
	return bc
}

func (bc *BadgerCache) Add(key string, value []byte) {
	bc.db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte(key), []byte(value))
		return nil
	})
}

func (bc *BadgerCache) Get(key string) ([]byte, bool) {
	var buf bytes.Buffer

	ok := false
	err := bc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == nil {
			item.Value(func(v []byte) error {
				buf.Write(v)
				ok = true
				return nil
			})
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes(), ok
}

func (bc *BadgerCache) Remove(key string) {
	bc.db.Update(func(txn *badger.Txn) error {
		txn.Delete([]byte(key))
		return nil
	})
}

func (bc *BadgerCache) Len() int {
	return -1
}

func (bc *BadgerCache) Clear() {
	log.Info("Can't clear cache with badger")
	return
}
