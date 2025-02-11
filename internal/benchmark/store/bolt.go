package store

import (
	"log/slog"
	"path/filepath"
	"sync/atomic"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"go.etcd.io/bbolt"
)

type BoltStore struct {
	name          string
	db            *bbolt.DB
	globalCounter atomic.Uint64
}

func NewBoltStore(namespace, dir string) (*BoltStore, error) {
	fp := filepath.Join(dir, namespace+".boltdb")
	db, err := bbolt.Open(fp, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &BoltStore{
		name: namespace,
		db:   db,
	}, nil
}

func (s *BoltStore) Set(key, value []byte) error {
	wr := walRecord{
		hlc:   storage.HLCNow(s.globalCounter.Add(1)),
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}

	val, err := wr.fbEncode()
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(s.name))
		if err != nil {
			return err
		}
		return bucket.Put(key, val)
	})

}

func (s *BoltStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(s.name))
		if bucket == nil {
			return nil
		}
		val := bucket.Get(key)
		value = make([]byte, len(val))
		copy(value, val)
		return nil
	})

	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, nil
	}

	if value == nil {
		return nil, nil
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Info("recovered from panic in BoltStore.Get: %s", r)
		}
	}()
	record := wrecord.GetRootAsWalRecord(value, 0)
	if record.Operation() == wrecord.LogOperationOpDelete {
		return nil, nil
	}

	return storage.DecompressLZ4(record.ValueBytes())
}

func (s *BoltStore) Delete(key []byte) error {
	wr := walRecord{
		hlc: storage.HLCNow(s.globalCounter.Add(1)),
		key: key,
		op:  wrecord.LogOperationOpDelete,
	}

	val, err := wr.fbEncode()
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(s.name))
		if err != nil {
			return err
		}
		return bucket.Put(key, val)
	})
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) TotalOpsCount() uint64 {
	return 0
}
