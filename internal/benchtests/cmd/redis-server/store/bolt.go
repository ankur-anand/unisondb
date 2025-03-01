package store

import (
	"log/slog"
	"path/filepath"
	"sync/atomic"

	"github.com/ankur-anand/kvalchemy/dbengine"
	"github.com/ankur-anand/kvalchemy/dbengine/compress"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
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
	compressed, err := compress.CompressLZ4(value)
	if err != nil {
		return err
	}
	wr := walrecord.Record{
		Hlc:          dbengine.HLCNow(s.globalCounter.Add(1)),
		Key:          key,
		Value:        compressed,
		LogOperation: walrecord.LogOperationInsert,
	}

	val, err := wr.FBEncode()
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
	record := walrecord.GetRootAsWalRecord(value, 0)
	if record.Operation() == walrecord.LogOperationDelete {
		return nil, nil
	}

	return compress.DecompressLZ4(record.ValueBytes())
}

func (s *BoltStore) Delete(key []byte) error {
	wr := walrecord.Record{
		Hlc:          dbengine.HLCNow(s.globalCounter.Add(1)),
		Key:          key,
		LogOperation: walrecord.LogOperationDelete,
	}

	val, err := wr.FBEncode()
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
