package store

import (
	"hash/crc32"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"go.etcd.io/bbolt"
)

type BoltStore struct {
	name          string
	db            *bbolt.DB
	globalCounter atomic.Uint64
	ticker        *time.Ticker
}

func NewBoltStore(namespace, dir string) (*BoltStore, error) {
	fp := filepath.Join(dir, namespace+".boltdb")
	db, err := bbolt.Open(fp, 0600, nil)
	if err != nil {
		return nil, err
	}
	db.NoSync = true
	ticker := time.NewTicker(time.Second)
	go func() {
		for range ticker.C {
			err := db.Sync()
			if err != nil {
				panic(err)
			}
		}
	}()
	return &BoltStore{
		name:   namespace,
		db:     db,
		ticker: ticker,
	}, nil
}

func (s *BoltStore) Set(key, value []byte) error {
	kvEncoded := logcodec.SerializeKVEntry(key, value)

	index := s.globalCounter.Add(1)
	wr := logcodec.LogRecord{
		LSN:           index,
		HLC:           dbkernel.HLCNow(),
		CRC32Checksum: crc32.ChecksumIEEE(kvEncoded),
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       [][]byte{kvEncoded},
	}

	val := wr.FBEncode(len(kvEncoded) + 512)

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
	record := logcodec.DeserializeLogRecord(value)
	kv := logcodec.DeserializeKVEntry(record.Entries[0])
	return kv.Value, nil
}

func (s *BoltStore) Close() error {
	s.ticker.Stop()
	return s.db.Close()
}

func (s *BoltStore) TotalOpsCount() uint64 {
	return 0
}
