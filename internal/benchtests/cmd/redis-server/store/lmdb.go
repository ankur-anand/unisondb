package store

import (
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

type LMDBStore struct {
	name          string
	env           *lmdb.Env
	dbi           lmdb.DBI
	globalCounter atomic.Uint64
	ticker        *time.Ticker
}

func NewLMDBStore(namespace, dir string) (*LMDBStore, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}

	if err := env.SetMaxDBs(1); err != nil {
		return nil, err
	}

	if err := env.SetMapSize(10 << 30); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(dir, namespace+".lmdb")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, err
	}

	if err := env.Open(dbPath, lmdb.NoSync, 0644); err != nil {
		return nil, err
	}

	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) error {
		var err error
		dbi, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		env.Close()
		return nil, err
	}

	store := &LMDBStore{
		name:   namespace,
		env:    env,
		dbi:    dbi,
		ticker: time.NewTicker(time.Second),
	}

	go func() {
		for range store.ticker.C {
			err := store.env.Sync(true)
			if err != nil {
				slog.Error("LMDB sync failed", "error", err)
			}
		}
	}()

	return store, nil
}

func (s *LMDBStore) Set(key, value []byte) error {
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

	return s.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(s.dbi, key, val, 0)
	})
}

func (s *LMDBStore) Get(key []byte) ([]byte, error) {
	var value []byte

	err := s.env.View(func(txn *lmdb.Txn) error {
		val, err := txn.Get(s.dbi, key)
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
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

	record := logcodec.DeserializeLogRecord(value)
	kv := logcodec.DeserializeKVEntry(record.Entries[0])
	return kv.Value, nil
}

func (s *LMDBStore) Close() error {
	s.ticker.Stop()
	s.env.Close()
	return nil
}

func (s *LMDBStore) TotalOpsCount() uint64 {
	return s.globalCounter.Load()
}
