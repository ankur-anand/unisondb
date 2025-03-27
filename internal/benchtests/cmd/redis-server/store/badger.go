package store

import (
	"errors"
	"hash/crc32"
	"path/filepath"
	"sync/atomic"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
	globalCounter atomic.Uint64
	db            *badger.DB
	opsCount      atomic.Uint64
}

func NewBadgerStore(dir string) (*BadgerStore, error) {
	fp := filepath.Join(dir, "badger")
	opts := badger.DefaultOptions(fp)
	opts.Dir, opts.ValueDir = fp, fp
	opts.MemTableSize = 4 << 20
	opts.ValueThreshold = 1 * 1024

	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerStore{db: badgerDB}, nil
}

func (b *BadgerStore) Set(key, value []byte) error {

	kvEncoded := logcodec.SerializeKVEntry(key, value)

	index := b.globalCounter.Add(1)
	wr := logcodec.LogRecord{
		LSN:           index,
		HLC:           dbkernel.HLCNow(index),
		CRC32Checksum: crc32.ChecksumIEEE(kvEncoded),
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       [][]byte{kvEncoded},
	}

	val := wr.FBEncode(len(kvEncoded) + 512)
	txn := b.db.NewTransaction(true)
	err := txn.Set([]byte(key), val)
	if errors.Is(err, badger.ErrTxnTooBig) {
		_ = txn.Commit()
		txn = b.db.NewTransaction(true)
		err = txn.Set(key, val)
	}
	return txn.Commit()
}

func (b *BadgerStore) Get(key []byte) ([]byte, error) {
	// View is a closure.
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {

		item, err := txn.Get(key)

		if err != nil {
			return err //
		}

		// Copy the value as the value provided Badger is only valid while the
		// transaction is open.
		return item.Value(func(val []byte) error {
			value = make([]byte, len(val))
			copy(value, val)
			return nil
		})

	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}

	record := logcodec.DeserializeLogRecord(value)
	kv := logcodec.DeserializeKVEntry(record.Entries[0])
	return kv.Value, nil
}

func (b *BadgerStore) Close() error {
	return b.db.Close()
}

func (b *BadgerStore) TotalOpsCount() uint64 {
	return 0
}
