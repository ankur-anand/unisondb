package store

import (
	"errors"
	"path/filepath"
	"sync/atomic"

	"github.com/ankur-anand/kvalchemy/dbkernel"
	"github.com/ankur-anand/kvalchemy/dbkernel/compress"
	"github.com/ankur-anand/kvalchemy/dbkernel/wal/walrecord"
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
	compressed, err := compress.CompressLZ4(value)
	if err != nil {
		return err
	}
	wr := walrecord.Record{
		Hlc:          dbkernel.HLCNow(b.globalCounter.Add(1)),
		Key:          key,
		Value:        compressed,
		LogOperation: walrecord.LogOperationInsert,
	}

	val, err := wr.FBEncode()
	if err != nil {
		return err
	}

	txn := b.db.NewTransaction(true)
	err = txn.Set([]byte(key), val)
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

	record := walrecord.GetRootAsWalRecord(value, 0)
	if record.Operation() == walrecord.LogOperationDelete {
		return nil, nil
	}

	return compress.DecompressLZ4(record.ValueBytes())
}

func (b *BadgerStore) Delete(key []byte) error {
	wr := walrecord.Record{
		Hlc:          dbkernel.HLCNow(b.globalCounter.Add(1)),
		Key:          key,
		LogOperation: walrecord.LogOperationDelete,
	}

	val, err := wr.FBEncode()
	if err != nil {
		return err
	}
	txn := b.db.NewTransaction(true)
	err = txn.Set([]byte(key), val)
	if errors.Is(err, badger.ErrTxnTooBig) {
		_ = txn.Commit()
		txn = b.db.NewTransaction(true)
		err = txn.Set(key, val)
	}
	return txn.Commit()
}

func (b *BadgerStore) Close() error {
	return b.db.Close()
}

func (b *BadgerStore) TotalOpsCount() uint64 {
	return 0
}
