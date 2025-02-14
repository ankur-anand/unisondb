package store

import (
	"errors"
	"path/filepath"
	"sync/atomic"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/dgraph-io/badger/v4"
	"github.com/rosedblabs/wal"
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
	opts.MemTableSize = 4 * wal.MB
	opts.ValueThreshold = 1 * wal.KB

	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerStore{db: badgerDB}, nil
}

func (b *BadgerStore) Set(key, value []byte) error {
	wr := walRecord{
		hlc:   storage.HLCNow(b.globalCounter.Add(1)),
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}

	val, err := wr.fbEncode()
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

	record := wrecord.GetRootAsWalRecord(value, 0)
	if record.Operation() == wrecord.LogOperationOpDelete {
		return nil, nil
	}

	return storage.DecompressLZ4(record.ValueBytes())
}

func (b *BadgerStore) Delete(key []byte) error {
	wr := walRecord{
		hlc: storage.HLCNow(b.globalCounter.Add(1)),
		key: key,
		op:  wrecord.LogOperationOpDelete,
	}

	val, err := wr.fbEncode()
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
