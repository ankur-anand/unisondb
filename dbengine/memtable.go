package dbengine

import (
	"errors"
	"log/slog"

	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
)

var (
	errArenaSizeWillExceed = errors.New("arena capacity will exceed the limit")

	dbBatchSize = 32
)

// Add a margin to avoid boundary issues, arena uses the same pool for itself.
const arenaSafetyMargin = 1 * 1024

// memTable hold the underlying skip list and
// the last chunk position in the wIO, associated
// with this skip list
//
//nolint:unused
type memTable struct {
	skipList    *skl.Skiplist
	lastOffset  *wal.Offset
	firstOffset *wal.Offset

	capacity    int64
	opCount     int
	bytesStored int
	db          BTreeStore
	wIO         *wal.WalIO
	namespace   string
}

// newMemTable returns an initialized mem-table.
func newMemTable(capacity int64, db BTreeStore, wIO *wal.WalIO, namespace string) *memTable {
	return &memTable{
		skipList:  skl.NewSkiplist(capacity),
		capacity:  capacity,
		db:        db,
		wIO:       wIO,
		namespace: namespace,
	}
}

// skip list arena has a fixed capacity, if adding the given key-value pair would exceed its capacity
// the skip list panic. verify before to avoid the panic.
func (table *memTable) canPut(key []byte, val y.ValueStruct) bool {
	return table.skipList.MemSize()+
		int64(len(y.KeyWithTs(key, 0)))+
		int64(val.EncodedSize())+arenaSafetyMargin <= table.capacity
}

func (table *memTable) put(key []byte, val y.ValueStruct, pos *wal.Offset) error {
	if !table.canPut(key, val) {
		return errArenaSizeWillExceed
	}
	// we only save one key no MVCC.
	table.skipList.Put(y.KeyWithTs(key, 0), val)

	if table.firstOffset == nil {
		table.firstOffset = pos
	}
	table.lastOffset = pos
	table.opCount++
	table.bytesStored = table.bytesStored + len(key) + len(val.Value)
	return nil
}

func (table *memTable) get(key []byte) y.ValueStruct {
	return table.skipList.Get(y.KeyWithTs(key, 0))
}

func (table *memTable) opsCount() int {
	return table.opCount
}

// flush writes all entries from MemTable to BtreeStore.
func (table *memTable) flush() (int, error) {
	slog.Debug("Flushing MemTable to BtreeStore...", "namespace", table.namespace)

	// Create an iterator for the MemTable
	it := table.skipList.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)

	count := 0

	var setKeys, setValues, deleteKeys [][]byte

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
		key := y.ParseKey(it.Key())
		entry := it.Value()

		if entry.Meta == byte(walrecord.LogOperationDelete) {
			deleteKeys = append(deleteKeys, key)
			continue
		}

		record, err := getWalRecord(entry, table.wIO)
		if err != nil {
			return 0, err
		}

		// record is txn commited and chunked
		if record.TxnStatus() == walrecord.TxnStatusCommit && record.ValueType() == walrecord.ValueTypeChunked {
			n, err := table.flushChunkedTxnCommit(record)
			if err != nil {
				return 0, err
			}
			// total number of batch record + one for batch start marker that was not part of the record.
			count += n + 1

			continue
		}

		// else it would be set value only.
		setKeys = append(setKeys, record.KeyBytes())
		setValues = append(setValues, record.ValueBytes())

		// Flush when batch capacity is reached
		if len(deleteKeys) >= dbBatchSize || len(setKeys) >= dbBatchSize {
			if err := table.processBatch(&setKeys, &setValues, &deleteKeys); err != nil {
				return 0, err
			}
		}
	}

	return count, table.processBatch(&setKeys, &setValues, &deleteKeys)
}

func (table *memTable) processBatch(setKeys, setValues, deleteKeys *[][]byte) error {
	if len(*deleteKeys) > 0 {
		if err := table.db.DeleteMany(*deleteKeys); err != nil {
			return err
		}
		*deleteKeys = nil
	}
	if len(*setKeys) > 0 {
		if err := table.db.SetMany(*setKeys, *setValues); err != nil {
			return err
		}
		*setKeys = nil
		*setValues = nil
	}
	return nil
}

// flushChunkedTxnCommit returns the number of batch record that was inserted.
func (table *memTable) flushChunkedTxnCommit(record *walrecord.WalRecord) (int, error) {
	return handleChunkedValuesTxn(record, table.wIO, table.db)
}
