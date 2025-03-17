package memtable

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
)

var (
	ErrArenaSizeWillExceed = errors.New("arena capacity will exceed the limit")

	dbBatchSize = 32
)

// Add a margin to avoid boundary issues, arena uses the same pool for itself.
const arenaSafetyMargin = 1 * 1024

// MemTable hold the underlying skip list and
// the last chunk position in the wIO, associated
// with this skip list
//
//nolint:unused
type MemTable struct {
	skipList    *skl.Skiplist
	lastOffset  *wal.Offset
	firstOffset *wal.Offset

	capacity      int64
	opCount       int
	bytesStored   int
	newTxnBatcher func(maxBatchSize int) internal.TxnBatcher
	wIO           *wal.WalIO
	namespace     string
}

// NewMemTable returns an initialized mem-table.
func NewMemTable(capacity int64, db internal.BTreeStore, wIO *wal.WalIO, namespace string,
	newTxnBatcher func(maxBatchSize int) internal.TxnBatcher) *MemTable {
	return &MemTable{
		skipList:      skl.NewSkiplist(capacity),
		capacity:      capacity,
		db:            db,
		wIO:           wIO,
		namespace:     namespace,
		newTxnBatcher: newTxnBatcher,
	}
}

// skip list arena has a fixed capacity, if adding the given key-value pair would exceed its capacity
// the skip list panic. verify before to avoid the panic.
func (table *MemTable) canPut(key []byte, val y.ValueStruct) bool {
	return table.skipList.MemSize()+
		int64(len(y.KeyWithTs(key, 0)))+
		int64(val.EncodedSize())+arenaSafetyMargin <= table.capacity
}

// Put the Key and its Value at the given offset in the mem-table.
// for rowKey Put
// It uses MVCC as there can be multiple update ops for different columns for the same Row.
func (table *MemTable) Put(key []byte, val y.ValueStruct, pos *wal.Offset) error {
	if !table.canPut(key, val) {
		return ErrArenaSizeWillExceed
	}

	putKey := y.KeyWithTs(key, 0)
	if val.UserMeta == internal.EntryTypeRow {
		// We cannot save only one key, as a wide column row can have
		// multiple column entity in different ops of transaction.
		putKey = y.KeyWithTs(key, uint64(time.Now().UnixNano()))
	}

	table.skipList.Put(putKey, val)
	if table.firstOffset == nil {
		table.firstOffset = pos
	}
	table.lastOffset = pos
	table.opCount++
	table.bytesStored = table.bytesStored + len(key) + len(val.Value)
	return nil
}

func (table *MemTable) Get(key []byte) y.ValueStruct {
	return table.skipList.Get(y.KeyWithTs(key, 0))
}

// GetRowYValue returns all the mem table entries associated with the provided rowKey.
func (table *MemTable) GetRowYValue(rowKey []byte) []y.ValueStruct {
	var result []y.ValueStruct
	it := table.skipList.NewIterator()
	for it.Seek(rowKey); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, rowKey) {
			break
		}

		value := it.Value()
		result = append(result, value)
	}
	slices.Reverse(result)
	return result
}

// Flush writes all entries from MemTable to BtreeStore.
func (table *MemTable) Flush(ctx context.Context) (int, error) {
	slog.Debug("[unisondb.memtable] Flushing MemTable to BtreeStore...",
		"namespace", table.namespace, "start_offset", table.firstOffset,
		"end_offset", table.lastOffset)

	// Create an iterator for the MemTable
	it := table.skipList.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)

	txn := table.newTxnBatcher(dbBatchSize)
	count := 0

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if ctx.Err() != nil {
			return count, ctx.Err()
		}
		count++

		if err := table.processEntry(it.Key(), it.Value(), flushMan); err != nil {
			return count, err
		}

	}

	return count + flushMan.txnRecordCount, flushMan.processBatch(table.db)
}

func (table *MemTable) processEntry(key []byte, entry y.ValueStruct, txn internal.TxnBatcher) error {
	parsedKey := y.ParseKey(key)

	switch entry.Meta {
	case byte(logrecord.LogOperationTypeDelete):
		if entry.UserMeta != byte(logrecord.LogEntryTypeRow) {
			return txn.BatchDelete([][]byte{parsedKey})
		}

	case byte(logrecord.LogOperationTypeDeleteRowByKey):
		return txn.BatchDeleteRows([][]byte{parsedKey})
	}

	record, err := internal.GetWalRecord(entry, table.wIO)
	if err != nil {
		return err
	}

	if record.TxnState() == logrecord.TransactionStateCommit && record.EntryType() == logrecord.LogEntryTypeChunked {
		n, err := table.flushChunkedTxnCommit(record, txn)
		if err != nil {
			return err
		}
		flushMan.incrementCount(n + 1)
		return nil
	}

	// column operations
	if record.EntryType() == logrecord.LogEntryTypeRow {
		dr := logcodec.DeserializeFBRootLogRecord(record)
		switch record.OperationType() {
		case logrecord.LogOperationTypeInsert:
			return txn.BatchPutRowColumns(internal.ConvertRowFromLogOperationData(&dr.Payload))
		case logrecord.LogOperationTypeDelete:
			return txn.BatchDeleteRowColumns(internal.ConvertRowFromLogOperationData(&dr.Payload))
		}
		return nil
	}

	flushMan.kvWriteBuffer.add(record.KeyBytes(), record.ValueBytes())
	return nil
}

// flushChunkedTxnCommit returns the number of batch record that was inserted.
func (table *MemTable) flushChunkedTxnCommit(record *logrecord.LogRecord, txn internal.TxnBatcher) (int, error) {
	return internal.HandleChunkedValuesTxn(record, table.wIO, txn)
}
