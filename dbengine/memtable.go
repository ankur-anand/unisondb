package dbengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

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

// put the Key and its Value at the given offset in the mem-table.
// for rowKey Put
// It uses MVCC as there can be multiple update ops for different columns for the same Row.
func (table *memTable) put(key []byte, val y.ValueStruct, pos *wal.Offset) error {
	if !table.canPut(key, val) {
		return errArenaSizeWillExceed
	}

	putKey := y.KeyWithTs(key, 0)
	if val.UserMeta == entryTypeRow {
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

func (table *memTable) get(key []byte) y.ValueStruct {
	return table.skipList.Get(y.KeyWithTs(key, 0))
}

// getRowYValue returns all the mem table entries associated with the provided rowKey.
func (table *memTable) getRowYValue(rowKey []byte) []y.ValueStruct {
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

// flush writes all entries from MemTable to BtreeStore.
func (table *memTable) flush(ctx context.Context) (int, error) {
	slog.Debug("[kvalchemy.dbengine] Flushing MemTable to BtreeStore...",
		"namespace", table.namespace, "start_offset", table.firstOffset,
		"end_offset", table.lastOffset)

	// Create an iterator for the MemTable
	it := table.skipList.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)

	count := 0
	flushMan := newFlushManager()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if ctx.Err() != nil {
			return count, ctx.Err()
		}
		count++

		if err := table.processEntry(it.Key(), it.Value(), flushMan); err != nil {
			return count, err
		}

		// Flush when batch capacity is reached
		if flushMan.isFull() {
			if err := flushMan.processBatch(table.db); err != nil {
				return 0, err
			}
		}
	}

	return count + flushMan.txnRecordCount, flushMan.processBatch(table.db)
}

func (table *memTable) processEntry(key []byte, entry y.ValueStruct, flushMan *flushManager) error {
	parsedKey := y.ParseKey(key)

	switch entry.Meta {
	case byte(walrecord.LogOperationDelete):
		if entry.UserMeta != entryTypeRow {
			flushMan.kvDeleteBuffer.add(parsedKey)
			return nil
		}

	case byte(walrecord.LogOperationDeleteRow):
		flushMan.rowDeleteBuffer.add(parsedKey)
		return nil
	}

	record, err := getWalRecord(entry, table.wIO)
	if err != nil {
		return err
	}

	if record.TxnStatus() == walrecord.TxnStatusCommit && record.EntryType() == walrecord.EntryTypeChunked {
		n, err := table.flushChunkedTxnCommit(record)
		if err != nil {
			return err
		}
		flushMan.incrementCount(n + 1)
		return nil
	}

	// column operations
	if record.EntryType() == walrecord.EntryTypeRow {
		switch record.Operation() {
		case walrecord.LogOperationInsert:
			flushMan.columnWriteBuffer.add(record.KeyBytes(), getColumnsValue(record))
		case walrecord.LogOperationDelete:
			flushMan.columnDeleteBuffer.add(record.KeyBytes(), getColumnsValue(record))
		}
		return nil
	}

	flushMan.kvWriteBuffer.add(record.KeyBytes(), record.ValueBytes())
	return nil
}

// flushChunkedTxnCommit returns the number of batch record that was inserted.
func (table *memTable) flushChunkedTxnCommit(record *walrecord.WalRecord) (int, error) {
	return handleChunkedValuesTxn(record, table.wIO, table.db)
}

type batchFlusher interface {
	flush(store BTreeStore) error
	reset()
}

type flushManager struct {
	kvWriteBuffer      *kvWriteBuffer
	kvDeleteBuffer     *kvDeleteBuffer
	rowDeleteBuffer    *rowDeleteBuffer
	columnWriteBuffer  *columnWriteBuffer
	columnDeleteBuffer *columnDeleteBuffer
	txnRecordCount     int
}

func newFlushManager() *flushManager {
	return &flushManager{
		kvWriteBuffer:      &kvWriteBuffer{},
		kvDeleteBuffer:     &kvDeleteBuffer{},
		rowDeleteBuffer:    &rowDeleteBuffer{},
		columnWriteBuffer:  &columnWriteBuffer{},
		columnDeleteBuffer: &columnDeleteBuffer{},
	}
}

func (f *flushManager) isFull() bool {
	return len(f.kvWriteBuffer.keys) >= dbBatchSize ||
		len(f.kvDeleteBuffer.keys) >= dbBatchSize ||
		len(f.columnWriteBuffer.keys) >= dbBatchSize ||
		len(f.columnDeleteBuffer.keys) >= dbBatchSize ||
		len(f.rowDeleteBuffer.keys) >= dbBatchSize
}

func (f *flushManager) incrementCount(n int) {
	f.txnRecordCount += n
}

func (f *flushManager) processBatch(db BTreeStore) error {
	buffers := []struct {
		name   string
		buffer batchFlusher
	}{
		{"kvWriteBuffer", f.kvWriteBuffer},
		{"kvDeleteBuffer", f.kvDeleteBuffer},
		{"columnWriteBuffer", f.columnWriteBuffer},
		{"rowDeleteBuffer", f.rowDeleteBuffer},
		{"columnDeleteBuffer", f.columnDeleteBuffer},
	}

	for _, buf := range buffers {
		if err := buf.buffer.flush(db); err != nil {
			return fmt.Errorf("failed to processBatch %s: %w", buf.name, err)
		}
		buf.buffer.reset()
	}
	return nil
}

type kvWriteBuffer struct {
	keys   [][]byte
	values [][]byte
}

func (b *kvWriteBuffer) add(key []byte, value []byte) {
	b.keys = append(b.keys, key)
	b.values = append(b.values, value)
}

func (b *kvWriteBuffer) flush(db BTreeStore) error {
	if len(b.keys) == 0 {
		return nil
	}
	return db.SetMany(b.keys, b.values)
}

func (b *kvWriteBuffer) reset() {
	b.keys = b.keys[:0]
	b.values = b.values[:0]
}

type kvDeleteBuffer struct {
	keys [][]byte
}

func (b *kvDeleteBuffer) add(key []byte) {
	b.keys = append(b.keys, key)
}

func (b *kvDeleteBuffer) flush(db BTreeStore) error {
	if len(b.keys) == 0 {
		return nil
	}
	return db.DeleteMany(b.keys)
}

func (b *kvDeleteBuffer) reset() {
	b.keys = b.keys[:0]
}

type rowDeleteBuffer struct {
	keys [][]byte
}

func (b *rowDeleteBuffer) add(key []byte) {
	b.keys = append(b.keys, key)
}

func (b *rowDeleteBuffer) flush(db BTreeStore) error {
	if len(b.keys) == 0 {
		return nil
	}
	_, err := db.DeleteEntireRows(b.keys)
	return err
}

func (b *rowDeleteBuffer) reset() {
	b.keys = b.keys[:0]
}

type columnWriteBuffer struct {
	keys [][]byte
	vals []map[string][]byte
}

func (b *columnWriteBuffer) add(key []byte, val map[string][]byte) {
	b.keys = append(b.keys, key)
	b.vals = append(b.vals, val)
}

func (b *columnWriteBuffer) flush(db BTreeStore) error {
	if len(b.keys) == 0 {
		return nil
	}
	return db.SetManyRowColumns(b.keys, b.vals)
}

func (b *columnWriteBuffer) reset() {
	b.keys = b.keys[:0]
	b.vals = b.vals[:0]
}

type columnDeleteBuffer struct {
	keys [][]byte
	vals []map[string][]byte
}

func (b *columnDeleteBuffer) add(key []byte, val map[string][]byte) {
	b.keys = append(b.keys, key)
	b.vals = append(b.vals, val)
}

func (b *columnDeleteBuffer) flush(db BTreeStore) error {
	if len(b.keys) == 0 {
		return nil
	}
	return db.DeleteMayRowColumns(b.keys, b.vals)
}

func (b *columnDeleteBuffer) reset() {
	b.keys = b.keys[:0]
	b.vals = b.vals[:0]
}

func getColumnsValue(record *walrecord.WalRecord) map[string][]byte {
	columnLen := record.ColumnsLength()
	columnEntries := make(map[string][]byte, columnLen)
	for i := 0; i < columnLen; i++ {
		var columnEntry walrecord.ColumnEntry
		record.Columns(&columnEntry, i)
		columnEntries[string(columnEntry.ColumnName())] = columnEntry.ColumnValueBytes()
	}
	return columnEntries
}
