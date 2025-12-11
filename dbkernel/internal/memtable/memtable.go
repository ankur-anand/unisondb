package memtable

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"slices"
	"sync"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
)

var (
	ErrArenaSizeWillExceed = errors.New("arena capacity will exceed the limit")

	dbBatchSize = 16
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
	offsetCount   int
	bytesStored   int
	newTxnBatcher func(maxBatchSize int) internal.TxnBatcher
	wIO           *wal.WalIO
	namespace     string

	chunkedFlushed int
	tsGenerator    *tsGenerator
	bloomFilter    *bloom.BloomFilter
	bloomMu        sync.RWMutex

	// Raft tracking (only used in raft mode)
	firstRaftIndex uint64
	lastRaftIndex  uint64
	firstRaftTerm  uint64
	lastRaftTerm   uint64
}

// NewMemTable returns an initialized mem-table.
func NewMemTable(capacity int64, wIO *wal.WalIO, namespace string,
	newTxnBatcher func(maxBatchSize int) internal.TxnBatcher) *MemTable {
	// assuming average entry size of 100 bytes (key + value + overhead)
	estimatedKeys := uint(capacity / 100)
	if estimatedKeys < 1000 {
		estimatedKeys = 1000
	}

	bloomFilter := bloom.NewWithEstimates(estimatedKeys, 0.0001)

	return &MemTable{
		skipList:      skl.NewSkiplist(capacity),
		capacity:      capacity,
		wIO:           wIO,
		namespace:     namespace,
		newTxnBatcher: newTxnBatcher,
		tsGenerator:   &tsGenerator{},
		bloomFilter:   bloomFilter,
	}
}

func (table *MemTable) IsEmpty() bool {
	return table.skipList.Empty()
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
func (table *MemTable) Put(key []byte, val y.ValueStruct) error {
	if !table.canPut(key, val) {
		return ErrArenaSizeWillExceed
	}

	putKey := y.KeyWithTs(key, 0)
	if val.UserMeta == internal.EntryTypeRow {
		// We cannot save only one key, as a wide column row can have
		// multiple column entity in different ops of transaction.
		ts := table.tsGenerator.Next()
		putKey = y.KeyWithTs(key, ts)
	}

	table.opCount++
	table.skipList.Put(putKey, val)
	table.bytesStored = table.bytesStored + len(key) + len(val.Value)

	table.bloomMu.Lock()
	table.bloomFilter.Add(key)
	table.bloomMu.Unlock()

	return nil
}

func (table *MemTable) SetOffset(offset *wal.Offset) {
	table.offsetCount++
	table.lastOffset = offset
	if table.firstOffset == nil {
		table.firstOffset = offset
	}
}

func (table *MemTable) IncrOffset() {
	table.offsetCount++
}

func (table *MemTable) GetBytesStored() int {
	return table.bytesStored
}

func (table *MemTable) GetLastOffset() *wal.Offset {
	return table.lastOffset
}

func (table *MemTable) GetFirstOffset() *wal.Offset {
	return table.firstOffset
}

// SetRaftPosition sets the raft log position for this memtable (only used in raft mode).
func (table *MemTable) SetRaftPosition(index, term uint64) {
	table.lastRaftIndex = index
	table.lastRaftTerm = term
	if table.firstRaftIndex == 0 {
		table.firstRaftIndex = index
		table.firstRaftTerm = term
	}
}

// GetLastRaftPosition returns the last raft log index and term for this memtable.
func (table *MemTable) GetLastRaftPosition() (index uint64, term uint64) {
	return table.lastRaftIndex, table.lastRaftTerm
}

// GetFirstRaftPosition returns the first raft log index and term for this memtable.
func (table *MemTable) GetFirstRaftPosition() (index uint64, term uint64) {
	return table.firstRaftIndex, table.firstRaftTerm
}

func (table *MemTable) Get(key []byte) y.ValueStruct {
	table.bloomMu.RLock()
	exists := table.bloomFilter.Test(key)
	table.bloomMu.RUnlock()

	if !exists {
		return y.ValueStruct{}
	}
	return table.skipList.Get(y.KeyWithTs(key, 0))
}

// GetRowYValue returns all the mem table entries associated with the provided rowKey.
func (table *MemTable) GetRowYValue(rowKey []byte) []y.ValueStruct {
	table.bloomMu.RLock()
	exists := table.bloomFilter.Test(rowKey)
	table.bloomMu.RUnlock()

	if !exists {
		return nil
	}

	var result []y.ValueStruct
	it := table.skipList.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)
	seekKey := y.KeyWithTs(rowKey, math.MaxUint64)
	for it.Seek(seekKey); it.Valid(); it.Next() {
		key := it.Key()
		parsed := y.ParseKey(key)
		if !bytes.Equal(parsed, rowKey) {
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
	slog.Debug("[memtable]", "message", "Flushing MemTable to BtreeStore",
		"namespace", table.namespace, "start_offset", table.firstOffset,
		"end_offset", table.lastOffset)

	// Create an iterator for the MemTable
	it := table.skipList.NewIterator()
	defer func(it *skl.Iterator) {
		_ = it.Close()
	}(it)

	var err error
	txn := table.newTxnBatcher(dbBatchSize)

	mvccRows := make(map[string]struct{})

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}

		parsedKey := y.ParseKey(it.Key())

		// rowEntry are saved as MVCC, So the newest value will get applied first.
		// skipping to apply it as MVCC row.
		// For Regular Key Value, Skip List is in place update.
		if it.Value().UserMeta == internal.EntryTypeRow {
			mvccRows[string(parsedKey)] = struct{}{}
			continue
		}

		if err = table.processEntry(parsedKey, it.Value(), txn); err != nil {
			slog.Error("[memtable]",
				slog.String("message", "Failed to flush and process entry"),
				slog.String("key", string(parsedKey)),
				slog.Any("error", err),
			)
			break
		}
	}

	for mvccRow := range mvccRows {
		entries := table.GetRowYValue([]byte(mvccRow))

		for _, entry := range entries {
			if err = table.processEntry([]byte(mvccRow), entry, txn); err != nil {
				slog.Error("[memtable]",
					slog.String("message", "Failed to flush and process entry"),
					slog.Any("key", mvccRow),
					slog.Any("error", err),
				)
				break
			}
		}
	}

	if err != nil {
		slog.Error("[memtable]",
			slog.String("message", "Failed to flush and process entry"),
			slog.Any("error", err),
		)
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		slog.Error("[memtable]",
			slog.String("message", "Failed to flush and process entry"),
			slog.Any("error", err),
		)
		return 0, err
	}

	return table.offsetCount + table.chunkedFlushed, err
}

func (table *MemTable) processEntry(key []byte, entry y.ValueStruct, txn internal.TxnBatcher) error {
	switch entry.Meta {
	case internal.LogOperationDelete:
		if entry.UserMeta != internal.EntryTypeRow {
			return txn.BatchDeleteKV([][]byte{key})
		}

	case byte(logrecord.LogOperationTypeDeleteRowByKey):
		return txn.BatchDeleteRows([][]byte{key})
	}

	// if Version Type is of TxnStateCommit.
	// We need to get the WAL Record and commit the entire WAL operation that is part of this TXN.
	// We directly store all the key and value even for Txn type that is not of Type Chunked.
	if entry.UserMeta == internal.EntryTypeChunked {
		record, err := internal.GetWalRecord(entry, table.wIO)
		if err != nil {
			return err
		}

		if record.TxnState() == logrecord.TransactionStateCommit && record.EntryType() == logrecord.LogEntryTypeChunked {
			n, err := table.flushChunkedTxnCommit(record, txn)
			if err != nil {
				return err
			}
			table.chunkedFlushed = table.chunkedFlushed + n + 1
			return nil
		}
	}

	// Do the Row Processing.
	if entry.UserMeta == internal.EntryTypeRow {
		re := logcodec.DeserializeRowUpdateEntry(entry.Value)
		var columnUpdates []map[string][]byte
		columnUpdates = append(columnUpdates, re.Columns)

		switch entry.Meta {
		case internal.LogOperationDelete:
			return txn.BatchDeleteCells([][]byte{key}, columnUpdates)
		case internal.LogOperationInsert:
			return txn.BatchSetCells([][]byte{key}, columnUpdates)
		}
	}

	// else it's Key Value.
	return txn.BatchPutKV([][]byte{key}, [][]byte{entry.Value})
}

// flushChunkedTxnCommit returns the number of batch record that was inserted.
func (table *MemTable) flushChunkedTxnCommit(record *logrecord.LogRecord, txn internal.TxnBatcher) (int, error) {
	return internal.HandleChunkedValuesTxn(record, table.wIO, txn)
}
