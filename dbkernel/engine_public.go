package dbkernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/hashicorp/go-metrics"
)

var (
	mKeyPutTotal           = append(packageKey, "put", "total")
	mKeyGetTotal           = append(packageKey, "get", "total")
	mKeyDeleteTotal        = append(packageKey, "delete", "total")
	mKeyPutDuration        = append(packageKey, "put", "durations", "seconds")
	mKeyGetDuration        = append(packageKey, "get", "durations", "seconds")
	mKeyDeleteDuration     = append(packageKey, "delete", "durations", "seconds")
	mKeySnapshotTotal      = append(packageKey, "snapshot", "total")
	mKeySnapshotDuration   = append(packageKey, "snapshot", "durations", "seconds")
	mKeySnapshotBytesTotal = append(packageKey, "snapshot", "bytes", "total")

	mKeyWaitForAppendTotal    = append(packageKey, "wait", "append", "total")
	mKeyWaitForAppendDuration = append(packageKey, "wait", "append", "durations", "seconds")

	mKeyRowSetTotal       = append(packageKey, "row", "set", "total")
	mKeyRowGetTotal       = append(packageKey, "row", "get", "total")
	mKeyRowDeleteTotal    = append(packageKey, "row", "delete", "total")
	mKeyRowSetDuration    = append(packageKey, "row", "set", "durations", "seconds")
	mKeyRowGetDuration    = append(packageKey, "row", "get", "durations", "seconds")
	mKeyRowDeleteDuration = append(packageKey, "row", "delete", "durations", "seconds")
)

// Offset represents the offset in the wal.
type Offset = wal.Offset

// DecodeOffset decodes the offset position from a byte slice.
func DecodeOffset(b []byte) *Offset {
	return wal.DecodeOffset(b)
}

// RecoveredWALCount returns the number of WAL entries successfully recovered.
func (e *Engine) RecoveredWALCount() int {
	return e.recoveredEntriesCount
}

func (e *Engine) Namespace() string {
	return e.namespace
}

type countingWriter struct {
	w     io.Writer
	count int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.count += int64(n)
	return n, err
}

// BtreeSnapshot returns the snapshot of the current btree store.
func (e *Engine) BtreeSnapshot(w io.Writer) (int64, error) {
	slog.Info("[unisondb.dbkernal] BTree snapshot received")
	startTime := time.Now()
	cw := &countingWriter{w: w}
	metrics.IncrCounterWithLabels(mKeySnapshotTotal, 1, e.metricsLabel)
	defer func() {
		metrics.MeasureSinceWithLabels(mKeySnapshotDuration, startTime, e.metricsLabel)
		metrics.IncrCounterWithLabels(mKeySnapshotBytesTotal, float32(cw.count), e.metricsLabel)
	}()

	err := e.dataStore.Snapshot(cw)
	if err != nil {
		slog.Error("[unisondb.dbkernal] BTree snapshot error", "error", err)
	}
	return cw.count, err
}

// OpsReceivedCount returns the total number of Put and Delete operations received.
func (e *Engine) OpsReceivedCount() uint64 {
	return e.writeSeenCounter.Load()
}

// OpsFlushedCount returns the total number of Put and Delete operations flushed to BtreeStore.
func (e *Engine) OpsFlushedCount() uint64 {
	return e.opsFlushedCounter.Load()
}

// CurrentOffset returns the current offset that it has seen.
func (e *Engine) CurrentOffset() *Offset {
	return e.currentOffset.Load()
}

// GetWalCheckPoint returns the last checkpoint metadata saved in the database.
func (e *Engine) GetWalCheckPoint() (*internal.Metadata, error) {
	data, err := e.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
	if err != nil && !errors.Is(err, kvdrivers.ErrKeyNotFound) {
		return nil, err
	}
	metadata := internal.UnmarshalMetadata(data)
	return &metadata, nil
}

// Put inserts a key-value pair.
func (e *Engine) Put(key, value []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyPutTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyPutDuration, startTime, e.metricsLabel)
	}()

	return e.persistKeyValue([][]byte{key}, [][]byte{value}, logrecord.LogOperationTypeInsert)
}

// Delete removes a key and its value pair from WAL and MemTable.
func (e *Engine) Delete(key []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	metrics.IncrCounterWithLabels(mKeyDeleteTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyDeleteDuration, startTime, e.metricsLabel)
	}()

	return e.persistKeyValue([][]byte{key}, nil, logrecord.LogOperationTypeDelete)
}

// WaitForAppend blocks until a put/delete operation occurs or timeout happens or context cancelled is done.
func (e *Engine) WaitForAppend(ctx context.Context, timeout time.Duration, lastSeen *Offset) error {
	metrics.IncrCounterWithLabels(mKeyWaitForAppendTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyWaitForAppendDuration, startTime, e.metricsLabel)
	}()

	currentPos := e.currentOffset.Load()
	if currentPos != nil && isNewChunkPosition(currentPos, lastSeen) {
		return nil
	}

	done := make(chan struct{})
	go func() {
		e.notifierMu.Lock()
		defer e.notifierMu.Unlock()
		e.notifier.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	case <-time.After(timeout):
		return ErrWaitTimeoutExceeded
	}
}

func isNewChunkPosition(current, lastSeen *Offset) bool {
	if lastSeen == nil {
		return true
	}
	// Compare SegmentId, BlockNumber, or Offset to check if a new chunk exists
	return current.SegmentId > lastSeen.SegmentId ||
		(current.SegmentId == lastSeen.SegmentId && current.BlockNumber > lastSeen.BlockNumber) ||
		(current.SegmentId == lastSeen.SegmentId && current.BlockNumber == lastSeen.BlockNumber && current.ChunkOffset > lastSeen.ChunkOffset)
}

// Get retrieves the value associated with the given key.
func (e *Engine) Get(key []byte) ([]byte, error) {
	if e.shutdown.Load() {
		return nil, ErrInCloseProcess
	}

	metrics.IncrCounterWithLabels(mKeyGetTotal, 1, e.metricsLabel)
	startTime := time.Now()

	defer func() {
		metrics.MeasureSinceWithLabels(mKeyGetDuration, startTime, e.metricsLabel)
	}()

	checkFunc := func() (y.ValueStruct, error) {
		// Retrieve entry from MemTable
		e.mu.RLock()
		defer e.mu.RUnlock()
		// fast negative check
		if !e.bloom.Test(key) {
			return y.ValueStruct{}, ErrKeyNotFound
		}
		yValue := e.activeMemTable.Get(key)
		if yValue.Meta == byte(logrecord.LogOperationTypeNoOperation) {
			// first latest value
			for i := len(e.sealedMemTables) - 1; i >= 0; i-- {
				if val := e.sealedMemTables[i].Get(key); val.Meta != byte(logrecord.LogOperationTypeNoOperation) {
					return val, nil
				}
			}
		}
		return yValue, nil
	}

	yValue, err := checkFunc()
	if err != nil {
		return nil, err
	}

	// if the mem table doesn't have this key associated action or log.
	// directly go to the boltdb to fetch the same.
	if yValue.Meta == byte(logrecord.LogOperationTypeNoOperation) {
		return e.dataStore.Get(key)
	}

	// key deleted
	if yValue.Meta == byte(logrecord.LogOperationTypeDelete) {
		return nil,
			ErrKeyNotFound
	}

	if yValue.UserMeta == internal.EntryTypeChunked {
		record, err := internal.GetWalRecord(yValue, e.walIO)
		if err != nil {
			return nil, err
		}
		return e.reconstructChunkedValue(record)
	}

	return yValue.Value, nil
}

// SetColumnsInRow inserts or updates the provided column entries.
//
// It's an upsert operation:
// - existing column value will get updated to newer value, else a new column entry will be created for the
// given row.
func (e *Engine) SetColumnsInRow(rowKey string, columnEntries map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowSetTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowSetDuration, startTime, e.metricsLabel)
	}()

	columnsEntries := make([]map[string][]byte, 0, 1)
	columnsEntries = append(columnsEntries, columnEntries)
	return e.persistRowColumnAction(logrecord.LogOperationTypeInsert, [][]byte{[]byte(rowKey)}, columnsEntries)
}

// DeleteColumnsFromRow removes the specified columns from the given row key.
func (e *Engine) DeleteColumnsFromRow(rowKey string, columnEntries map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowDeleteTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowDeleteDuration, startTime, e.metricsLabel)
	}()
	columnsEntries := make([]map[string][]byte, 0, 1)
	columnsEntries = append(columnsEntries, columnEntries)
	return e.persistRowColumnAction(logrecord.LogOperationTypeDelete, [][]byte{[]byte(rowKey)}, columnsEntries)
}

// DeleteRow removes an entire row and all its associated column entries.
func (e *Engine) DeleteRow(rowKey string) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowDeleteTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowDeleteDuration, startTime, e.metricsLabel)
	}()

	return e.persistRowColumnAction(logrecord.LogOperationTypeDeleteRowByKey, [][]byte{[]byte(rowKey)}, nil)
}

// GetRowColumns returns all the column value associated with the row. It's filters columns if predicate
// function is provided and only returns those columns for which predicate return true.
func (e *Engine) GetRowColumns(rowKey string, predicate func(columnKey string) bool) (map[string][]byte, error) {
	if e.shutdown.Load() {
		return nil, ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowGetTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowGetDuration, startTime, e.metricsLabel)
	}()

	key := []byte(rowKey)

	checkFunc := func() ([]y.ValueStruct, error) {
		// Retrieve entry from MemTable
		e.mu.RLock()
		defer e.mu.RUnlock()

		if !e.bloom.Test(key) {
			return nil, ErrKeyNotFound
		}
		first := e.activeMemTable.GetRowYValue(key)
		if len(first) != 0 && first[len(first)-1].Meta == byte(logrecord.LogOperationTypeDeleteRowByKey) {
			return nil, ErrKeyNotFound
		}
		// get the columns value from the old sealed table to new mem table.
		// and build the column value.
		var vs []y.ValueStruct
		for _, sm := range e.sealedMemTables {
			v := sm.GetRowYValue(key)
			if len(v) != 0 {
				vs = append(vs, v...)
			}
		}
		if len(first) != 0 {
			vs = append(vs, first...)
		}

		return vs, nil
	}

	vs, err := checkFunc()
	if err != nil {
		return nil, err
	}

	predicateFunc := func(columnKey []byte) bool {
		if predicate == nil {
			return true
		}
		return predicate(string(columnKey))
	}

	// get the oldest value from the store.
	columnsValue, err := e.dataStore.GetRowColumns(key, predicateFunc)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	buildColumnMap(columnsValue, vs)

	for columnKey := range columnsValue {
		if predicate != nil && !predicate(columnKey) {
			delete(columnsValue, columnKey)
		}
	}

	return columnsValue, nil
}

func (e *Engine) reconstructChunkedValue(record *logrecord.LogRecord) ([]byte, error) {
	records, err := e.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct batch value: %w", err)
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	estimatedSize := len(preparedRecords) * (1 << 20)
	fullValue := bytes.NewBuffer(make([]byte, 0, estimatedSize))
	for _, record := range preparedRecords {
		r := logcodec.DeserializeFBRootLogRecord(record)
		kv := logcodec.DeserializeKVEntry(r.Entries[0])
		fullValue.Write(kv.Value)
	}

	value := fullValue.Bytes()

	return value, nil
}

// Close all the associated resource.
func (e *Engine) Close(ctx context.Context) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	slog.Info("[unisondb.dbkernel]: Closing Down", "namespace", e.namespace,
		"ops_received", e.writeSeenCounter.Load(),
		"ops_flushed", e.opsFlushedCounter.Load())

	return e.close(ctx)
}

type Reader = wal.Reader

// NewReader return a reader that reads from the beginning, until EOF is encountered.
// It returns io.EOF when it reaches end of file.
func (e *Engine) NewReader() (*Reader, error) {
	return e.walIO.NewReader()
}

// NewReaderWithStart return a reader that reads from provided offset, until EOF is encountered.
// It returns io.EOF when it reaches end of file.
func (e *Engine) NewReaderWithStart(startPos *Offset) (*Reader, error) {
	// get current offset.
	curOffset := e.currentOffset.Load()
	if startPos == nil {
		return e.NewReader()
	}

	if curOffset == nil {
		return nil, ErrInvalidOffset
	}

	if startPos.SegmentId > curOffset.SegmentId {
		return nil, ErrInvalidOffset
	}

	if startPos.SegmentId == curOffset.SegmentId {
		if startPos.BlockNumber > curOffset.BlockNumber {
			return nil, ErrInvalidOffset
		}

		if startPos.BlockNumber == curOffset.BlockNumber {
			if startPos.ChunkOffset > curOffset.ChunkOffset {
				return nil, ErrInvalidOffset
			}
		}
	}

	return e.walIO.NewReaderWithStart(startPos)
}
