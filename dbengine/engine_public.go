package dbengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"time"

	"github.com/ankur-anand/kvalchemy/dbengine/compress"
	"github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
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
	slog.Info("[kvalchemy.dbengine] BTree snapshot received")
	startTime := time.Now()
	cw := &countingWriter{w: w}
	metrics.IncrCounterWithLabels(mKeySnapshotTotal, 1, e.metricsLabel)
	defer func() {
		metrics.MeasureSinceWithLabels(mKeySnapshotDuration, startTime, e.metricsLabel)
		metrics.IncrCounterWithLabels(mKeySnapshotBytesTotal, float32(cw.count), e.metricsLabel)
	}()

	err := e.dataStore.Snapshot(cw)
	if err != nil {
		slog.Error("[kvalchemy.dbengine] BTree snapshot error", "error", err)
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
func (e *Engine) GetWalCheckPoint() (*Metadata, error) {
	data, err := e.dataStore.RetrieveMetadata(sysKeyWalCheckPoint)
	if err != nil && !errors.Is(err, kvdb.ErrKeyNotFound) {
		return nil, err
	}
	metadata := UnmarshalMetadata(data)
	return &metadata, nil
}

// Put inserts a key-value pair into WAL and MemTable.
func (e *Engine) Put(key, value []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyPutTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyPutDuration, startTime, e.metricsLabel)
	}()

	compressed, err := compress.CompressLZ4(value)
	if err != nil {
		return err
	}

	return e.persistKeyValue(key, compressed, walrecord.LogOperationInsert)
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

	return e.persistKeyValue(key, nil, walrecord.LogOperationDelete)
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

// Get retrieves a value from MemTable, WAL, or BoltDB.
// 1. check bloom filter for key presence.
// 2. check recent mem-table
// 3. Check BoltDB.
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
		it := e.activeMemTable.get(key)
		if it.Meta == byte(walrecord.LogOperationNoop) {
			// first latest value
			for i := len(e.sealedMemTables) - 1; i >= 0; i-- {
				if val := e.sealedMemTables[i].get(key); val.Meta != byte(walrecord.LogOperationNoop) {
					return val, nil
				}
			}
		}
		return it, nil
	}

	it, err := checkFunc()
	if err != nil {
		return nil, err
	}

	// if the mem table doesn't have this key associated action or log.
	// directly go to the boltdb to fetch the same.
	if it.Meta == byte(walrecord.LogOperationNoop) {
		compressedValue, err := e.dataStore.Get(key)
		if err != nil {
			return nil, err
		}
		return compress.DecompressLZ4(compressedValue)
	}

	// key deleted
	if it.Meta == byte(walrecord.LogOperationDelete) {
		return nil,
			ErrKeyNotFound
	}

	record, err := getWalRecord(it, e.walIO)
	if err != nil {
		return nil, err
	}

	if record.ValueType() == walrecord.ValueTypeChunked {
		return e.reconstructBatchValue(record)
	}
	decompressed, err := compress.DecompressLZ4(record.ValueBytes())
	if err != nil {
		return nil, fmt.Errorf("failed to decompress value for key %s: %w", string(key), err)
	}
	if crc32.ChecksumIEEE(record.ValueBytes()) != record.Crc32Checksum() {
		return nil, ErrRecordCorrupted
	}

	return decompressed, nil
}

// PutRowColumns insert the provided columnsEntries for the rowKey into WAL and mem-table.
func (e *Engine) PutRowColumns(rowKey string, columnEntries map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyPutTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyPutDuration, startTime, e.metricsLabel)
	}()

	return e.persistRowColumnAction(walrecord.LogOperationInsert, []byte(rowKey), columnEntries)
}

// DeleteRowColumns delete the provided columnsEntries for the rowKey into WAL and mem-table.
func (e *Engine) DeleteRowColumns(rowKey string, columnEntries map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyPutTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyPutDuration, startTime, e.metricsLabel)
	}()

	return e.persistRowColumnAction(walrecord.LogOperationDelete, []byte(rowKey), columnEntries)
}

// DeleteRow deletes the row and all the column entries associated with the row key.
func (e *Engine) DeleteRow(rowKey string) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyDeleteTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyDeleteDuration, startTime, e.metricsLabel)
	}()

	return e.persistRowColumnAction(walrecord.LogOperationDeleteEntireRow, []byte(rowKey), nil)
}

// GetRowColumns returns all the column value associated with the row. It's filters columns if predicate
// function is provided and only returns those columns for which predicate return true.
func (e *Engine) GetRowColumns(rowKey string, predicate func(columnKey string) bool) (map[string][]byte, error) {
	if e.shutdown.Load() {
		return nil, ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyGetTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyGetDuration, startTime, e.metricsLabel)
	}()

	key := []byte(rowKey)
	// Retrieve entry from MemTable
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.bloom.Test(key) {
		return nil, ErrKeyNotFound
	}

	// get the columns value from the old sealed table to new mem table.
	// and build the column value.
	var vs []y.ValueStruct
	for _, sm := range e.sealedMemTables {
		v := sm.getRowYValue(key)
		if len(v) != 0 {
			vs = append(vs, v...)
		}
	}

	v := e.activeMemTable.getRowYValue(key)
	if len(v) != 0 {
		vs = append(vs, v...)
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

	err = buildColumnMap(columnsValue, vs, e.walIO)
	if err != nil {
		return nil, err
	}

	for columnKey := range columnsValue {
		if predicate != nil && !predicate(columnKey) {
			delete(columnsValue, columnKey)
		}
	}

	return columnsValue, nil
}

func (e *Engine) reconstructBatchValue(record *walrecord.WalRecord) ([]byte, error) {
	records, err := e.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct batch value: %w", err)
	}
	checksum := unmarshalChecksum(record.ValueBytes())

	// remove the begins part from the
	preparedRecords := records[1:]

	var estimatedSize int
	for _, rec := range preparedRecords {
		estimatedSize += len(rec.ValueBytes())
	}

	fullValue := bytes.NewBuffer(make([]byte, 0, estimatedSize))
	for _, record := range preparedRecords {
		value, err := compress.DecompressLZ4(record.ValueBytes())
		if err != nil {
			return nil, err
		}
		fullValue.Write(value)
	}

	value := fullValue.Bytes()

	if crc32.ChecksumIEEE(value) != checksum {
		return nil, ErrRecordCorrupted
	}

	return value, nil
}

// Close all the associated resource.
func (e *Engine) Close(ctx context.Context) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	slog.Info("[kvalchemy.dbengine]: Closing Down", "namespace", e.namespace,
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
