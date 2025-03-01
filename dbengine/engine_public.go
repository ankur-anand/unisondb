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
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/hashicorp/go-metrics"
)

var (
	engineMetricsPutTotal           = append(packageKey, "put", "total")
	engineMetricsGetTotal           = append(packageKey, "get", "total")
	engineMetricsDeleteTotal        = append(packageKey, "delete", "total")
	engineMetricsPutDuration        = append(packageKey, "put", "durations", "seconds")
	engineMetricsGetDuration        = append(packageKey, "get", "durations", "seconds")
	engineMetricsDeleteDuration     = append(packageKey, "delete", "durations", "seconds")
	engineMetricsSnapshotTotal      = append(packageKey, "snapshot", "total")
	engineMetricsSnapshotDuration   = append(packageKey, "snapshot", "durations", "seconds")
	engineMetricsSnapshotBytesTotal = append(packageKey, "snapshot", "bytes", "total")
)

// RecoveredWALCount returns the number of WAL entries successfully recovered.
func (e *Engine) RecoveredWALCount() int {
	return e.recoveredEntriesCount
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
	metrics.IncrCounterWithLabels(engineMetricsSnapshotTotal, 1, e.metricsLabel)
	defer func() {
		metrics.MeasureSinceWithLabels(engineMetricsSnapshotDuration, startTime, e.metricsLabel)
		metrics.IncrCounterWithLabels(engineMetricsSnapshotBytesTotal, float32(cw.count), e.metricsLabel)
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
	metrics.IncrCounterWithLabels(engineMetricsPutTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(engineMetricsPutDuration, startTime, e.metricsLabel)
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

	metrics.IncrCounterWithLabels(engineMetricsDeleteTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(engineMetricsDeleteDuration, startTime, e.metricsLabel)
	}()

	return e.persistKeyValue(key, nil, walrecord.LogOperationDelete)
}

// WaitForAppend blocks until a put/delete operation occurs or timeout/context is reached.
func (e *Engine) WaitForAppend(ctx context.Context, timeout time.Duration, lastSeen *wal.Offset) error {
	currentPos := e.currentOffset.Load()
	if currentPos != nil && isNewChunkPosition(currentPos, lastSeen) {
		return nil
	}

	e.notifierMu.Lock()
	defer e.notifierMu.Unlock()

	done := make(chan struct{})
	go func() {
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

func isNewChunkPosition(current, lastSeen *wal.Offset) bool {
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

	metrics.IncrCounterWithLabels(engineMetricsGetTotal, 1, e.metricsLabel)
	startTime := time.Now()

	defer func() {
		metrics.MeasureSinceWithLabels(engineMetricsGetDuration, startTime, e.metricsLabel)
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
	if it.Meta == byte(wrecord.LogOperationNoop) {
		compressedValue, err := e.dataStore.Get(key)
		if err != nil {
			return nil, err
		}
		return compress.DecompressLZ4(compressedValue)
	}

	// key deleted
	if it.Meta == byte(wrecord.LogOperationDelete) {
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
