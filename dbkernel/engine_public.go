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
	"github.com/dustin/go-humanize"
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

	mKeyRowSetTotal       = append(packageKey, "row", "put", "total")
	mKeyRowGetTotal       = append(packageKey, "row", "get", "total")
	mKeyRowDeleteTotal    = append(packageKey, "row", "delete", "total")
	mKeyRowSetDuration    = append(packageKey, "row", "put", "durations", "seconds")
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
	slog.Info("[dbkernel]",
		slog.String("message", "Snapshot requested for BTree"),
		slog.Group("engine",
			slog.String("namespace", e.namespace),
		),
		slog.Group("ops",
			slog.Uint64("received", e.writeSeenCounter.Load()),
			slog.Uint64("flushed", e.opsFlushedCounter.Load()),
		),
	)

	startTime := time.Now()
	cw := &countingWriter{w: w}
	metrics.IncrCounterWithLabels(mKeySnapshotTotal, 1, e.metricsLabel)
	defer func() {
		metrics.MeasureSinceWithLabels(mKeySnapshotDuration, startTime, e.metricsLabel)
		metrics.IncrCounterWithLabels(mKeySnapshotBytesTotal, float32(cw.count), e.metricsLabel)
	}()

	err := e.dataStore.Snapshot(cw)
	if err == nil {
		slog.Info("[dbkernel]",
			slog.String("message", "BTree snapshot completed"),
			slog.Group("engine",
				slog.String("namespace", e.namespace),
			),
			slog.Group("ops",
				slog.Uint64("received", e.writeSeenCounter.Load()),
				slog.Uint64("flushed", e.opsFlushedCounter.Load()),
			),
			slog.Group("snapshot",
				slog.String("duration", humanizeDuration(time.Since(startTime))),
				slog.String("bytes", humanize.Bytes(uint64(cw.count))),
			),
		)
	}

	if err != nil {
		slog.Error("[dbkernel]",
			slog.String("message", "BTree snapshot failed"),
			slog.String("error", err.Error()),
			slog.Group("engine",
				slog.String("namespace", e.namespace),
			),
			slog.Group("ops",
				slog.Uint64("received", e.writeSeenCounter.Load()),
				slog.Uint64("flushed", e.opsFlushedCounter.Load()),
			),
			slog.Group("snapshot",
				slog.String("duration", humanizeDuration(time.Since(startTime))),
				slog.String("bytes", humanize.Bytes(uint64(cw.count))),
			),
		)
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
	if len(data) == 0 {
		return nil, nil
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

// BatchPut insert the associated Key Value Pair.
func (e *Engine) BatchPut(key, value [][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyPutTotal, float32(len(key)), e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyPutDuration, startTime, e.metricsLabel)
	}()
	return e.persistKeyValue(key, value, logrecord.LogOperationTypeInsert)
}

// Delete removes a key and its value pair.
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

// BatchDelete removes all the key and its value pair.
func (e *Engine) BatchDelete(keys [][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	metrics.IncrCounterWithLabels(mKeyDeleteTotal, float32(len(keys)), e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyDeleteDuration, startTime, e.metricsLabel)
	}()

	return e.persistKeyValue(keys, nil, logrecord.LogOperationTypeDelete)
}

// WaitForAppendOrDone blocks until a put/delete operation occurs or timeout happens or context cancelled is done.
func (e *Engine) WaitForAppendOrDone(callerDone chan struct{}, timeout time.Duration, lastSeen *Offset) error {
	currentPos := e.currentOffset.Load()
	if currentPos != nil && hasNewWriteSince(currentPos, lastSeen) {
		return nil
	}

	e.notifierMu.RLock()
	done := e.appendNotify
	e.notifierMu.RUnlock()

	// check again (ca)
	currentPos = e.currentOffset.Load()
	if currentPos != nil && hasNewWriteSince(currentPos, lastSeen) {
		return nil
	}

	select {
	case <-callerDone:
		return context.Canceled
	case <-done:
		return nil
	case <-time.After(timeout):
		return ErrWaitTimeoutExceeded
	}
}

func hasNewWriteSince(current, lastSeen *Offset) bool {
	if lastSeen == nil {
		return true
	}
	return current.SegmentID > lastSeen.SegmentID ||
		(current.SegmentID == lastSeen.SegmentID && current.Offset > lastSeen.Offset)
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

// PutColumnsForRow inserts or updates the provided column entries.
//
// It's an upsert operation:
// - existing column value will get updated to newer value, else a new column entry will be created for the
// given row.
func (e *Engine) PutColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowSetTotal, 1, e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowSetDuration, startTime, e.metricsLabel)
	}()

	columnsEntries := []map[string][]byte{columnEntries}
	return e.persistRowColumnAction(logrecord.LogOperationTypeInsert, [][]byte{rowKey}, columnsEntries)
}

func (e *Engine) PutColumnsForRows(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowSetTotal, float32(len(rowKeys)), e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowSetDuration, startTime, e.metricsLabel)
	}()
	return e.persistRowColumnAction(logrecord.LogOperationTypeInsert, rowKeys, columnEntriesPerRow)
}

// DeleteColumnsForRow removes the specified columns from the given row key.
func (e *Engine) DeleteColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error {
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
	return e.persistRowColumnAction(logrecord.LogOperationTypeDelete, [][]byte{rowKey}, columnsEntries)
}

// DeleteColumnsForRows removes specified columns from multiple rows.
func (e *Engine) DeleteColumnsForRows(rowKeys [][]byte, columnEntries []map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowDeleteTotal, float32(len(rowKeys)), e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowDeleteDuration, startTime, e.metricsLabel)
	}()
	return e.persistRowColumnAction(logrecord.LogOperationTypeDelete, rowKeys, columnEntries)
}

// DeleteRow removes an entire row and all its associated column entries.
func (e *Engine) DeleteRow(rowKey []byte) error {
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

func (e *Engine) BatchDeleteRows(rowKeys [][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}
	metrics.IncrCounterWithLabels(mKeyRowDeleteTotal, float32(len(rowKeys)), e.metricsLabel)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(mKeyRowDeleteDuration, startTime, e.metricsLabel)
	}()

	return e.persistRowColumnAction(logrecord.LogOperationTypeDeleteRowByKey, rowKeys, nil)
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
	err := e.close(ctx)

	slog.Info("[dbkernel]",
		slog.String("message", "Closed DB engine"),
		slog.Group("engine",
			slog.String("namespace", e.namespace),
		),
		slog.Group("ops",
			slog.Uint64("received", e.writeSeenCounter.Load()),
			slog.Uint64("flushed", e.opsFlushedCounter.Load()),
		),
	)

	return err
}

type Reader = wal.Reader

// NewReader return a reader that reads from the beginning, until EOF is encountered.
// It returns io.EOF when it reaches end of file.
func (e *Engine) NewReader() (*Reader, error) {
	return e.walIO.NewReader()
}

// NewReaderWithStart return a reader that reads from provided offset, until EOF is encountered.
// It returns io.EOF when it reaches end of file.
func (e *Engine) NewReaderWithStart(startPos *Offset) (r *Reader, err error) {
	// protect against very bad client
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("[dbkernel]",
				slog.String("message", "Recovered from panic in NewReader"),
				slog.Any("panic", rec),
				slog.Any("start_offset", startPos),
			)
			r = nil
			err = fmt.Errorf("%w: can't create reader from offset %s", ErrInvalidOffset, startPos.String())
		}
	}()

	if startPos == nil {
		return e.NewReader()
	}
	curOffset := e.currentOffset.Load()
	if curOffset == nil && startPos != nil {
		return nil, ErrInvalidOffset
	}

	if curOffset == nil {
		return e.NewReader()
	}

	return e.walIO.NewReaderWithStart(startPos)
}

// NewReaderWithTail returns a reader that starts from the provided offset,
// and supports tail-following behavior.
// It returns ErrNoNewData when no new entries are available *yet*,
// instead of io.EOF.
func (e *Engine) NewReaderWithTail(startPos *Offset) (*Reader, error) {
	if startPos == nil {
		return e.walIO.NewReader(wal.WithActiveTail(true))
	}
	return e.walIO.NewReaderWithStart(startPos, wal.WithActiveTail(true))
}
