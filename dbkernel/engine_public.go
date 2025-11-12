package dbkernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/memtable"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/keycodec"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dustin/go-humanize"
	"github.com/uber-go/tally/v4"
)

var (
	mRequestsTotal         = "requests_total"
	mRequestLatencySeconds = "request_latency_seconds"
	mSnapshotBytes         = "snapshot_bytes"

	kvPutSurface = map[string]string{"surface": "client", "op": "put", "entry": "kv"}
	kvGetSurface = map[string]string{"surface": "client", "op": "get", "entry": "kv"}
	kvDelSurface = map[string]string{"surface": "client", "op": "delete", "entry": "kv"}

	wideColumnPutSurface = map[string]string{"surface": "client", "op": "put", "entry": "row"}
	wideColumnGetSurface = map[string]string{"surface": "client", "op": "get", "entry": "row"}
	wideColumnDelSurface = map[string]string{"surface": "client", "op": "delete", "entry": "row"}

	lobSurface = map[string]string{"surface": "client", "op": "get", "entry": "lob"}

	snapshotSurface = map[string]string{"surface": "client", "op": "create", "entry": "snapshot"}
)

var (
	snapshotBuckets = tally.DurationBuckets{
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
		20 * time.Minute,
		30 * time.Minute,
		1 * time.Hour,
	}
)

const (
	metaNoOp           = byte(logrecord.LogOperationTypeNoOperation)
	metaDelete         = byte(logrecord.LogOperationTypeDelete)
	metaDeleteRowByKey = byte(logrecord.LogOperationTypeDeleteRowByKey)
)

// Offset represents the offset in the wal.
type Offset = wal.Offset

// SegmentID represents a WAL segment identifier.
type SegmentID = wal.SegID

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

// BackupRoot returns the namespace-specific backup root on disk.
func (e *Engine) BackupRoot() string {
	return e.backupNamespaceRoot
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
	snapScope := e.taggedScope.Tagged(snapshotSurface)
	snapScope.Counter(mRequestsTotal).Inc(1)

	defer func() {
		snapScope.Histogram(mRequestLatencySeconds, snapshotBuckets).RecordDuration(time.Since(startTime))
		snapScope.Counter(mSnapshotBytes).Inc(cw.count)
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

// BackupBtree writes a full snapshot of the B-Tree store to backupPath atomically.
func (e *Engine) BackupBtree(backupPath string) (int64, error) {
	resolvedPath, err := e.resolveBackupFile(backupPath)
	if err != nil {
		return 0, err
	}

	tmpPath := resolvedPath + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, fmt.Errorf("create temp backup file: %w", err)
	}

	written, snapshotErr := e.BtreeSnapshot(file)
	if snapshotErr == nil {
		if err := file.Sync(); err != nil {
			snapshotErr = fmt.Errorf("sync backup file: %w", err)
		}
	}

	if closeErr := file.Close(); snapshotErr == nil && closeErr != nil {
		snapshotErr = closeErr
	}

	if snapshotErr != nil {
		_ = os.Remove(tmpPath)
		return 0, snapshotErr
	}

	_ = os.Remove(resolvedPath)
	if err := os.Rename(tmpPath, resolvedPath); err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("finalize backup: %w", err)
	}

	return written, nil
}

// OpsReceivedCount returns the total number of Put and Delete operations received.
func (e *Engine) OpsReceivedCount() uint64 {
	return e.writeSeenCounter.Load()
}

// OpsFlushedCount returns the total number of Put and Delete operations flushed to BtreeStore.
func (e *Engine) OpsFlushedCount() uint64 {
	return e.opsFlushedCounter.Load()
}

// SealedMemTableCount returns the current number of sealed memtables.
func (e *Engine) SealedMemTableCount() int {
	e.mu.RLock()
	count := len(e.sealedMemTables)
	e.mu.RUnlock()
	return count
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

// BackupWalSegmentsAfter copies sealed WAL segments whose IDs are greater than afterID into backupDir.
func (e *Engine) BackupWalSegmentsAfter(afterID SegmentID, backupDir string) (map[SegmentID]string, error) {
	resolvedDir, err := e.resolveBackupDir(backupDir)
	if err != nil {
		return nil, err
	}

	return e.walIO.BackupSegmentsAfter(afterID, resolvedDir)
}

// PutKV inserts a key-value pair.
func (e *Engine) PutKV(key, value []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	key = keycodec.KeyKV(key)

	putScope := e.taggedScope.Tagged(kvPutSurface)
	putScope.Counter(mRequestsTotal).Inc(1)

	start := putScope.Timer(mRequestLatencySeconds).Start()
	defer start.Stop()

	return e.persistKeyValue([][]byte{key}, [][]byte{value}, logrecord.LogOperationTypeInsert)
}

// BatchPutKV insert the associated Key Value Pair.
func (e *Engine) BatchPutKV(key, value [][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	for i := range key {
		key[i] = keycodec.KeyKV(key[i])
	}

	putScope := e.taggedScope.Tagged(kvPutSurface)
	putScope.Counter(mRequestsTotal).Inc(int64(len(key)))

	start := putScope.Timer(mRequestLatencySeconds).Start()
	defer start.Stop()

	return e.persistKeyValue(key, value, logrecord.LogOperationTypeInsert)
}

// DeleteKV removes a key and its value pair.
func (e *Engine) DeleteKV(key []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	key = keycodec.KeyKV(key)

	deleteScope := e.taggedScope.Tagged(kvDelSurface)
	deleteScope.Counter(mRequestsTotal).Inc(1)

	timer := deleteScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	return e.persistKeyValue([][]byte{key}, nil, logrecord.LogOperationTypeDelete)
}

// BatchDeleteKV removes all the key and its value pair.
func (e *Engine) BatchDeleteKV(keys [][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	for i := range keys {
		keys[i] = keycodec.KeyKV(keys[i])
	}

	deleteScope := e.taggedScope.Tagged(kvDelSurface)
	deleteScope.Counter(mRequestsTotal).Inc(int64(len(keys)))

	timer := deleteScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	return e.persistKeyValue(keys, nil, logrecord.LogOperationTypeDelete)
}

// WaitForAppendOrDone blocks until a put/delete operation occurs or context cancelled is done.
func (e *Engine) WaitForAppendOrDone(callerDone chan struct{}, lastSeen *Offset) error {
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
	}
}

func hasNewWriteSince(current, lastSeen *Offset) bool {
	if lastSeen == nil {
		return true
	}
	return current.SegmentID > lastSeen.SegmentID ||
		(current.SegmentID == lastSeen.SegmentID && current.Offset > lastSeen.Offset)
}

func isWithinDir(root, target string) bool {
	root = filepath.Clean(root)
	target = filepath.Clean(target)
	if root == target {
		return true
	}
	sep := string(os.PathSeparator)
	return strings.HasPrefix(target, root+sep)
}

func (e *Engine) resolveBackupDir(rel string) (string, error) {
	clean, err := e.cleanBackupRelativePath(rel, true)
	if err != nil {
		return "", err
	}

	target := filepath.Join(e.backupNamespaceRoot, clean)
	if !isWithinDir(e.backupNamespaceRoot, target) {
		return "", errors.New("backup directory escapes allowed root")
	}
	if err := os.MkdirAll(target, 0o755); err != nil {
		return "", fmt.Errorf("create backup directory: %w", err)
	}
	return target, nil
}

func (e *Engine) resolveBackupFile(rel string) (string, error) {
	clean, err := e.cleanBackupRelativePath(rel, false)
	if err != nil {
		return "", err
	}

	target := filepath.Join(e.backupNamespaceRoot, clean)
	if !isWithinDir(e.backupNamespaceRoot, target) {
		return "", errors.New("backup path escapes allowed root")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", fmt.Errorf("create backup directory: %w", err)
	}
	return target, nil
}

func (e *Engine) cleanBackupRelativePath(rel string, allowRoot bool) (string, error) {
	trimmed := strings.TrimSpace(rel)
	if trimmed == "" {
		if allowRoot {
			return ".", nil
		}
		return "", errors.New("backup path cannot be empty")
	}
	if filepath.IsAbs(trimmed) {
		return "", errors.New("backup path must be relative")
	}

	clean := filepath.Clean(trimmed)
	if clean == "." && !allowRoot {
		return "", errors.New("backup file path cannot reference backup root directly")
	}

	sep := string(os.PathSeparator)
	for _, part := range strings.Split(clean, sep) {
		if part == ".." {
			return "", errors.New("backup path cannot traverse outside backup root")
		}
	}
	return clean, nil
}

// GetKV retrieves the value associated with the given key.
func (e *Engine) GetKV(key []byte) ([]byte, error) {
	if e.shutdown.Load() {
		return nil, ErrInCloseProcess
	}

	key = keycodec.KeyKV(key)

	getScope := e.taggedScope.Tagged(kvGetSurface)
	getScope.Counter(mRequestsTotal).Inc(1)

	timer := getScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	e.mu.RLock()
	activeMemTable := e.activeMemTable
	sealedTables := append([]*memtable.MemTable(nil), e.sealedMemTables...)
	e.mu.RUnlock()

	yValue := activeMemTable.Get(key)

	if yValue.Meta != metaNoOp {
		if yValue.Meta == metaDelete {
			return nil, ErrKeyNotFound
		}

		return yValue.Value, nil
	}

	for i := len(sealedTables) - 1; i >= 0; i-- {
		if val := sealedTables[i].Get(key); val.Meta != metaNoOp {
			yValue = val
			if yValue.Meta == metaDelete {
				return nil, ErrKeyNotFound
			}

			return yValue.Value, nil
		}
	}

	return e.dataStore.GetKV(key)
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

	rowKey = keycodec.RowKey(rowKey)
	rowSetScope := e.taggedScope.Tagged(wideColumnPutSurface)
	rowSetScope.Counter(mRequestsTotal).Inc(1)

	timer := rowSetScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	columnsEntries := []map[string][]byte{columnEntries}
	return e.persistRowColumnAction(logrecord.LogOperationTypeInsert, [][]byte{rowKey}, columnsEntries)
}

func (e *Engine) PutColumnsForRows(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	for i := range rowKeys {
		rowKeys[i] = keycodec.RowKey(rowKeys[i])
	}

	rowSetScope := e.taggedScope.Tagged(wideColumnPutSurface)
	rowSetScope.Counter(mRequestsTotal).Inc(int64(len(rowKeys)))

	timer := rowSetScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	return e.persistRowColumnAction(logrecord.LogOperationTypeInsert, rowKeys, columnEntriesPerRow)
}

// DeleteColumnsForRow removes the specified columns from the given row key.
func (e *Engine) DeleteColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	rowKey = keycodec.RowKey(rowKey)

	rowDeleteScope := e.taggedScope.Tagged(wideColumnDelSurface)
	rowDeleteScope.Counter(mRequestsTotal).Inc(1)

	timer := rowDeleteScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	columnsEntries := make([]map[string][]byte, 0, 1)
	columnsEntries = append(columnsEntries, columnEntries)
	return e.persistRowColumnAction(logrecord.LogOperationTypeDelete, [][]byte{rowKey}, columnsEntries)
}

// DeleteColumnsForRows removes specified columns from multiple rows.
func (e *Engine) DeleteColumnsForRows(rowKeys [][]byte, columnEntries []map[string][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	for i := range rowKeys {
		rowKeys[i] = keycodec.RowKey(rowKeys[i])
	}

	rowDeleteScope := e.taggedScope.Tagged(wideColumnDelSurface)
	rowDeleteScope.Counter(mRequestsTotal).Inc(int64(len(rowKeys)))

	timer := rowDeleteScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	return e.persistRowColumnAction(logrecord.LogOperationTypeDelete, rowKeys, columnEntries)
}

// DeleteRow removes an entire row and all its associated column entries.
func (e *Engine) DeleteRow(rowKey []byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	rowKey = keycodec.RowKey(rowKey)

	rowDeleteScope := e.taggedScope.Tagged(wideColumnDelSurface)
	rowDeleteScope.Counter(mRequestsTotal).Inc(1)

	timer := rowDeleteScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	return e.persistRowColumnAction(logrecord.LogOperationTypeDeleteRowByKey, [][]byte{[]byte(rowKey)}, nil)
}

func (e *Engine) BatchDeleteRows(rowKeys [][]byte) error {
	if e.shutdown.Load() {
		return ErrInCloseProcess
	}

	for i := range rowKeys {
		rowKeys[i] = keycodec.RowKey(rowKeys[i])
	}

	rowDeleteScope := e.taggedScope.Tagged(wideColumnDelSurface)
	rowDeleteScope.Counter(mRequestsTotal).Inc(int64(len(rowKeys)))

	timer := rowDeleteScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	return e.persistRowColumnAction(logrecord.LogOperationTypeDeleteRowByKey, rowKeys, nil)
}

// GetRowColumns returns all the column value associated with the row. It's filters columns if predicate
// function is provided and only returns those columns for which predicate return true.
func (e *Engine) GetRowColumns(rowKey string, predicate func(columnKey string) bool) (map[string][]byte, error) {
	if e.shutdown.Load() {
		return nil, ErrInCloseProcess
	}

	key := keycodec.RowKey([]byte(rowKey))
	rowGetScope := e.taggedScope.Tagged(wideColumnGetSurface)
	rowGetScope.Counter(mRequestsTotal).Inc(1)

	timer := rowGetScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	e.mu.RLock()
	activeMemTable := e.activeMemTable
	sealedTables := append([]*memtable.MemTable(nil), e.sealedMemTables...)
	e.mu.RUnlock()

	first := activeMemTable.GetRowYValue(key)
	if len(first) != 0 && first[len(first)-1].Meta == metaDeleteRowByKey {
		return nil, ErrKeyNotFound
	}

	var vs []y.ValueStruct
	for _, sm := range sealedTables {
		v := sm.GetRowYValue(key)
		if len(v) != 0 {
			vs = append(vs, v...)
		}
	}

	if len(first) != 0 {
		vs = append(vs, first...)
	}

	predicateFunc := func(columnKey []byte) bool {
		if predicate == nil {
			return true
		}
		return predicate(string(columnKey))
	}

	// get the oldest value from the store.
	columnsValue, err := e.dataStore.ScanRowCells(key, predicateFunc)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	// row was found in dataStore
	foundInDataStore := err == nil

	// ensure non-nil map before merging memtable deltas.
	if columnsValue == nil {
		columnsValue = make(map[string][]byte)
	}

	buildColumnMap(columnsValue, vs)

	for columnKey := range columnsValue {
		if predicate != nil && !predicate(columnKey) {
			delete(columnsValue, columnKey)
		}
	}

	// no columns were found and the row doesn't exist in either memtables or dataStore,
	// return ErrKeyNotFound. If there's memtable data (vs is not empty), the row was touched
	// at some point, even if all columns were deleted.
	if len(columnsValue) == 0 && len(vs) == 0 && !foundInDataStore {
		return nil, ErrKeyNotFound
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

// GetLOB returns the full LOB value (joined).
func (e *Engine) GetLOB(key []byte) ([]byte, error) {
	if e.shutdown.Load() {
		return nil, ErrInCloseProcess
	}

	key = keycodec.KeyBlobChunk(key, 0)

	lobGetScope := e.taggedScope.Tagged(lobSurface)
	lobGetScope.Counter(mRequestsTotal).Inc(1)

	timer := lobGetScope.Timer(mRequestLatencySeconds).Start()
	defer timer.Stop()

	e.mu.RLock()
	activeMemTable := e.activeMemTable
	sealedTables := append([]*memtable.MemTable(nil), e.sealedMemTables...)
	e.mu.RUnlock()

	yv := activeMemTable.Get(key)

	if yv.Meta == metaNoOp {
		for i := len(sealedTables) - 1; i >= 0; i-- {
			if val := sealedTables[i].Get(key); val.Meta != metaNoOp {
				yv = val
				break
			}
		}
	}

	if yv.UserMeta == internal.EntryTypeChunked {
		rec, err := internal.GetWalRecord(yv, e.walIO)
		if err != nil {
			return nil, err
		}
		return e.reconstructChunkedValue(rec)
	}

	chunks, err := e.dataStore.GetLOBChunks(key)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	out := make([]byte, 0, total)
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out, nil
}
