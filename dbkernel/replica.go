package dbkernel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/uber-go/tally/v4"
)

var (
	ErrInvalidLSN    = errors.New("invalid LSN")
	ErrInvalidOffset = errors.New("appendLog: offset does not match record")
)

var (
	replicaInsertKV    = map[string]string{"surface": "replica", "op": "insert", "entry": "kv"}
	replicaInsertRow   = map[string]string{"surface": "replica", "op": "insert", "entry": "row"}
	replicaInsertChunk = map[string]string{"surface": "replica", "op": "insert", "entry": "chunked"}

	replicaDeleteKV     = map[string]string{"surface": "replica", "op": "delete", "entry": "kv"}
	replicaDeleteRow    = map[string]string{"surface": "replica", "op": "delete", "entry": "row"}
	replicaDeleteRowKey = map[string]string{"surface": "replica", "op": "delete_row", "entry": "row"}

	replicaCommitKV  = map[string]string{"surface": "replica", "op": "commit", "entry": "kv"}
	replicaCommitRow = map[string]string{"surface": "replica", "op": "commit", "entry": "row"}
)

const mReplicationLatencySeconds = "replication_latency_physical_seconds"

var replicationLatencyBuckets = tally.DurationBuckets{
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
}

// ReplicaWALHandler processes and applies incoming WAL records during replication.
type ReplicaWALHandler struct {
	mu     sync.Mutex
	engine *Engine
}

func NewReplicaWALHandler(engine *Engine) *ReplicaWALHandler {
	return &ReplicaWALHandler{engine: engine}
}

// ApplyRecord validates and applies a WAL record to the mem table which later get flushed to Btree Store.
func (wh *ReplicaWALHandler) ApplyRecord(encodedWal []byte, receivedOffset Offset) error {
	if receivedOffset.Offset == 0 {
		slog.Error("[dbkernel]",
			slog.String("message", "Failed to apply record: nil offset received"),
			slog.Group("engine",
				slog.String("namespace", wh.engine.namespace),
			),
			slog.Group("ops",
				slog.Uint64("received", wh.engine.writeSeenCounter.Load()),
				slog.Uint64("flushed", wh.engine.opsFlushedCounter.Load()),
			),
		)
		return ErrInvalidOffset
	}

	wh.mu.Lock()
	defer wh.mu.Unlock()

	lsn := wh.engine.writeSeenCounter.Load() + 1
	decoded := logrecord.GetRootAsLogRecord(encodedWal, 0)
	if lsn != decoded.Lsn() {
		return fmt.Errorf("%w %d, expected %d", ErrInvalidLSN, decoded.Lsn(), lsn)
	}

	wh.engine.writeSeenCounter.Add(1)
	offset, err := wh.engine.walIO.Append(encodedWal)
	if err != nil {
		return err
	}

	if !isEqualOffset(offset, receivedOffset) {
		slog.Error("[dbkernel]",
			slog.String("message", "Failed to apply record: WAL offset mismatch"),
			slog.Group("engine",
				slog.String("namespace", wh.engine.namespace),
			),
			slog.Group("ops",
				slog.Uint64("received", wh.engine.writeSeenCounter.Load()),
				slog.Uint64("flushed", wh.engine.opsFlushedCounter.Load()),
			),
			slog.Group("offset",
				slog.Any("received", receivedOffset),
				slog.Any("expected", offset),
				slog.Int("entry_size", len(encodedWal)),
			),
		)
		return ErrInvalidOffset
	}

	// measure physical latency
	remoteHLC := decoded.Hlc()
	nowMs := HLCNow()
	var physicalLatencyMs uint64
	if nowMs >= remoteHLC {
		physicalLatencyMs = nowMs - remoteHLC
	} else {
		physicalLatencyMs = 0
	}

	latency := time.Duration(physicalLatencyMs) * time.Millisecond
	wh.engine.taggedScope.Histogram(mReplicationLatencySeconds, replicationLatencyBuckets).RecordDuration(latency)

	// just a small optimization to skip debug log if not enabled upfront.
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("[dbkernel]",
			slog.String("message", "Measured replication apply latency"),
			slog.Group("replication",
				slog.Uint64("remote_hlc", remoteHLC),
				slog.Uint64("physical_latency_ms", physicalLatencyMs),
			),
		)
	}

	return wh.handleRecord(decoded, offset)
}

// ApplyRecords validates and applies multiple WAL records in batch to the mem table.
// This is more efficient than calling ApplyRecord multiple times as it reduces lock contention
// and leverages batch WAL writes.
func (wh *ReplicaWALHandler) ApplyRecords(encodedWals [][]byte, receivedOffsets []Offset) error {
	if len(encodedWals) == 0 {
		return nil
	}

	if len(encodedWals) != len(receivedOffsets) {
		return fmt.Errorf("mismatch: %d records but %d offsets", len(encodedWals), len(receivedOffsets))
	}

	wh.mu.Lock()
	defer wh.mu.Unlock()

	expectedLSN := wh.engine.writeSeenCounter.Load() + 1
	decodedRecords := make([]*logrecord.LogRecord, len(encodedWals))

	for i, encodedWal := range encodedWals {
		if receivedOffsets[i].Offset == 0 {
			slog.Error("[dbkernel]",
				slog.String("message", "Failed to apply record: nil offset received"),
				slog.Group("engine",
					slog.String("namespace", wh.engine.namespace),
				),
				slog.Int("record_index", i),
			)
			return ErrInvalidOffset
		}

		decoded := logrecord.GetRootAsLogRecord(encodedWal, 0)
		if expectedLSN+uint64(i) != decoded.Lsn() {
			return fmt.Errorf("%w at index %d: got %d, expected %d", ErrInvalidLSN, i, decoded.Lsn(), expectedLSN+uint64(i))
		}
		decodedRecords[i] = decoded
	}

	offsets, err := wh.engine.walIO.BatchAppend(encodedWals)
	if err != nil {
		return err
	}
	
	for i, offset := range offsets {
		if !isEqualOffset(offset, receivedOffsets[i]) {
			slog.Error("[dbkernel]",
				slog.String("message", "Failed to apply record: WAL offset mismatch in batch"),
				slog.Group("engine",
					slog.String("namespace", wh.engine.namespace),
				),
				slog.Int("record_index", i),
				slog.Group("offset",
					slog.Any("received", receivedOffsets[i]),
					slog.Any("expected", offset),
				),
			)
			return ErrInvalidOffset
		}
	}

	wh.engine.writeSeenCounter.Add(uint64(len(encodedWals)))

	for i, decoded := range decodedRecords {
		remoteHLC := decoded.Hlc()
		nowMs := HLCNow()
		var physicalLatencyMs uint64
		if nowMs >= remoteHLC {
			physicalLatencyMs = nowMs - remoteHLC
		} else {
			physicalLatencyMs = 0
		}

		latency := time.Duration(physicalLatencyMs) * time.Millisecond
		wh.engine.taggedScope.Histogram(mReplicationLatencySeconds, replicationLatencyBuckets).RecordDuration(latency)

		if err := wh.handleRecord(decoded, offsets[i]); err != nil {
			return fmt.Errorf("failed to handle record at index %d: %w", i, err)
		}
	}

	return nil
}

func isEqualOffset(local *Offset, remote Offset) bool {
	if local.SegmentID == remote.SegmentID && local.Offset == remote.Offset {
		return true
	}
	return false
}

func (wh *ReplicaWALHandler) handleRecord(record *logrecord.LogRecord, offset *Offset) error {
	switch record.TxnState() {
	case logrecord.TransactionStateNone:
		switch record.OperationType() {
		case logrecord.LogOperationTypeInsert:
			return wh.handleInsert(record, offset)
		case logrecord.LogOperationTypeDelete:
			return wh.handleDelete(record, offset)
		case logrecord.LogOperationTypeDeleteRowByKey:
			return wh.handleDeleteRowByKey(record, offset)
		}
	case logrecord.TransactionStateCommit:
		return wh.handleTxnCommited(record, offset)
	}

	return nil
}

// handleInsert insert the provided record into the mem table entry.
func (wh *ReplicaWALHandler) handleInsert(record *logrecord.LogRecord, offset *Offset) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		wh.engine.taggedScope.Tagged(replicaInsertKV).Counter(mRequestsTotal).Inc(1)
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			memValue := getValueStruct(internal.LogOperationInsert, internal.EntryTypeKV, kvEntry.Value)
			err := wh.engine.memTableWrite(kvEntry.Key, memValue)
			if err != nil {
				return err
			}
		}
		wh.engine.writeOffset(offset)
	case logrecord.LogEntryTypeRow:
		wh.engine.taggedScope.Tagged(replicaInsertRow).Counter(mRequestsTotal).Inc(1)
		for _, entry := range logEntry.Entries {
			rowEntry := logrecord.GetRootAsRowUpdateEntry(entry, 0)
			rowKey := rowEntry.KeyBytes()
			memValue := getValueStruct(internal.LogOperationInsert, internal.EntryTypeRow, entry)
			err := wh.engine.memTableWrite(rowKey, memValue)
			if err != nil {
				return err
			}
		}
		wh.engine.writeOffset(offset)
	}
	return nil
}

func (wh *ReplicaWALHandler) handleDelete(record *logrecord.LogRecord, offset *Offset) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		wh.engine.taggedScope.Tagged(replicaDeleteKV).Counter(mRequestsTotal).Inc(1)
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			memValue := getValueStruct(internal.LogOperationDelete, internal.EntryTypeKV, kvEntry.Value)
			err := wh.engine.memTableWrite(kvEntry.Key, memValue)
			if err != nil {
				return err
			}
		}
		wh.engine.writeOffset(offset)
	case logrecord.LogEntryTypeRow:
		wh.engine.taggedScope.Tagged(replicaDeleteRow).Counter(mRequestsTotal).Inc(1)
		for _, entry := range logEntry.Entries {
			rowEntry := logrecord.GetRootAsRowUpdateEntry(entry, 0)
			rowKey := rowEntry.KeyBytes()
			memValue := getValueStruct(internal.LogOperationDelete, internal.EntryTypeRow, entry)
			err := wh.engine.memTableWrite(rowKey, memValue)
			if err != nil {
				return err
			}
		}
		wh.engine.writeOffset(offset)
	}

	return nil
}

func (wh *ReplicaWALHandler) handleDeleteRowByKey(record *logrecord.LogRecord, offset *Offset) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	wh.engine.taggedScope.Tagged(replicaDeleteRowKey).Counter(mRequestsTotal).Inc(1)
	for _, entry := range logEntry.Entries {
		rowEntry := logrecord.GetRootAsRowUpdateEntry(entry, 0)
		rowKey := rowEntry.KeyBytes()
		memValue := getValueStruct(internal.LogOperationDeleteRowByKey, internal.EntryTypeRow, entry)
		err := wh.engine.memTableWrite(rowKey, memValue)
		if err != nil {
			return err
		}
	}

	wh.engine.writeOffset(offset)
	return nil
}

// handleTxnCommited handles the current commited txn, for chunked, insert and delete ops.
func (wh *ReplicaWALHandler) handleTxnCommited(record *logrecord.LogRecord, offset *Offset) error {
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		return wh.handleKVValuesTxn(record, offset)
	case logrecord.LogEntryTypeChunked:
		return wh.handleChunkedValuesTxn(record, offset)
	case logrecord.LogEntryTypeRow:
		return wh.handleRowColumnTxn(record, offset)
	}

	return nil
}

// handleKVValuesTxn Handles the insert and delete operation of Txn and updates
// the same to the underlying btree bases store.
func (wh *ReplicaWALHandler) handleKVValuesTxn(record *logrecord.LogRecord, offset *Offset) error {
	// TODO: Optimize this with cache or read Cache at WAL that is planned.
	records, err := wh.engine.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return err
	}

	// remove the begins part from the
	preparedRecords := records[1:]
	// Empty Increment the offset as this operation was carried but don't provide any offset value
	// newer offset is present.
	wh.engine.writeNilOffset()

	var applied int64
	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			memValue := getValueStruct(byte(record.OperationType()), internal.EntryTypeKV, kvEntry.Value)
			err := wh.engine.memTableWrite(kvEntry.Key, memValue)
			if err != nil {
				return err
			}

			applied++
			// empty offset just to increment the offset count that will be flushed.
			// new offset should or shouldn't be present.
			wh.engine.writeNilOffset()
		}
	}

	wh.engine.taggedScope.Tagged(replicaCommitKV).Counter(mRequestsTotal).Inc(applied)
	// finally write the current offset.
	wh.engine.writeOffset(offset)
	return nil
}

// handleRowColumnTxn Handles the insert and delete operation of Txn for RowUpdate and updates
// the same to the underlying btree bases store.
func (wh *ReplicaWALHandler) handleRowColumnTxn(record *logrecord.LogRecord, offset *Offset) error {
	records, err := wh.engine.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return err
	}

	// remove the begins part from the
	preparedRecords := records[1:]
	// Empty Increment the offset as this operation was carried but don't provide any offset value
	// newer offset is present.
	wh.engine.writeOffset(nil)

	var applied int64

	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			rowEntry := logcodec.DeserializeRowUpdateEntry(entry)
			memValue := getValueStruct(byte(record.OperationType()), internal.EntryTypeRow, entry)
			err := wh.engine.memTableWrite(rowEntry.Key, memValue)
			if err != nil {
				return err
			}

			applied++
			wh.engine.writeNilOffset()
		}
	}

	wh.engine.taggedScope.Tagged(replicaCommitRow).Counter(mRequestsTotal).Inc(applied)
	wh.engine.writeOffset(offset)
	return nil
}

// handleChunkedValuesTxn saves all the chunked value that is part of the current commit txn.
// to the provided btree based store.
func (wh *ReplicaWALHandler) handleChunkedValuesTxn(record *logrecord.LogRecord, offset *Offset) error {
	wh.engine.taggedScope.Tagged(replicaInsertChunk).Counter(mRequestsTotal).Inc(1)
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	kvEntry := logcodec.DeserializeKVEntry(logEntry.Entries[0])
	chunkedKey := kvEntry.Key
	memValue := getValueStruct(byte(logrecord.LogOperationTypeInsert), byte(logrecord.LogEntryTypeChunked), offset.Encode())
	err := wh.engine.memTableWrite(chunkedKey, memValue)
	if err != nil {
		return err
	}
	wh.engine.writeOffset(offset)
	return err
}
