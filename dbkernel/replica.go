package dbkernel

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ErrInvalidLSN    = errors.New("invalid LSN")
	ErrInvalidOffset = errors.New("appendLog: offset does not match record")
)

var (
	replicationPhysicalLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "unisondb",
			Subsystem: "dbkernel",
			Name:      "replication_latency_physical_ms",
			Help:      "Physical replication latency in milliseconds.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		},
		[]string{"namespace"},
	)

	replicationCausalLag = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "unisondb",
			Subsystem: "dbkernel",
			Name:      "replication_causal_lag_ms",
			Help:      "Causal replication lag in milliseconds.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		},
		[]string{"namespace"},
	)
)

// ReplicaWALHandler processes and applies incoming WAL records during replication.
type ReplicaWALHandler struct {
	mu     sync.Mutex
	engine *Engine
}

func NewReplicaWALHandler(engine *Engine) *ReplicaWALHandler {
	return &ReplicaWALHandler{engine: engine}
}

// ApplyRecord validates and applies a WAL record to the mem table which later get flushed to Btree Store.
func (wh *ReplicaWALHandler) ApplyRecord(encodedWal []byte, receivedOffset []byte) error {
	if receivedOffset == nil {
		slog.Error("[unisondb.dbkernal]",
			slog.String("event_type", "apply.record.errored"),
			slog.String("error", "received nil offset while applying record"),
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

	if !isEqualOffset(offset, DecodeOffset(receivedOffset)) {
		slog.Error("[unisondb.dbkernal]",
			slog.String("event_type", "apply.record.errored"),
			slog.String("error", " expected offset of wal entry didn't matched the received offset"),
			slog.Group("engine",
				slog.String("namespace", wh.engine.namespace),
			),
			slog.Group("ops",
				slog.Uint64("received", wh.engine.writeSeenCounter.Load()),
				slog.Uint64("flushed", wh.engine.opsFlushedCounter.Load()),
			),
			slog.Group("offset",
				slog.Any("received", wal.DecodeOffset(receivedOffset)),
				slog.Any("inserted", offset),
				slog.Int("entry_size", len(encodedWal))),
		)

		return ErrInvalidOffset
	}

	// measure physical latency
	remoteHLC := decoded.Hlc()
	eventRemoteTimeMs, _ := HLCDecode(remoteHLC)
	nowMs := uint64(time.Now().UnixMilli()) - CustomEpochMs
	physicalLatencyMs := nowMs - eventRemoteTimeMs

	// Measuring Physical latency alone is not enough.
	// It doesn't help us answer, the latency is due to what
	// 1. NTP skew
	// - If lag behind, we underreport the physical latency.
	// - If forward, we over report latency.
	// what that mean is even if the physical latency is reported as low,
	// assumption of perfect synchronization is invalid.
	// Measuring causal lag help us in detecting
	// 1. NTP skew.
	// If causalLagMs is always zero, it means either:
	// 1. The system has low replication latency and is causally up-to-date.
	// 2. Or, the replica's clock is behind the primary (clock skew), causing underreporting of causal lag.
	// causalLagMs Increasing Continuously
	// Replica is consistently falling behind; causal consistency is degrading.
	// Causal Lag is:
	// "How far ahead is my local time compared to the time when this event was created?"
	// High Physical Latency + Low Causal Lag
	// Likely NTP skew (replica clock behind).
	// Low Physical Latency + High Causal Lag
	// Replica applied event late; slow WAL replay
	// High Physical & Causal Lag + High Segment Lag(if calculated from remote)
	// Replication pipeline bottleneck
	//
	localHLC := HLCNow()
	eventLocalTimeMs, _ := HLCDecode(localHLC)
	causalLagMs := max(0, eventLocalTimeMs-eventRemoteTimeMs)

	namespace := wh.engine.namespace
	replicationPhysicalLatency.WithLabelValues(namespace).Observe(float64(physicalLatencyMs))
	replicationCausalLag.WithLabelValues(namespace).Observe(float64(causalLagMs))

	slog.Debug("[unisondb.dbkernel]",
		slog.String("event_type", "apply.record.replication.latency"),
		slog.Uint64("remote_hlc", remoteHLC),
		slog.Uint64("event_remote_time_ms", eventRemoteTimeMs),
		slog.Uint64("physical_latency_ms", physicalLatencyMs),
		slog.Uint64("causal_lag_ms", causalLagMs),
	)

	return wh.handleRecord(decoded, offset)
}

func isEqualOffset(local, remote *Offset) bool {
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

	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			memValue := getValueStruct(byte(record.OperationType()), internal.EntryTypeKV, kvEntry.Value)
			err := wh.engine.memTableWrite(kvEntry.Key, memValue)
			if err != nil {
				return err
			}
			// empty offset just to increment the offset count that will be flushed.
			// new offset should or shouldn't be present.
			wh.engine.writeNilOffset()
		}
	}

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

	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			rowEntry := logcodec.DeserializeRowUpdateEntry(entry)
			memValue := getValueStruct(byte(record.OperationType()), internal.EntryTypeRow, entry)
			err := wh.engine.memTableWrite(rowEntry.Key, memValue)
			if err != nil {
				return err
			}
			wh.engine.writeNilOffset()
		}
	}

	wh.engine.writeOffset(offset)
	return nil
}

// handleChunkedValuesTxn saves all the chunked value that is part of the current commit txn.
// to the provided btree based store.
func (wh *ReplicaWALHandler) handleChunkedValuesTxn(record *logrecord.LogRecord, offset *Offset) error {
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
