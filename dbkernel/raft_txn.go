package dbkernel

import (
	"bytes"
	"errors"
	"hash/crc32"
	"time"

	"github.com/ankur-anand/unisondb/internal/keycodec"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/segmentio/ksuid"
)

var (
	// ErrRaftTxnNotLeader is returned when the current node is not the Raft leader.
	ErrRaftTxnNotLeader = errors.New("raft txn failed: not leader")

	// ErrRaftTxnNoApplier is returned when attempting to start a transaction without a Raft applier.
	ErrRaftTxnNoApplier = errors.New("raft applier not configured")
)

// RaftTxn represents a transaction in Raft mode.
type RaftTxn struct {
	txnID           []byte
	engine          *Engine
	beginRaftIndex  uint64
	prevWalOffset   []byte
	rowKey          []byte
	operation       logrecord.LogOperationType
	entryType       logrecord.LogEntryType
	checksum        uint32
	chunkedChecksum uint32
	valuesCount     int
	committed       bool
	aborted         bool
	startTime       time.Time
}

var _ Transaction = (*RaftTxn)(nil)

// NewRaftTxn creates a new transaction in Raft mode.
func (e *Engine) NewRaftTxn(op logrecord.LogOperationType, entryType logrecord.LogEntryType) (*RaftTxn, error) {
	if !e.raftState.raftMode {
		return nil, ErrNotSupportedInRaftMode
	}

	if e.raftState.applier == nil {
		return nil, ErrRaftTxnNoApplier
	}

	if e.readOnly {
		return nil, ErrEngineReadOnly
	}

	if op == logrecord.LogOperationTypeNoOperation {
		return nil, ErrUnsupportedTxnType
	}
	if op == logrecord.LogOperationTypeDelete && entryType == logrecord.LogEntryTypeChunked {
		return nil, ErrUnsupportedTxnType
	}

	uuid, err := ksuid.New().MarshalBinary()
	if err != nil {
		return nil, err
	}

	beginRecord := logcodec.LogRecord{
		LSN:             1,
		HLC:             HLCNow(),
		TxnID:           uuid,
		EntryType:       entryType,
		TxnState:        logrecord.TransactionStateBegin,
		PrevTxnWalIndex: nil,
	}

	encoded := beginRecord.FBEncode(512)
	raftIndex, err := e.raftState.applier.Apply(encoded)
	if err != nil {
		return nil, wrapRaftError(err)
	}

	var prevWalOffset []byte
	if e.raftState.positionLookup != nil {
		if pos, ok := e.raftState.positionLookup(raftIndex); ok {
			prevWalOffset = walfs.RecordPosition{SegmentID: pos.SegmentID, Offset: pos.Offset}.Encode()
		}
	}

	return &RaftTxn{
		txnID:          uuid,
		engine:         e,
		beginRaftIndex: raftIndex,
		prevWalOffset:  prevWalOffset,
		operation:      op,
		entryType:      entryType,
		startTime:      time.Now(),
	}, nil
}

// AppendKVTxn appends a key-value pair to the transaction.
func (t *RaftTxn) AppendKVTxn(key, value []byte) error {
	if err := t.checkState(); err != nil {
		return err
	}

	if t.entryType == logrecord.LogEntryTypeRow {
		return ErrUnsupportedTxnType
	}

	if t.entryType == logrecord.LogEntryTypeChunked {
		chunkKey := keycodec.KeyBlobChunk(key, 0)
		if t.rowKey == nil {
			t.rowKey = chunkKey
		} else if !bytes.Equal(t.rowKey, chunkKey) {
			return ErrKeyChangedForChunkedType
		}
		key = chunkKey
		t.chunkedChecksum = crc32.Update(t.chunkedChecksum, crc32.IEEETable, value)
	} else {
		key = keycodec.KeyKV(key)
	}

	return t.appendEntry(key, value)
}

// AppendColumnTxn appends a row with columns to the transaction.
func (t *RaftTxn) AppendColumnTxn(rowKey []byte, columns map[string][]byte) error {
	if err := t.checkState(); err != nil {
		return err
	}

	if t.entryType != logrecord.LogEntryTypeRow {
		return ErrUnsupportedTxnType
	}

	if len(columns) == 0 {
		return ErrEmptyColumns
	}

	rowKey = keycodec.RowKey(rowKey)
	rce := logcodec.SerializeRowUpdateEntry(rowKey, columns)
	return t.appendEntry(rowKey, rce)
}

func (t *RaftTxn) AppendChunk(key, chunkData []byte) error {
	if t.entryType != logrecord.LogEntryTypeChunked {
		return ErrUnsupportedTxnType
	}
	return t.AppendKVTxn(key, chunkData)
}

// appendEntry is the internal method that creates and proposes a Prepare record.
func (t *RaftTxn) appendEntry(key, value []byte) error {
	var kvEncoded []byte
	if t.entryType == logrecord.LogEntryTypeRow {
		kvEncoded = value
	} else {
		kvEncoded = logcodec.SerializeKVEntry(key, value)
	}

	checksum := crc32.ChecksumIEEE(kvEncoded)
	t.checksum = crc32.Update(t.checksum, crc32.IEEETable, kvEncoded)

	prepareRecord := logcodec.LogRecord{
		LSN:             1,
		HLC:             HLCNow(),
		CRC32Checksum:   checksum,
		OperationType:   t.operation,
		TxnState:        logrecord.TransactionStatePrepare,
		EntryType:       t.entryType,
		TxnID:           t.txnID,
		PrevTxnWalIndex: t.prevWalOffset,
		Entries:         [][]byte{kvEncoded},
	}

	encoded := prepareRecord.FBEncode(len(kvEncoded) + 128)
	raftIndex, err := t.engine.raftState.applier.Apply(encoded)
	if err != nil {
		t.aborted = true
		return wrapRaftError(err)
	}

	if t.engine.raftState.positionLookup != nil {
		if pos, ok := t.engine.raftState.positionLookup(raftIndex); ok {
			t.prevWalOffset = walfs.RecordPosition{SegmentID: pos.SegmentID, Offset: pos.Offset}.Encode()
		}
	}

	t.valuesCount++
	return nil
}

func (t *RaftTxn) Commit() error {
	if err := t.checkState(); err != nil {
		return err
	}

	var entries [][]byte
	if t.entryType == logrecord.LogEntryTypeChunked && t.rowKey != nil {
		entries = [][]byte{logcodec.SerializeKVEntry(t.rowKey, nil)}
	}

	commitRecord := logcodec.LogRecord{
		LSN:             1,
		HLC:             HLCNow(),
		CRC32Checksum:   t.checksum,
		OperationType:   t.operation,
		TxnState:        logrecord.TransactionStateCommit,
		EntryType:       t.entryType,
		TxnID:           t.txnID,
		PrevTxnWalIndex: t.prevWalOffset,
		Entries:         entries,
	}

	encoded := commitRecord.FBEncode(256)
	_, err := t.engine.raftState.applier.Apply(encoded)
	if err != nil {
		t.aborted = true
		return wrapRaftError(err)
	}

	t.committed = true
	return nil
}

func (t *RaftTxn) Abort() {
	if t.committed || t.aborted {
		return
	}
	t.aborted = true
}

func (t *RaftTxn) TxnID() []byte {
	return t.txnID
}

func (t *RaftTxn) Checksum() uint32 {
	return t.checksum
}

func (t *RaftTxn) ChunkedValueChecksum() uint32 {
	return t.chunkedChecksum
}

func (t *RaftTxn) ValuesCount() int {
	return t.valuesCount
}

func (t *RaftTxn) BeginRaftIndex() uint64 {
	return t.beginRaftIndex
}

func (t *RaftTxn) checkState() error {
	if t.committed {
		return ErrTxnAlreadyCommitted
	}
	if t.aborted {
		return ErrTxnAborted
	}
	return nil
}

func wrapRaftError(err error) error {
	if err == nil {
		return nil
	}
	errStr := err.Error()
	if contains(errStr, "not leader") || contains(errStr, "leadership") {
		return errors.Join(ErrRaftTxnNotLeader, err)
	}
	return err
}

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
