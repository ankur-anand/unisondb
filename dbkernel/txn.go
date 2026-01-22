package dbkernel

import (
	"bytes"
	"errors"
	"hash/crc32"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/keycodec"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/segmentio/ksuid"
)

var (
	ErrTxnAlreadyCommitted      = errors.New("txn already committed")
	ErrTxnAborted               = errors.New("txn aborted")
	ErrKeyChangedForChunkedType = errors.New("chunked txn type cannot change key from first value")
	ErrUnsupportedTxnType       = errors.New("unsupported txn type")
	ErrEmptyColumns             = errors.New("empty column set")
)

var (
	mTxnBeginTotal       = "txn_begin_total"
	mTxnCommitTotal      = "txn_commit_total"
	mTxnLifecycleSeconds = "txn_lifecycle_seconds"
)

type txMemTableEntry struct {
	value  y.ValueStruct
	key    []byte
	offset *wal.Offset
}

// Txn ensures atomicity at WAL. Writes/Deletes/Chunks wouldn't be visible
// that are part of the batch until commited.
type Txn struct {
	err        error
	engine     *Engine
	prevOffset *wal.Offset
	// dataStore all the memTableEntries that can be stored on memTable after the commit has been called.
	memTableEntries      []txMemTableEntry
	rowKey               []byte
	txnID                []byte
	startTime            time.Time
	valuesCount          int
	checksum             uint32 // Rolling checksum
	chunkedValueChecksum uint32
	txnOperation         logrecord.LogOperationType
	txnEntryType         logrecord.LogEntryType
}

// Transaction exposes the common operations supported by both local and Raft-backed transactions.
type Transaction interface {
	TxnID() []byte
	AppendKVTxn(key []byte, value []byte) error
	AppendColumnTxn(rowKey []byte, columnEntries map[string][]byte) error
	Commit() error
	Abort()
	ChunkedValueChecksum() uint32
}

// NewTransaction returns a transaction implementation based on the current engine mode.
// In Raft mode, it returns a Raft-backed transaction; otherwise it returns a local transaction.
func (e *Engine) NewTransaction(txnType logrecord.LogOperationType, valueType logrecord.LogEntryType) (Transaction, error) {
	if e.IsRaftMode() {
		return e.NewRaftTxn(txnType, valueType)
	}
	return e.NewTxn(txnType, valueType)
}

var _ Transaction = (*Txn)(nil)

// validateTxnStart checks if a transaction can be started.
func (e *Engine) validateTxnStart(txnType logrecord.LogOperationType, valueType logrecord.LogEntryType) error {
	switch {
	case e.readOnly:
		return ErrEngineReadOnly
	case e.raftState.raftMode:
		return ErrNotSupportedInRaftMode
	case txnType == logrecord.LogOperationTypeNoOperation:
		return ErrUnsupportedTxnType
	case txnType == logrecord.LogOperationTypeDelete && valueType == logrecord.LogEntryTypeChunked:
		return ErrUnsupportedTxnType
	}
	return nil
}

// NewTxn returns a new initialized batch Txn.
// Transactions are not allowed in RaftMode.
func (e *Engine) NewTxn(txnType logrecord.LogOperationType, valueType logrecord.LogEntryType) (*Txn, error) {
	if err := e.validateTxnStart(txnType, valueType); err != nil {
		return nil, err
	}

	uuid, err := ksuid.New().MarshalBinary()
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// start the batch marker in wal
	index := e.writeSeenCounter.Add(1)
	record := logcodec.LogRecord{
		LSN:             index,
		HLC:             HLCNow(),
		TxnID:           uuid,
		EntryType:       valueType,
		TxnState:        logrecord.TransactionStateBegin,
		PrevTxnWalIndex: nil,
	}

	encoded := record.FBEncode(512)
	offset, err := e.walIO.Append(encoded, index)
	if err != nil {
		return nil, err
	}

	e.taggedScope.Counter(mTxnBeginTotal).Inc(1)
	return &Txn{
		txnID:           uuid,
		prevOffset:      offset,
		err:             err,
		engine:          e,
		checksum:        0,
		startTime:       time.Now(),
		valuesCount:     0,
		memTableEntries: make([]txMemTableEntry, 0),
		txnOperation:    txnType,
		txnEntryType:    valueType,
	}, nil
}

// AppendKVTxn append a key, value to the WAL as part of a Txn.
func (t *Txn) AppendKVTxn(key []byte, value []byte) error {
	if t.err != nil {
		return t.err
	}

	if t.txnEntryType == logrecord.LogEntryTypeRow {
		return ErrUnsupportedTxnType
	}

	if t.txnOperation == logrecord.LogOperationTypeInsert && t.txnEntryType != logrecord.LogEntryTypeKV && t.rowKey == nil {
		t.rowKey = keycodec.KeyBlobChunk(key, 0)
	}

	if t.txnEntryType == logrecord.LogEntryTypeChunked {
		key = keycodec.KeyBlobChunk(key, 0)
	} else {
		key = keycodec.KeyKV(key)
	}

	if t.txnOperation == logrecord.LogOperationTypeInsert && t.txnEntryType != logrecord.LogEntryTypeKV && !bytes.Equal(t.rowKey, key) {
		t.err = ErrKeyChangedForChunkedType
		return t.err
	}

	kvEncoded := logcodec.SerializeKVEntry(key, value)
	checksum := crc32.ChecksumIEEE(kvEncoded)
	t.chunkedValueChecksum = crc32.Update(t.chunkedValueChecksum, crc32.IEEETable, value)
	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)

	record := logcodec.LogRecord{
		LSN:             index,
		HLC:             HLCNow(),
		CRC32Checksum:   checksum,
		OperationType:   t.txnOperation,
		TxnState:        logrecord.TransactionStatePrepare,
		EntryType:       t.txnEntryType,
		TxnID:           t.txnID,
		PrevTxnWalIndex: t.prevOffset.Encode(),
		Entries:         [][]byte{kvEncoded},
	}

	encoded := record.FBEncode(len(kvEncoded) + 128)
	// Write to WAL
	offset, err := t.engine.walIO.Append(encoded, index)

	if err != nil {
		t.err = err
		return err
	}

	t.prevOffset = offset

	// for chunked type we just dataStore the last offset.
	if t.txnEntryType != logrecord.LogEntryTypeChunked {
		memValue := getValueStruct(byte(t.txnOperation), byte(t.txnEntryType), value)
		t.memTableEntries = append(t.memTableEntries, txMemTableEntry{
			key:    key,
			offset: offset,
			value:  memValue,
		})
	}

	t.checksum = crc32.Update(t.checksum, crc32.IEEETable, kvEncoded)
	t.valuesCount++
	return nil
}

// AppendColumnTxn appends the Columns update type Txn to wal for the provided rowKey.
// Update/DeleteKV Ops for column is decided by the Log Operation type.
// Single Txn Cannot contain both update and delete ops.
// Caller can set the Columns Key to empty value, if deleted needs to be part of same Txn.
func (t *Txn) AppendColumnTxn(rowKey []byte, columnEntries map[string][]byte) error {
	if t.err != nil {
		return t.err
	}
	if t.txnEntryType != logrecord.LogEntryTypeRow {
		return ErrUnsupportedTxnType
	}

	if len(columnEntries) == 0 {
		return ErrEmptyColumns
	}

	rowKey = keycodec.RowKey(rowKey)

	rce := logcodec.SerializeRowUpdateEntry(rowKey, columnEntries)
	checksum := crc32.ChecksumIEEE(rce)

	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)

	record := logcodec.LogRecord{
		LSN:             index,
		HLC:             HLCNow(),
		CRC32Checksum:   checksum,
		OperationType:   t.txnOperation,
		TxnState:        logrecord.TransactionStatePrepare,
		EntryType:       t.txnEntryType,
		TxnID:           t.txnID,
		PrevTxnWalIndex: t.prevOffset.Encode(),
		Entries:         [][]byte{rce},
	}

	encoded := record.FBEncode(len(rce) + 128)

	// Write to WAL
	offset, err := t.engine.walIO.Append(encoded, index)

	if err != nil {
		t.err = err
		return err
	}

	t.prevOffset = offset

	memValue := getValueStruct(byte(t.txnOperation), byte(t.txnEntryType), rce)
	t.memTableEntries = append(t.memTableEntries, txMemTableEntry{
		key:    rowKey,
		offset: offset,
		value:  memValue,
	})

	t.checksum = crc32.Update(t.checksum, crc32.IEEETable, encoded)
	t.valuesCount++
	return nil
}

// Commit the Txn.
func (t *Txn) Commit() error {
	if t.err != nil {
		return t.err
	}

	kv := logcodec.SerializeKVEntry(t.rowKey, nil)
	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)

	record := logcodec.LogRecord{
		LSN:             index,
		HLC:             HLCNow(),
		CRC32Checksum:   t.checksum,
		OperationType:   t.txnOperation,
		TxnState:        logrecord.TransactionStateCommit,
		EntryType:       t.txnEntryType,
		TxnID:           t.txnID,
		PrevTxnWalIndex: t.prevOffset.Encode(),
		Entries:         [][]byte{kv},
	}

	// Encode WAL record
	encoded := record.FBEncode(len(kv) + 128)

	// Write to WAL
	offset, err := t.engine.walIO.Append(encoded, index)

	if err != nil {
		t.err = err
		return err
	}

	defer func() {
		t.engine.taggedScope.Counter(mTxnCommitTotal).Inc(1)
		t.engine.taggedScope.Timer(mTxnLifecycleSeconds).Record(time.Since(t.startTime))
	}()

	// flush all the writes on mem-table
	t.prevOffset = offset

	var mErr error
	switch t.txnEntryType {
	case logrecord.LogEntryTypeKV, logrecord.LogEntryTypeRow:
		mErr = t.memWriteFull()
	case logrecord.LogEntryTypeChunked:
		mErr = t.memWriteChunk(offset.Encode())
	}

	if mErr != nil {
		t.err = mErr
		return mErr
	}

	t.err = ErrTxnAlreadyCommitted
	return nil
}

func (t *Txn) memWriteChunk(encoded []byte) error {
	memValue := getValueStruct(byte(logrecord.LogOperationTypeInsert), byte(logrecord.LogEntryTypeChunked), encoded)
	err := t.engine.memTableWrite(t.rowKey, memValue)
	if err != nil {
		t.err = err
		return err
	}

	t.engine.writeOffset(t.prevOffset)
	return nil
}

func (t *Txn) memWriteFull() error {
	for _, memValue := range t.memTableEntries {
		err := t.engine.memTableWrite(memValue.key, memValue.value)
		//time.Sleep(1 * time.Nanosecond)
		if err != nil {
			t.err = err
			return err
		}
		// newer offset could have been written in mem-table at this point.
		// the only newer offset at this point of time is the commit offset.
		t.engine.writeNilOffset()
	}

	t.engine.writeOffset(t.prevOffset)
	return nil
}

func (t *Txn) TxnID() []byte {
	return t.txnID
}

// Abort releases txn bookkeeping without committing.
func (t *Txn) Abort() {
	t.err = ErrTxnAborted
}

func (t *Txn) Checksum() uint32 {
	return t.checksum
}

func (t *Txn) ChunkedValueChecksum() uint32 {
	return t.chunkedValueChecksum
}

func (t *Txn) CommitOffset() *Offset {
	return t.prevOffset
}
