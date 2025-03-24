package dbkernel

import (
	"bytes"
	"errors"
	"hash/crc32"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/hashicorp/go-metrics"
	"github.com/segmentio/ksuid"
)

var (
	ErrTxnAlreadyCommitted      = errors.New("txn already committed")
	ErrKeyChangedForChunkedType = errors.New("chunked txn type cannot change key from first value")
	ErrUnsupportedTxnType       = errors.New("unsupported txn type")
	ErrEmptyColumns             = errors.New("empty column set")
)

var (
	mTxnKeyCommitedTotal     = append(packageKey, "txn", "commited", "total")
	mTxnKeyBeginTotal        = append(packageKey, "txn", "begin", "total")
	mTxnKeyLifecycleDuration = append(packageKey, "txn", "lifecycle", "durations", "seconds")
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
	memTableEntries []txMemTableEntry
	rowKey          []byte
	txnID           []byte
	startTime       time.Time
	valuesCount     int
	checksum        uint32 // Rolling checksum
	txnOperation    logrecord.LogOperationType
	txnEntryType    logrecord.LogEntryType
}

// NewTxn returns a new initialized batch Txn.
func (e *Engine) NewTxn(txnType logrecord.LogOperationType, valueType logrecord.LogEntryType) (*Txn, error) {
	if txnType == logrecord.LogOperationTypeNoOperation {
		return nil, ErrUnsupportedTxnType
	}

	if txnType == logrecord.LogOperationTypeDelete && valueType == logrecord.LogEntryTypeChunked {
		return nil, ErrUnsupportedTxnType
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
		HLC:             HLCNow(index),
		TxnID:           uuid,
		EntryType:       valueType,
		TxnState:        logrecord.TransactionStateBegin,
		PrevTxnWalIndex: nil,
	}

	encoded := record.FBEncode(512)
	offset, err := e.walIO.Append(encoded)
	if err != nil {
		return nil, err
	}

	metrics.IncrCounterWithLabels(mTxnKeyBeginTotal, 1, e.metricsLabel)
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
		t.rowKey = key
	}

	if t.txnOperation == logrecord.LogOperationTypeInsert && t.txnEntryType != logrecord.LogEntryTypeKV && !bytes.Equal(t.rowKey, key) {
		t.err = ErrKeyChangedForChunkedType
		return t.err
	}

	kvEncoded := logcodec.SerializeKVEntry(key, value)
	checksum := crc32.ChecksumIEEE(kvEncoded)

	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)

	record := logcodec.LogRecord{
		LSN:             index,
		HLC:             HLCNow(index),
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
	offset, err := t.engine.walIO.Append(encoded)

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
// Update/Delete Ops for column is decided by the Log Operation type.
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

	rce := logcodec.SerializeRowUpdateEntry(rowKey, columnEntries)
	checksum := crc32.ChecksumIEEE(rce)

	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)

	record := logcodec.LogRecord{
		LSN:             index,
		HLC:             HLCNow(index),
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
	offset, err := t.engine.walIO.Append(encoded)

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
		HLC:             HLCNow(index),
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
	offset, err := t.engine.walIO.Append(encoded)

	if err != nil {
		t.err = err
		return err
	}

	defer func() {
		metrics.IncrCounterWithLabels(mTxnKeyCommitedTotal, 1, t.engine.metricsLabel)
		metrics.MeasureSinceWithLabels(mTxnKeyLifecycleDuration, t.startTime, t.engine.metricsLabel)
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
		if err != nil {
			t.err = err
			return err
		}
		t.engine.writeOffset(memValue.offset)
	}

	return nil
}

func (t *Txn) TxnID() []byte {
	return t.txnID
}

func (t *Txn) Checksum() uint32 {
	return t.checksum
}
