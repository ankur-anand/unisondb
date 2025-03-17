package dbkernel

import (
	"bytes"
	"errors"
	"hash/crc32"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/dbkernel/wal/walrecord"
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
	err     error
	engine  *Engine
	lastPos *wal.Offset
	// dataStore all the memTableEntries that can be stored on memTable after the commit has been called.
	memTableEntries []txMemTableEntry
	rowKey          []byte
	txnID           []byte
	startTime       time.Time
	valuesCount     int
	checksum        uint32 // Rolling checksum
	txnOperation    walrecord.LogOperation
	txnEntryType    walrecord.EntryType
}

// NewTxn returns a new initialized batch Txn.
func (e *Engine) NewTxn(txnType walrecord.LogOperation, valueType walrecord.EntryType) (*Txn, error) {
	if txnType == walrecord.LogOperationNoop {
		return nil, ErrUnsupportedTxnType
	}

	if txnType == walrecord.LogOperationDelete && valueType == walrecord.EntryTypeChunked {
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
	record := walrecord.Record{
		Index:         index,
		Hlc:           HLCNow(index),
		Key:           []byte("batch_tx_begin"),
		Value:         nil,
		LogOperation:  walrecord.LogOperationTxnMarker,
		TxnID:         uuid,
		TxnStatus:     walrecord.TxnStatusBegin,
		EntryType:     valueType,
		PrevTxnOffset: nil,
	}

	encoded, err := record.FBEncode()
	if err != nil {
		return nil, err
	}

	offset, err := e.walIO.Append(encoded)
	if err != nil {
		return nil, err
	}

	metrics.IncrCounterWithLabels(mTxnKeyBeginTotal, 1, e.metricsLabel)
	return &Txn{
		txnID:           uuid,
		lastPos:         offset,
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

	if t.txnEntryType == walrecord.EntryTypeRow {
		return ErrUnsupportedTxnType
	}

	if t.txnOperation == walrecord.LogOperationInsert && t.txnEntryType != walrecord.EntryTypeKV && t.rowKey == nil {
		t.rowKey = key
	}

	if t.txnOperation == walrecord.LogOperationInsert && t.txnEntryType != walrecord.EntryTypeKV && !bytes.Equal(t.rowKey, key) {
		t.err = ErrKeyChangedForChunkedType
		return t.err
	}

	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)

	record := &walrecord.Record{
		Index:         index,
		Hlc:           HLCNow(index),
		Key:           key,
		Value:         value,
		LogOperation:  t.txnOperation,
		TxnID:         t.txnID,
		TxnStatus:     walrecord.TxnStatusPrepare,
		EntryType:     t.txnEntryType,
		PrevTxnOffset: t.lastPos,
	}

	// Encode and compress WAL record
	encoded, err := record.FBEncode()

	if err != nil {
		t.err = err
		return err
	}

	// Write to WAL
	offset, err := t.engine.walIO.Append(encoded)

	if err != nil {
		t.err = err
		return err
	}

	t.lastPos = offset

	// for chunked type we just dataStore the last offset.
	if t.txnEntryType != walrecord.EntryTypeChunked {
		var memValue y.ValueStruct
		if int64(len(value)) <= t.engine.config.ValueThreshold {
			memValue = getValueStruct(byte(t.txnOperation), true, encoded)
		} else {
			memValue = getValueStruct(byte(t.txnOperation), false, offset.Encode())
		}

		t.memTableEntries = append(t.memTableEntries, txMemTableEntry{
			key:    key,
			offset: offset,
			value:  memValue,
		})
	}

	t.checksum = crc32.Update(t.checksum, crc32.IEEETable, value)
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
	if t.txnEntryType != walrecord.EntryTypeRow {
		return ErrUnsupportedTxnType
	}

	if len(columnEntries) == 0 {
		return ErrEmptyColumns
	}

	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)

	record := &walrecord.Record{
		Index:         index,
		Hlc:           HLCNow(index),
		Key:           rowKey,
		Value:         nil,
		LogOperation:  t.txnOperation,
		TxnID:         t.txnID,
		TxnStatus:     walrecord.TxnStatusPrepare,
		EntryType:     t.txnEntryType,
		PrevTxnOffset: t.lastPos,
		ColumnEntries: columnEntries,
	}

	// Encode and compress WAL record
	encoded, err := record.FBEncode()

	if err != nil {
		t.err = err
		return err
	}

	// Write to WAL
	offset, err := t.engine.walIO.Append(encoded)

	if err != nil {
		t.err = err
		return err
	}

	t.lastPos = offset

	var memValue y.ValueStruct
	if int64(len(encoded)) <= t.engine.config.ValueThreshold {
		memValue = getValueStruct(byte(t.txnOperation), true, encoded)
	} else {
		memValue = getValueStruct(byte(t.txnOperation), false, offset.Encode())
	}

	memValue.UserMeta = entryTypeRow
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

	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.writeSeenCounter.Add(1)
	record := &walrecord.Record{
		Index:         index,
		Hlc:           HLCNow(index),
		Key:           t.rowKey,
		Value:         marshalChecksum(t.checksum),
		LogOperation:  t.txnOperation,
		TxnID:         t.txnID,
		TxnStatus:     walrecord.TxnStatusCommit,
		EntryType:     t.txnEntryType,
		PrevTxnOffset: t.lastPos,
	}

	// Encode and compress WAL record
	encoded, err := record.FBEncode()

	if err != nil {
		t.err = err
		return err
	}

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
	t.lastPos = offset

	var mErr error
	switch t.txnEntryType {
	case walrecord.EntryTypeKV, walrecord.EntryTypeRow:
		mErr = t.memWriteFull()
	case walrecord.EntryTypeChunked:
		mErr = t.memWriteChunk(encoded)
	}

	if mErr != nil {
		t.err = mErr
		return mErr
	}

	t.err = ErrTxnAlreadyCommitted
	return nil
}

func (t *Txn) memWriteChunk(encoded []byte) error {
	memValue := getValueStruct(byte(walrecord.LogOperationInsert), true, encoded)
	err := t.engine.memTableWrite(t.rowKey, memValue, t.lastPos)
	if err != nil {
		t.err = err
		return err
	}

	return nil
}

func (t *Txn) memWriteFull() error {
	for _, memValue := range t.memTableEntries {
		err := t.engine.memTableWrite(memValue.key, memValue.value, memValue.offset)
		if err != nil {
			t.err = err
			return err
		}
	}

	return nil
}

func (t *Txn) TxnID() []byte {
	return t.txnID
}

func (t *Txn) Checksum() uint32 {
	return t.checksum
}
