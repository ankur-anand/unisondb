package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/hashicorp/go-metrics"
	"github.com/rosedblabs/wal"
	"github.com/segmentio/ksuid"
)

var (
	ErrTxnAlreadyCommitted = errors.New("txn already committed")
)

type txMemTableEntry struct {
	value    y.ValueStruct
	key      []byte
	chunkPos *wal.ChunkPosition
}

// Txn ensures atomicity at WAL. Writes/Deletes/Chunks wouldn't be visible
// that are part of the batch until commited.
type Txn struct {
	txnID   []byte
	lastPos *wal.ChunkPosition
	err     error

	engine      *Engine
	checksum    uint32 // Rolling checksum
	startTime   time.Time
	valuesCount int
	// store all the memTableEntries that can be stored on memTable after the commit has been called.
	memTableEntries []txMemTableEntry
	chunkedKey      []byte

	txnType   wrecord.LogOperation
	valueType wrecord.ValueType
}

// NewTxn returns a new initialized batch Txn.
func (e *Engine) NewTxn(txnType wrecord.LogOperation, valueType wrecord.ValueType) (*Txn, error) {
	if txnType == wrecord.LogOperationNoop {
		return &Txn{}, errors.New("txn is a no-op")
	}

	if txnType == wrecord.LogOperationDelete && valueType == wrecord.ValueTypeChunked {
		return &Txn{}, errors.New("unsupported txn type")
	}

	uuid, err := ksuid.New().MarshalBinary()
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// start the batch marker in wal
	index := e.globalCounter.Add(1)
	record := walRecord{
		index:        index,
		hlc:          HLCNow(index),
		key:          []byte("batch_tx_begin"),
		value:        nil,
		op:           wrecord.LogOperationTxnMarker,
		txnID:        uuid,
		txnStatus:    wrecord.TxnStatusBegin,
		valueType:    valueType,
		prevTxnChunk: nil,
	}

	encoded, err := record.fbEncode()
	if err != nil {
		return nil, err
	}

	chunkPos, err := e.walIO.Write(encoded)
	if err != nil {
		return nil, err
	}

	return &Txn{
		txnID:           uuid,
		lastPos:         chunkPos,
		err:             err,
		engine:          e,
		checksum:        0,
		startTime:       time.Now(),
		valuesCount:     0,
		memTableEntries: make([]txMemTableEntry, 0),
		txnType:         txnType,
		valueType:       valueType,
	}, nil
}

// AppendTxnEntry append an entry to the WAL as part of a Txn.
func (t *Txn) AppendTxnEntry(key []byte, value []byte) error {
	if t.err != nil {
		return t.err
	}

	if t.txnType == wrecord.LogOperationInsert && t.chunkedKey == nil {
		t.chunkedKey = key
	}

	if t.txnType == wrecord.LogOperationInsert && !bytes.Equal(t.chunkedKey, key) {
		t.err = errors.New("chunked txn type cannot change key from first value")
		return t.err
	}

	t.engine.mu.Lock()
	defer t.engine.mu.Unlock()
	index := t.engine.globalCounter.Add(1)
	record := &walRecord{
		index:        index,
		hlc:          HLCNow(index),
		key:          key,
		value:        value,
		op:           t.txnType,
		txnID:        t.txnID,
		txnStatus:    wrecord.TxnStatusPrepare,
		valueType:    t.valueType,
		prevTxnChunk: t.lastPos,
	}
	// Encode and compress WAL record
	encoded, err := record.fbEncode()

	if err != nil {
		t.err = err
		return err
	}

	// Write to WAL
	chunkPos, err := t.engine.walIO.Write(encoded)

	if err != nil {
		t.err = err
		return err
	}

	t.lastPos = chunkPos
	// Update the rolling checksum
	var memValue []byte
	if int64(len(value)) <= t.engine.storageConfig.ValueThreshold {
		memValue = append(directValuePrefix, encoded...) // Store directly
	} else {
		memValue = append(walReferencePrefix, chunkPos.Encode()...) // Store reference
	}

	t.memTableEntries = append(t.memTableEntries, txMemTableEntry{
		key:      key,
		chunkPos: chunkPos,
		value: y.ValueStruct{
			Meta:  byte(t.txnType),
			Value: memValue,
		}})

	t.checksum = crc32.Update(t.checksum, crc32.IEEETable, value)
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
	index := t.engine.globalCounter.Add(1)
	record := &walRecord{
		index:        index,
		hlc:          HLCNow(index),
		key:          t.chunkedKey,
		value:        marshalChecksum(t.checksum),
		op:           wrecord.LogOperationTxnMarker,
		txnID:        t.txnID,
		txnStatus:    wrecord.TxnStatusCommit,
		valueType:    t.valueType,
		prevTxnChunk: t.lastPos,
	}

	// Encode and compress WAL record
	encoded, err := record.fbEncode()

	if err != nil {
		t.err = err
		return err
	}

	// Write to WAL
	chunkPos, err := t.engine.walIO.Write(encoded)

	if err != nil {
		t.err = err
		return err
	}

	metrics.IncrCounterWithLabels([]string{"kvalchemy", "storage", "txn", "commited", "total"}, 1, t.engine.label)
	metrics.MeasureSinceWithLabels([]string{"kvalchemy", "storage", "txn", "duration"}, t.startTime, t.engine.label)

	// flush all the writes on mem-table
	t.lastPos = chunkPos
	if t.valueType != wrecord.ValueTypeChunked {
		for _, memValue := range t.memTableEntries {
			err := t.engine.memTableWrite(memValue.key, memValue.value, memValue.chunkPos)
			if err != nil {
				t.err = err
				return err
			}
		}
	}

	if t.valueType == wrecord.ValueTypeChunked {
		memValue := append(walReferencePrefix, chunkPos.Encode()...)
		err := t.engine.memTableWrite(t.chunkedKey, y.ValueStruct{
			Meta:  byte(t.txnType),
			Value: memValue,
		}, chunkPos)
		if err != nil {
			t.err = err
			return err
		}
	}

	t.err = ErrTxnAlreadyCommitted
	return nil
}

func (t *Txn) TxnID() []byte {
	return t.txnID
}

func (t *Txn) Checksum() uint32 {
	return t.checksum
}

func marshalChecksum(checksum uint32) []byte {
	buf := make([]byte, 4) // uint32 takes 4 bytes
	binary.LittleEndian.PutUint32(buf, checksum)
	return buf
}

func unmarshalChecksum(data []byte) uint32 {
	if len(data) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}
