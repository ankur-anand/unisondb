package storage

import (
	"encoding/binary"
	"hash/crc32"
	"time"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/hashicorp/go-metrics"
	"github.com/rosedblabs/wal"
	"github.com/segmentio/ksuid"
)

// Batch can be used to save a batch of key and values.
// Batch must be commited for write to be visible.
// UnCommited Batch will never be visible to reader until commited.
type Batch struct {
	key         []byte
	batchID     []byte // a private Batch ID
	lastPos     *wal.ChunkPosition
	err         error
	commited    bool
	engine      *Engine
	checksum    uint32 // Rolling checksum
	startTime   time.Time
	valuesCount int
}

// NewBatch return an initialized batch, with a start marker in wal.
func (e *Engine) NewBatch(key []byte) (*Batch, error) {
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
		key:          key,
		value:        nil,
		op:           wrecord.LogOperationOpBatchStart,
		batchID:      uuid,
		lastBatchPos: nil,
	}

	encoded, err := record.fbEncode()
	if err != nil {
		return nil, err
	}

	chunkPos, err := e.walIO.Write(encoded)
	if err != nil {
		return nil, err
	}

	return &Batch{
		key:       key,
		batchID:   uuid,
		lastPos:   chunkPos,
		err:       err,
		engine:    e,
		startTime: time.Now(),
	}, nil
}

func (b *Batch) Key() []byte {
	return b.key
}

func (b *Batch) BatchID() []byte {
	return b.batchID
}

func (b *Batch) LastPos() wal.ChunkPosition {
	return *b.lastPos
}

// Put the value for the key.
func (b *Batch) Put(value []byte) error {
	b.engine.mu.Lock()
	defer b.engine.mu.Unlock()

	if b.err != nil {
		return b.err
	}
	index := b.engine.globalCounter.Add(1)
	record := &walRecord{
		index:        index,
		hlc:          HLCNow(index),
		key:          b.key,
		value:        value,
		op:           wrecord.LogOperationOPBatchInsert,
		batchID:      b.batchID,
		lastBatchPos: b.lastPos,
	}

	// Encode and compress WAL record
	encoded, err := record.fbEncode()

	if err != nil {
		b.err = err
		return err
	}

	// Write to WAL
	chunkPos, err := b.engine.walIO.Write(encoded)

	if err != nil {
		b.err = err
		return err
	}

	b.lastPos = chunkPos
	// Update the rolling checksum

	b.checksum = crc32.Update(b.checksum, crc32.IEEETable, value)
	b.valuesCount++
	return nil
}

// Commit the given Batch to wIO.
func (b *Batch) Commit() error {
	if b.err != nil {
		return b.err
	}

	metrics.IncrCounterWithLabels([]string{"kvalchemy", "chunk", "commited", "kv", "total"}, float32(b.valuesCount), b.engine.label)
	metrics.MeasureSinceWithLabels([]string{"kvalchemy", "chunk", "init", "to", "commit", "duration", "msec"}, b.startTime, b.engine.label)
	return b.engine.persistKeyValue(b.key, marshalChecksum(b.checksum), wrecord.LogOperationOpBatchCommit, b.batchID, b.lastPos)
}

// Checksum returns the current checksum as present in the batch.
func (b *Batch) Checksum() uint32 {
	return b.checksum
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
