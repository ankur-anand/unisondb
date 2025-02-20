package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rosedblabs/wal"
)

// Metadata represents a checkpoint in the Write-Ahead Log (WAL).
// It encodes the last known chunk position (`Pos`) within the segment file. This is primarily used for
// recovery and replication tracking.
type Metadata struct {
	RecordProcessed uint64             // (monotonic)
	Pos             *wal.ChunkPosition // Position of the last written chunk in WAL
}

// MarshalBinary encodes a Metadata struct to a byte slice.
func (m *Metadata) MarshalBinary() []byte {
	// Encode the chunk position
	encodedPos := m.Pos.Encode()
	result := make([]byte, len(encodedPos)+8)
	binary.LittleEndian.PutUint64(result, m.RecordProcessed)

	copy(result[8:], encodedPos)

	return result
}

// UnmarshalMetadata decodes a Metadata struct from a byte slice.
func UnmarshalMetadata(data []byte) Metadata {
	index := binary.LittleEndian.Uint64(data[:8])

	// Decode ChunkPosition from the remaining bytes
	pos := wal.DecodeChunkPosition(data[8:])

	return Metadata{
		RecordProcessed: index,
		Pos:             pos,
	}
}

// walRecord.
type walRecord struct {
	index uint64
	hlc   uint64
	key   []byte
	value []byte
	op    wrecord.LogOperation

	batchID      []byte
	lastBatchPos *wal.ChunkPosition
}

func (w *walRecord) fbEncode() ([]byte, error) {
	initSize := 1 + len(w.key) + len(w.value)
	builder := flatbuffers.NewBuilder(initSize)

	keyOffset := builder.CreateByteVector(w.key)

	// Ensure valueOffset is always valid
	var valueOffset flatbuffers.UOffsetT

	if len(w.value) > 0 {
		compressValue, err := CompressLZ4(w.value)
		if err != nil {
			return nil, fmt.Errorf("compress lz4: %w", err)
		}
		valueOffset = builder.CreateByteVector(compressValue)
	} else {
		valueOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	// Ensure batchOffset is always valid
	var batchOffset flatbuffers.UOffsetT
	var lastBatchPosOffset flatbuffers.UOffsetT

	if w.lastBatchPos != nil {
		lastBatchPosOffset = builder.CreateByteVector(w.lastBatchPos.Encode())
	}

	if len(w.batchID) > 0 {
		batchOffset = builder.CreateByteVector(w.batchID)
	} else {
		batchOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	checksum := crc32.ChecksumIEEE(w.value)

	// Start building the WAL Record
	wrecord.WalRecordStart(builder)
	wrecord.WalRecordAddIndex(builder, w.index)
	wrecord.WalRecordAddHlc(builder, w.hlc)
	wrecord.WalRecordAddOperation(builder, w.op)
	wrecord.WalRecordAddKey(builder, keyOffset)
	wrecord.WalRecordAddValue(builder, valueOffset)
	wrecord.WalRecordAddBatchId(builder, batchOffset)
	wrecord.WalRecordAddRecordChecksum(builder, checksum)
	wrecord.WalRecordAddLastBatchPos(builder, lastBatchPosOffset)

	walRecordOffset := wrecord.WalRecordEnd(builder)

	// Finish FlatBuffer
	builder.Finish(walRecordOffset)

	return builder.FinishedBytes(), nil
}
