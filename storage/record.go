package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rosedblabs/wal"
)

// Metadata represents a checkpoint in the Write-Ahead Log (WAL).
// It encodes the last known WAL sequence index (`Index`) and the corresponding
// chunk position (`Pos`) within the segment file. This is primarily used for
// recovery and replication tracking.
type Metadata struct {
	Index uint64             // (monotonic, for ordering)
	Pos   *wal.ChunkPosition // Position of the last written chunk in WAL
}

// MarshalBinary encodes a Metadata struct to a byte slice.
func (m *Metadata) MarshalBinary() []byte {
	// Encode the chunk position
	encodedPos := m.Pos.Encode()
	result := make([]byte, len(encodedPos)+8)
	binary.LittleEndian.PutUint64(result, m.Index)

	copy(result[8:], encodedPos)

	return result
}

// UnmarshalMetadata decodes a Metadata struct from a byte slice.
func UnmarshalMetadata(data []byte) Metadata {
	index := binary.LittleEndian.Uint64(data[:8])

	// Decode ChunkPosition from the remaining bytes
	pos := wal.DecodeChunkPosition(data[8:])

	return Metadata{
		Index: index,
		Pos:   pos,
	}
}

func FbEncode(index uint64, key []byte, value []byte, op wrecord.LogOperation, batchID []byte) ([]byte, error) {
	initSize := 1 + len(key) + len(value)
	builder := flatbuffers.NewBuilder(initSize)

	keyOffset := builder.CreateByteVector(key)

	// Ensure valueOffset is always valid
	var valueOffset flatbuffers.UOffsetT
	if len(value) > 0 {
		compressValue, err := CompressLZ4(value)
		if err != nil {
			return nil, fmt.Errorf("compress lz4: %w", err)
		}
		valueOffset = builder.CreateByteVector(compressValue)
	} else {
		valueOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	// Ensure batchOffset is always valid
	var batchOffset flatbuffers.UOffsetT
	if len(batchID) > 0 {
		batchOffset = builder.CreateByteVector(batchID)
	} else {
		batchOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	// Start building the WAL Record
	wrecord.WalRecordStart(builder)
	wrecord.WalRecordAddIndex(builder, index)
	wrecord.WalRecordAddOperation(builder, op)
	wrecord.WalRecordAddKey(builder, keyOffset)
	wrecord.WalRecordAddValue(builder, valueOffset)
	wrecord.WalRecordAddBatchId(builder, batchOffset)
	walRecordOffset := wrecord.WalRecordEnd(builder)

	// Finish FlatBuffer
	builder.Finish(walRecordOffset)

	return builder.FinishedBytes(), nil
}
