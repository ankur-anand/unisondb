package storage

import (
	"encoding/binary"

	"github.com/google/uuid"
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

// WalRecord represents a WAL entry.
type WalRecord struct {
	Index     uint64       // index of the
	Operation LogOperation // Operation type
	Key       []byte       // Key in WAL
	Value     []byte       // Value in WAL
	BatchID   uuid.UUID    // Batch identifier for atomic commits
}

// EncodeWalRecord  serializes  record into bytes.
func EncodeWalRecord(record *WalRecord) []byte {
	keySize := len(record.Key)
	valueSize := len(record.Value)
	totalSize := 8 + 1 + 16 + 4 + keySize + 4 + valueSize

	buf := make([]byte, totalSize)
	index := 0

	binary.LittleEndian.PutUint64(buf[index:index+8], record.Index)
	index += 8

	buf[index] = record.Operation
	index++

	batchIDBytes, _ := record.BatchID.MarshalBinary()
	copy(buf[index:index+16], batchIDBytes)
	index += 16

	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(keySize))
	index += 4
	copy(buf[index:index+keySize], record.Key)
	index += keySize

	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(valueSize))
	index += 4
	copy(buf[index:], record.Value)

	return buf
}

// DecodeWalRecord deserializes bytes into a `WalRecord` struct.
func DecodeWalRecord(buf []byte) *WalRecord {
	index := 0
	logIndex := binary.LittleEndian.Uint64(buf[index : index+8])
	index += 8

	operation := buf[index]
	index++

	var batchID uuid.UUID
	_ = batchID.UnmarshalBinary(buf[index : index+16])
	index += 16

	keySize := binary.LittleEndian.Uint32(buf[index : index+4])
	index += 4

	key := make([]byte, keySize)
	copy(key, buf[index:index+int(keySize)])
	index += int(keySize)

	valueSize := binary.LittleEndian.Uint32(buf[index : index+4])
	index += 4

	value := make([]byte, valueSize)
	copy(value, buf[index:])

	return &WalRecord{
		Index:     logIndex,
		Operation: operation,
		Key:       key,
		Value:     value,
		BatchID:   batchID,
	}
}
