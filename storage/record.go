package storage

import (
	"encoding/binary"

	"github.com/google/uuid"
)

// walRecord represents a WAL entry.
type walRecord struct {
	Operation LogOperation // Operation type
	Key       []byte       // Key in WAL
	Value     []byte       // Value in WAL
	BatchID   uuid.UUID    // Batch identifier for atomic commits
}

func encodeWalRecord(record *walRecord) []byte {
	keySize := len(record.Key)
	valueSize := len(record.Value)
	totalSize := 1 + 16 + 4 + keySize + 4 + valueSize

	buf := make([]byte, totalSize)
	index := 0

	buf[index] = byte(record.Operation)
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

// decodeWalRecord deserializes bytes into a `walRecord` struct.
func decodeWalRecord(buf []byte) *walRecord {
	index := 0

	operation := LogOperation(buf[index])
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

	return &walRecord{
		Operation: operation,
		Key:       key,
		Value:     value,
		BatchID:   batchID,
	}
}
