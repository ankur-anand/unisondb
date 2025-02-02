package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

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
	buf := new(bytes.Buffer)

	// Write Operation Type (1 byte)
	_ = buf.WriteByte(byte(record.Operation))

	// Write BatchID only for batch-related operations
	if record.BatchID != uuid.Nil {
		batchIDBytes, _ := record.BatchID.MarshalBinary()
		buf.Write(batchIDBytes)
	}

	// Encode and write Key length & Key
	keyLen := uint64(len(record.Key))
	keyLenBuf := make([]byte, binary.MaxVarintLen64)
	keyLenSize := binary.PutUvarint(keyLenBuf, keyLen)
	buf.Write(keyLenBuf[:keyLenSize])
	buf.Write(record.Key)

	// Encode and write Value length & Value (only if not batch marker)
	if record.Operation == OpInsert {
		valueLen := uint64(len(record.Value))
		valueLenBuf := make([]byte, binary.MaxVarintLen64)
		valueLenSize := binary.PutUvarint(valueLenBuf, valueLen)
		buf.Write(valueLenBuf[:valueLenSize])
		buf.Write(record.Value)
	}

	return buf.Bytes()
}

// decodeWalRecord deserializes bytes into a `walRecord` struct.
func decodeWalRecord(data []byte) (*walRecord, error) {
	buf := bytes.NewReader(data)
	record := &walRecord{}

	// Read Operation Type (1 byte)
	opType, err := buf.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read operation type: %w", err)
	}
	record.Operation = opType

	// Read BatchID if operation is batch-related
	if record.Operation == OpBatchStart || record.Operation == OpBatchCommit {
		var batchIDBytes [16]byte
		if _, err := buf.Read(batchIDBytes[:]); err != nil {
			return nil, fmt.Errorf("failed to read BatchID: %w", err)
		}
		if err := record.BatchID.UnmarshalBinary(batchIDBytes[:]); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BatchID: %w", err)
		}
	}

	// Decode Key
	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key length: %w", err)
	}

	record.Key = make([]byte, keyLen)
	if _, err := buf.Read(record.Key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	// Decode Value for insert ops.
	if record.Operation == OpInsert {
		valueLen, err := binary.ReadUvarint(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value length: %w", err)
		}

		record.Value = make([]byte, valueLen)
		if _, err := buf.Read(record.Value); err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}
	}

	return record, nil
}
