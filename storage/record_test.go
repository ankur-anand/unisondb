package storage_test

import (
	"testing"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/google/uuid"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
)

func TestMetadataMarshalUnmarshal(t *testing.T) {
	originalMetadata := storage.Metadata{
		Index: 123456789,
		Pos: &wal.ChunkPosition{
			SegmentId:   5,
			BlockNumber: 10,
			ChunkOffset: 2048,
			ChunkSize:   512,
		},
	}

	encoded := originalMetadata.MarshalBinary()

	expectedSize := 8 + len(originalMetadata.Pos.Encode())
	assert.Equal(t, expectedSize, len(encoded), "Encoded metadata size mismatch")

	decodedMetadata := storage.UnmarshalMetadata(encoded)

	assert.Equal(t, originalMetadata.Index, decodedMetadata.Index, "WAL Index mismatch after unmarshaling")

	assert.Equal(t, originalMetadata.Pos.SegmentId, decodedMetadata.Pos.SegmentId, "SegmentId mismatch")
	assert.Equal(t, originalMetadata.Pos.BlockNumber, decodedMetadata.Pos.BlockNumber, "BlockNumber mismatch")
	assert.Equal(t, originalMetadata.Pos.ChunkOffset, decodedMetadata.Pos.ChunkOffset, "ChunkOffset mismatch")
	assert.Equal(t, originalMetadata.Pos.ChunkSize, decodedMetadata.Pos.ChunkSize, "ChunkSize mismatch")
}

func TestUnmarshalMetadataHandlesInvalidData(t *testing.T) {
	invalidData := []byte{1, 2, 3} // Corrupt or incomplete metadata

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic on invalid metadata but function executed normally")
		}
	}()

	// This should panic due to invalid input size
	_ = storage.UnmarshalMetadata(invalidData)
}

func TestEncodeDecodeWalRecord(t *testing.T) {

	originalRecord := storage.WalRecord{
		Index:     123456789,
		Operation: 1, // Example operation
		Key:       []byte("test_key"),
		Value:     []byte("test_value"),
		BatchID:   uuid.New(),
	}

	encoded := storage.EncodeWalRecord(&originalRecord)

	expectedSize := 8 + 1 + 16 + 4 + len(originalRecord.Key) + 4 + len(originalRecord.Value)
	assert.Equal(t, expectedSize, len(encoded), "Encoded WAL record size mismatch")

	decodedRecord := storage.DecodeWalRecord(encoded)

	// Validate that all fields are correctly restored
	assert.Equal(t, originalRecord.Index, decodedRecord.Index, "WAL Index mismatch")
	assert.Equal(t, originalRecord.Operation, decodedRecord.Operation, "Operation mismatch")
	assert.Equal(t, originalRecord.Key, decodedRecord.Key, "Key mismatch")
	assert.Equal(t, originalRecord.Value, decodedRecord.Value, "Value mismatch")
	assert.Equal(t, originalRecord.BatchID, decodedRecord.BatchID, "BatchID mismatch")
}

func TestDecodeWalRecordHandlesInvalidData(t *testing.T) {
	invalidData := []byte{1, 2, 3} // Corrupt or incomplete WAL entry

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic on invalid WAL record but function executed normally")
		}
	}()

	// This should panic due to invalid input size
	_ = storage.DecodeWalRecord(invalidData)
}
