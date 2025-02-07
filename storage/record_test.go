package storage_test

import (
	"testing"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
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

	BatchID, err := uuid.New().MarshalBinary()
	assert.NoError(t, err)

	encoded, err := storage.FbEncode(123456789, []byte("test_key"), []byte("test_value"), wrecord.LogOperationOpInsert, BatchID)
	assert.NoError(t, err)

	record := wrecord.GetRootAsWalRecord(encoded, 0)
	data, err := storage.DecompressLZ4(record.ValueBytes())
	assert.NoError(t, err)
	// Validate that all fields are correctly restored
	assert.Equal(t, uint64(123456789), record.Index(), "WAL Index mismatch")
	assert.Equal(t, wrecord.LogOperationOpInsert, record.Operation(), "Operation mismatch")
	assert.Equal(t, []byte("test_key"), record.KeyBytes(), "Key mismatch")
	assert.Equal(t, []byte("test_value"), data, "Value mismatch")
	assert.Equal(t, BatchID, record.BatchIdBytes(), "BatchID mismatch")
}

func TestDecodeWalRecordHandlesInvalidData(t *testing.T) {
	invalidData := []byte{1, 2, 3} // Corrupt or incomplete WAL entry

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic on invalid WAL record but function executed normally")
		}
	}()

	// This should panic due to invalid input size
	_ = wrecord.GetRootAsWalRecord(invalidData, 0)
}
