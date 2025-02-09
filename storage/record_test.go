package storage

import (
	"hash/crc32"
	"testing"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataMarshalUnmarshal(t *testing.T) {
	originalMetadata := Metadata{
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

	decodedMetadata := UnmarshalMetadata(encoded)

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
	_ = UnmarshalMetadata(invalidData)
}

func TestEncodeDecodeWalRecord(t *testing.T) {

	BatchID, err := uuid.New().MarshalBinary()
	assert.NoError(t, err)

	encoded, err := FbEncode(123456789, []byte("test_key"), []byte("test_value"), wrecord.LogOperationOpInsert, BatchID)
	assert.NoError(t, err)

	record := wrecord.GetRootAsWalRecord(encoded, 0)
	data, err := DecompressLZ4(record.ValueBytes())
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

// Test encoding of a normal WAL record
func TestWalRecordFbEncode(t *testing.T) {

	t.Run("normal_ops", func(t *testing.T) {
		record := &walRecord{
			index: 1,
			key:   []byte("test-key"),
			value: []byte("test-value"),
			op:    wrecord.LogOperationOpInsert,
		}

		encoded, err := record.fbEncode()
		require.NoError(t, err)
		assert.NotEmpty(t, encoded, "Encoded data should not be empty")
	})

	t.Run("empty_kv", func(t *testing.T) {
		record := &walRecord{
			index:   2,
			key:     []byte{},
			value:   []byte{},
			op:      wrecord.LogOperationOpInsert,
			batchID: []byte{},
		}

		encoded, err := record.fbEncode()
		require.NoError(t, err)
		assert.NotEmpty(t, encoded, "Encoded data should not be empty even when key and value are empty")
	})

	t.Run("validate_encoded", func(t *testing.T) {
		largeValue := gofakeit.LetterN(1024) // Generate a large value

		record := &walRecord{
			index:   3,
			key:     []byte("large-key"),
			value:   []byte(largeValue),
			op:      wrecord.LogOperationOpInsert,
			batchID: []byte("batch-456"),
			lastBatchPos: &wal.ChunkPosition{
				SegmentId:   2,
				ChunkOffset: 100,
			},
		}

		encoded, err := record.fbEncode()
		require.NoError(t, err)
		assert.NotEmpty(t, encoded, "Encoded data should not be empty")

		// Verify compression
		compressed, _ := CompressLZ4([]byte(largeValue))
		calculatedChecksum := crc32.ChecksumIEEE(record.value)
		wr := wrecord.GetRootAsWalRecord(encoded, 0)
		assert.Equal(t, wr.ValueBytes(), compressed, "Value should be compressed")
		assert.Equal(t, wr.BatchIdBytes(), []byte("batch-456"), "BatchId should be batch-456")
		assert.Equal(t, wr.RecordChecksum(), calculatedChecksum, "Checksum should be calculated")
		assert.Equal(t, wr.LastBatchPosBytes(), record.lastBatchPos.Encode(), "last batch post should match")
	})

}
