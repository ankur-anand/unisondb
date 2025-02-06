package logchunk_test

import (
	"bytes"
	"testing"

	"github.com/ankur-anand/kvalchemy/logchunk"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

const chunkSizeMB = 1 * 1024 * 1024 // 1MB

// GenerateTestData generates a large dataset for WAL chunking.
func GenerateTestData(size int) []byte {
	value := []byte(gofakeit.LetterN(20))
	data := bytes.Repeat(value, size/(len(value))+1)
	return data[:size]
}

func TestChunkWALRecord(t *testing.T) {
	metadata := []byte("test-metadata")

	t.Run("Single Chunk - Data Less Than 1MB", func(t *testing.T) {
		data := make([]byte, 512*1024) // 512KB

		chunks := logchunk.ChunkWALRecord(metadata, data)
		assert.Len(t, chunks, 1, "Expected a single chunk for data < 1MB")

		assert.False(t, chunks[0].IsChunked, "Expected IsChunked to be false for small data")
		assert.Equal(t, metadata, chunks[0].Metadata, "Metadata should be unchanged")
		assert.Equal(t, data, chunks[0].CompressedData, "Data should be unchanged")
	})

	t.Run("Multiple Chunks - Data More Than 1MB", func(t *testing.T) {
		data := make([]byte, 3*chunkSizeMB+512*1024) // 3.5MB

		chunks := logchunk.ChunkWALRecord(metadata, data)
		expectedChunks := 4 // 3 full 1MB chunks + 1 last half chunk

		assert.Len(t, chunks, expectedChunks, "Expected multiple chunks for data > 1MB")

		// Validate chunk metadata
		for i, chunk := range chunks {
			assert.True(t, chunk.IsChunked, "Expected chunking for large data")
			assert.Equal(t, metadata, chunk.Metadata, "Metadata should be unchanged")

			if i == 0 {
				assert.Equal(t, "ChunkTypeFirst", chunk.ChunkType, "First chunk type should be correct")
			} else if i == expectedChunks-1 {
				assert.Equal(t, "ChunkTypeLast", chunk.ChunkType, "Last chunk type should be correct")
			} else {
				assert.Equal(t, "ChunkTypeMiddle", chunk.ChunkType, "Middle chunk type should be correct")
			}
		}
	})
}

func TestChunkAndAssembleWALRecord(t *testing.T) {
	metadata := []byte("test-metadata")

	t.Run("Small Data (No Chunking)", func(t *testing.T) {
		data := make([]byte, 512*1024) // 512KB
		chunks := logchunk.ChunkWALRecord(metadata, data)
		assert.Len(t, chunks, 1, "Expected a single chunk")

		assembled, err := logchunk.AssembleChunks(chunks)
		assert.NoError(t, err, "Assembly should not fail")
		assert.Equal(t, data, assembled, "Reassembled data should match original")
	})

	t.Run("Large Data (Chunking & Assembly)", func(t *testing.T) {
		data := make([]byte, 3*chunkSizeMB+512*1024) // 3.5MB
		chunks := logchunk.ChunkWALRecord(metadata, data)
		assert.Len(t, chunks, 4, "Expected 4 chunks")

		assembled, err := logchunk.AssembleChunks(chunks)
		assert.NoError(t, err, "Assembly should not fail")
		assert.Equal(t, data, assembled, "Reassembled data should match original")
	})

	t.Run("Checksum Mismatch", func(t *testing.T) {
		data := make([]byte, 2*chunkSizeMB) // 2MB
		chunks := logchunk.ChunkWALRecord(metadata, data)

		// Modify a chunk to create an invalid checksum
		chunks[1].CompressedData[0] ^= 0xFF

		_, err := logchunk.AssembleChunks(chunks)
		assert.Error(t, err, "Expected checksum mismatch error")
	})
}

// TestChunkingAndAssembling tests chunking and assembling of WAL records.
func TestChunkingAndAssembling(t *testing.T) {
	// Generate real data (5MB)
	data := GenerateTestData(5 * chunkSizeMB)

	chunks := logchunk.ChunkWALRecord([]byte("metadata"), data)
	assert.Greater(t, len(chunks), 1, "Data should be chunked into multiple parts")

	// Assemble the chunks back
	reassembledData, err := logchunk.AssembleChunks(chunks)
	assert.NoError(t, err, "Assembling should not fail")

	// Verify data integrity
	assert.Equal(t, data, reassembledData, "Reassembled data should match original compressed data")
}
