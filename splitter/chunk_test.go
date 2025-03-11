package splitter_test

import (
	"bytes"
	"testing"

	"github.com/ankur-anand/unisondb/splitter"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

const chunkSizeMB = 1 * 1024 * 1024 // 1MB

// GenerateTestData generates a large dataset for chunking.
func GenerateTestData(size int) []byte {
	value := []byte(gofakeit.LetterN(20))
	data := bytes.Repeat(value, size/(len(value))+1)
	return data[:size]
}

func TestChunkSplit(t *testing.T) {

	t.Run("Single Chunk - Data Less Than 1MB", func(t *testing.T) {
		data := make([]byte, 512*1024) // 512KB

		chunks := splitter.SplitIntoChunks(data)
		assert.Len(t, chunks, 1, "Expected a single chunk for data < 1MB")

		assert.Equal(t, chunks[0].ChunkType, "ChunkTypeLast", "Expected IsChunked to be false for small data")
		assert.Equal(t, data, chunks[0].Data, "Data should be unchanged")
	})

	t.Run("Multiple Chunks - Data More Than 1MB", func(t *testing.T) {
		data := make([]byte, 3*chunkSizeMB+512*1024) // 3.5MB

		chunks := splitter.SplitIntoChunks(data)
		expectedChunks := 4 // 3 full 1MB chunks + 1 last half chunk

		assert.Len(t, chunks, expectedChunks, "Expected multiple chunks for data > 1MB")

		// Validate chunk metadata
		for i, chunk := range chunks {
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

func TestChunkAndAssemble(t *testing.T) {

	t.Run("Small Data (No Chunking)", func(t *testing.T) {
		data := make([]byte, 512*1024) // 512KB
		chunks := splitter.SplitIntoChunks(data)
		assert.Len(t, chunks, 1, "Expected a single chunk")

		assembled := splitter.AssembleChunks(chunks)
		assert.Equal(t, data, assembled, "Reassembled data should match original")
	})

	t.Run("Large Data (Chunking & Assembly)", func(t *testing.T) {
		data := make([]byte, 3*chunkSizeMB+512*1024) // 3.5MB
		chunks := splitter.SplitIntoChunks(data)
		assert.Len(t, chunks, 4, "Expected 4 chunks")

		assembled := splitter.AssembleChunks(chunks)
		assert.Equal(t, data, assembled, "Reassembled data should match original")
	})
}

func TestChunkingAndAssembling(t *testing.T) {
	// Generate real data (5MB)
	data := GenerateTestData(5 * chunkSizeMB)

	chunks := splitter.SplitIntoChunks(data)
	assert.Greater(t, len(chunks), 1, "Data should be chunked into multiple parts")

	reassembledData := splitter.AssembleChunks(chunks)

	// Verify data integrity
	assert.Equal(t, data, reassembledData, "Reassembled data should match original compressed data")
}
