package storage

import (
	"os"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
)

func TestWALChunkPositionConsistency(t *testing.T) {
	// Create temporary WAL directories
	tempDir1, err := os.MkdirTemp("", "wal_test1")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir1)

	tempDir2, err := os.MkdirTemp("", "wal_test2")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir2)

	// Open two separate WAL instances
	wal1, err := wal.Open(wal.Options{DirPath: tempDir1, SegmentFileExt: ".seg", SegmentSize: defaultSegmentSize})
	assert.NoError(t, err)
	defer wal1.Close()

	wal2, err := wal.Open(wal.Options{DirPath: tempDir2, SegmentFileExt: ".seg", SegmentSize: defaultSegmentSize})
	assert.NoError(t, err)
	defer wal2.Close()

	for i := 0; i < 1000; i++ {
		value := []byte(gofakeit.Sentence((i + 1) * 10))

		// Write to both WAL instances
		chunkPos1, err := wal1.Write(value)
		assert.NoError(t, err, "Failed to write to WAL1")

		chunkPos2, err := wal2.Write(value)
		assert.NoError(t, err, "Failed to write to WAL2")

		// Assert that both WALs return the same chunk position
		assert.Equal(t, chunkPos1, chunkPos2, "Chunk positions should be identical for identical input")
	}

}
