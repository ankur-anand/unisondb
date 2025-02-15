package storage

import (
	"hash/crc32"
	"os"
	"testing"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
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

func TestWALChunkRead(t *testing.T) {
	// Create temporary WAL directories
	tempDir1, err := os.MkdirTemp("", "wal_test1")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir1)

	// Open two separate WAL instances
	walI, err := wal.Open(wal.Options{DirPath: tempDir1, SegmentFileExt: ".seg", SegmentSize: defaultSegmentSize})
	assert.NoError(t, err)
	defer walI.Close()

	id := []byte(gofakeit.UUID())
	key := []byte(gofakeit.Name())

	// insert a batch start marker.
	// start the batch marker in wal
	record := walRecord{
		hlc:          1,
		key:          key,
		value:        nil,
		op:           wrecord.LogOperationOpBatchStart,
		batchID:      id,
		lastBatchPos: nil,
	}

	encoded, err := record.fbEncode()
	assert.NoError(t, err)

	var chunkPos *wal.ChunkPosition
	chunkPos, err = walI.Write(encoded)
	assert.NoError(t, err)

	var chunks [][]byte
	var checksum uint32

	for i := 2; i < 10; i++ {
		chunk := []byte(gofakeit.Sentence(10))
		chunks = append(chunks, chunk)
		checksum = crc32.Update(checksum, crc32.IEEETable, chunk)
		record := walRecord{
			hlc:          uint64(i),
			key:          key,
			value:        chunk,
			op:           wrecord.LogOperationOPBatchInsert,
			batchID:      id,
			lastBatchPos: chunkPos,
		}
		encoded, err := record.fbEncode()
		assert.NoError(t, err)

		chunkPos, err = walI.Write(encoded)
		assert.NoError(t, err)
	}

	// commit
	record = walRecord{
		hlc:          10,
		key:          key,
		value:        marshalChecksum(checksum),
		op:           wrecord.LogOperationOpBatchCommit,
		batchID:      id,
		lastBatchPos: chunkPos,
	}
	encoded, err = record.fbEncode()
	assert.NoError(t, err)

	_, err = walI.Write(encoded)
	assert.NoError(t, err)

	wIO := &walIO{
		WAL:   walI,
		label: nil,
	}
	gotChunks, err := readChunksFromWal(wIO, chunkPos, id, checksum)
	assert.NoError(t, err)
	assert.Equal(t, len(chunks), len(gotChunks))
	for i, chunk := range gotChunks {
		assert.Equal(t, chunks[i], chunk)
	}
}
