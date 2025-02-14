package storage

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/rosedblabs/wal"
)

var (
	sysBucketMetaData   = "storage-metadata"
	sysKeyWalCheckPoint = []byte("wal-checkpoint")
	sysKeyBloomFilter   = []byte("bloom-filter")
)

// LZ4WriterPool reuses writers to optimize performance.
var LZ4WriterPool = sync.Pool{
	New: func() any {
		return lz4.NewWriter(nil) // Create new LZ4 writer
	},
}

// CompressLZ4 compresses data using LZ4.
func CompressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	writer := LZ4WriterPool.Get().(*lz4.Writer)
	writer.Reset(&buf) // Reset writer for new use

	_, err := writer.Write(data)
	if err != nil {
		LZ4WriterPool.Put(writer)
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		LZ4WriterPool.Put(writer)
		return nil, err
	}

	LZ4WriterPool.Put(writer)

	return buf.Bytes(), nil
}

func DecompressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	reader := lz4.NewReader(bytes.NewReader(data))
	_, err := buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeChunkPositionWithValue decodes a MemTable entry into either a ChunkPosition (WAL lookup) or a direct value.
func decodeChunkPositionWithValue(data []byte) (*wal.ChunkPosition, []byte, error) {
	if len(data) == 0 {
		return nil, nil, ErrKeyNotFound
	}

	flag := data[0] // First byte determines type

	switch flag {
	case 1:
		// Direct value stored
		return nil, data[1:], nil
	case 0:
		// Stored ChunkPosition (WAL lookup required)
		chunkPos := wal.DecodeChunkPosition(data[1:])

		return chunkPos, nil, nil
	default:
		return nil, nil, fmt.Errorf("invalid MemTable entry flag: %d", flag)
	}
}

// LoadMetadata retrieves the WAL checkpoint from BoltDB.
//
//nolint:unused
func LoadMetadata(db BTreeStore) (Metadata, error) {
	var metadata Metadata

	data, err := db.RetrieveMetadata(sysKeyWalCheckPoint)
	if err != nil {
		return metadata, err
	}
	metadata = UnmarshalMetadata(data)
	return metadata, err
}

// SaveMetadata saves the WAL checkpoint to BoltDB.
//
//nolint:unused
func SaveMetadata(db BTreeStore, pos *wal.ChunkPosition, index uint64) error {
	metaData := Metadata{
		RecordProcessed: index,
		Pos:             pos,
	}
	value := metaData.MarshalBinary()

	return db.StoreMetadata(sysKeyWalCheckPoint, value)
}

func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
