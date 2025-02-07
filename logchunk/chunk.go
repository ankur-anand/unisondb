package logchunk

import (
	"errors"
	"fmt"
	"hash/crc32"

	v1 "github.com/ankur-anand/kvalchemy/proto/gen/go/kvalchemy/replicator/v1"
)

const (
	chunkSizeMB = 1 * 1024 * 1024 // 1MB chunks
)

// ChunkWALRecord chunks the provided compressed data into 1MB, chunks.
// If the data size is less than 1 MB, it returns the original data without chunking.
// else return the data in the slice of chunks.
func ChunkWALRecord(metadata, data []byte) []*v1.WALRecord {
	dataSize := len(data)
	checksum := ComputeChecksum(data)
	if dataSize <= chunkSizeMB {
		return []*v1.WALRecord{{Metadata: metadata, CompressedData: data, IsChunked: false, Checksum: checksum}}
	}

	// Otherwise, split into multiple 1MB chunks
	var chunks []*v1.WALRecord
	var chunkIndex uint32
	var offset int

	for offset < dataSize {
		end := offset + chunkSizeMB
		if end > dataSize {
			end = dataSize
		}

		chunkType := DetermineChunkType(offset, end, dataSize)

		chunk := &v1.WALRecord{
			Metadata:       metadata,
			CompressedData: data[offset:end],
			ChunkIndex:     chunkIndex,
			IsChunked:      true,
			ChunkType:      chunkType,
			Checksum:       checksum,
		}

		chunks = append(chunks, chunk)
		offset = end
		chunkIndex++
	}

	return chunks
}

func DetermineChunkType(start, end, totalSize int) string {
	if start == 0 {
		return "ChunkTypeFirst"
	} else if end == totalSize {
		return "ChunkTypeLast"
	}
	return "ChunkTypeMiddle"
}

// AssembleChunks reconstructs the original data from chunked WAL records.
func AssembleChunks(chunks []*v1.WALRecord) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, errors.New("no chunks provided")
	}

	// Verify checksum
	expectedChecksum := chunks[0].Checksum
	var data []byte

	for i, chunk := range chunks {
		if chunk.Checksum != expectedChecksum {
			return nil, fmt.Errorf("checksum mismatch at chunk %d", i)
		}
		data = append(data, chunk.CompressedData...)
	}

	// Validate full assembled data against checksum
	if ComputeChecksum(data) != expectedChecksum {
		return nil, errors.New("final checksum mismatch")
	}

	return data, nil
}

// ComputeChecksum calculates CRC32 checksum.
func ComputeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
