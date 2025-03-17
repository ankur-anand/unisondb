package splitter

import (
	"hash/crc32"
)

const (
	chunkSizeMB = 1 * 1024 * 1024 // 1MB chunks
)

type Chunk struct {
	Data      []byte
	ChunkType string
}

// SplitIntoChunks chunks the provided data into 1MB, chunks.
// If the data size is less than 1 MB, it returns the original data without chunking.
// else return the data in the slice of chunks.
func SplitIntoChunks(data []byte) []Chunk {
	dataSize := len(data)

	if dataSize <= chunkSizeMB {
		return []Chunk{{Data: data, ChunkType: DetermineChunkType(dataSize, dataSize, dataSize)}}
	}

	// Otherwise, split into multiple 1MB chunks
	var chunks []Chunk
	var chunkIndex uint32
	var offset int

	for offset < dataSize {
		end := offset + chunkSizeMB
		if end > dataSize {
			end = dataSize
		}

		chunkType := DetermineChunkType(offset, end, dataSize)

		chunk := Chunk{Data: data[offset:end],
			ChunkType: chunkType}

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

// AssembleChunks reconstructs the original data from chunked records.
func AssembleChunks(chunks []Chunk) []byte {
	if len(chunks) == 0 {
		return nil
	}

	data := make([]byte, 0, chunkSizeMB*len(chunks))

	for _, chunk := range chunks {
		data = append(data, chunk.Data...)
	}

	return data
}

// ComputeChecksum calculates CRC32 checksum.
func ComputeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
