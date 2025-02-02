package storage

import (
	"bytes"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// LZ4WriterPool reuses writers to optimize performance
var LZ4WriterPool = sync.Pool{
	New: func() any {
		return lz4.NewWriter(nil) // Create new LZ4 writer
	},
}

// compressLZ4 compresses data using LZ4
func compressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	// Get writer from pool
	writer := LZ4WriterPool.Get().(*lz4.Writer)
	writer.Reset(&buf) // Reset writer for new use

	// Write data to compress
	_, err := writer.Write(data)
	if err != nil {
		LZ4WriterPool.Put(writer) // Return writer to pool before exiting
		return nil, err
	}

	// Close the writer to flush the buffer
	err = writer.Close()
	if err != nil {
		LZ4WriterPool.Put(writer)
		return nil, err
	}

	// Return writer to pool
	LZ4WriterPool.Put(writer)

	// Return compressed bytes
	return buf.Bytes(), nil
}

func decompressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	reader := lz4.NewReader(bytes.NewReader(data))
	_, err := buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
