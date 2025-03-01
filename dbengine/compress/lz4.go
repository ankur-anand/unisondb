package compress

import (
	"bytes"
	"sync"

	"github.com/pierrec/lz4/v4"
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
