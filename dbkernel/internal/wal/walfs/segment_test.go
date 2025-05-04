package walfs

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkPositionEncodeDecode(t *testing.T) {
	tests := []ChunkPosition{
		{SegmentID: 1, Offset: 0},
		{SegmentID: 42, Offset: 1024},
		{SegmentID: 9999, Offset: 1 << 32},
		{SegmentID: 0, Offset: -1},
	}

	for _, original := range tests {
		encoded := original.Encode()

		decoded, err := DecodeChunkPosition(encoded)
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	}
}

func TestDecodeChunkPosition_InvalidData(t *testing.T) {
	shortData := []byte{1, 2, 3, 4, 5}
	_, err := DecodeChunkPosition(shortData)
	assert.Error(t, err)
}

func TestSegment_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name string
		data []byte
	}{
		{"small data", []byte("hello world")},
		{"medium data", bytes.Repeat([]byte("data"), 1000)},
		{"large data", bytes.Repeat([]byte("large"), 5000)},
	}

	// Step 1: Open segment and write all entries
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	var positions []*ChunkPosition
	for _, tt := range tests {
		pos, err := seg.Write(tt.data)
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	assert.NoError(t, seg.Close())

	// Step 2: Reopen and verify all entries
	seg2, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, seg2.Close())
	}()

	for i, pos := range positions {
		readData, next, err := seg2.Read(pos.Offset)
		assert.NoError(t, err)
		assert.Equal(t, tests[i].data, readData, "mismatch at index %d", i)

		expectedNextOffset := pos.Offset + int64(len(tests[i].data)+chunkHeaderSize) + int64(chunkTrailerSize)
		assert.Equal(t, expectedNextOffset, next.Offset, "wrong next offset at index %d", i)
	}
}

func TestSegment_SequentialWrites(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	var positions []*ChunkPosition
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		pos, err := seg.Write(data)
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	reader := seg.NewReader()
	for i := 0; i < 10; i++ {
		data, next, err := reader.Next()
		assert.NoError(t, err)

		expected := []byte(fmt.Sprintf("entry-%d", i))
		assert.Equal(t, expected, data, "read data doesn't match written data")

		if i < 9 && next.Offset != positions[i+1].Offset {
			t.Errorf("entry %d: wrong next offset", i)
		}
	}

	_, _, err = reader.Next()
	assert.ErrorIs(t, err, io.EOF)
}

func TestSegment_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	testData := make([]*ChunkPosition, 100)
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		pos, err := seg.Write(data)
		assert.NoError(t, err)
		testData[i] = pos
	}

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, pos := range testData {
				data, _, err := seg.Read(pos.Offset)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.HasPrefix(data, []byte("test-")) {
					errs <- fmt.Errorf("unexpected data: %s", string(data))
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}
}

func TestSegment_InvalidCRC(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	data := []byte("corrupt me")
	pos, err := seg.Write(data)
	assert.NoError(t, err)

	seg.mmapData[pos.Offset] ^= 0xFF
	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrInvalidCRC)
}

func TestSegment_CloseAndReopen(t *testing.T) {
	tmpDir := t.TempDir()

	seg1, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	testData := []byte("persist this")
	pos, err := seg1.Write(testData)
	assert.NoError(t, err)

	assert.NoError(t, seg1.Close())

	seg2, err := openSegmentFile(tmpDir, ".wal", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})

	readData, _, err := seg2.Read(pos.Offset)
	assert.NoError(t, err)
	assert.Equal(t, testData, readData, "read data doesn't match written data")
}

func TestLargeWriteBoundary(t *testing.T) {
	dir := t.TempDir()
	seg, err := openSegmentFile(dir, ".wal", 1)
	assert.NoError(t, err)

	seg.writeOffset.Store(segmentSize - 64*1024 + 1)

	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = seg.Write(data)
	assert.Error(t, err)
}

func TestSegment_WriteRead_1KBTo1MB(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	sizes := []int{
		1 << 10, // 1KB
		2 << 10,
		4 << 10,
		8 << 10,
		16 << 10,
		32 << 10,
		64 << 10,
		128 << 10,
		256 << 10,
		512 << 10,
		1 << 20, // 1MB
	}

	var positions []*ChunkPosition
	var original [][]byte

	for i, sz := range sizes {
		data := make([]byte, sz)
		for j := range data {
			data[j] = byte(i + j)
		}
		pos, err := seg.Write(data)
		assert.NoError(t, err)
		positions = append(positions, pos)
		original = append(original, data)
	}

	for i, pos := range positions {
		readData, next, err := seg.Read(pos.Offset)
		assert.NoError(t, err, "failed at size index %d, size=%d", i, len(original[i]))
		expectedNextOffset := pos.Offset + int64(chunkHeaderSize+len(original[i])) + int64(chunkTrailerSize)
		assert.Equal(t, expectedNextOffset, next.Offset, "unexpected next offset at index %d", i)
		assert.Equal(t, original[i], readData, fmt.Sprintf("data mismatch at index %d", i))
	}
}

func TestSegment_CorruptHeader(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	data := []byte("hello")
	pos, err := seg.Write(data)
	assert.NoError(t, err)

	copy(seg.mmapData[pos.Offset+4:pos.Offset+8], []byte{0xFF, 0xFF, 0xFF, 0xFF})

	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrCorruptHeader)
}

func TestSegment_CorruptData(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	data := []byte("hello")
	pos, err := seg.Write(data)
	assert.NoError(t, err)
	copy(seg.mmapData[pos.Offset+chunkHeaderSize:], []byte{})
	seg.writeOffset.Store(pos.Offset + chunkHeaderSize - 1)
	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, io.EOF)
}

func TestSegment_ReadAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	data := []byte("data")
	pos, err := seg.Write(data)
	assert.NoError(t, err)

	assert.NoError(t, seg.Close())

	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_WriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	assert.NoError(t, seg.Close())

	_, err = seg.Write([]byte("should fail"))
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_TruncatedRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	seg1, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	data := []byte("valid")
	_, err = seg1.Write(data)
	assert.NoError(t, err)

	offset := seg1.Size()
	copy(seg1.mmapData[offset:], "garbage")
	assert.NoError(t, seg1.Sync())
	assert.NoError(t, seg1.Close())

	seg2, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})

	assert.Equal(t, offset, seg2.Size(), "recovered write offset should skip invalid data")
}

func TestSegment_WriteAtExactBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	size := segmentSize - chunkHeaderSize - chunkTrailerSize
	data := make([]byte, size)
	for i := range data {
		data[i] = 'A'
	}

	_, err = seg.Write(data)
	assert.NoError(t, err)

	assert.Equal(t, int64(segmentSize), seg.Size())

	_, err = seg.Write([]byte("extra"))
	assert.Error(t, err)
}

func TestSegment_ConcurrentReadWhileWriting(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			data := []byte(fmt.Sprintf("msg-%d", i))
			_, err := seg.Write(data)
			assert.NoError(t, err)
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		reader := seg.NewReader()
		for {
			_, _, err := reader.Next()
			if err == io.EOF {
				break
			}
		}
	}()

	close(start)
	wg.Wait()
}

func TestSegment_SyncOnWriteOption(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1, WithSyncOption(MsyncOnWrite))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	_, err = seg.Write([]byte("sync test"))
	assert.NoError(t, err)
}

func TestSegment_EmptyReaderEOF(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	reader := seg.NewReader()
	_, _, err = reader.Next()
	assert.ErrorIs(t, err, io.EOF)
}

func TestSegment_MSync(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	err = seg.MSync()
	assert.NoError(t, err)
}

func TestSegment_Sync_ErrorPaths(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	seg.closed.Store(true)
	err = seg.Sync()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_Close_Twice(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	assert.NoError(t, seg.Close())
	assert.NoError(t, seg.Close())
}

func TestOpenSegmentFile_BadChecksum(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	_, err = seg.Write([]byte("valid"))
	assert.NoError(t, err)

	offset := seg.Size()

	copy(seg.mmapData[offset:], []byte{
		0x00, 0x00, 0x00, 0x00, // checksum
		0x05, 0x00, 0x00, 0x00, // length = 5
		ChunkTypeFull,
	})
	copy(seg.mmapData[offset+chunkHeaderSize:], "corru")

	assert.NoError(t, seg.Sync())
	assert.NoError(t, seg.Close())

	seg2, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})

	assert.Equal(t, offset, seg2.Size())
}

func TestChunkPosition_String(t *testing.T) {
	cp := ChunkPosition{SegmentID: 123, Offset: 456}
	s := cp.String()
	assert.Contains(t, s, "SegmentID=123")
	assert.Contains(t, s, "Offset=456")
}

func TestSegment_MSync_Closed(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	seg.closed.Store(true)

	err = seg.MSync()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_WillExceed(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	defer seg.Close()

	assert.False(t, seg.WillExceed(1024))

	seg.writeOffset.Store(seg.mmapSize - chunkHeaderSize - 1)
	assert.True(t, seg.WillExceed(2)) // would exceed by 1 byte
}

func TestSegment_TrailerValidation(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	data := []byte("trailer test")
	pos, err := seg.Write(data)
	assert.NoError(t, err)

	trailerOffset := pos.Offset + int64(chunkHeaderSize+len(data))
	copy(seg.mmapData[trailerOffset:], []byte{0x00, 0x00, 0x00, 0x00})

	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrIncompleteChunk)
}
