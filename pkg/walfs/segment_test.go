package walfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestChunkPositionEncodeDecode(t *testing.T) {
	tests := []RecordPosition{
		{SegmentID: 1, Offset: 0},
		{SegmentID: 42, Offset: 1024},
		{SegmentID: 9999, Offset: 1 << 32},
		{SegmentID: 0, Offset: -1},
	}

	for _, original := range tests {
		encoded := original.Encode()

		decoded, err := DecodeRecordPosition(encoded)
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	}
}

func TestDecodeChunkPosition_InvalidData(t *testing.T) {
	shortData := []byte{1, 2, 3, 4, 5}
	_, err := DecodeRecordPosition(shortData)
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

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	var positions []*RecordPosition
	for _, tt := range tests {
		pos, err := seg.Write(tt.data)
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	assert.NoError(t, seg.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, seg2.Close())
	}()

	for i, pos := range positions {
		readData, next, err := seg2.Read(pos.Offset)
		assert.NoError(t, err)
		assert.Equal(t, tests[i].data, readData, "mismatch at index %d", i)

		expectedNextOffset := pos.Offset + calculateAlignedFrameSize(len(tests[i].data))
		assert.Equal(t, expectedNextOffset, next.Offset, "wrong next offset at index %d", i)
	}
}

func TestSegment_SequentialWrites(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	var positions []*RecordPosition
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		pos, err := seg.Write(data)
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	reader := seg.NewReader()
	defer reader.Close()
	for i := 0; i < 10; i++ {
		data, current, err := reader.Next()
		assert.NoError(t, err)

		expected := []byte(fmt.Sprintf("entry-%d", i))
		assert.Equal(t, expected, data, "read data doesn't match written data")

		assert.Equal(t, positions[i].Offset, current.Offset, "entry %d: wrong current offset", i)
	}

	_, _, err = reader.Next()
	assert.ErrorIs(t, err, io.EOF)
}

func TestSegment_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	testData := make([]*RecordPosition, 100)
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

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
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

	seg1, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	testData := []byte("persist this")
	pos, err := seg1.Write(testData)
	assert.NoError(t, err)

	assert.NoError(t, seg1.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
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
	seg, err := OpenSegmentFile(dir, ".wal", 1)
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

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	sizes := []int{
		// 1KB
		1 << 10,
		2 << 10,
		4 << 10,
		8 << 10,
		16 << 10,
		32 << 10,
		64 << 10,
		128 << 10,
		256 << 10,
		512 << 10,
		// 1MB
		1 << 20,
	}

	var positions []*RecordPosition
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
		expectedNextOffset := pos.Offset + int64(recordHeaderSize+len(original[i])) + int64(recordTrailerMarkerSize)
		assert.Equal(t, expectedNextOffset, next.Offset, "unexpected next offset at index %d", i)
		assert.Equal(t, original[i], readData, fmt.Sprintf("data mismatch at index %d", i))
	}
}

func TestSegment_CorruptHeader(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
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
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	data := []byte("hello")
	pos, err := seg.Write(data)
	assert.NoError(t, err)
	copy(seg.mmapData[pos.Offset+recordHeaderSize:], []byte{})
	seg.writeOffset.Store(pos.Offset + recordHeaderSize - 1)
	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, io.EOF)
}

func TestSegment_ReadAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
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
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	assert.NoError(t, seg.Close())

	_, err = seg.Write([]byte("should fail"))
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_TruncatedRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	seg1, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	data := []byte("valid")
	_, err = seg1.Write(data)
	assert.NoError(t, err)

	offset := seg1.WriteOffset()
	copy(seg1.mmapData[offset:], "garbage")
	assert.NoError(t, seg1.Sync())
	assert.NoError(t, seg1.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})

	assert.Equal(t, offset, seg2.WriteOffset(), "recovered write offset should skip invalid data")
}

func TestSegment_WriteAtExactBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	size := segmentSize - segmentHeaderSize - recordHeaderSize - recordTrailerMarkerSize
	data := make([]byte, size)
	for i := range data {
		data[i] = 'A'
	}

	_, err = seg.Write(data)
	assert.NoError(t, err)

	assert.Equal(t, int64(segmentSize), seg.WriteOffset())

	_, err = seg.Write([]byte("extra"))
	assert.Error(t, err)
}

func TestSegment_ConcurrentReadWhileWriting(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
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
		defer reader.Close()
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
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSyncOption(MsyncOnWrite))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	_, err = seg.Write([]byte("sync test"))
	assert.NoError(t, err)
}

func TestSegment_EmptyReaderEOF(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	reader := seg.NewReader()
	_, _, err = reader.Next()
	assert.ErrorIs(t, err, io.EOF)
	reader.Close()
}

func TestSegment_MSync(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	err = seg.MSync()
	assert.NoError(t, err)
}

func TestSegment_Sync_ErrorPaths(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	seg.closed.Store(true)
	err = seg.Sync()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_Close_Twice(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	assert.NoError(t, seg.Close())
	assert.NoError(t, seg.Close())
}

func TestOpenSegmentFile_BadChecksum(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	_, err = seg.Write([]byte("valid"))
	assert.NoError(t, err)

	offset := seg.WriteOffset()

	copy(seg.mmapData[offset:], []byte{
		0x00, 0x00, 0x00, 0x00, // checksum
		0x05, 0x00, 0x00, 0x00, // length = 5
	})
	copy(seg.mmapData[offset+recordHeaderSize:], "corru")

	assert.NoError(t, seg.Sync())
	assert.NoError(t, seg.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})

	assert.Equal(t, offset, seg2.WriteOffset())
}

func TestChunkPosition_String(t *testing.T) {
	cp := RecordPosition{SegmentID: 123, Offset: 456}
	s := cp.String()
	assert.Contains(t, s, "SegmentID=123")
	assert.Contains(t, s, "Offset=456")
}

func TestSegment_MSync_Closed(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	seg.closed.Store(true)

	err = seg.MSync()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_WillExceed(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	defer seg.Close()

	assert.False(t, seg.WillExceed(1024))

	seg.writeOffset.Store(seg.mmapSize - recordHeaderSize - 1)
	assert.True(t, seg.WillExceed(2))
}

func TestSegment_TrailerValidation(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	data := []byte("trailer test")
	pos, err := seg.Write(data)
	assert.NoError(t, err)

	trailerOffset := pos.Offset + int64(recordHeaderSize+len(data))
	copy(seg.mmapData[trailerOffset:], []byte{0x00, 0x00, 0x00, 0x00})

	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrIncompleteChunk)
	assert.NoError(t, seg.Sync())
	assert.NoError(t, seg.Close())
	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})
}

func TestNewSegment_MetadataInitialization(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, seg.Close()) })

	metaBuf := seg.mmapData[:segmentHeaderSize]
	meta, err := decodeSegmentHeader(metaBuf)
	assert.NoError(t, err)

	assert.Equal(t, uint32(segmentMagicNumber), meta.Magic, "Magic number mismatch")
	assert.Equal(t, uint32(segmentHeaderVersion), meta.Version, "Version mismatch")
	assert.Equal(t, int64(segmentHeaderSize), meta.WriteOffset, "Initial write offset should be after metadata header")
	assert.Equal(t, int64(0), seg.GetEntryCount(), "Entry count should start at 0")
	assert.Equal(t, int64(0), meta.EntryCount, "Entry count should start at 0")
	assert.Equal(t, FlagActive, meta.Flags, "Initial flags should have 'active' set")
	assert.Equal(t, FlagActive, seg.GetFlags(), "Initial flags should have 'active' set")

	now := time.Now().UnixNano()
	assert.InDelta(t, now, meta.CreatedAt, float64(30*time.Second), "CreatedAt too far from now")
	assert.InDelta(t, now, meta.LastModifiedAt, float64(30*time.Second), "LastModifiedAt too far from now")
	assert.InDelta(t, now, seg.GetLastModifiedAt(), float64(30*time.Second), "LastModifiedAt too far from now")
}

func TestSegment_SealAndPreventWrite(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = seg.Close() })

	_, err = seg.Write([]byte("hello"))
	assert.NoError(t, err, "initial write should succeed")

	err = seg.SealSegment()
	assert.NoError(t, err, "sealing Segment should not fail")

	_, err = seg.Write([]byte("how are you?"))
	assert.ErrorIs(t, err, ErrSegmentSealed)

	_ = seg.Close()
	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	defer seg2.Close()

	meta, err := decodeSegmentHeader(seg2.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.True(t, IsSealed(meta.Flags), "Segment should remain sealed after reopen")
	assert.False(t, IsActive(meta.Flags), "Segment should remain active after reopen")
}

func TestSegmentOffsetBehavior(t *testing.T) {
	t.Run("new Segment should start at segmentHeaderSize", func(t *testing.T) {
		dir := t.TempDir()
		seg, err := OpenSegmentFile(dir, ".wal", 1)
		assert.NoError(t, err)
		defer seg.Close()

		assert.Equal(t, int64(segmentHeaderSize), seg.writeOffset.Load())
	})

	t.Run("sealed Segment should restore write offset from metadata", func(t *testing.T) {
		dir := t.TempDir()
		seg, err := OpenSegmentFile(dir, ".wal", 1)
		assert.NoError(t, err)

		data := []byte("sealed-data")
		_, err = seg.Write(data)
		assert.NoError(t, err)
		expectedOffset := seg.writeOffset.Load()

		err = seg.SealSegment()
		assert.NoError(t, err)
		assert.NoError(t, seg.Close())

		seg2, err := OpenSegmentFile(dir, ".wal", 1)
		assert.NoError(t, err)
		defer seg2.Close()

		assert.Equal(t, expectedOffset, seg2.writeOffset.Load())
	})

	t.Run("active Segment with crash should recover valid offset by scanning", func(t *testing.T) {
		dir := t.TempDir()
		seg, err := OpenSegmentFile(dir, ".wal", 1)
		assert.NoError(t, err)

		data := []byte("valid")
		_, err = seg.Write(data)
		assert.NoError(t, err)

		offset := seg.writeOffset.Load()

		copy(seg.mmapData[offset:], []byte{
			// invalid checksum
			0x00, 0x00, 0x00, 0x00,
			// length = 5
			0x05, 0x00, 0x00, 0x00,
		})
		copy(seg.mmapData[offset+recordHeaderSize:], "corru")

		assert.NoError(t, seg.Sync())
		assert.NoError(t, seg.Close())

		seg2, err := OpenSegmentFile(dir, ".wal", 1)
		assert.NoError(t, err)
		defer seg2.Close()

		assert.Equal(t, offset, seg2.writeOffset.Load())
	})
}

func TestWithSegmentSize(t *testing.T) {
	tmpDir := t.TempDir()
	const customSize int64 = 8 * 1024 * 1024

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(customSize))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	assert.Equal(t, int64(64), seg.WriteOffset(), "initial write size should 64 bytes")
	assert.Equal(t, customSize, seg.GetSegmentSize(), "initial write offset should match")
}

func TestSegment_Align_Uo(t *testing.T) {
	testCases := []int64{
		8,
		9,
	}

	for i, n := range testCases {
		aligned := alignUp(n)
		assert.Equal(t, int64(i+1)*8, aligned, "aligned segment should align up")
	}
}

func TestSegment_WriteRead_UnalignedEntriesAlignedOffsets(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	inputs := [][]byte{
		[]byte("abc"),
		[]byte("1234567"),
		bytes.Repeat([]byte("X"), 15),
		bytes.Repeat([]byte("Y"), 33),
		bytes.Repeat([]byte("Z"), 127),
		bytes.Repeat([]byte("P"), 1023),
	}

	var positions []*RecordPosition

	for _, data := range inputs {
		pos, err := seg.Write(data)
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	for i, pos := range positions {
		data, next, err := seg.Read(pos.Offset)
		assert.NoError(t, err, "failed to read record at index %d", i)
		assert.Equal(t, inputs[i], data, "data mismatch at index %d", i)

		assert.Equal(t, int64(0), next.Offset%8,
			"next offset not 8-byte aligned at index %d: got %d", i, next.Offset)
	}
}

func TestSegmentReader_Next_AlignedOffsets(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, seg.Close()) })

	inputs := [][]byte{
		[]byte("x"),
		[]byte("hello"),
		bytes.Repeat([]byte("A"), 29),
		bytes.Repeat([]byte("B"), 123),
		bytes.Repeat([]byte("C"), 509),
	}

	for _, data := range inputs {
		_, err := seg.Write(data)
		assert.NoError(t, err)
	}

	reader := seg.NewReader()
	defer reader.Close()
	i := 0
	for {
		data, current, err := reader.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		assert.Equal(t, inputs[i], data, "mismatch at index %d", i)
		assert.Equal(t, int64(0), current.Offset%8, "unaligned offset at index %d: %d", i, current.Offset)
		i++
	}
}

func TestSegment_ScanStopsAt_Trailer_Corruption(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	t.Cleanup(func() { assert.NoError(t, seg.Close()) })

	data1 := []byte("hello")
	data2 := []byte("world")

	pos1, err := seg.Write(data1)
	assert.NoError(t, err)

	pos2, err := seg.Write(data2)
	assert.NoError(t, err)

	headerSize := int64(recordHeaderSize)
	dataSize := int64(len(data2))
	trailerOffset := pos2.Offset + headerSize + dataSize
	// corrupting
	seg.mmapData[trailerOffset] = 0x00
	assert.NoError(t, seg.mmapData.Flush())
	assert.NoError(t, seg.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, seg2.Close()) })

	actualOffset := seg2.WriteOffset()
	entrySize1 := alignUp(int64(recordHeaderSize + len(data1) + recordTrailerMarkerSize))
	expectedRecoveryOffset := pos1.Offset + entrySize1
	assert.Equal(t, expectedRecoveryOffset, actualOffset, "scanForLastOffset should stop before corrupted entry")
}

func TestSegment_ParallelStreamReaders(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, seg.Close()) })

	payload := []byte("concurrent-read-entry")
	var positions []*RecordPosition
	for i := 0; i < 500; i++ {
		pos, err := seg.Write(payload)
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	var wg sync.WaitGroup
	for r := 0; r < 1000; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			reader := seg.NewReader()
			defer reader.Close()
			var count int
			for {
				data, _, err := reader.Next()
				if err == io.EOF {
					break
				}
				assert.NoError(t, err)
				assert.Equal(t, payload, data)
				count++
			}
			assert.Equal(t, len(positions), count)
		}(r)
	}
	wg.Wait()
}

func TestSegmentReader_ReferenceCountingAndDeletion(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	_, err = seg.Write([]byte("hello"))
	assert.NoError(t, err)

	reader1 := seg.NewReader()
	assert.NotNil(t, reader1)
	assert.Equal(t, int64(1), seg.refCount.Load())

	reader2 := seg.NewReader()
	assert.NotNil(t, reader2)
	assert.Equal(t, int64(2), seg.refCount.Load())

	seg.MarkForDeletion()
	assert.True(t, seg.markedForDeletion.Load())

	assert.FileExists(t, seg.path)

	reader1.Close()
	assert.Equal(t, int64(1), seg.refCount.Load())
	assert.FileExists(t, seg.path)

	reader2.Close()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(0), seg.refCount.Load())

	_, statErr := os.Stat(seg.path)
	assert.True(t, os.IsNotExist(statErr), "segment file should be deleted")
}

func TestSegmentReader_CleanupFallback(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 2)
	assert.NoError(t, err)

	_, err = seg.Write([]byte("world"))
	assert.NoError(t, err)

	reader := seg.NewReader()
	assert.NotNil(t, reader)

	reader2 := seg.NewReader()
	assert.NotNil(t, reader2)
	defer reader2.Close()

	seg.MarkForDeletion()
	assert.True(t, seg.markedForDeletion.Load())

	// dropping reader reference, with-out close
	reader = nil
	// forcing two gc cycle.
	// https://go.dev/blog/cleanups-and-weak
	// runtime.Finalizer takes at a minimum two full garbage collection cycles to reclaim the memory
	// so we are just being safe.
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	assert.Greater(t, seg.refCount.Load(), int64(0), "segment should still have active references")
	_, statErr := os.Stat(seg.path)
	assert.NoError(t, statErr, "segment file should NOT be deleted yet")

	reader2.Close()

	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int64(0), seg.refCount.Load())
	_, statErr = os.Stat(seg.path)
	assert.True(t, os.IsNotExist(statErr), "segment file should be deleted after all readers are gone")
}

func TestSegmentReader_PreventAccessAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 3)
	assert.NoError(t, err)

	_, err = seg.Write([]byte("data"))
	assert.NoError(t, err)

	reader := seg.NewReader()
	assert.NotNil(t, reader)

	_, _, err = reader.Next()
	assert.NoError(t, err)

	reader.Close()
	_, _, err = reader.Next()
	assert.ErrorIs(t, err, ErrSegmentReaderClosed)
}

func TestSegmentReader_LastRecordPosition(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	assert.NoError(t, err)
	defer seg.Close()

	records := [][]byte{
		[]byte("entry1"),
		[]byte("entry2-longer"),
		[]byte("entry3-even-longer-than-before"),
	}

	var expectedOffsets []int64
	for _, rec := range records {
		pos, err := seg.Write(rec)
		assert.NoError(t, err)
		expectedOffsets = append(expectedOffsets, pos.Offset)
	}

	reader := seg.NewReader()
	defer reader.Close()

	for i, expected := range expectedOffsets {
		data, pos, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, records[i], data)

		last := reader.LastRecordPosition()
		assert.NotNil(t, last)
		assert.Equal(t, seg.ID(), last.SegmentID)
		assert.Equal(t, expected, pos.Offset)
		assert.Equal(t, expected, last.Offset)
	}

	data, pos, err := reader.Next()
	assert.Nil(t, data)
	assert.Nil(t, pos)
	assert.ErrorIs(t, err, io.EOF)
}

func TestSegment_Reader_MarkForDeletion(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	assert.NoError(t, err)
	defer seg.Close()
	seg.MarkForDeletion()
	reader := seg.NewReader()
	assert.Nil(t, reader)

	seg2, err := OpenSegmentFile(dir, ".wal", 2)
	assert.NoError(t, err)
	assert.NoError(t, seg2.Close())
	reader = seg2.NewReader()
	assert.Nil(t, reader)
}

func TestSegmentReader_CloseOnlyOnce(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegmentFile(dir, ".wal", 1)
	assert.NoError(t, err)

	initialRef := seg.refCount.Load()
	reader := seg.NewReader()
	assert.NotNil(t, reader)

	id := reader.id
	assert.Equal(t, initialRef+1, seg.refCount.Load(), "refCount should increase by 1 after NewReader")

	reader.Close()

	afterClose := seg.refCount.Load()
	assert.Equal(t, initialRef, afterClose, "refCount should decrement after Close()")

	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	finalRef := seg.refCount.Load()
	assert.Equal(t, afterClose, finalRef, "refCount should not decrement again after GC")

	_, ok := seg.activeReaders.Load(id)
	assert.False(t, ok, "reader ID should be removed from activeReaders after Close()")
}

func TestSegmentReader_GCDecrementsRefOnlyOnce(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegmentFile(dir, ".wal", 2)
	assert.NoError(t, err)

	initialRef := seg.refCount.Load()

	func() {
		reader := seg.NewReader()
		assert.NotNil(t, reader)

		assert.Equal(t, initialRef+1, seg.refCount.Load())

		reader = nil
	}()

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	finalRef := seg.refCount.Load()

	assert.Equal(t, initialRef, finalRef, "refCount should return to initial after GC cleanup")

	assert.False(t, seg.HasActiveReaders(), "activeReaders should be empty after GC cleanup")
}

func TestSegmentReader_ActiveReader(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegmentFile(dir, ".wal", 1)
	assert.NoError(t, err)
	defer seg.Close()
	reader := seg.NewReader()
	assert.NotNil(t, reader)
	hasReader := seg.HasActiveReaders()
	assert.True(t, hasReader)
	reader.Close()
	hasReader = seg.HasActiveReaders()
	assert.False(t, hasReader)
}

func TestSegment_Close_WaitsForReaders_AndBroadcastIsSafe(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	reader := seg.NewReader()
	assert.NotNil(t, reader)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	closeStarted := make(chan struct{})

	go func() {
		defer wg.Done()
		closeStarted <- struct{}{}
		err := seg.Close()
		assert.NoError(t, err)
	}()

	select {
	case <-closeStarted:
	case <-ctx.Done():
		t.Fatal("Close() did not start in time")
	}

	time.Sleep(100 * time.Millisecond)

	reader.Close()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Close() did not finish; possibly stuck waiting on reader cleanup")
	}
}

func calculateAlignedFrameSize(dataLen int) int64 {
	raw := int64(recordHeaderSize + dataLen + recordTrailerMarkerSize)
	return alignUp(raw)
}
