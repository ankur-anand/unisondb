package walfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestSegmentSizeLimit(t *testing.T) {
	dir := t.TempDir()

	overSize := int64(4*1024*1024*1024 + 1)

	_, err := OpenSegmentFile(dir, ".wal", 1, WithSegmentSize(overSize))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment size exceeds 4 GiB limit")
}

func TestRead_CRCCheckBehavior_AfterCloseReopen(t *testing.T) {
	dir := t.TempDir()
	data := []byte("hello-crc-test")

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	assert.NoError(t, err, "failed to open segment")
	pos, err := seg.Write(data, 1)
	assert.NoError(t, err, "failed to write data")
	err = seg.SealSegment()
	assert.NoError(t, err, "failed to seal segment")

	err = seg.Close()
	assert.NoError(t, err, "failed to close segment")

	seg2, err := OpenSegmentFile(dir, ".wal", 1)
	assert.NoError(t, err, "failed to open segment")

	t.Run("CRCChecked", func(t *testing.T) {
		_, _, err := seg2.Read(pos.Offset)
		assert.NoError(t, err, "failed to read segment")
	})

	t.Run("CRCBypassed", func(t *testing.T) {
		badCRC := []byte{0xDE, 0xAD, 0xBE, 0xEF}
		copy(seg2.mmapData[pos.Offset:], badCRC)

		_, _, err := seg2.Read(pos.Offset)
		assert.ErrorIs(t, err, ErrInvalidCRC)
		seg2.MarkSealedInMemory()
		_, _, err = seg2.Read(pos.Offset)
		assert.NoError(t, err)
	})

	_ = seg2.Close()
	_ = os.Remove(filepath.Join(dir, "000000001.wal"))
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

	var positions []RecordPosition
	for _, tt := range tests {
		pos, err := seg.Write(tt.data, 0)
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

func TestSegmentHeaderStoresFirstLogIndex(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer seg.Close()

	_, err = seg.Write([]byte("record-1"), 42)
	require.NoError(t, err)

	require.NoError(t, seg.SealSegment())
	require.NoError(t, seg.Close())

	reopened, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer reopened.Close()

	meta, err := decodeSegmentHeader(reopened.mmapData[:segmentHeaderSize])
	require.NoError(t, err)
	assert.Equal(t, uint64(42), meta.FirstLogIndex)
}

func TestSegmentFirstLogIndexSetOnWrite(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer seg.Close()

	_, err = seg.Write([]byte("entry-a"), 101)
	require.NoError(t, err)
	_, err = seg.Write([]byte("entry-b"), 102)
	require.NoError(t, err)

	require.Equal(t, uint64(101), seg.firstLogIndex)
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	require.NoError(t, err)
	assert.Equal(t, uint64(101), meta.FirstLogIndex)
}

func TestSegmentFirstLogIndexSetOnWriteBatch(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer seg.Close()

	records := [][]byte{[]byte("batch-1"), []byte("batch-2"), []byte("batch-3")}
	indexes := []uint64{21, 22, 23}
	_, _, err = seg.WriteBatch(records, indexes)
	require.NoError(t, err)

	require.Equal(t, uint64(21), seg.firstLogIndex)
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	require.NoError(t, err)
	assert.Equal(t, uint64(21), meta.FirstLogIndex)
}

func TestSegmentFirstLogIndexPersistsWhenUnsealed(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)

	_, err = seg.Write([]byte("one"), 500)
	require.NoError(t, err)
	require.NoError(t, seg.Close())

	reopened, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer reopened.Close()

	meta, err := decodeSegmentHeader(reopened.mmapData[:segmentHeaderSize])
	require.NoError(t, err)
	assert.Equal(t, uint64(500), meta.FirstLogIndex)
}

func TestSegmentFirstLogIndexPersistsWhenSealed(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)

	_, err = seg.Write([]byte("sealed-entry"), 777)
	require.NoError(t, err)
	require.NoError(t, seg.SealSegment())
	require.NoError(t, seg.Close())

	reopened, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer reopened.Close()

	meta, err := decodeSegmentHeader(reopened.mmapData[:segmentHeaderSize])
	require.NoError(t, err)
	assert.Equal(t, uint64(777), meta.FirstLogIndex)
}

func TestSegment_SequentialWrites(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	var positions []RecordPosition
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		pos, err := seg.Write(data, uint64(1))
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
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, ErrNoNewData))
}

func TestSegment_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	testData := make([]RecordPosition, 100)
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("test-%d", i))
		pos, err := seg.Write(data, uint64(1))
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
	pos, err := seg.Write(data, uint64(1))
	assert.NoError(t, err)

	seg.mmapData[pos.Offset] ^= 0xFF
	_, _, err = seg.Read(pos.Offset)
	assert.NoError(t, err)
	seg.isSealed.Store(true)
	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrInvalidCRC)
	seg.isSealed.Store(false)
	_, _, err = seg.Read(pos.Offset)
	assert.NoError(t, err)
	err = seg.SealSegment()
	assert.NoError(t, err)
	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrInvalidCRC)

	assert.NoError(t, seg.Close())

	seg, err = OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrInvalidCRC)
}

func TestSegment_CloseAndReopen(t *testing.T) {
	tmpDir := t.TempDir()

	seg1, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	testData := []byte("persist this")
	pos, err := seg1.Write(testData, 1)
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

	_, err = seg.Write(data, 0)
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

	var positions []RecordPosition
	var original [][]byte

	for i, sz := range sizes {
		data := make([]byte, sz)
		for j := range data {
			data[j] = byte(i + j)
		}
		pos, err := seg.Write(data, 0)
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
	pos, err := seg.Write(data, 0)
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
	pos, err := seg.Write(data, uint64(1))
	assert.NoError(t, err)
	copy(seg.mmapData[pos.Offset+recordHeaderSize:], []byte{})
	seg.writeOffset.Store(pos.Offset + recordHeaderSize - 1)
	_, _, err = seg.Read(pos.Offset)
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, ErrNoNewData))
}

func TestSegment_ReadAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	data := []byte("data")
	pos, err := seg.Write(data, uint64(1))
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

	_, err = seg.Write([]byte("should fail"), 0)
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSegment_TruncatedRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	seg1, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	data := []byte("valid")
	_, err = seg1.Write(data, 0)
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

	_, err = seg.Write(data, uint64(1))
	assert.NoError(t, err)

	assert.Equal(t, int64(segmentSize), seg.WriteOffset())

	_, err = seg.Write([]byte("extra"), 0)
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
			_, err := seg.Write(data, uint64(1))
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
			if err == io.EOF || errors.Is(err, ErrNoNewData) {
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

	_, err = seg.Write([]byte("sync test"), 0)
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
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, ErrNoNewData))
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

	_, err = seg.Write([]byte("valid"), 0)
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
	pos, err := seg.Write(data, uint64(1))
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

	_, err = seg.Write([]byte("hello"), 0)
	assert.NoError(t, err, "initial write should succeed")

	err = seg.SealSegment()
	assert.NoError(t, err, "sealing Segment should not fail")

	_, err = seg.Write([]byte("how are you?"), 0)
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
		_, err = seg.Write(data, uint64(1))
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
		_, err = seg.Write(data, uint64(1))
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

	var positions []RecordPosition

	for _, data := range inputs {
		pos, err := seg.Write(data, uint64(1))
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
		_, err := seg.Write(data, uint64(1))
		assert.NoError(t, err)
	}

	reader := seg.NewReader()
	defer reader.Close()
	i := 0
	for {
		data, current, err := reader.Next()
		if err == io.EOF || errors.Is(err, ErrNoNewData) {
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

	pos1, err := seg.Write(data1, uint64(1))
	assert.NoError(t, err)

	pos2, err := seg.Write(data2, uint64(1))
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
	var positions []RecordPosition
	for i := 0; i < 500; i++ {
		pos, err := seg.Write(payload, uint64(1))
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
				if err == io.EOF || errors.Is(err, ErrNoNewData) {
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

	_, err = seg.Write([]byte("hello"), 0)
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

	_, err = seg.Write([]byte("world"), 0)
	assert.NoError(t, err)

	reader := seg.NewReader()
	assert.NotNil(t, reader)

	reader2 := seg.NewReader()
	assert.NotNil(t, reader2)
	defer reader2.Close()

	seg.MarkForDeletion()
	assert.True(t, seg.markedForDeletion.Load())

	// dropping reader reference, with-out close
	runtime.KeepAlive(reader)
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

	_, err = seg.Write([]byte("data"), 0)
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
		pos, err := seg.Write(rec, uint64(1))
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
	assert.Equal(t, pos, NilRecordPosition)
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, ErrNoNewData))
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

	ok := seg.activeReaders.Contains(id)
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

		runtime.KeepAlive(reader)
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

func TestSegmentReader_WaitForNewData(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	defer seg.Close()

	reader := seg.NewReader()
	assert.NotNil(t, reader)
	defer reader.Close()

	data, pos, err := reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, pos, NilRecordPosition)
	assert.ErrorIs(t, err, ErrNoNewData)

	payload := []byte("new-wal-entry")
	_, err = seg.Write(payload, uint64(1))
	assert.NoError(t, err)

	data, pos, err = reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.NotNil(t, pos)

	data, pos, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, pos, NilRecordPosition)
	assert.ErrorIs(t, err, ErrNoNewData)

	err = seg.SealSegment()
	assert.NoError(t, err)

	data, pos, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, pos, NilRecordPosition)
	assert.ErrorIs(t, err, io.EOF)
}

func TestSegmentReader_ReadAfterSealHasNewData(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	defer seg.Close()

	entry1 := []byte("entry-before-seal-1")
	entry2 := []byte("entry-before-seal-2")

	_, err = seg.Write(entry1, uint64(1))
	assert.NoError(t, err)

	_, err = seg.Write(entry2, uint64(1))
	assert.NoError(t, err)

	err = seg.SealSegment()
	assert.NoError(t, err)

	reader := seg.NewReader()
	assert.NotNil(t, reader)
	defer reader.Close()

	data, pos, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, entry1, data)
	assert.NotNil(t, pos)

	data, pos, err = reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, entry2, data)
	assert.NotNil(t, pos)

	data, pos, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, pos, NilRecordPosition)
	assert.ErrorIs(t, err, io.EOF)
}

func TestEncodeRecordPositionTo(t *testing.T) {
	pos := RecordPosition{
		SegmentID: 0x11223344,
		Offset:    0x5566778899AABBCC,
	}

	buf := EncodeRecordPositionTo(pos, nil)
	if len(buf) != 12 {
		t.Fatalf("expected 12-byte buffer, got %d", len(buf))
	}

	segID := binary.LittleEndian.Uint32(buf[0:4])
	offset := binary.LittleEndian.Uint64(buf[4:12])

	if segID != pos.SegmentID {
		t.Errorf("SegmentID mismatch: got %x, want %x", segID, pos.SegmentID)
	}
	if offset != uint64(pos.Offset) {
		t.Errorf("Offset mismatch: got %x, want %x", offset, pos.Offset)
	}

	dst := make([]byte, 20)
	buf2 := EncodeRecordPositionTo(pos, dst)

	if &buf2[0] != &dst[0] {
		t.Errorf("expected in-place write to reuse buffer")
	}
}

func calculateAlignedFrameSize(dataLen int) int64 {
	raw := int64(recordHeaderSize + dataLen + recordTrailerMarkerSize)
	return alignUp(raw)
}

func TestSegment_WriteBatch(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2-longer"),
		[]byte("record3-even-longer"),
		[]byte("r4"),
		[]byte("record5-medium-size"),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)
	assert.Equal(t, len(records), len(positions))

	for i, pos := range positions {
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}

	assert.Equal(t, int64(len(records)), seg.GetEntryCount())
}

func TestSegment_WriteBatch_Overflow(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(1024))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := make([][]byte, 20)
	for i := range records {
		records[i] = bytes.Repeat([]byte("x"), 100)
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.Error(t, err)
	assert.Greater(t, written, 0)
	assert.Less(t, written, len(records))
	assert.Equal(t, written, len(positions))

	for i := 0; i < written; i++ {
		data, _, readErr := seg.Read(positions[i].Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}
}

func TestSegment_WriteBatch_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	positions, written, err := seg.WriteBatch([][]byte{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, written)
	assert.Nil(t, positions)
}

func TestSegment_WriteBatch_AfterSealed(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	err = seg.SealSegment()
	assert.NoError(t, err)

	records := [][]byte{[]byte("test")}
	_, _, err = seg.WriteBatch(records, nil)
	assert.ErrorIs(t, err, ErrSegmentSealed)
}

func TestSegment_WriteBatch_RecordExceedsSegmentCapacity(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(1024))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	oversizedRecord := make([]byte, 2048)
	for i := range oversizedRecord {
		oversizedRecord[i] = byte(i % 256)
	}

	records := [][]byte{
		[]byte("small record that fits"),
		oversizedRecord,
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum segment capacity")
	assert.Equal(t, 0, written, "no records should be written when one exceeds capacity")
	assert.Nil(t, positions)
}

func TestSegment_WriteBatch_EachRecordValidated(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(2048))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		bytes.Repeat([]byte("a"), 50),
		bytes.Repeat([]byte("b"), 50),
		bytes.Repeat([]byte("c"), 5000),
		bytes.Repeat([]byte("d"), 50),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "record at index 2")
	assert.Contains(t, err.Error(), "exceeds maximum segment capacity")
	assert.Equal(t, 0, written, "should fail early before writing anything")
	assert.Nil(t, positions)
}

func TestSegment_WriteBatch_ReadbackAllRecords(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("a"),
		[]byte("ab"),
		[]byte("abc"),
		[]byte("abcd"),
		[]byte("abcdefg"),
		[]byte("abcdefgh"),
		[]byte("abcdefghi"),
		bytes.Repeat([]byte("x"), 15),
		bytes.Repeat([]byte("y"), 16),
		bytes.Repeat([]byte("z"), 17),
		bytes.Repeat([]byte("m"), 63),
		bytes.Repeat([]byte("n"), 64),
		bytes.Repeat([]byte("o"), 65),
		bytes.Repeat([]byte("p"), 127),
		bytes.Repeat([]byte("q"), 128),
		bytes.Repeat([]byte("r"), 255),
		bytes.Repeat([]byte("s"), 256),
		bytes.Repeat([]byte("t"), 1023),
		bytes.Repeat([]byte("u"), 1024),
		bytes.Repeat([]byte("v"), 4095),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)
	assert.Equal(t, len(records), len(positions))

	for i, pos := range positions {
		data, next, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr, "record %d at offset %d should be readable", i, pos.Offset)
		assert.Equal(t, records[i], data)

		if i < len(positions)-1 {
			assert.Equal(t, positions[i+1].Offset, next.Offset,
				"next pointer for record %d should point to record %d", i, i+1)
		}
	}

	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, int64(len(records)), meta.EntryCount)
	assert.Equal(t, seg.WriteOffset(), meta.WriteOffset)
}

func TestSegment_WriteBatch_ReadbackAtSegmentBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	segmentSize := int64(2048)
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(segmentSize))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := make([][]byte, 30)
	for i := range records {
		records[i] = bytes.Repeat([]byte{byte(i)}, 50)
	}

	positions, written, err := seg.WriteBatch(records, nil)
	if err != nil {
		assert.ErrorIs(t, err, ErrSegmentFull)
	}
	assert.Greater(t, written, 0)
	assert.Equal(t, written, len(positions))

	for i := 0; i < written; i++ {
		pos := positions[i]
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr, "record %d at offset %d (near boundary) should be readable", i, pos.Offset)
		assert.Equal(t, records[i], data)
	}

	if written > 0 {
		lastIdx := written - 1
		lastPos := positions[lastIdx]
		lastData, _, err := seg.Read(lastPos.Offset)
		assert.NoError(t, err)
		assert.Equal(t, records[lastIdx], lastData)

		finalOffset := seg.WriteOffset()
		assert.Greater(t, finalOffset, segmentSize-200)
	}

	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, int64(written), meta.EntryCount)
}

func TestSegment_WriteBatch_HeaderUpdateAfterPartialWrite(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(1024))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	initialMeta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	initialOffset := initialMeta.WriteOffset
	initialCount := initialMeta.EntryCount

	records := make([][]byte, 20)
	for i := range records {
		records[i] = bytes.Repeat([]byte("x"), 80)
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.Error(t, err)
	assert.Greater(t, written, 0)
	assert.Less(t, written, len(records))

	updatedMeta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, initialCount+int64(written), updatedMeta.EntryCount)
	assert.Greater(t, updatedMeta.WriteOffset, initialOffset)
	assert.Equal(t, seg.WriteOffset(), updatedMeta.WriteOffset)
	assert.Greater(t, updatedMeta.LastModifiedAt, initialMeta.LastModifiedAt)

	for i := 0; i < written; i++ {
		data, _, readErr := seg.Read(positions[i].Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}

	extraRecord := []byte("after-partial-write")
	pos, err := seg.Write(extraRecord, uint64(1))
	if err == nil {
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, extraRecord, data)
	} else {
		assert.Contains(t, err.Error(), "exceeds Segment size")
	}
}

func TestSegment_WriteBatch_HeaderCRCValidation(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	_, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)

	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, uint32(segmentMagicNumber), meta.Magic)
	assert.Equal(t, uint32(segmentHeaderVersion), meta.Version)
	assert.Equal(t, int64(len(records)), meta.EntryCount)
	assert.Equal(t, seg.WriteOffset(), meta.WriteOffset)
	assert.Greater(t, meta.LastModifiedAt, int64(0))
	assert.True(t, IsActive(meta.Flags))
	assert.False(t, IsSealed(meta.Flags))
}

func TestSegment_WriteBatch_MultipleConsecutiveBatches(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	var allRecords [][]byte
	var allPositions []RecordPosition
	totalWritten := 0

	for batchNum := 0; batchNum < 3; batchNum++ {
		batch := make([][]byte, 5)
		for i := range batch {
			batch[i] = []byte(fmt.Sprintf("batch%d-record%d", batchNum, i))
		}
		allRecords = append(allRecords, batch...)

		positions, written, err := seg.WriteBatch(batch, nil)
		assert.NoError(t, err)
		assert.Equal(t, len(batch), written)
		allPositions = append(allPositions, positions...)
		totalWritten += written

		meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
		assert.NoError(t, err)
		assert.Equal(t, int64(totalWritten), meta.EntryCount)
	}

	for i, pos := range allPositions {
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, allRecords[i], data)
	}

	finalMeta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, int64(totalWritten), finalMeta.EntryCount)
	assert.Equal(t, seg.WriteOffset(), finalMeta.WriteOffset)
}

func TestSegment_WriteBatch_ReadbackAfterCloseReopen(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	records := [][]byte{
		[]byte("persistent-record-1"),
		[]byte("persistent-record-2"),
		[]byte("persistent-record-3"),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)

	assert.NoError(t, seg.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})

	meta, err := decodeSegmentHeader(seg2.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, int64(len(records)), meta.EntryCount)

	for i, pos := range positions {
		data, _, readErr := seg2.Read(pos.Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}
}

func TestSegment_WriteBatch_ReadbackWithMixedSizes(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("t"),
		bytes.Repeat([]byte("s"), 10),
		bytes.Repeat([]byte("m"), 100),
		bytes.Repeat([]byte("l"), 1000),
		[]byte("T"),
		bytes.Repeat([]byte("S"), 15),
		bytes.Repeat([]byte("M"), 200),
		bytes.Repeat([]byte("L"), 2000),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)

	for i, pos := range positions {
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr, "record %d (size %d) should be readable", i, len(records[i]))
		assert.Equal(t, len(records[i]), len(data))
		assert.Equal(t, records[i], data)
	}

	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, int64(len(records)), meta.EntryCount)
}

func TestSegment_WriteBatch_ZeroLengthRecords(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("record1"),
		[]byte{},
		[]byte("record2"),
		[]byte{},
		[]byte("record3"),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)
	assert.Equal(t, len(records), len(positions))

	for i, pos := range positions {
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}

	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err)
	assert.Equal(t, int64(len(records)), meta.EntryCount)
}

func TestSegment_WriteBatch_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(1<<20))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	const numGoroutines = 10
	const recordsPerGoroutine = 20

	var wg sync.WaitGroup
	var totalWritten atomic.Int64
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			records := make([][]byte, recordsPerGoroutine)
			for j := range records {
				records[j] = []byte{byte(id), byte(j)}
			}

			positions, written, err := seg.WriteBatch(records, nil)
			if err != nil && err != ErrSegmentFull {
				errors <- err
				return
			}

			assert.Equal(t, written, len(positions), "written count should match positions length")
			totalWritten.Add(int64(written))

			for k := 0; k < written; k++ {
				data, _, readErr := seg.Read(positions[k].Offset)
				if readErr != nil {
					errors <- readErr
					return
				}
				assert.Equal(t, records[k], data)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent write error: %v", err)
	}

	assert.Equal(t, totalWritten.Load(), seg.GetEntryCount())
}

func TestSegment_WriteBatch_WithMSyncOnWrite(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSyncOption(MsyncOnWrite))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)
	assert.Equal(t, len(records), len(positions))

	for i, pos := range positions {
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr)
		assert.Equal(t, records[i], data)
	}
}

func TestSegment_WriteBatch_PartialWriteExactCount(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(512))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := make([][]byte, 50)
	for i := range records {
		records[i] = bytes.Repeat([]byte("x"), 50)
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.ErrorIs(t, err, ErrSegmentFull)
	assert.Greater(t, written, 0)
	assert.Less(t, written, len(records))

	assert.Equal(t, written, len(positions), "written count MUST exactly match positions length")

	for i := 0; i < written; i++ {
		data, _, readErr := seg.Read(positions[i].Offset)
		assert.NoError(t, readErr, "position %d should be readable", i)
		assert.Equal(t, records[i], data, "data at position %d should match", i)
	}

	assert.Equal(t, int64(written), seg.GetEntryCount())
}

func TestSegment_WriteBatch_OversizedRecordNoStateChange(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(1024))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	initialOffset := seg.WriteOffset()
	initialEntryCount := seg.GetEntryCount()
	initialFlags := seg.GetFlags()

	oversizedRecord := make([]byte, 2048)
	records := [][]byte{oversizedRecord}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum segment capacity")
	assert.Equal(t, 0, written)
	assert.Nil(t, positions)

	assert.Equal(t, initialOffset, seg.WriteOffset(), "write offset should not change")
	assert.Equal(t, initialEntryCount, seg.GetEntryCount(), "entry count should not change")
	assert.Equal(t, initialFlags, seg.GetFlags(), "flags should not change")

	validRecord := []byte("valid-record")
	pos, err := seg.Write(validRecord, uint64(1))
	assert.NoError(t, err)
	assert.Equal(t, initialOffset, pos.Offset, "should write at original offset")
}

func TestSegment_WriteBatch_MetadataConsistency(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(800))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	initialOffset := seg.WriteOffset()
	initialCount := seg.GetEntryCount()
	initialModifiedAt := seg.GetLastModifiedAt()

	records := make([][]byte, 20)
	for i := range records {
		records[i] = bytes.Repeat([]byte("y"), 60)
	}

	time.Sleep(2 * time.Millisecond)

	_, written, err := seg.WriteBatch(records, nil)
	assert.Error(t, err)
	assert.Greater(t, written, 0)

	newOffset := seg.WriteOffset()
	newCount := seg.GetEntryCount()
	newModifiedAt := seg.GetLastModifiedAt()

	assert.Greater(t, newOffset, initialOffset, "write offset should advance")
	assert.Equal(t, initialCount+int64(written), newCount, "entry count should match written")
	assert.Greater(t, newModifiedAt, initialModifiedAt, "last modified should be updated")

	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	assert.NoError(t, err, "segment header should have valid CRC")
	assert.Equal(t, newOffset, meta.WriteOffset)
	assert.Equal(t, newCount, meta.EntryCount)
}

func TestSegment_WriteBatch_WithClosedSegment(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	records := [][]byte{[]byte("test")}

	assert.NoError(t, seg.Close())

	_, written, err := seg.WriteBatch(records, nil)
	assert.ErrorIs(t, err, ErrClosed)
	assert.Equal(t, 0, written)
}

func TestSegment_WriteBatch_ExactlyFullSegment(t *testing.T) {
	tmpDir := t.TempDir()
	segmentSize := int64(1024)
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(segmentSize))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	availableSpace := segmentSize - segmentHeaderSize

	dataLen := 88
	numRecords := int(availableSpace / alignUp(int64(recordHeaderSize+dataLen+recordTrailerMarkerSize)))

	records := make([][]byte, numRecords)
	for i := range records {
		records[i] = bytes.Repeat([]byte("a"), dataLen)
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err, "should write successfully")
	assert.Equal(t, numRecords, written)
	assert.Equal(t, numRecords, len(positions))

	finalOffset := seg.WriteOffset()
	assert.LessOrEqual(t, finalOffset, segmentSize, "should not exceed segment size")
	assert.Greater(t, finalOffset, segmentSize-200, "should be nearly full")

	_, written2, err := seg.WriteBatch([][]byte{[]byte("extra")}, nil)
	if err == nil {
		assert.Equal(t, 1, written2, "should have written 1 record")
	} else {
		assert.ErrorIs(t, err, ErrSegmentFull)
	}
}

func TestSegment_WriteBatch_AlignmentVerification(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("a"),
		[]byte("abc"),
		[]byte("abcdefg"),
		bytes.Repeat([]byte("x"), 13),
		bytes.Repeat([]byte("y"), 29),
		bytes.Repeat([]byte("z"), 101),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)

	for i, pos := range positions {
		assert.Equal(t, int64(0), pos.Offset%8, "position %d offset %d should be 8-byte aligned", i, pos.Offset)

		_, next, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr)

		if i < len(positions)-1 {
			assert.Equal(t, int64(0), next.Offset%8, "next offset %d should be 8-byte aligned", next.Offset)
			assert.Equal(t, positions[i+1].Offset, next.Offset, "next offset should match next position")
		}
	}
}

func TestSegment_WriteBatch_CRCValidation(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2-with-different-length"),
		[]byte("r3"),
		bytes.Repeat([]byte("long"), 100),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)

	err = seg.SealSegment()
	assert.NoError(t, err)

	for i, pos := range positions {
		data, _, readErr := seg.Read(pos.Offset)
		assert.NoError(t, readErr, "CRC validation should pass for record %d", i)
		assert.Equal(t, records[i], data)
	}

	assert.NoError(t, seg.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg2.Close())
	})

	for i, pos := range positions {
		data, _, readErr := seg2.Read(pos.Offset)
		assert.NoError(t, readErr, "CRC validation should pass after reopen for record %d", i)
		assert.Equal(t, records[i], data)
	}
}

func TestSegment_WriteBatch_TrailerValidation(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)

	for i, pos := range positions {
		dataLen := len(records[i])
		trailerOffset := pos.Offset + int64(recordHeaderSize) + int64(dataLen)

		trailer := seg.mmapData[trailerOffset : trailerOffset+recordTrailerMarkerSize]
		assert.True(t, bytes.Equal(trailer, trailerMarker),
			"record %d should have valid trailer marker at offset %d", i, trailerOffset)
	}
}

func TestSegment_WriteBatch_StateOpenCheck(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)

	records := [][]byte{[]byte("test")}

	seg.state.Store(StateClosing)

	_, written, err := seg.WriteBatch(records, nil)
	assert.ErrorIs(t, err, ErrClosed)
	assert.Equal(t, 0, written)
}

func TestSegment_WriteBatch_PaddingZeroed(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	records := [][]byte{
		[]byte("a"),
		[]byte("abc"),
		[]byte("abcdefg"),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(records), written)

	for i, pos := range positions {
		dataLen := int64(len(records[i]))
		headerSize := int64(recordHeaderSize)
		trailerSize := int64(recordTrailerMarkerSize)
		rawSize := headerSize + dataLen + trailerSize
		entrySize := alignUp(rawSize)
		paddingSize := entrySize - rawSize

		if paddingSize > 0 {
			paddingStart := pos.Offset + rawSize
			paddingEnd := pos.Offset + entrySize

			for offset := paddingStart; offset < paddingEnd; offset++ {
				assert.Equal(t, byte(0), seg.mmapData[offset],
					"padding byte at offset %d for record %d should be zero", offset, i)
			}
		}
	}
}

func TestSegment_WriteBatch_SegmentFullOnFirstRecord(t *testing.T) {
	tmpDir := t.TempDir()
	seg, err := OpenSegmentFile(tmpDir, ".wal", 1, WithSegmentSize(256))
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, seg.Close())
	})

	filler := bytes.Repeat([]byte("x"), 150)
	_, err = seg.Write(filler, uint64(1))
	assert.NoError(t, err)

	records := [][]byte{
		bytes.Repeat([]byte("y"), 100),
		bytes.Repeat([]byte("z"), 50),
	}

	positions, written, err := seg.WriteBatch(records, nil)
	assert.ErrorIs(t, err, ErrSegmentFull)
	assert.Equal(t, 0, written, "no records should be written if first doesn't fit")
	assert.Nil(t, positions)
}

func TestSegment_TruncateTo_UnsealsAndKeepsSegment(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer seg.Close()

	var positions []RecordPosition
	for i := 1; i <= 4; i++ {
		pos, err := seg.Write([]byte(fmt.Sprintf("data-%d", i)), uint64(i))
		require.NoError(t, err)
		positions = append(positions, pos)
	}

	require.NoError(t, seg.SealSegment())
	require.True(t, IsSealed(seg.GetFlags()))

	require.NoError(t, seg.TruncateTo(2))

	assert.False(t, IsSealed(seg.GetFlags()), "Segment should be unsealed after truncate")
	assert.Equal(t, int64(2), seg.GetEntryCount())

	entries := seg.IndexEntries()
	require.Len(t, entries, 2)

	pos, err := seg.Write([]byte("data-3-new"), 3)
	require.NoError(t, err)
	assert.Equal(t, SegmentID(1), pos.SegmentID)

	data, _, err := seg.Read(pos.Offset)
	require.NoError(t, err)
	assert.Equal(t, []byte("data-3-new"), data)
}

func TestSegment_TruncateTo_Errors(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)
	defer seg.Close()

	t.Run("empty segment", func(t *testing.T) {
		err := seg.TruncateTo(1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "segment is empty, cannot truncate to")
	})

	_, err = seg.Write([]byte("data-1"), 5)
	require.NoError(t, err)
	_, err = seg.Write([]byte("data-2"), 6)
	require.NoError(t, err)

	t.Run("before segment start", func(t *testing.T) {
		err := seg.TruncateTo(3)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "before segment start")
	})

	t.Run("index not found", func(t *testing.T) {
		err := seg.TruncateTo(10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in segment index")
	})
}

func TestSegment_RemoveDeletesFiles(t *testing.T) {
	dir := t.TempDir()

	seg, err := OpenSegmentFile(dir, ".wal", 1)
	require.NoError(t, err)

	_, err = seg.Write([]byte("payload"), 1)
	require.NoError(t, err)

	require.NoError(t, seg.SealSegment())

	require.NoError(t, seg.Remove())

	_, err = os.Stat(seg.path)
	assert.True(t, os.IsNotExist(err), "segment file should be removed")

	if seg.indexPath != "" {
		_, err = os.Stat(seg.indexPath)
		assert.True(t, os.IsNotExist(err), "index file should be removed")
	}
}
