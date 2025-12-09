package walfs

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Key scenarios tested:
// 1. Single-bit errors in data should be detected via CRC
// 2. Single-bit errors in metadata should be detected
// 3. File truncation should preserve valid records before truncation point
// 4. Corruption in middle of file should not affect records before corruption
// 5. Zero-fill corruption (like disk sector failures)

// TestCorruption_SingleBitFlip_InData verifies that a single bit flip in
// the data portion of a record is detected via CRC validation.
func TestCorruption_SingleBitFlip_InData(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)

	data := []byte("like corruption test data payload")
	pos, err := seg.Write(data, 1)
	require.NoError(t, err)

	require.NoError(t, seg.SealSegment())

	// Flip a single bit in the data portion
	dataOffset := pos.Offset + recordHeaderSize
	// flip lowest bit
	seg.mmapData[dataOffset] ^= 0x01

	// should fail with CRC error
	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrInvalidCRC, "single bit flip in data should cause CRC error")
	require.NoError(t, seg.Close())
}

// TestCorruption_SingleBitFlip_InCRC verifies that corruption of the CRC
// itself is detected (the computed CRC won't match the corrupted stored CRC).
func TestCorruption_SingleBitFlip_InCRC(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)

	data := []byte("test payload for CRC corruption")
	pos, err := seg.Write(data, 1)
	require.NoError(t, err)
	require.NoError(t, seg.SealSegment())

	crcOffset := pos.Offset
	seg.mmapData[crcOffset] ^= 0x01

	_, _, err = seg.Read(pos.Offset)
	assert.ErrorIs(t, err, ErrInvalidCRC, "corrupted CRC should be detected")
	require.NoError(t, seg.Close())
}

// TestCorruption_SingleBitFlip_InLength verifies that corruption of the
// length field causes ErrCorruptHeader (length becomes invalid).
func TestCorruption_SingleBitFlip_InLength(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)

	data := []byte("test payload")
	pos, err := seg.Write(data, 1)
	require.NoError(t, err)

	// Length is at offset 4-7 in record header
	lengthOffset := pos.Offset + 4
	// Flip high bit - likely makes length huge
	seg.mmapData[lengthOffset] ^= 0x80

	_, _, err = seg.Read(pos.Offset)
	assert.True(t, errors.Is(err, ErrCorruptHeader) || errors.Is(err, ErrInvalidCRC),
		"corrupted length should be detected, got: %v", err)
	require.NoError(t, seg.Close())
}

// TestCorruption_Truncation_PreservesValidRecords verifies that when a segment
// file is truncated, reopening it recovers all valid records before the truncation.
func TestCorruption_Truncation_PreservesValidRecords(t *testing.T) {
	tmpDir := t.TempDir()

	const numRecords = 100
	testData := make([][]byte, numRecords)
	var positions []RecordPosition

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)

	for i := 0; i < numRecords; i++ {
		testData[i] = make([]byte, 256)
		rand.Read(testData[i])
		pos, err := seg.Write(testData[i], uint64(i+1))
		require.NoError(t, err)
		positions = append(positions, pos)
	}
	require.NoError(t, seg.Close())

	segPath := SegmentFileName(tmpDir, ".wal", 1)
	truncateAt := positions[50].Offset

	err = os.Truncate(segPath, truncateAt)
	require.NoError(t, err)

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)
	defer seg2.Close()

	reader := seg2.NewReader()
	defer reader.Close()

	recovered := 0
	for {
		data, _, err := reader.Next()
		if err == io.EOF || errors.Is(err, ErrNoNewData) {
			break
		}
		if err != nil {
			t.Logf("Read error at record %d: %v", recovered, err)
			break
		}

		// data matches original ?
		if recovered < 50 {
			assert.Equal(t, testData[recovered], data, "data mismatch at record %d", recovered)
		}
		recovered++
	}
	assert.Equal(t, 50, recovered, "should recover all complete records before truncation")
}

// TestCorruption_MiddleRecord_PreservesBefore verifies that corruption in a
// middle record doesn't affect reading records before it.
func TestCorruption_MiddleRecord_PreservesBefore(t *testing.T) {
	tmpDir := t.TempDir()

	const numRecords = 20
	testData := make([][]byte, numRecords)
	var positions []RecordPosition

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)

	for i := 0; i < numRecords; i++ {
		testData[i] = make([]byte, 64)
		rand.Read(testData[i])
		pos, err := seg.Write(testData[i], uint64(i+1))
		require.NoError(t, err)
		positions = append(positions, pos)
	}

	corruptPos := positions[10]
	seg.mmapData[corruptPos.Offset] ^= 0xFF

	require.NoError(t, seg.SealSegment())
	require.NoError(t, seg.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)
	defer seg2.Close()

	reader := seg2.NewReader()
	defer reader.Close()

	recovered := 0
	for {
		data, _, err := reader.Next()
		if err == io.EOF || errors.Is(err, ErrNoNewData) {
			break
		}
		if errors.Is(err, ErrInvalidCRC) {
			t.Logf("CRC error at record %d (expected at 10)", recovered)
			break
		}
		require.NoError(t, err)
		assert.Equal(t, testData[recovered], data, "data mismatch at record %d", recovered)
		recovered++
	}

	assert.Equal(t, 10, recovered, "should recover all records before corrupted one")
}

// TestCorruption_ZeroFill simulates disk sector failures that zero out portions
// of the file.
func TestCorruption_ZeroFill(t *testing.T) {
	tmpDir := t.TempDir()

	const numRecords = 10
	testData := make([][]byte, numRecords)
	var positions []RecordPosition

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)

	for i := 0; i < numRecords; i++ {
		testData[i] = make([]byte, 128)
		rand.Read(testData[i])
		pos, err := seg.Write(testData[i], uint64(i+1))
		require.NoError(t, err)
		positions = append(positions, pos)
	}

	// 0 out record 5 completely
	zeroStart := positions[5].Offset
	zeroEnd := positions[6].Offset
	for i := zeroStart; i < zeroEnd; i++ {
		seg.mmapData[i] = 0x00
	}

	require.NoError(t, seg.SealSegment())
	require.NoError(t, seg.Close())

	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)
	defer seg2.Close()

	reader := seg2.NewReader()
	defer reader.Close()

	recovered := 0
	for {
		data, _, err := reader.Next()
		if err == io.EOF || errors.Is(err, ErrNoNewData) {
			break
		}
		if err != nil {
			t.Logf("Error at record %d: %v", recovered, err)
			break
		}
		assert.Equal(t, testData[recovered], data)
		recovered++
	}

	assert.Equal(t, 5, recovered, "should recover records before zero-filled section")
}

// TestCorruption_TrailerMarker_Protects verifies that trailer marker corruption
// stops recovery at that point.
func TestCorruption_TrailerMarker_Protects(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)

	data1 := []byte("record-one")
	data2 := []byte("record-two")
	data3 := []byte("record-three")

	pos1, err := seg.Write(data1, 1)
	require.NoError(t, err)
	_, err = seg.Write(data2, 2)
	require.NoError(t, err)
	_, err = seg.Write(data3, 3)
	require.NoError(t, err)

	trailerOffset := pos1.Offset + recordHeaderSize + int64(len(data1))
	// corrupt trailer marker
	seg.mmapData[trailerOffset] = 0x00

	require.NoError(t, seg.mmapData.Flush())
	require.NoError(t, seg.Close())
	seg2, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)
	defer seg2.Close()

	assert.Equal(t, pos1.Offset, seg2.WriteOffset(),
		"write offset should be at first record, not after corrupted trailer")
}

// TestCorruption_MultipleSegments_Isolation verifies corruption in one segment
// doesn't affect reading from earlier segments in a WAL.
func TestCorruption_MultipleSegments_Isolation(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := NewWALog(tmpDir, ".wal", WithMaxSegmentSize(4*1024))
	require.NoError(t, err)

	const numRecords = 50
	testData := make([][]byte, numRecords)

	for i := 0; i < numRecords; i++ {
		testData[i] = make([]byte, 200)
		rand.Read(testData[i])
		_, err := wal.Write(testData[i], uint64(i+1))
		require.NoError(t, err)
	}

	require.NoError(t, wal.Close())

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	require.NoError(t, err)
	require.Greater(t, len(files), 1, "should have multiple segments")
	t.Logf("Created %d segments", len(files))

	lastSegIdx := len(files) - 1
	seg1Data, err := os.ReadFile(files[lastSegIdx])
	require.NoError(t, err)

	if len(seg1Data) > segmentHeaderSize+100 {
		seg1Data[segmentHeaderSize+100] ^= 0xFF
		err = os.WriteFile(files[lastSegIdx], seg1Data, 0644)
		require.NoError(t, err)
		t.Logf("Corrupted segment: %s", files[lastSegIdx])
	}

	wal2, err := NewWALog(tmpDir, ".wal", WithMaxSegmentSize(4*1024))
	require.NoError(t, err)
	defer wal2.Close()

	reader := wal2.NewReader()
	defer reader.Close()

	var recovered [][]byte
	for {
		data, _, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Logf("Read error at record %d: %v", len(recovered), err)
			break
		}
		recovered = append(recovered, data)
	}

	t.Logf("Recovered %d out of %d records", len(recovered), numRecords)
	assert.Greater(t, len(recovered), 0, "should recover records from uncorrupted segments")
	assert.Less(t, len(recovered), numRecords, "should not recover records from corrupted segment")
}

// TestCorruption_MiddleSegment_StopsReading verifies that corruption in a middle
// segment stops reading entirely - valid records in later segments are NOT recovered.
func TestCorruption_MiddleSegment_StopsReading(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := NewWALog(tmpDir, ".wal", WithMaxSegmentSize(4*1024))
	require.NoError(t, err)

	const numRecords = 50
	const recordSize = 200
	testData := make([][]byte, numRecords)

	for i := 0; i < numRecords; i++ {
		testData[i] = make([]byte, recordSize)
		rand.Read(testData[i])
		_, err := wal.Write(testData[i], uint64(i+1))
		require.NoError(t, err)
	}

	require.NoError(t, wal.Close())
	files, err := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(files), 3, "need at least 3 segments for this test")

	sort.Strings(files)
	t.Logf("Created %d segments", len(files))
	for i, f := range files {
		info, _ := os.Stat(f)
		t.Logf("  Segment %d: %s (%d bytes)", i+1, filepath.Base(f), info.Size())
	}

	middleSegIdx := 1
	segData, err := os.ReadFile(files[middleSegIdx])
	require.NoError(t, err)

	corruptOffset := segmentHeaderSize + recordHeaderSize + 50
	require.Less(t, corruptOffset, len(segData), "corrupt offset must be within segment")

	segData[corruptOffset] ^= 0xFF
	err = os.WriteFile(files[middleSegIdx], segData, 0644)
	require.NoError(t, err)
	t.Logf("Corrupted middle segment %d (%s) at offset %d",
		middleSegIdx+1, filepath.Base(files[middleSegIdx]), corruptOffset)

	wal2, err := NewWALog(tmpDir, ".wal", WithMaxSegmentSize(4*1024))
	require.NoError(t, err)
	defer wal2.Close()

	reader := wal2.NewReader()
	defer reader.Close()

	recovered := 0
	var lastErr error
	for {
		data, _, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			lastErr = err
			t.Logf("Read error at record %d: %v", recovered, err)
			break
		}

		if recovered < len(testData) {
			assert.Equal(t, testData[recovered], data, "data mismatch at record %d", recovered)
		}
		recovered++
	}

	// Verify behavior:
	// We exhibits is the fail-fast behavior: for Raft/consensus, log continuity is required.

	assert.Greater(t, recovered, 0, "should recover records before corruption")

	assert.Less(t, recovered, numRecords, "should stop at corruption, not read segment 3")
	assert.ErrorIs(t, lastErr, ErrInvalidCRC, "should fail with CRC error")

	seg3Info, err := os.Stat(files[2])
	require.NoError(t, err)
	assert.Greater(t, seg3Info.Size(), int64(segmentHeaderSize),
		"segment 3 should have data that we're intentionally not reading")
}

// TestCorruption_BitFlipEveryPosition tests that bit flips at every position
// in a record are detected.
func TestCorruption_BitFlipEveryPosition(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := OpenSegmentFile(tmpDir, ".wal", 1)
	require.NoError(t, err)
	defer seg.Close()

	data := []byte("test data for comprehensive bit flip testing")
	pos, err := seg.Write(data, 1)
	require.NoError(t, err)
	require.NoError(t, seg.SealSegment())

	recordSize := recordHeaderSize + len(data) + recordTrailerMarkerSize

	undetected := 0
	for i := 0; i < recordSize-recordTrailerMarkerSize; i++ {
		for bit := 0; bit < 8; bit++ {
			seg.mmapData[pos.Offset+int64(i)] ^= 1 << bit

			readData, _, err := seg.Read(pos.Offset)
			if err == nil && bytes.Equal(readData, data) {
				undetected++
			}
		}
	}

	assert.Equal(t, 0, undetected, "all bit flips should be detected")
}
