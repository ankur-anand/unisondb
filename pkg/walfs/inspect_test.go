package walfs

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInspectSegmentFile(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegmentFile(dir, ".seg", 7, WithSegmentSize(4096))
	require.NoError(t, err)
	records := [][]byte{[]byte("first"), {}, make([]byte, 3000)}
	var expected []InspectedIndexEntry
	for i, record := range records {
		pos, err := seg.Write(record, uint64(i+10))
		require.NoError(t, err)
		expected = append(expected, InspectedIndexEntry{Offset: pos.Offset, Length: uint32(len(record))})
	}
	offset := seg.WriteOffset()
	require.NoError(t, seg.Close())
	path := SegmentFileName(dir, ".seg", 7)
	require.NoError(t, os.Chmod(path, 0444))
	t.Cleanup(func() { require.NoError(t, os.Chmod(path, 0644)) })
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, withIndex := range []bool{false, true} {
		inspection, err := InspectSegmentFile(path, withIndex)
		require.NoError(t, err)
		assert.Equal(t, int64(4096), inspection.Size)
		assert.Equal(t, int64(3), inspection.Header.EntryCount)
		assert.Equal(t, uint64(10), inspection.Header.FirstLogIndex)
		assert.Equal(t, offset, inspection.Header.WriteOffset)
		assert.False(t, IsSealed(inspection.Header.Flags))
		if withIndex {
			assert.Equal(t, expected, inspection.IndexEntries)
		} else {
			assert.Empty(t, inspection.IndexEntries)
		}
	}
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestInspectSegmentFileReportsCorruptionWithoutRepair(t *testing.T) {
	for _, corruption := range []string{"short header", "header CRC", "magic", "version", "offset", "negative count", "count mismatch", "length", "record CRC", "trailer", "truncated file"} {
		t.Run(corruption, func(t *testing.T) {
			dir := t.TempDir()
			seg, err := OpenSegmentFile(dir, ".seg", 1, WithSegmentSize(1024))
			require.NoError(t, err)
			pos, err := seg.Write([]byte("payload"), 1)
			require.NoError(t, err)
			require.NoError(t, seg.Close())
			path := SegmentFileName(dir, ".seg", 1)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			switch corruption {
			case "short header":
				data = data[:10]
			case "header CRC":
				data[56] ^= 0xff
			case "magic":
				data[0] ^= 0xff
			case "version":
				binary.LittleEndian.PutUint32(data[4:8], 999)
			case "offset":
				binary.LittleEndian.PutUint64(data[24:32], uint64(len(data)+8))
			case "negative count":
				binary.LittleEndian.PutUint64(data[32:40], ^uint64(0))
			case "count mismatch":
				binary.LittleEndian.PutUint64(data[32:40], 2)
			case "length":
				binary.LittleEndian.PutUint32(data[pos.Offset+4:pos.Offset+8], ^uint32(0))
			case "record CRC":
				data[pos.Offset+recordHeaderSize] ^= 0xff
			case "trailer":
				data[pos.Offset+recordHeaderSize+7] ^= 0xff
			case "truncated file":
				data = data[:pos.Offset+recordHeaderSize+3]
			}
			if corruption != "short header" && corruption != "header CRC" {
				binary.LittleEndian.PutUint32(data[56:60], crc32.Checksum(data[:56], crcTable))
			}
			require.NoError(t, os.WriteFile(path, data, 0644))
			_, err = InspectSegmentFile(path, true)
			require.Error(t, err)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, data, after)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			assert.Len(t, entries, 1)
		})
	}
}
