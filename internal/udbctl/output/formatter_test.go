package output

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFormatter(t *testing.T) {
	t.Run("returns TableFormatter for table format", func(t *testing.T) {
		f := NewFormatter(FormatTable)
		_, ok := f.(*TableFormatter)
		assert.True(t, ok)
	})

	t.Run("returns JSONFormatter for json format", func(t *testing.T) {
		f := NewFormatter(FormatJSON)
		_, ok := f.(*JSONFormatter)
		assert.True(t, ok)
	})

	t.Run("returns TableFormatter for unknown format", func(t *testing.T) {
		f := NewFormatter("unknown")
		_, ok := f.(*TableFormatter)
		assert.True(t, ok)
	})
}

func TestTableFormatter_WriteSegmentList(t *testing.T) {
	var buf bytes.Buffer
	f := &TableFormatter{}

	segments := []SegmentInfo{
		{ID: 1, Status: "Sealed", Size: 64 * 1024 * 1024, EntryCount: 100, FirstLogIndex: 1, LastModified: time.Now()},
		{ID: 2, Status: "Active", Size: 32 * 1024 * 1024, EntryCount: 50, FirstLogIndex: 101, LastModified: time.Now()},
	}

	err := f.WriteSegmentList(&buf, segments)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "ID")
	assert.Contains(t, output, "STATUS")
	assert.Contains(t, output, "Sealed")
	assert.Contains(t, output, "Active")
	assert.Contains(t, output, "00000001")
	assert.Contains(t, output, "00000002")
}

func TestTableFormatter_WriteSegmentDetail(t *testing.T) {
	var buf bytes.Buffer
	f := &TableFormatter{}

	detail := SegmentDetail{
		ID:            1,
		Status:        "Sealed",
		Size:          64 * 1024 * 1024,
		SizeHuman:     "67 MB",
		WriteOffset:   64 * 1024 * 1024,
		EntryCount:    100,
		FirstLogIndex: 1,
		Flags:         0x02,
		LastModified:  time.Now(),
	}

	err := f.WriteSegmentDetail(&buf, detail)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "Segment Details")
	assert.Contains(t, output, "Sealed")
	assert.Contains(t, output, "67 MB")
}

func TestTableFormatter_WriteSegmentDetail_WithIndex(t *testing.T) {
	var buf bytes.Buffer
	f := &TableFormatter{}

	detail := SegmentDetail{
		ID:            1,
		Status:        "Active",
		Size:          1024,
		WriteOffset:   512,
		EntryCount:    3,
		FirstLogIndex: 1,
		Flags:         0x01,
		LastModified:  time.Now(),
		IndexEntries: []IndexEntryInfo{
			{Index: 1, Offset: 64, Length: 100},
			{Index: 2, Offset: 164, Length: 200},
			{Index: 3, Offset: 364, Length: 150},
		},
	}

	err := f.WriteSegmentDetail(&buf, detail)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "Index Entries (3 total)")
	assert.Contains(t, output, "OFFSET")
	assert.Contains(t, output, "LENGTH")
}

func TestTableFormatter_WriteWalStats(t *testing.T) {
	var buf bytes.Buffer
	f := &TableFormatter{}

	stats := WalStats{
		TotalSegments:  3,
		SealedCount:    2,
		ActiveCount:    1,
		TotalEntries:   129443,
		TotalSize:      160 * 1024 * 1024,
		TotalSizeHuman: "168 MB",
		FirstLogIndex:  1,
		LastLogIndex:   129443,
	}

	err := f.WriteWalStats(&buf, stats)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "WAL Statistics")
	assert.Contains(t, output, "Total Segments:    3")
	assert.Contains(t, output, "Sealed:          2")
	assert.Contains(t, output, "Active:          1")
	assert.Contains(t, output, "129,443")
	assert.Contains(t, output, "168 MB")
}

func TestTableFormatter_WriteRestoreResult(t *testing.T) {
	var buf bytes.Buffer
	f := &TableFormatter{}

	result := RestoreResult{
		BTreeRestored:     true,
		BTreePath:         "/data/test/data.mdb",
		BTreeBytesWritten: 256 * 1024 * 1024,
		WALRestored:       true,
		WALPath:           "/data/test/wal",
		SegmentsRestored:  5,
	}

	err := f.WriteRestoreResult(&buf, result)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "Restored B-Tree")
	assert.Contains(t, output, "MB")
	assert.Contains(t, output, "Restored 5 WAL segments")
	assert.Contains(t, output, "Restore completed successfully")
}

func TestJSONFormatter_WriteSegmentList(t *testing.T) {
	var buf bytes.Buffer
	f := &JSONFormatter{}

	now := time.Now().Truncate(time.Second)
	segments := []SegmentInfo{
		{ID: 1, Status: "Sealed", Size: 64, EntryCount: 100, FirstLogIndex: 1, LastModified: now},
	}

	err := f.WriteSegmentList(&buf, segments)
	require.NoError(t, err)

	var result []SegmentInfo
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, uint32(1), result[0].ID)
	assert.Equal(t, "Sealed", result[0].Status)
	assert.Equal(t, int64(100), result[0].EntryCount)
}

func TestJSONFormatter_WriteSegmentDetail(t *testing.T) {
	var buf bytes.Buffer
	f := &JSONFormatter{}

	detail := SegmentDetail{
		ID:            1,
		Status:        "Active",
		Size:          1024,
		WriteOffset:   512,
		EntryCount:    10,
		FirstLogIndex: 1,
		Flags:         0x01,
		LastModified:  time.Now().Truncate(time.Second),
	}

	err := f.WriteSegmentDetail(&buf, detail)
	require.NoError(t, err)

	var result SegmentDetail
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)

	assert.Equal(t, uint32(1), result.ID)
	assert.Equal(t, "Active", result.Status)
}

func TestJSONFormatter_WriteWalStats(t *testing.T) {
	var buf bytes.Buffer
	f := &JSONFormatter{}

	stats := WalStats{
		TotalSegments: 3,
		SealedCount:   2,
		ActiveCount:   1,
		TotalEntries:  1000,
		TotalSize:     1024,
		FirstLogIndex: 1,
		LastLogIndex:  1000,
	}

	err := f.WriteWalStats(&buf, stats)
	require.NoError(t, err)

	var result WalStats
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)

	assert.Equal(t, 3, result.TotalSegments)
	assert.Equal(t, int64(1000), result.TotalEntries)
}

func TestJSONFormatter_WriteRestoreResult(t *testing.T) {
	var buf bytes.Buffer
	f := &JSONFormatter{}

	result := RestoreResult{
		BTreeRestored:     true,
		BTreePath:         "/data/test/data.mdb",
		BTreeBytesWritten: 1024,
		WALRestored:       false,
	}

	err := f.WriteRestoreResult(&buf, result)
	require.NoError(t, err)

	var parsed RestoreResult
	err = json.Unmarshal(buf.Bytes(), &parsed)
	require.NoError(t, err)

	assert.True(t, parsed.BTreeRestored)
	assert.Equal(t, "/data/test/data.mdb", parsed.BTreePath)
	assert.False(t, parsed.WALRestored)
}
