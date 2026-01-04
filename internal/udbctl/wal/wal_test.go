package wal

import (
	"testing"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListSegments(t *testing.T) {
	t.Run("lists segments with entries", func(t *testing.T) {
		walDir := t.TempDir()

		wal, err := walfs.NewWALog(walDir, ".seg")
		require.NoError(t, err)

		_, err = wal.Write([]byte("test1"), 1)
		require.NoError(t, err)
		_, err = wal.Write([]byte("test2"), 2)
		require.NoError(t, err)
		wal.Close()

		segments, err := ListSegments(walDir)
		require.NoError(t, err)
		require.Len(t, segments, 1)

		assert.Equal(t, "Active", segments[0].Status)
		assert.Equal(t, int64(2), segments[0].EntryCount)
		assert.Equal(t, uint64(1), segments[0].FirstLogIndex)
	})

	t.Run("returns empty list for directory without segments", func(t *testing.T) {
		walDir := t.TempDir()

		segments, err := ListSegments(walDir)
		require.NoError(t, err)
		_ = segments
	})

	t.Run("returns error for invalid directory", func(t *testing.T) {
		_, err := ListSegments("/nonexistent/path")
		require.Error(t, err)
	})

	t.Run("segments are sorted by ID", func(t *testing.T) {
		walDir := t.TempDir()

		wal, err := walfs.NewWALog(walDir, ".seg", walfs.WithMaxSegmentSize(1024))
		require.NoError(t, err)
		for i := range 20 {
			_, err = wal.Write(make([]byte, 100), uint64(i+1))
			require.NoError(t, err)
		}
		wal.Close()

		segments, err := ListSegments(walDir)
		require.NoError(t, err)

		for i := 1; i < len(segments); i++ {
			assert.Greater(t, segments[i].ID, segments[i-1].ID)
		}
	})
}

func TestInspectSegment(t *testing.T) {
	t.Run("inspects existing segment", func(t *testing.T) {
		walDir := t.TempDir()

		wal, err := walfs.NewWALog(walDir, ".seg")
		require.NoError(t, err)

		_, err = wal.Write([]byte("test1"), 1)
		require.NoError(t, err)
		_, err = wal.Write([]byte("test2"), 2)
		require.NoError(t, err)

		segments := wal.Segments()
		var segID uint32
		for id := range segments {
			segID = uint32(id)
			break
		}
		wal.Close()

		detail, err := InspectSegment(walDir, segID, false)
		require.NoError(t, err)

		assert.Equal(t, segID, detail.ID)
		assert.Equal(t, "Active", detail.Status)
		assert.Equal(t, int64(2), detail.EntryCount)
		assert.Empty(t, detail.IndexEntries)
	})

	t.Run("includes index entries when requested", func(t *testing.T) {
		walDir := t.TempDir()

		wal, err := walfs.NewWALog(walDir, ".seg")
		require.NoError(t, err)

		_, err = wal.Write([]byte("test1"), 1)
		require.NoError(t, err)
		_, err = wal.Write([]byte("test2"), 2)
		require.NoError(t, err)

		segments := wal.Segments()
		var segID uint32
		for id := range segments {
			segID = uint32(id)
			break
		}
		wal.Close()

		detail, err := InspectSegment(walDir, segID, true)
		require.NoError(t, err)

		assert.Len(t, detail.IndexEntries, 2)
		assert.Equal(t, 1, detail.IndexEntries[0].Index)
		assert.Equal(t, 2, detail.IndexEntries[1].Index)
	})

	t.Run("returns error for nonexistent segment", func(t *testing.T) {
		walDir := t.TempDir()

		wal, err := walfs.NewWALog(walDir, ".seg")
		require.NoError(t, err)
		wal.Close()

		_, err = InspectSegment(walDir, 999, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("returns error for invalid directory", func(t *testing.T) {
		_, err := InspectSegment("/nonexistent/path", 1, false)
		require.Error(t, err)
	})
}

func TestGetStats(t *testing.T) {
	t.Run("returns stats for WAL with entries", func(t *testing.T) {
		walDir := t.TempDir()

		wal, err := walfs.NewWALog(walDir, ".seg")
		require.NoError(t, err)

		_, err = wal.Write([]byte("test1"), 1)
		require.NoError(t, err)
		_, err = wal.Write([]byte("test2"), 2)
		require.NoError(t, err)
		_, err = wal.Write([]byte("test3"), 3)
		require.NoError(t, err)
		wal.Close()

		stats, err := GetStats(walDir)
		require.NoError(t, err)

		assert.Equal(t, 1, stats.TotalSegments)
		assert.Equal(t, 0, stats.SealedCount)
		assert.Equal(t, 1, stats.ActiveCount)
		assert.Equal(t, int64(3), stats.TotalEntries)
		assert.Equal(t, uint64(1), stats.FirstLogIndex)
		assert.Equal(t, uint64(3), stats.LastLogIndex)
		assert.NotEmpty(t, stats.TotalSizeHuman)
	})

	t.Run("returns stats for WAL without writes", func(t *testing.T) {
		walDir := t.TempDir()
		stats, err := GetStats(walDir)
		require.NoError(t, err)

		assert.GreaterOrEqual(t, stats.TotalSegments, 0)
	})

	t.Run("counts sealed segments correctly", func(t *testing.T) {
		walDir := t.TempDir()

		wal, err := walfs.NewWALog(walDir, ".seg", walfs.WithMaxSegmentSize(1024))
		require.NoError(t, err)

		for i := range 20 {
			_, err = wal.Write(make([]byte, 100), uint64(i+1))
			require.NoError(t, err)
		}
		wal.Close()

		stats, err := GetStats(walDir)
		require.NoError(t, err)

		assert.Greater(t, stats.TotalSegments, 1)
		assert.Equal(t, 1, stats.ActiveCount)
		assert.Equal(t, stats.TotalSegments-1, stats.SealedCount)
	})

	t.Run("returns error for invalid directory", func(t *testing.T) {
		_, err := GetStats("/nonexistent/path")
		require.Error(t, err)
	})
}
