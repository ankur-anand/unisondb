package wal

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
		assert.Empty(t, segments)
		entries, err := os.ReadDir(walDir)
		require.NoError(t, err)
		assert.Empty(t, entries)
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

		assert.Zero(t, stats.TotalSegments)
		entries, err := os.ReadDir(walDir)
		require.NoError(t, err)
		assert.Empty(t, entries)
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

func TestInspectionDoesNotModifyStorage(t *testing.T) {
	operations := map[string]func(string) error{
		"list":    func(dir string) error { _, err := ListSegments(dir); return err },
		"inspect": func(dir string) error { _, err := InspectSegment(dir, 1, true); return err },
		"stats":   func(dir string) error { _, err := GetStats(dir); return err },
	}
	for name, inspect := range operations {
		for _, sealed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sealed=%t", name, sealed), func(t *testing.T) {
				dir := t.TempDir()
				for id := uint32(1); id <= 2; id++ {
					seg, err := walfs.OpenSegmentFile(dir, ".seg", id, walfs.WithSegmentSize(1024))
					require.NoError(t, err)
					_, err = seg.Write([]byte("record"), uint64(id))
					require.NoError(t, err)
					if sealed {
						require.NoError(t, seg.SealSegment())
					}
					require.NoError(t, seg.Close())
					if sealed {
						// Inspection must not recreate a missing index sidecar.
						require.NoError(t, os.Remove(walfs.SegmentIndexFileName(dir, ".seg", id)))
					}
				}
				before := snapshotStorage(t, dir)
				require.NoError(t, inspect(dir))
				assert.Equal(t, before, snapshotStorage(t, dir))
			})
		}
	}
}

func TestInspectionOnReadOnlyStorage(t *testing.T) {
	dir := t.TempDir()
	seg, err := walfs.OpenSegmentFile(dir, ".seg", 1, walfs.WithSegmentSize(1024))
	require.NoError(t, err)
	_, err = seg.Write([]byte("record"), 1)
	require.NoError(t, err)
	require.NoError(t, seg.Close())
	path := walfs.SegmentFileName(dir, ".seg", 1)
	require.NoError(t, os.Chmod(path, 0444))
	require.NoError(t, os.Chmod(dir, 0555))
	t.Cleanup(func() {
		require.NoError(t, os.Chmod(dir, 0755))
		require.NoError(t, os.Chmod(path, 0644))
	})
	before := snapshotStorage(t, dir)
	segments, err := ListSegments(dir)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	assert.Equal(t, int64(1024), segments[0].Size)
	detail, err := InspectSegment(dir, 1, true)
	require.NoError(t, err)
	assert.Len(t, detail.IndexEntries, 1)
	stats, err := GetStats(dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1024), stats.TotalSize)
	assert.Equal(t, before, snapshotStorage(t, dir))
}

func TestInspectionErrorsDoNotModifyStorage(t *testing.T) {
	t.Run("missing segment", func(t *testing.T) {
		dir := t.TempDir()
		_, err := InspectSegment(dir, 1, true)
		require.ErrorContains(t, err, "not found")
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
	t.Run("corrupt segment", func(t *testing.T) {
		dir := t.TempDir()
		path := walfs.SegmentFileName(dir, ".seg", 1)
		require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0644))
		before := snapshotStorage(t, dir)
		_, err := ListSegments(dir)
		require.Error(t, err)
		_, err = InspectSegment(dir, 1, true)
		require.Error(t, err)
		_, err = GetStats(dir)
		require.Error(t, err)
		assert.Equal(t, before, snapshotStorage(t, dir))
	})
}

func snapshotStorage(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	files := make(map[string][32]byte)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = sha256.Sum256(data)
		return nil
	})
	require.NoError(t, err)
	return files
}
