package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/gofrs/flock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const truncateTestNS = "ns"

// newNamespaceWAL writes records first..first+n-1 into <data-dir>/ns/wal, in
// small segments so they span several, and returns the data directory.
func newNamespaceWAL(t *testing.T, first uint64, n int) string {
	t.Helper()
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, truncateTestNS, walDirName)
	require.NoError(t, os.MkdirAll(walDir, 0o755))
	wl, err := walfs.NewWALog(walDir, segmentExt, walfs.WithMaxSegmentSize(1024))
	require.NoError(t, err)
	for lsn := first; lsn < first+uint64(n); lsn++ {
		payload := make([]byte, 100)
		binary.LittleEndian.PutUint64(payload, lsn)
		_, err := wl.Write(payload, lsn)
		require.NoError(t, err)
	}
	require.NoError(t, wl.Close())
	return dataDir
}

func openNamespaceWAL(t *testing.T, dataDir string) *walfs.WALog {
	t.Helper()
	wl, err := walfs.NewWALog(filepath.Join(dataDir, truncateTestNS, walDirName), segmentExt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wl.Close() })
	return wl
}

func TestTruncateRemovesRecordsAfterLSN(t *testing.T) {
	dataDir := newNamespaceWAL(t, 1, 40)
	result, err := Truncate(TruncateOptions{DataDir: dataDir, Namespace: truncateTestNS, KeepThrough: 12})
	require.NoError(t, err)
	assert.False(t, result.DryRun)
	assert.EqualValues(t, 1, result.FirstLSN)
	assert.EqualValues(t, 40, result.LastLSNBefore)
	assert.EqualValues(t, 28, result.RecordsRemoved)
	assert.Positive(t, result.SegmentsRemoved)

	wl := openNamespaceWAL(t, dataDir)
	first, last := wl.GetBounds()
	assert.EqualValues(t, 1, first)
	assert.EqualValues(t, 12, last)

	r := wl.NewReader()
	defer r.Close()
	for lsn := uint64(1); lsn <= 12; lsn++ {
		data, _, err := r.Next()
		require.NoError(t, err)
		require.Equal(t, lsn, binary.LittleEndian.Uint64(data))
	}
	_, _, err = r.Next()
	require.True(t, errors.Is(err, walfs.ErrNoNewData) || errors.Is(err, io.EOF), "got %v", err)

	_, err = wl.Write(make([]byte, 100), 13)
	require.NoError(t, err, "the WAL accepts the next LSN after truncation")
}

func TestTruncateDryRunKeepsRecords(t *testing.T) {
	dataDir := newNamespaceWAL(t, 1, 40)
	result, err := Truncate(TruncateOptions{DataDir: dataDir, Namespace: truncateTestNS, KeepThrough: 12, DryRun: true})
	require.NoError(t, err)
	assert.True(t, result.DryRun)
	assert.EqualValues(t, 28, result.RecordsRemoved)

	wl := openNamespaceWAL(t, dataDir)
	_, last := wl.GetBounds()
	assert.EqualValues(t, 40, last)
}

func TestTruncateRefusesWhileServerHoldsLock(t *testing.T) {
	dataDir := newNamespaceWAL(t, 1, 40)
	server := flock.New(filepath.Join(dataDir, truncateTestNS, pidLockName))
	locked, err := server.TryLock()
	require.NoError(t, err)
	require.True(t, locked)
	defer func() { _ = server.Unlock() }()

	walDir := filepath.Join(dataDir, truncateTestNS, walDirName)
	before := snapshotStorage(t, walDir)
	_, err = Truncate(TruncateOptions{DataDir: dataDir, Namespace: truncateTestNS, KeepThrough: 12})
	require.ErrorContains(t, err, "pid.lock held")
	assert.Equal(t, before, snapshotStorage(t, walDir), "nothing may change while the server holds the lock")
}

func TestTruncateRejectsOutOfRangeLSN(t *testing.T) {
	dataDir := newNamespaceWAL(t, 10, 20) // LSN 10..29
	for _, tc := range []struct {
		keep uint64
		want string
	}{
		{0, "at least 1"},
		{9, "older than the oldest record"},
		{29, "nothing to truncate"},
		{100, "nothing to truncate"},
	} {
		_, err := Truncate(TruncateOptions{DataDir: dataDir, Namespace: truncateTestNS, KeepThrough: tc.keep})
		require.ErrorContains(t, err, tc.want, "keep-through %d", tc.keep)
	}
	wl := openNamespaceWAL(t, dataDir)
	first, last := wl.GetBounds()
	assert.EqualValues(t, 10, first)
	assert.EqualValues(t, 29, last)
}

func TestTruncateMissingNamespaceCreatesNothing(t *testing.T) {
	dataDir := t.TempDir()
	_, err := Truncate(TruncateOptions{DataDir: dataDir, Namespace: "missing", KeepThrough: 5})
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dataDir, "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
