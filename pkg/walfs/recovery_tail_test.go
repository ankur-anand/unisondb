package walfs

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	b := make([]byte, 1)
	_, err = f.ReadAt(b, off)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b, off)
	require.NoError(t, err)
}

func readAllRecords(w *WALog) ([]string, error) {
	r := w.NewReader()
	defer r.Close()
	var out []string
	for {
		d, _, err := r.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, ErrNoNewData) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, string(d))
	}
}

// A crash can persist a later record while an earlier one is torn (page
// write-back order is not guaranteed). Recovery keeps the valid prefix; the
// stale record after it must not reconnect once a same-size rewrite lands.
func TestRecoveryTailCannotResurrectRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	var pos []RecordPosition
	for i, p := range []string{"rec-1-old", "rec-2-old", "rec-3-old"} {
		rp, err := w.Write([]byte(p), uint64(i+1))
		require.NoError(t, err)
		pos = append(pos, rp)
	}
	require.NoError(t, w.Close())
	// Crash image: record 2 torn, record 3 persisted.
	flipByte(t, SegmentFileName(dir, ".wal", 1), pos[1].Offset)

	w, err = NewWALog(dir, ".wal")
	require.NoError(t, err)
	_, last := w.GetBounds()
	require.Equal(t, uint64(1), last, "recovery keeps only the valid prefix")
	_, err = w.Write([]byte("rec-2-new"), 2)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	w, err = NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer w.Close()
	_, last = w.GetBounds()
	got, err := readAllRecords(w)
	require.NoError(t, err)
	require.Equal(t, uint64(2), last, "stale record resurrected: %q", got)
	require.Equal(t, []string{"rec-1-old", "rec-2-new"}, got)
}

// A sealed segment whose sidecar is missing and whose middle record is
// corrupt must not silently shrink: records past the corruption were durable,
// so opening fails instead of dropping them from lookups and bounds.
func TestSealedCorruptionWithoutSidecarIsNotSilent(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	var pos []RecordPosition
	for i, p := range []string{"rec-1", "rec-2", "rec-3"} {
		rp, err := w.Write([]byte(p), uint64(i+1))
		require.NoError(t, err)
		pos = append(pos, rp)
	}
	require.NoError(t, w.RotateSegment())
	_, err = w.Write([]byte("rec-4"), 4)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.NoError(t, os.Remove(SegmentIndexFileName(dir, ".wal", 1)))
	flipByte(t, SegmentFileName(dir, ".wal", 1), pos[1].Offset+recordHeaderSize) // payload of rec-2

	_, err = NewWALog(dir, ".wal")
	require.ErrorIs(t, err, ErrSegmentCorrupt)
}

// Recovery rewrites the active segment's header count to the valid prefix, so
// sealing it later records the true count. Otherwise the strict sealed rebuild
// would reject a healthy segment whose sidecar is missing.
func TestRecoveredCountSurvivesSealAndSidecarRebuild(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	var pos []RecordPosition
	for i, p := range []string{"rec-1", "rec-2", "rec-3"} {
		rp, err := w.Write([]byte(p), uint64(i+1))
		require.NoError(t, err)
		pos = append(pos, rp)
	}
	require.NoError(t, w.Close())
	flipByte(t, SegmentFileName(dir, ".wal", 1), pos[2].Offset) // lose rec-3 at the "crash"

	w, err = NewWALog(dir, ".wal")
	require.NoError(t, err)
	require.NoError(t, w.RotateSegment())
	_, err = w.Write([]byte("rec-3-new"), 3)
	require.NoError(t, err)
	w.Current().WaitForIndexFlush()
	for _, seg := range w.Segments() {
		seg.WaitForIndexFlush()
	}
	require.NoError(t, w.Close())
	require.NoError(t, os.Remove(SegmentIndexFileName(dir, ".wal", 1)))

	w, err = NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer w.Close()
	first, last := w.GetBounds()
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(3), last)
	got, err := readAllRecords(w)
	require.NoError(t, err)
	require.Equal(t, []string{"rec-1", "rec-2", "rec-3-new"}, got)
}
