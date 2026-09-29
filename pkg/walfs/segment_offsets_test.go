package walfs

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Varied record sizes make sparse samples land at irregular ordinals, so the
// header walk is exercised across sampled boundaries.
func TestPositionForIndex_EveryRecordAcrossSealAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := []WALogOptions{WithMaxSegmentSize(256 << 10)}
	w, err := NewWALog(dir, ".wal", opts...)
	require.NoError(t, err)

	rnd := rand.New(rand.NewSource(1))
	const n = 5000
	positions := make([]RecordPosition, n+1)
	for i := uint64(1); i <= n; i++ {
		pos, err := w.Write(bytes.Repeat([]byte{byte(i)}, 1+rnd.Intn(3000)), i)
		require.NoError(t, err)
		positions[i] = pos
	}
	require.Greater(t, len(w.Segments()), 10)

	check := func(w *WALog) {
		t.Helper()
		first, last := w.GetBounds()
		require.Equal(t, uint64(1), first)
		require.Equal(t, uint64(n), last)
		for i := uint64(1); i <= n; i++ {
			got, err := w.PositionForIndex(i)
			require.NoError(t, err, "index %d", i)
			require.Equal(t, positions[i], got, "index %d", i)
		}
		_, err := w.PositionForIndex(n + 1)
		require.Error(t, err)
		_, err = w.PositionForIndex(0)
		require.Error(t, err)
		for _, seg := range w.Segments() {
			if seg.IsSealed() {
				assert.Nil(t, seg.dense.Load(), "sealed segment %d keeps a per-record index", seg.ID())
			}
		}
	}
	check(w)
	require.NoError(t, w.Close())

	w, err = NewWALog(dir, ".wal", opts...)
	require.NoError(t, err)
	defer w.Close()
	check(w)

	// Appends continue the positional mapping after reopen.
	pos, err := w.Write([]byte("next"), n+1)
	require.NoError(t, err)
	got, err := w.PositionForIndex(n + 1)
	require.NoError(t, err)
	assert.Equal(t, pos, got)
}

func TestPositionForIndex_ConcurrentWithWritesAndRotation(t *testing.T) {
	w, err := NewWALog(t.TempDir(), ".wal", WithMaxSegmentSize(64<<10))
	require.NoError(t, err)
	defer w.Close()

	var positions sync.Map
	var written atomic.Uint64
	done := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-done:
					return
				default:
				}
				last := written.Load()
				if last == 0 {
					continue
				}
				idx := 1 + uint64(rnd.Int63n(int64(last)))
				got, err := w.PositionForIndex(idx)
				if !assert.NoError(t, err, "index %d", idx) {
					return
				}
				want, _ := positions.Load(idx)
				if !assert.Equal(t, want, got, "index %d", idx) {
					return
				}
			}
		}(int64(r))
	}
	for i := uint64(1); i <= 20000; i++ {
		pos, err := w.Write(bytes.Repeat([]byte{'x'}, 100), i)
		require.NoError(t, err)
		positions.Store(i, pos)
		written.Store(i)
	}
	close(done)
	wg.Wait()
}

func TestPositionForIndex_AfterTruncate(t *testing.T) {
	w, err := NewWALog(t.TempDir(), ".wal", WithMaxSegmentSize(16<<10))
	require.NoError(t, err)
	defer w.Close()

	for i := uint64(1); i <= 300; i++ {
		_, err := w.Write(bytes.Repeat([]byte{'t'}, 90), i)
		require.NoError(t, err)
	}
	require.NoError(t, w.Truncate(150))

	first, last := w.GetBounds()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(150), last)
	_, err = w.PositionForIndex(151)
	assert.Error(t, err)
	_, err = w.PositionForIndex(150)
	assert.NoError(t, err)

	pos, err := w.Write([]byte("after"), 151)
	require.NoError(t, err)
	got, err := w.PositionForIndex(151)
	require.NoError(t, err)
	assert.Equal(t, pos, got)
	data, err := w.Read(got)
	require.NoError(t, err)
	assert.Equal(t, []byte("after"), data)
}

// Lookups walk a sealed segment's mapping and hold no reference, so Close
// must not unmap while one is mid-walk. Retention deletes segment 1 while
// lookups and IndexEntries hammer it; before the fix this crashed (SIGSEGV).
func TestLookupDuringSegmentRemovalDoesNotCrash(t *testing.T) {
	for iter := 0; iter < 50; iter++ { // the unfixed code crashes within 50
		w, err := NewWALog(t.TempDir(), ".wal", WithMaxSegmentSize(64<<10), WithAutoCleanupPolicy(0, 0, 1, true))
		require.NoError(t, err)
		// 1-byte records: many records between samples, so each lookup walks far.
		var lsn uint64
		for len(w.Segments()) < 3 {
			lsn++
			_, err := w.Write([]byte{1}, lsn)
			require.NoError(t, err)
		}
		seg1 := w.Segments()[1]
		last1 := seg1.firstLSN.Load() + seg1.indexedCount() - 1

		var stop atomic.Bool
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for !stop.Load() {
					if g == 0 {
						_ = seg1.IndexEntries()
					} else {
						_, _ = w.PositionForIndex(last1)
					}
				}
			}(g)
		}
		w.MarkSegmentsForDeletion()
		w.cleanPendingSegments(func(SegmentID) bool { return true })
		stop.Store(true)
		wg.Wait()

		_, err = w.PositionForIndex(last1)
		require.Error(t, err, "removed segment must not resolve")
		require.Nil(t, seg1.IndexEntries(), "closed segment has no entries")
		require.NoError(t, w.Close())
	}
}
