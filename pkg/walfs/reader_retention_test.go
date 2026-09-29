package walfs

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newNumberedWAL writes n records whose payload starts with their LSN, into
// small segments so they span several of them.
func newNumberedWAL(t *testing.T, n int, opts ...WALogOptions) *WALog {
	t.Helper()
	opts = append([]WALogOptions{WithMaxSegmentSize(1024)}, opts...)
	w, err := NewWALog(t.TempDir(), ".wal", opts...)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		payload := make([]byte, 100)
		binary.LittleEndian.PutUint64(payload, uint64(i))
		_, err := w.Write(payload, uint64(i))
		require.NoError(t, err)
	}
	return w
}

func lsnOf(data []byte) uint64 { return binary.LittleEndian.Uint64(data) }

func segmentIDs(w *WALog) []SegmentID {
	var ids []SegmentID
	for _, seg := range *w.segmentSnapshot.Load() {
		ids = append(ids, seg.ID())
	}
	return ids
}

// A reader pinned on the oldest segment used to let the cleaner delete the
// younger queued segments behind it, and the reader then jumped the hole.
func TestRetentionNeverDeletesBehindPinnedSegment(t *testing.T) {
	w := newNumberedWAL(t, 40, WithAutoCleanupPolicy(0, 1, 2, true))
	defer w.Close()
	require.GreaterOrEqual(t, len(w.Segments()), 5)

	r := w.NewReader()
	defer r.Close()
	data, _, err := r.Next() // pins segment 1
	require.NoError(t, err)
	require.EqualValues(t, 1, lsnOf(data))

	w.MarkSegmentsForDeletion()
	require.Greater(t, len(w.QueuedSegmentsForDeletion()), 1)
	before := segmentIDs(w)
	w.cleanPendingSegments(func(SegmentID) bool { return true })
	require.Equal(t, before, segmentIDs(w), "nothing may go while the oldest segment is pinned")

	// The reader still sees every record, in order.
	prev := lsnOf(data)
	for {
		data, _, err := r.Next()
		if errors.Is(err, ErrNoNewData) {
			break
		}
		require.NoError(t, err)
		require.Equal(t, prev+1, lsnOf(data))
		prev = lsnOf(data)
	}
	require.EqualValues(t, 40, prev)

	// Once released, the prefix goes, oldest first.
	r.Close()
	w.cleanPendingSegments(func(SegmentID) bool { return true })
	after := segmentIDs(w)
	require.Less(t, len(after), len(before))
	require.Equal(t, before[len(before)-len(after):], after, "retention trims a prefix")
}

// The first segment the predicate rejects stops the pass, even when a later
// segment would be allowed.
func TestRetentionStopsAtFirstRejectedSegment(t *testing.T) {
	w := newNumberedWAL(t, 40, WithAutoCleanupPolicy(0, 1, 2, true))
	defer w.Close()
	w.MarkSegmentsForDeletion()
	before := segmentIDs(w)
	w.cleanPendingSegments(func(id SegmentID) bool { return id != 1 })
	require.Equal(t, before, segmentIDs(w))

	w.cleanPendingSegments(func(id SegmentID) bool { return id <= 2 })
	require.Equal(t, before[2:], segmentIDs(w))
}

// A reader whose next segment was removed before it got there must fail
// loudly instead of continuing from a later segment.
func TestReaderReportsSegmentRemovedAhead(t *testing.T) {
	w := newNumberedWAL(t, 40, WithAutoCleanupPolicy(0, 1, 2, true))
	defer w.Close()
	pos, err := w.PositionForIndex(12)
	require.NoError(t, err)
	require.Greater(t, pos.SegmentID, SegmentID(1))

	r, err := w.NewReaderWithStart(pos) // no pin until the first Next
	require.NoError(t, err)
	defer r.Close()

	w.MarkSegmentsForDeletion()
	w.cleanPendingSegments(func(SegmentID) bool { return true })
	require.NotContains(t, segmentIDs(w), pos.SegmentID)

	for range 3 {
		data, _, err := r.Next()
		require.ErrorIs(t, err, ErrSegmentUnavailable, "got a record for LSN %d instead", lsnOf(append(data, make([]byte, 8)...)))
	}
}

// Close wins the race: it switches the segment to Closing after NewReader has
// pinned but before it checks the state. The reader must withdraw its pin and
// return nil, and Close must wait for that instead of unmapping under it.
func TestNewReaderWithdrawsPinWhenCloseWins(t *testing.T) {
	w := newNumberedWAL(t, 3)
	seg := w.Current()

	closed := make(chan error, 1)
	newReaderPinnedHook = func(s *Segment) {
		go func() { closed <- s.Close() }()
		require.Eventually(t, func() bool { return s.state.Load() == StateClosing }, time.Second, time.Millisecond)
		select {
		case err := <-closed:
			closed <- err
			t.Error("Close finished while a reader held a pin")
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Cleanup(func() { newReaderPinnedHook = nil })

	require.Nil(t, seg.NewReader())
	newReaderPinnedHook = nil
	require.NoError(t, <-closed)
	require.EqualValues(t, 0, seg.refCount.Load())
	require.NoError(t, w.Close())
}

// The reader wins the race: once it holds a pin on an open segment, Close
// waits for it and the mapping stays valid for every read.
func TestCloseWaitsForPinnedReader(t *testing.T) {
	w := newNumberedWAL(t, 3)
	seg := w.Current()
	reader := seg.NewReader()
	require.NotNil(t, reader)

	closed := make(chan error, 1)
	go func() { closed <- seg.Close() }()
	require.Eventually(t, func() bool { return seg.state.Load() == StateClosing }, time.Second, time.Millisecond)
	for i := uint64(1); i <= 3; i++ {
		data, _, err := reader.Next()
		require.NoError(t, err)
		require.Equal(t, i, lsnOf(data))
	}
	select {
	case <-closed:
		t.Fatal("Close finished while a reader held a pin")
	case <-time.After(20 * time.Millisecond):
	}
	reader.Close()
	require.NoError(t, <-closed)
	require.NoError(t, w.Close())
}

// Readers stream while retention deletes segments. Every reader either sees
// contiguous LSNs or stops with an error: never a crash, never a silent gap.
func TestStreamingReadersRaceRetention(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		w := newNumberedWAL(t, 60, WithAutoCleanupPolicy(0, 1, 2, true))
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := w.NewReader()
				defer r.Close()
				var prev uint64
				for {
					data, _, err := r.Next()
					if err != nil {
						if !errors.Is(err, ErrNoNewData) && !errors.Is(err, ErrSegmentUnavailable) {
							t.Errorf("unexpected error: %v", err)
						}
						return
					}
					lsn := lsnOf(data)
					if prev != 0 && lsn != prev+1 {
						t.Errorf("silent gap: LSN %d after %d", lsn, prev)
						return
					}
					prev = lsn
				}
			}()
		}
		w.MarkSegmentsForDeletion()
		w.cleanPendingSegments(func(SegmentID) bool { return true })
		wg.Wait()
		require.NoError(t, w.Close())
	}
}

// runCleaner runs one cleaner pass in the background.
func runCleaner(w *WALog) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		w.cleanPendingSegments(func(SegmentID) bool { return true })
		close(done)
	}()
	return done
}

func setUnpublishedHook(t *testing.T, fn func(*Segment)) {
	var once sync.Once
	segmentUnpublishedHook = func(s *Segment) { once.Do(func() { fn(s) }) }
	t.Cleanup(func() { segmentUnpublishedHook = nil })
}

// A reader that pins the oldest segment after the cleaner's check makes Close
// wait. That wait must happen outside writeMu: writes carry on, the segment is
// already gone from lookups, and the reader keeps a valid mapping.
func TestRetentionWaitForReaderDoesNotBlockWriters(t *testing.T) {
	w := newNumberedWAL(t, 40, WithAutoCleanupPolicy(0, 1, 2, true))
	defer w.Close()
	oldest := w.Segments()[1]
	count := uint64(oldest.GetEntryCount())

	pinned := make(chan *SegmentReader, 1)
	setUnpublishedHook(t, func(s *Segment) {
		// A reader created from an earlier snapshot pins it now.
		pinned <- s.NewReader()
	})
	w.MarkSegmentsForDeletion()
	done := runCleaner(w)
	reader := <-pinned
	require.NotNil(t, reader)
	require.Eventually(t, func() bool { return oldest.state.Load() == StateClosing }, time.Second, time.Millisecond)

	wrote := make(chan error, 1)
	go func() {
		payload := make([]byte, 100)
		binary.LittleEndian.PutUint64(payload, 41)
		_, err := w.Write(payload, 41)
		wrote <- err
	}()
	select {
	case err := <-wrote:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		reader.Close() // let the cleaner finish so the deferred Close returns
		<-wrote
		t.Fatal("Write blocked while the cleaner waited for a reader")
	}
	_, err := w.PositionForIndex(1)
	require.Error(t, err, "an unpublished segment must not resolve")

	for i := uint64(1); i <= count; i++ {
		data, _, err := reader.Next()
		require.NoError(t, err)
		require.Equal(t, i, lsnOf(data))
	}
	select {
	case <-done:
		t.Fatal("cleaner finished while a reader still held the segment")
	case <-time.After(20 * time.Millisecond):
	}
	reader.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleaner did not finish after the reader let go")
	}
	_, err = os.Stat(SegmentFileName(w.dir, ".wal", oldest.ID()))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// WALog.Close must not return while the cleaner is still closing and deleting
// a segment it already unpublished.
func TestWALCloseWaitsForInFlightRemoval(t *testing.T) {
	w := newNumberedWAL(t, 40, WithAutoCleanupPolicy(0, 1, 2, true))
	entered := make(chan struct{})
	release := make(chan struct{})
	setUnpublishedHook(t, func(*Segment) {
		close(entered)
		<-release
	})
	w.MarkSegmentsForDeletion()
	done := runCleaner(w)
	<-entered

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	select {
	case err := <-closed:
		closed <- err
		t.Error("WALog.Close returned while a removal was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done
	require.NoError(t, <-closed)
}

// A removal that fails after unpublishing (here the unlink, because the
// directory is read-only) is finished first on the next pass, before anything
// younger is deleted, so segment files leave the disk oldest first.
func TestRetentionFinishesFailedRemovalFirst(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	w := newNumberedWAL(t, 40, WithAutoCleanupPolicy(0, 1, 2, true))
	defer w.Close()
	before := segmentIDs(w)
	w.MarkSegmentsForDeletion()

	require.NoError(t, os.Chmod(w.dir, 0o555))
	w.cleanPendingSegments(func(SegmentID) bool { return true })
	require.NoError(t, os.Chmod(w.dir, 0o755))
	require.Equal(t, before[1:], segmentIDs(w), "only the failed segment is unpublished")
	require.NotNil(t, w.unlinking)
	for _, id := range before {
		_, err := os.Stat(SegmentFileName(w.dir, ".wal", id))
		require.NoError(t, err, "segment %d: nothing may be deleted after a failed removal", id)
	}

	w.cleanPendingSegments(func(SegmentID) bool { return true })
	require.Nil(t, w.unlinking)
	require.Len(t, segmentIDs(w), 2)
	for _, id := range before[:len(before)-2] {
		_, err := os.Stat(SegmentFileName(w.dir, ".wal", id))
		require.ErrorIs(t, err, os.ErrNotExist, "segment %d left on disk", id)
	}
}

// A crash after unpublishing but before the unlink leaves the file on disk.
// On reopen it is the oldest segment again, contiguous with the rest, and
// retention removes it again.
func TestUnpublishedSegmentSurvivingCrashIsHarmless(t *testing.T) {
	w := newNumberedWAL(t, 40, WithAutoCleanupPolicy(0, 1, 2, true))
	dir := w.dir
	var saved []byte
	var savedID SegmentID
	setUnpublishedHook(t, func(s *Segment) {
		savedID = s.ID()
		var err error
		saved, err = os.ReadFile(SegmentFileName(dir, ".wal", savedID))
		require.NoError(t, err)
	})
	first := segmentIDs(w)[0]
	w.MarkSegmentsForDeletion()
	// The crash stops the pass at its first segment.
	w.cleanPendingSegments(func(id SegmentID) bool { return id == first })
	require.NoError(t, w.Close())
	require.Equal(t, first, savedID)
	// Put the file back as if the unlink never reached disk.
	require.NoError(t, os.WriteFile(SegmentFileName(dir, ".wal", savedID), saved, 0o644))

	w, err := NewWALog(dir, ".wal", WithMaxSegmentSize(1024), WithAutoCleanupPolicy(0, 1, 2, true))
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, savedID, segmentIDs(w)[0])
	pos, err := w.PositionForIndex(1)
	require.NoError(t, err)
	r, err := w.NewReaderWithStart(pos)
	require.NoError(t, err)
	var prev uint64
	for {
		data, _, err := r.Next()
		if err != nil {
			require.ErrorIs(t, err, ErrNoNewData)
			break
		}
		lsn := lsnOf(data)
		if lsn != prev+1 {
			t.Fatalf("gap after restored segment: LSN %d after %d", lsn, prev)
		}
		prev = lsn
	}
	r.Close()

	w.MarkSegmentsForDeletion()
	w.cleanPendingSegments(func(SegmentID) bool { return true })
	require.NotContains(t, segmentIDs(w), savedID)
}
