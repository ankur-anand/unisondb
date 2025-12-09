package walfs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTruncateWithActiveReaders(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
	require.NoError(t, err)
	defer wl.Close()

	payload := make([]byte, 100)
	for i := 0; i < 50; i++ {
		_, err := wl.Write(payload, uint64(i+1))
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	segments := wl.Segments()
	require.Greater(t, len(segments), 2, "Expected multiple segments")

	targetSegID := SegmentID(1)
	targetSeg, ok := segments[targetSegID]
	require.True(t, ok, "Segment 1 should exist")

	reader := targetSeg.NewReader()
	require.NotNil(t, reader)
	defer reader.Close()

	_, _, err = reader.Next()
	require.NoError(t, err)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- wl.Truncate(0)
	}()

	select {
	case err := <-doneCh:
		require.Error(t, err, "Truncate should fail when active readers exist for full delete")
	case <-time.After(2 * time.Second):
		t.Fatal("Truncate blocked for too long")
	}

	_, err = os.Stat(targetSeg.path)
	assert.NoError(t, err, "Segment file should still exist due to active reader and failed truncate")

	currentSegments := wl.Segments()
	_, exists := currentSegments[targetSegID]
	assert.True(t, exists, "Segment should remain in WAL after failed truncate")
}

func TestTruncateWithActiveReaders_NewSegmentIDDoesNotReuse(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
	require.NoError(t, err)
	defer wl.Close()

	payload := make([]byte, 100)
	for i := 0; i < 50; i++ {
		_, err := wl.Write(payload, uint64(i+1))
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	segments := wl.Segments()
	require.Greater(t, len(segments), 2, "Expected multiple segments")

	var maxID SegmentID
	for id := range segments {
		if id > maxID {
			maxID = id
		}
	}

	targetSeg := segments[1]
	reader := targetSeg.NewReader()
	require.NotNil(t, reader)
	defer reader.Close()

	_, _, err = reader.Next()
	require.NoError(t, err)

	err = wl.Truncate(0)
	require.Error(t, err, "Truncate to 0 should fail when active readers exist")

	current := wl.Current()
	require.NotNil(t, current)
	assert.Equal(t, maxID, current.ID(), "No new segment should be created when truncate fails")

	_, err = os.Stat(targetSeg.path)
	assert.NoError(t, err, "Original segment should remain on disk")
}

func TestTruncateWithinTailWithActiveReaders(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
	require.NoError(t, err)
	defer wl.Close()

	payload := make([]byte, 100)
	for i := 0; i < 50; i++ {
		_, err := wl.Write(payload, uint64(i+1))
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	segments := wl.Segments()
	require.Greater(t, len(segments), 2, "Expected multiple segments")

	var maxID SegmentID
	for id := range segments {
		if id > maxID {
			maxID = id
		}
	}

	targetID, _, err := wl.SegmentForIndex(45)
	require.NoError(t, err)

	targetSeg := segments[1]
	reader := targetSeg.NewReader()
	require.NotNil(t, reader)
	defer reader.Close()
	_, _, err = reader.Next()
	require.NoError(t, err)

	err = wl.Truncate(45)
	require.NoError(t, err)

	current := wl.Current()
	require.NotNil(t, current)
	assert.Equal(t, targetID, current.ID(), "Truncating within the tail should keep the segment containing the cut as current")
	assert.LessOrEqual(t, current.ID(), maxID, "Truncate should not create a new segment ID when cutting within tail")

	_, err = os.Stat(targetSeg.path)
	assert.NoError(t, err, "Earlier segment should stay on disk due to active reader")

	reader.Close()
}

func TestTruncateTargetSegmentWithActiveReader(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
	require.NoError(t, err)
	defer wl.Close()

	payload := make([]byte, 100)
	for i := 0; i < 50; i++ {
		_, err := wl.Write(payload, uint64(i+1))
		require.NoError(t, err)
	}

	segments := wl.Segments()

	var maxID SegmentID
	for id := range segments {
		if id > maxID {
			maxID = id
		}
	}
	targetID, _, err := wl.SegmentForIndex(45)
	require.NoError(t, err)

	targetSeg := segments[targetID]

	reader := targetSeg.NewReader()
	require.NotNil(t, reader)
	defer reader.Close()

	err = wl.Truncate(45)
	require.NoError(t, err, "Truncate on active segment with reader should be allowed")

	reader.Close()
}

func TestTruncateWithActiveReaderOnDeletedSegment(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWALog(dir, ".wal", WithMaxSegmentSize(1024))
	require.NoError(t, err)
	defer wl.Close()

	payload := make([]byte, 100)
	for i := 0; i < 50; i++ {
		_, err := wl.Write(payload, uint64(i+1))
		require.NoError(t, err)
	}

	segments := wl.Segments()
	var maxID SegmentID
	for id := range segments {
		if id > maxID {
			maxID = id
		}
	}
	lastSeg := segments[maxID]
	firstIndex := lastSeg.FirstLogIndex()
	truncateIndex := firstIndex - 1

	reader := lastSeg.NewReader()
	require.NotNil(t, reader)
	defer reader.Close()

	err = wl.Truncate(truncateIndex)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has active readers")

	reader.Close()
}

func TestClearIndexOnFlush_EnabledClearsAfterRotation(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWALog(dir, ".wal",
		WithClearIndexOnFlush(),
	)
	require.NoError(t, err)
	defer wal.Close()

	data := make([]byte, 100)
	for i := 0; i < 3; i++ {
		_, err := wal.Write(data, uint64(i+1))
		require.NoError(t, err)
	}

	seg1 := wal.Current()
	seg1ID := seg1.ID()

	require.NoError(t, wal.RotateSegment())
	seg1.WaitForIndexFlush()

	require.True(t, seg1.IsSealed(), "segment 1 should be sealed after rotation")
	entries := seg1.IndexEntries()
	assert.Empty(t, entries, "sealed segment %d should have cleared index after flush", seg1ID)

	for i := 0; i < 2; i++ {
		_, err := wal.Write(data, uint64(i+4))
		require.NoError(t, err)
	}

	current := wal.Current()
	require.NotNil(t, current)
	require.False(t, current.IsSealed(), "current segment should be active")
	entries = current.IndexEntries()
	assert.NotEmpty(t, entries, "active segment should still have index")
}

func TestClearIndexOnFlush_ReopenedSegmentsNotCleared(t *testing.T) {
	dir := t.TempDir()

	wal1, err := NewWALog(dir, ".wal",
		WithClearIndexOnFlush(),
	)
	require.NoError(t, err)

	data := make([]byte, 100)
	for i := 0; i < 3; i++ {
		_, err := wal1.Write(data, uint64(i+1))
		require.NoError(t, err)
	}

	seg1 := wal1.Current()
	require.NoError(t, wal1.RotateSegment())
	seg1.WaitForIndexFlush()

	require.True(t, seg1.IsSealed())
	assert.Empty(t, seg1.IndexEntries(), "sealed segment should be cleared after flush in first session")

	require.NoError(t, wal1.Close())

	wal2, err := NewWALog(dir, ".wal",
		WithClearIndexOnFlush(),
	)
	require.NoError(t, err)
	defer wal2.Close()

	sealedWithIndex := 0
	for _, seg := range wal2.Segments() {
		if seg.IsSealed() {
			entries := seg.IndexEntries()
			if len(entries) > 0 {
				sealedWithIndex++
			}
		}
	}
	assert.Greater(t, sealedWithIndex, 0,
		"reopened sealed segments should have index loaded from disk even with ClearIndexOnFlush enabled")

	for _, seg := range wal2.Segments() {
		seg.ClearIndexFromMemory()
	}

	for id, seg := range wal2.Segments() {
		if seg.IsSealed() {
			entries := seg.IndexEntries()
			assert.Empty(t, entries, "manually cleared segment %d should have empty index", id)
		}
	}
}

func TestClearIndexOnFlush_DisabledDoesNotClearOnRotation(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer wal.Close()

	data := make([]byte, 100)
	for i := 0; i < 3; i++ {
		_, err := wal.Write(data, uint64(i+1))
		require.NoError(t, err)
	}

	seg1 := wal.Current()
	require.NoError(t, wal.RotateSegment())
	seg1.WaitForIndexFlush()

	require.True(t, seg1.IsSealed())
	entries := seg1.IndexEntries()
	assert.NotEmpty(t, entries, "sealed segment should still have index when clear disabled")

	current := wal.Current()
	_, err = wal.Write(data, 4)
	require.NoError(t, err)
	entries = current.IndexEntries()
	assert.NotEmpty(t, entries, "active segment should have index")
}

func TestClearIndexFromMemory_ManualClearOnlySealedNotActive(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer wal.Close()

	data := make([]byte, 100)
	for i := 0; i < 3; i++ {
		_, err := wal.Write(data, uint64(i+1))
		require.NoError(t, err)
	}

	seg1 := wal.Current()
	require.NoError(t, wal.RotateSegment())
	seg1.WaitForIndexFlush()

	for i := 0; i < 2; i++ {
		_, err := wal.Write(data, uint64(i+4))
		require.NoError(t, err)
	}

	activeSegment := wal.Current()
	require.True(t, seg1.IsSealed(), "seg1 should be sealed")
	require.False(t, activeSegment.IsSealed(), "active segment should not be sealed")

	seg1.ClearIndexFromMemory()
	assert.Empty(t, seg1.IndexEntries(), "sealed segment should be cleared")
	activeSegment.ClearIndexFromMemory()
	assert.NotEmpty(t, activeSegment.IndexEntries(), "active segment should NOT be cleared")
}

func TestClearIndexFromMemory_ActiveSegmentCannotBeCleared(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer wal.Close()

	data := make([]byte, 50)
	for i := 0; i < 5; i++ {
		_, err := wal.Write(data, uint64(i+1))
		require.NoError(t, err)
	}

	current := wal.Current()
	require.NotNil(t, current)
	require.False(t, current.IsSealed(), "segment should be active")

	entriesBefore := current.IndexEntries()
	require.NotEmpty(t, entriesBefore, "active segment should have entries")
	current.ClearIndexFromMemory()

	entriesAfter := current.IndexEntries()
	assert.Equal(t, len(entriesBefore), len(entriesAfter), "active segment should NOT be cleared")
	assert.NotEmpty(t, entriesAfter, "active segment should still have entries")
}

func TestClearIndexOnFlush_MultipleRotations(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWALog(dir, ".wal",
		WithClearIndexOnFlush(),
	)
	require.NoError(t, err)
	defer wal.Close()

	data := make([]byte, 50)
	var sealedSegments []*Segment

	for round := 0; round < 3; round++ {
		for i := 0; i < 2; i++ {
			_, err := wal.Write(data, uint64(round*2+i+1))
			require.NoError(t, err)
		}

		seg := wal.Current()
		require.NoError(t, wal.RotateSegment())
		seg.WaitForIndexFlush()
		sealedSegments = append(sealedSegments, seg)
	}

	for i, seg := range sealedSegments {
		require.True(t, seg.IsSealed(), "segment %d should be sealed", i)
		assert.Empty(t, seg.IndexEntries(), "sealed segment %d should have cleared index", i)
	}
	_, err = wal.Write(data, 7)
	require.NoError(t, err)
	assert.NotEmpty(t, wal.Current().IndexEntries(), "active segment should have index")
}
