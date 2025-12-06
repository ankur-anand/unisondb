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
