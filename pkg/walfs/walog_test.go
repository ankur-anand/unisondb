package walfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/stretchr/testify/assert"
)

func TestSegmentManager_RecoverSegments_Sealing(t *testing.T) {
	dir := t.TempDir()
	ext := ".wal"

	seg1, err := walfs.OpenSegmentFile(dir, ext, 1)
	assert.NoError(t, err)
	_, err = seg1.Write([]byte("data1"))
	assert.NoError(t, err)
	err = seg1.SealSegment()
	assert.NoError(t, err)
	assert.True(t, walfs.IsSealed(seg1.GetFlags()))
	assert.NoError(t, seg1.Close())

	seg2, err := walfs.OpenSegmentFile(dir, ext, 2)
	assert.NoError(t, err)
	_, err = seg2.Write([]byte("data2"))
	assert.NoError(t, err)
	assert.False(t, walfs.IsSealed(seg2.GetFlags()))
	assert.NoError(t, seg2.Close())

	seg3, err := walfs.OpenSegmentFile(dir, ext, 3)
	assert.NoError(t, err)
	_, err = seg3.Write([]byte("data3"))
	assert.NoError(t, err)
	assert.False(t, walfs.IsSealed(seg3.GetFlags()))
	assert.NoError(t, seg3.Close())

	manager, err := walfs.NewWALog(dir, ext, walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	assert.Len(t, manager.Segments(), 3)

	assert.True(t, walfs.IsSealed(manager.Segments()[1].GetFlags()))
	assert.True(t, walfs.IsSealed(manager.Segments()[2].GetFlags()))
	assert.Equal(t, walfs.SegmentID(3), manager.Current().ID())
	assert.False(t, walfs.IsSealed(manager.Current().GetFlags()))
}

func TestSegmentManager_EmptyDirectory_CreatesInitialSegment(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)
	segments := manager.Segments()
	assert.Len(t, segments, 1, "should create one initial segment")
	current := manager.Current()
	assert.NotNil(t, current, "current segment should not be nil")
	assert.Equal(t, walfs.SegmentID(1), current.ID(), "initial segment ID should be 1")
	assert.False(t, walfs.IsSealed(current.GetFlags()), "initial segment should not be sealed")

	expectedPath := filepath.Join(tmpDir, "000000001.wal")
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err, "expected WAL segment file to exist on disk")
}

func TestSegmentManager_SkipNonNumericSegments(t *testing.T) {
	tmpDir := t.TempDir()

	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "foo.wal"), []byte("dummy"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "123abc.wal"), []byte("dummy"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "!!invalid.wal"), []byte("dummy"), 0644))

	seg, err := walfs.OpenSegmentFile(tmpDir, ".wal", 42)
	assert.NoError(t, err)
	defer seg.Close()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	segments := manager.Segments()
	assert.Len(t, segments, 1, "only numeric segment should be recovered")

	current := manager.Current()
	assert.Equal(t, walfs.SegmentID(42), current.ID(), "numeric segment ID should be recovered")
}

func TestSegmentManager_RotateSegment(t *testing.T) {
	dir := t.TempDir()
	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	_, err = manager.Current().Write([]byte("initial-data"))
	assert.NoError(t, err)

	initial := manager.Current()
	initialID := initial.ID()
	assert.False(t, walfs.IsSealed(initial.GetFlags()))

	err = manager.RotateSegment()
	assert.NoError(t, err)

	assert.True(t, walfs.IsSealed(initial.GetFlags()), "previous segment should be sealed")
	assert.Equal(t, initialID+1, manager.Current().ID(), "new segment ID should be incremented")
	assert.False(t, walfs.IsSealed(manager.Current().GetFlags()), "new segment should not be sealed")
}

func TestSegmentManager_NewReader(t *testing.T) {
	dir := t.TempDir()

	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(1<<10))
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := manager.Current().Write([]byte(fmt.Sprintf("segment-%d", i+1)))
		assert.NoError(t, err)
		assert.NoError(t, manager.RotateSegment())
	}

	reader := manager.NewReader()
	assert.NotNil(t, reader)

	var seen []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		seen = append(seen, string(data))
	}

	assert.Equal(t, []string{"segment-1", "segment-2", "segment-3"}, seen)
}

func TestSegmentManager_NewReaderWithStart(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)

	for i := 1; i <= 3; i++ {
		seg := manager.Current()
		_, err = seg.Write([]byte(fmt.Sprintf("segment-%d-entry-1", i)))
		assert.NoError(t, err)
		_, err = seg.Write([]byte(fmt.Sprintf("segment-%d-entry-2", i)))
		assert.NoError(t, err)

		if i < 3 {
			assert.NoError(t, manager.RotateSegment())
		}
	}

	start := walfs.RecordPosition{SegmentID: 2, Offset: 0}
	reader, err := manager.NewReaderWithStart(start)
	assert.NotNil(t, reader)
	assert.NoError(t, err)

	var results []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		results = append(results, string(data))
	}

	expected := []string{
		"segment-2-entry-1",
		"segment-2-entry-2",
		"segment-3-entry-1",
		"segment-3-entry-2",
	}
	assert.Equal(t, expected, results)
}

func TestSegmentManager_NewReaderWithStart_Errors(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("record")
	pos, err := manager.Current().Write(data)
	assert.NoError(t, err)

	badOffset := pos.Offset + 4096*1024
	_, err = manager.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: pos.SegmentID,
		Offset:    badOffset,
	})

	assert.ErrorIs(t, err, walfs.ErrOffsetOutOfBounds)

	_, err = manager.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: 9999,
		Offset:    0,
	})
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)

	reader, err := manager.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: pos.SegmentID,
		Offset:    0,
	})
	assert.NoError(t, err)

	val, next, readErr := reader.Next()
	assert.NoError(t, readErr)
	assert.Equal(t, data, val)
	assert.NotNil(t, next)
}

func TestSegmentManager_WriteWithRotation(t *testing.T) {
	tmpDir := t.TempDir()

	maxSegmentSize := int64(528 + 64 + 1)

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(maxSegmentSize))
	assert.NoError(t, err)

	data := make([]byte, 512)

	pos1, err := manager.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, walfs.SegmentID(1), pos1.SegmentID)

	pos2, err := manager.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, walfs.SegmentID(2), pos2.SegmentID)

	segments := manager.Segments()
	assert.Len(t, segments, 2)

	assert.True(t, walfs.IsSealed(segments[walfs.SegmentID(1)].GetFlags()))
	assert.False(t, walfs.IsSealed(manager.Current().GetFlags()))
	assert.Equal(t, walfs.SegmentID(2), manager.Current().ID())
}

func TestSegmentManager_Read_Errors(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("record")
	pos, err := manager.Write(data)
	assert.NoError(t, err)

	_, err = manager.Read(walfs.RecordPosition{SegmentID: 9999, Offset: 0})
	assert.ErrorIs(t, err, walfs.ErrSegmentNotFound)

	_, err = manager.Read(walfs.RecordPosition{SegmentID: pos.SegmentID, Offset: 0})
	assert.ErrorIs(t, err, walfs.ErrOffsetBeforeHeader)

	badOffset := pos.Offset + 4096*1024
	_, err = manager.Read(walfs.RecordPosition{SegmentID: pos.SegmentID, Offset: badOffset})
	assert.ErrorIs(t, err, walfs.ErrOffsetOutOfBounds)

	val, err := manager.Read(pos)
	assert.NoError(t, err)
	assert.Equal(t, data, val)
}

func TestSegmentManager_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("sync-test")
	_, err = manager.Write(data)
	assert.NoError(t, err)

	err = manager.Sync()
	assert.NoError(t, err)
}

func TestSegmentManager_Close(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	data := []byte("close-test")
	_, err = manager.Write(data)
	assert.NoError(t, err)

	err = manager.Close()
	assert.NoError(t, err)

	//for _, seg := range manager.Segments() {
	//	assert.True(t, seg())
	//}
}

func TestSegmentManager_WithMSyncEveryWrite(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1024*1024),
		walfs.WithMSyncEveryWrite(true),
	)
	assert.NoError(t, err)

	data := []byte("msync-on-write")
	_, err = manager.Write(data)
	assert.NoError(t, err)
}

func TestSegmentManager_WriteFailsOnClosedSegment(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024*1024))
	assert.NoError(t, err)

	err = manager.Current().Close()
	assert.NoError(t, err)

	_, err = manager.Write([]byte("should fail"))
	assert.Error(t, err)
}

func TestSegmentManager_Rotation_NoDataLoss(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(128))
	assert.NoError(t, err)

	entries := []string{}
	for i := 0; i < 50; i++ {
		payload := fmt.Sprintf("data-%d", i)
		entries = append(entries, payload)
		_, err := manager.Write([]byte(payload))
		assert.NoError(t, err)
	}

	reader := manager.NewReader()
	var results []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		results = append(results, string(data))
	}

	assert.Equal(t, entries, results)
}

func TestSegmentManager_WriteRecordTooLarge(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(512))
	assert.NoError(t, err)
	defer manager.Close()

	data := make([]byte, 1024)

	_, err = manager.Write(data)
	assert.ErrorIs(t, err, walfs.ErrRecordTooLarge, "should fail with ErrRecordTooLarge")
}

func TestSegmentManager_WriteRecordWithByteSync(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(false),
		walfs.WithBytesPerSync(32*1024),
	)
	assert.NoError(t, err)
	data := make([]byte, 1024)
	for i := 0; i < 150; i++ {
		_, err = manager.Write(data)
		assert.NoError(t, err)
	}

	minCall := (150 * (1024 + 8 + 8)) / (32 * 1024)
	assert.Equal(t, int64(minCall), manager.BytesPerSyncCallCount())
}

func TestSegmentManager_ConcurrentReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(4096))
	assert.NoError(t, err)

	var wg sync.WaitGroup
	numWriters := 5
	numReaders := 3
	numRecords := 50
	writeData := []byte("stress-data")

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numRecords; j++ {
				_, err := manager.Write([]byte(fmt.Sprintf("%s-%d-%d", writeData, id, j)))
				assert.NoError(t, err)
			}
		}(i)
	}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader := manager.NewReader()
			for {
				data, _, err := reader.Next()
				if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
					break
				}
				assert.NoError(t, err)
				_ = data
			}
		}()
	}

	wg.Wait()
}

func TestWALog_ConcurrentWriteRead_WithSegmentRotation(t *testing.T) {
	tmpDir := t.TempDir()

	manager, err := walfs.NewWALog(
		tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<18),
		walfs.WithBytesPerSync(16*1024),
	)
	assert.NoError(t, err)

	const totalRecords = 50000
	dataTemplate := "entry-%05d"

	var written []string
	var writtenMu sync.Mutex

	go func() {
		for i := 0; i < totalRecords; i++ {
			payload := []byte(fmt.Sprintf(dataTemplate, i))
			_, err := manager.Write(payload)
			assert.NoError(t, err)

			writtenMu.Lock()
			written = append(written, string(payload))
			writtenMu.Unlock()

			if i%200 == 0 {
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}
	}()

	var (
		readEntries  []string
		lastPosition walfs.RecordPosition
		retries      int
	)

retryRead:
	for {
		var reader *walfs.Reader
		if lastPosition.Offset == 0 {
			reader = manager.NewReader()
		} else {
			var err error
			reader, err = manager.NewReaderWithStart(lastPosition)
			assert.NoError(t, err)
			assert.NoError(t, reader.SeekNext())
		}

		for {
			data, pos, err := reader.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
				retries++
				time.Sleep(10 * time.Millisecond)
				continue retryRead
			}
			assert.NoError(t, err)
			assert.NotNil(t, pos)
			readEntries = append(readEntries, string(data))
			lastPosition = pos
			if len(readEntries) >= totalRecords {
				break retryRead
			}
		}
	}

	writtenMu.Lock()
	defer writtenMu.Unlock()

	assert.Equal(t, written, readEntries, "read data should match written in order")

	singleRecordSize := recordOverhead(int64(len([]byte(fmt.Sprintf(dataTemplate, totalRecords)))))
	totalSize := singleRecordSize * totalRecords
	rotationExpected := totalSize / (1 << 18)

	assert.Equal(t, rotationExpected, manager.SegmentRotatedCount())
	assert.GreaterOrEqual(t, retries, int(rotationExpected))
}

func TestManagerReader_LastRecordPosition(t *testing.T) {
	dir := t.TempDir()

	manager, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(512))
	assert.NoError(t, err)
	defer manager.Close()

	entriesSeg1 := [][]byte{
		[]byte("s1-record1"),
		[]byte("s1-record2"),
	}

	entriesSeg2 := [][]byte{
		[]byte("s2-record1"),
		[]byte("s2-record2"),
	}

	for _, entry := range entriesSeg1 {
		_, err := manager.Write(entry)
		assert.NoError(t, err)
	}

	assert.NoError(t, manager.RotateSegment())

	for _, entry := range entriesSeg2 {
		_, err := manager.Write(entry)
		assert.NoError(t, err)
	}

	reader := manager.NewReader()
	defer reader.Close()

	var allEntries [][]byte
	allEntries = append(allEntries, entriesSeg1...)
	allEntries = append(allEntries, entriesSeg2...)

	for i := 0; i < len(allEntries); i++ {
		data, pos, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, allEntries[i], data)

		last := reader.LastRecordPosition()
		assert.NotNil(t, last)

		assert.Equal(t, pos.SegmentID, last.SegmentID)
		assert.LessOrEqual(t, last.Offset, pos.Offset)
	}

	data, pos, err := reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, pos, walfs.NilRecordPosition)
	assert.True(t, errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData))
}

func TestWALog_NewReaderAfter(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)
	defer wal.Close()

	var positions []*walfs.RecordPosition
	for i := 1; i <= 3; i++ {
		payload := []byte(fmt.Sprintf("entry-%d", i))
		pos, err := wal.Write(payload)
		assert.NoError(t, err)
		positions = append(positions, &pos)
	}

	reader, err := wal.NewReaderAfter(*positions[0])
	assert.NoError(t, err)
	defer reader.Close()

	var actual []string
	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		actual = append(actual, string(data))
	}

	expected := []string{"entry-2", "entry-3"}
	assert.Equal(t, expected, actual)
}

func TestReader_SeekNext(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(t, err)
	defer wal.Close()

	var positions []walfs.RecordPosition
	for i := 1; i <= 3; i++ {
		pos, err := wal.Write([]byte(fmt.Sprintf("entry-%d", i)))
		assert.NoError(t, err)
		positions = append(positions, pos)
	}

	reader, err := wal.NewReaderWithStart(positions[0])
	assert.NoError(t, err)
	defer reader.Close()

	err = reader.SeekNext()
	assert.NoError(t, err)

	data, pos, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, "entry-2", string(data))
	assert.Equal(t, positions[1], pos)
}

func TestReader_NextClosesOlderSegmentReaders(t *testing.T) {
	dir := t.TempDir()

	walog, err := walfs.NewWALog(
		dir, ".wal",
		walfs.WithMaxSegmentSize(1024),
		walfs.WithBytesPerSync(512),
	)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("entry-%03d", i))
		_, err := walog.Write(data)
		assert.NoError(t, err)
	}

	segmentRefs := walog.Segments()
	currSegID := walog.Current().ID()

	reader := walog.NewReader()
	defer reader.Close()

	for {
		data, pos, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		assert.NoError(t, err)
		assert.NotNil(t, pos)
		assert.True(t, bytes.HasPrefix(data, []byte("entry-")))
	}

	for _, seg := range segmentRefs {
		if seg.ID() != currSegID {
			assert.False(t, seg.HasActiveReaders())
		}
	}
}

func TestWALog_MarkSegmentsForDeletion(t *testing.T) {
	tmpDir := t.TempDir()
	t.Run("honour_max_segment", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 3, true))
		assert.NoError(t, err)
		for i := 0; i < 50; i++ {
			_, err := wal.Write(make([]byte, 1024*1024))
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.Len(t, segments, 47, "only 3 segment is allowed to be kept")
	})

	t.Run("honour_min_segment", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 10, 40, true))
		assert.NoError(t, err)
		for i := 0; i < 50; i++ {
			_, err := wal.Write(make([]byte, 1024*1024))
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.GreaterOrEqual(t, len(segments), 10, "at least 10 segment is allowed to be kept")
	})

	t.Run("cleanup_disabled", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 3, false))
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err := wal.Write(make([]byte, 1024*1024))
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.Len(t, segments, 0, "No segments should be queued when cleanup is disabled")
	})

	t.Run("latest_segment_not_deleted", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 2, true))
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err := wal.Write(make([]byte, 1024*1024))
			assert.NoError(t, err)
		}

		wal.MarkSegmentsForDeletion()
		currentID := wal.Current().ID()
		segments := wal.QueuedSegmentsForDeletion()

		_, queued := segments[currentID]
		assert.False(t, queued, "Current active segment should never be queued for deletion")
	})

	t.Run("age_based_deletion", func(t *testing.T) {
		wal, err := walfs.NewWALog(tmpDir, ".wal",
			walfs.WithMaxSegmentSize(2<<20),
			walfs.WithAutoCleanupPolicy(time.Millisecond*10, 1, 100, true))
		assert.NoError(t, err)

		for i := 0; i < 5; i++ {
			_, err := wal.Write(make([]byte, 1024*1024))
			assert.NoError(t, err)
		}
		time.Sleep(20 * time.Millisecond)

		wal.MarkSegmentsForDeletion()
		segments := wal.QueuedSegmentsForDeletion()
		assert.Greater(t, len(segments), 0, "Old segments should be queued based on age")
	})
}

func TestWALog_StartPendingSegmentCleaner(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(2<<20),
		walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 5, true))
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := wal.Write(make([]byte, 1024*1024))
		assert.NoError(t, err)
	}

	wal.MarkSegmentsForDeletion()

	canDeleteFn := func(segID walfs.SegmentID) bool {
		return true
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal.StartPendingSegmentCleaner(ctx, 50*time.Millisecond, canDeleteFn)

	time.Sleep(500 * time.Millisecond)

	for id := range wal.QueuedSegmentsForDeletion() {
		segmentPath := walfs.SegmentFileName(tmpDir, ".wal", id)
		_, err := os.Stat(segmentPath)
		assert.True(t, os.IsNotExist(err), fmt.Sprintf("Segment file %s should have been deleted", segmentPath))
	}
}

func TestWALog_CleanupStalePendingSegments(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(2<<20),
		walfs.WithAutoCleanupPolicy(time.Millisecond*100, 1, 5, true))
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err := wal.Write(make([]byte, 1024*1024))
		assert.NoError(t, err)
	}
	wal.MarkSegmentsForDeletion()

	deletionQueued := wal.QueuedSegmentsForDeletion()
	wal.CleanupStalePendingSegments()

	for segID := range deletionQueued {
		_, stillPending := wal.QueuedSegmentsForDeletion()[segID]
		assert.False(t, stillPending, "segment should be removed from pendingDeletion")
		_, stillInSegments := wal.Segments()[segID]
		assert.False(t, stillInSegments, "segment should be removed from segments map")
	}
}

func TestWALog_SegmentRotationCallback(t *testing.T) {
	tmpDir := t.TempDir()
	called := make(chan struct{})
	callback := func() {
		close(called)
	}

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithOnSegmentRotated(callback))
	assert.NoError(t, err)
	err = wal.RotateSegment()
	assert.NoError(t, err, "rotate segment should not return an error")
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Errorf("error waiting for the RotateSegment Callback")
	}

}

func TestWALog_SealedSegmentReturnsEOFAndHasNoActiveReaders(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1024))
	assert.NoError(t, err)
	defer wal.Close()

	payload := []byte("hello-world")
	pos, err := wal.Write(payload)
	assert.NoError(t, err)
	reader, err := wal.NewReaderWithStart(pos)
	assert.NoError(t, err)

	data, next, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.NotNil(t, next)

	data, next, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, next, walfs.NilRecordPosition)
	assert.ErrorIs(t, err, walfs.ErrNoNewData)

	err = wal.RotateSegment()
	assert.NoError(t, err)

	data, next, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, next, walfs.NilRecordPosition)
	assert.ErrorIs(t, err, io.EOF)

	reader.Close()

	seg1 := wal.Current().ID() - 1
	segMap := wal.Segments()
	seg, ok := segMap[seg1]
	assert.True(t, ok, "Segment 1 should still exist")
	assert.False(t, seg.HasActiveReaders(), "Segment 1 should have no active readers after Close")
	assert.True(t, walfs.IsSealed(seg.GetFlags()), "Segment 1 should be sealed after rotation")
}

func TestWALogReader_ErrNoNewDataOnActiveTail(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := walfs.NewWALog(tmpDir, ".wal")
	assert.NoError(t, err)
	defer wal.Close()

	payload := []byte("test-record")
	pos, err := wal.Write(payload)
	assert.NoError(t, err)

	reader, err := wal.NewReaderWithStart(walfs.RecordPosition{
		SegmentID: 0,
	})
	assert.NoError(t, err)
	defer reader.Close()

	data, current, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, payload, data)
	assert.Equal(t, pos.SegmentID, current.SegmentID)

	data, current, err = reader.Next()
	assert.Nil(t, data)
	assert.Equal(t, current, walfs.NilRecordPosition)
	assert.ErrorIs(t, err, walfs.ErrNoNewData)
}

func TestRotateSegment_MarksInMemorySealed(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	assert.NoError(t, err)

	record := []byte("hello-wal")
	_, err = wal.Write(record)
	assert.NoError(t, err)

	initialSegment := wal.Current()
	assert.NotNil(t, initialSegment)
	err = wal.RotateSegment()
	assert.NoError(t, err)

	rotated := wal.Current()
	assert.NotEqual(t, initialSegment.ID(), rotated.ID())
	assert.True(t, walfs.IsSealed(initialSegment.GetFlags()), "expected original segment to be sealed")
	assert.True(t, initialSegment.IsInMemorySealed(), "expected original segment to be in memory sealed")
}

func BenchmarkSegmentManager_Write_NoSync(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(false),
		walfs.WithBytesPerSync(0),
	)
	assert.NoError(b, err)

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Write(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentManager_Write_WithMSyncEveryWrite(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(true),
	)
	assert.NoError(b, err)

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Write(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentManager_Write_WithBytesPerSync(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal",
		walfs.WithMaxSegmentSize(1<<20),
		walfs.WithMSyncEveryWrite(false),
		walfs.WithBytesPerSync(32*1024),
	)
	assert.NoError(b, err)

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Write(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentManager_Read(b *testing.B) {
	tmpDir := b.TempDir()
	manager, err := walfs.NewWALog(tmpDir, ".wal", walfs.WithMaxSegmentSize(1<<20))
	assert.NoError(b, err)

	payload := []byte("bench-read")
	var positions []walfs.RecordPosition
	for i := 0; i < b.N; i++ {
		pos, err := manager.Write(payload)
		assert.NoError(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := manager.Read(positions[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func recordOverhead(dataLen int64) int64 {
	return alignUp(dataLen) + 8 + 8
}

func alignUp(n int64) int64 {
	return (n + 8) & ^7
}
