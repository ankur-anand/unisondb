package walfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSegmentNotFound    = errors.New("segment not found")
	ErrOffsetOutOfBounds  = errors.New("start offset is beyond segment size")
	ErrOffsetBeforeHeader = errors.New("start offset is within reserved segment header")
	ErrFsync              = errors.New("fsync error")
	ErrRecordTooLarge     = errors.New("record size exceeds maximum segment capacity")
)

// DeletionPredicate is a function that determines if a segment ID is safe to delete.
type DeletionPredicate func(segID SegmentID) bool

type WALogOptions func(*WALog)

// WithMaxSegmentSize options sets the MaxSize of the Segment file.
func WithMaxSegmentSize(size int64) WALogOptions {
	return func(sm *WALog) {
		sm.maxSegmentSize = size
	}
}

// WithBytesPerSync sets the threshold in bytes after which a msync is triggered.
// Useful for batching writes.
// 0 disable this feature.
func WithBytesPerSync(bytes int64) WALogOptions {
	return func(sm *WALog) {
		sm.bytesPerSync = bytes
	}
}

// WithMSyncEveryWrite enables msync() after every write operation.
func WithMSyncEveryWrite(enabled bool) WALogOptions {
	return func(sm *WALog) {
		if enabled {
			sm.forceSyncEveryWrite = MsyncOnWrite
		}
	}
}

// WithOnSegmentRotated registers a fn callback function that will be called immediately after a WAL segment is rotated.
func WithOnSegmentRotated(fn func()) WALogOptions {
	return func(sm *WALog) {
		if fn != nil {
			sm.rotationCallback = fn
		}
	}
}

// WithAutoCleanupPolicy configures the automatic segment cleanup policy for the WAL.
// - maxAge: Segments older than this duration are eligible for deletion.
// - minSegments: Minimum number of WAL segments to always retain, regardless of age.
// - maxSegments: If the total number of segments exceeds this limit, older segments will be deleted irrespective of its age.
func WithAutoCleanupPolicy(maxAge time.Duration, minSegments, maxSegments int, enable bool) WALogOptions {
	return func(sm *WALog) {
		if minSegments > 0 {
			sm.minSegmentsToRetain = minSegments
		}
		if maxSegments > 0 {
			sm.maxSegmentsToRetain = maxSegments
		}
		if maxAge > 0 {
			sm.segmentMaxAge = maxAge
		}
		sm.segmentMaxAge = maxAge
		sm.minSegmentsToRetain = minSegments
		sm.maxSegmentsToRetain = maxSegments
		sm.enableAutoCleanup = enable
	}
}

// WALog manages the lifecycle of each individual segments, including creation, rotation,
// recovery, and read/write operations.
type WALog struct {
	dir            string
	ext            string
	maxSegmentSize int64

	// number of bytes to write before calling msync in write path
	bytesPerSync        int64
	unSynced            int64
	forceSyncEveryWrite MsyncOption
	bytesPerSyncCalled  atomic.Int64
	segmentRotated      atomic.Int64
	rotationCallback    func()

	mu             sync.RWMutex
	currentSegment *Segment
	segments       map[SegmentID]*Segment

	segmentMaxAge       time.Duration
	minSegmentsToRetain int
	maxSegmentsToRetain int
	enableAutoCleanup   bool
	deletionMu          sync.Mutex
	pendingDeletion     map[SegmentID]*Segment
}

// NewWALog returns an initialized WALog that manages the segments in the provided dir with the given ext.
func NewWALog(dir string, ext string, opts ...WALogOptions) (*WALog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	manager := &WALog{
		dir:                 dir,
		ext:                 ext,
		maxSegmentSize:      segmentSize,
		segments:            make(map[SegmentID]*Segment),
		forceSyncEveryWrite: MsyncNone,
		bytesPerSync:        0,
		segmentMaxAge:       0,
		minSegmentsToRetain: 0,
		maxSegmentsToRetain: 0,
		enableAutoCleanup:   false,
		pendingDeletion:     make(map[SegmentID]*Segment),
		rotationCallback:    func() {},
	}

	for _, opt := range opts {
		opt(manager)
	}

	// recover existing segments
	if err := manager.recoverSegments(); err != nil {
		return nil, fmt.Errorf("segment recovery failed: %w", err)
	}

	return manager, nil
}

// openSegment opens segment with the provided ID.
func (wl *WALog) openSegment(id uint32) (*Segment, error) {
	return OpenSegmentFile(wl.dir, wl.ext, id, WithSegmentSize(wl.maxSegmentSize), WithSyncOption(wl.forceSyncEveryWrite))
}

func (wl *WALog) recoverSegments() error {
	files, err := os.ReadDir(wl.dir)
	if err != nil {
		return fmt.Errorf("failed to read segment directory: %w", err)
	}

	var segmentIDs []SegmentID

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), wl.ext) {
			continue
		}
		// e.g. "000000001.wal" -> 1
		base := strings.TrimSuffix(file.Name(), wl.ext)
		id, err := strconv.ParseUint(base, 10, 32)
		if err != nil {
			// skip non-numeric segment files
			continue
		}
		segID := SegmentID(id)
		segmentIDs = append(segmentIDs, segID)
	}

	// 000000001.wal
	// 000000002.wal
	// 000000003.wal
	sort.Slice(segmentIDs, func(i, j int) bool {
		return segmentIDs[i] < segmentIDs[j]
	})

	if len(segmentIDs) == 0 {
		seg, err := wl.openSegment(1)
		if err != nil {
			return fmt.Errorf("failed to create initial segment: %w", err)
		}

		wl.segments[1] = seg
		wl.currentSegment = seg
		return nil
	}

	for i, id := range segmentIDs {
		seg, err := wl.openSegment(id)
		if err != nil {
			return fmt.Errorf("failed to open segment %d: %w", id, err)
		}
		if i < len(segmentIDs)-1 && !IsSealed(seg.GetFlags()) {
			err := seg.SealSegment()
			if err != nil {
				return err
			}
		}
		wl.segments[id] = seg
		wl.currentSegment = seg
	}

	return nil
}

func (wl *WALog) Sync() error {
	wl.mu.Lock()
	activeSegment := wl.currentSegment
	wl.mu.Unlock()
	if activeSegment == nil {
		return errors.New("no active segment")
	}
	if activeSegment.closed.Load() {
		return nil
	}
	if err := activeSegment.Sync(); err != nil {
		return fmt.Errorf("%w: %v", ErrFsync, err)
	}
	return nil
}

func (wl *WALog) Close() error {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	var cErr error
	for _, seg := range wl.segments {
		err := seg.Close()
		if err != nil {
			cErr = errors.Join(cErr, err)
		}
	}
	return cErr
}

// Write appends the given data as a new record to the active segment.
func (wl *WALog) Write(data []byte) (RecordPosition, error) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	if wl.currentSegment == nil {
		return RecordPosition{}, errors.New("no active segment")
	}

	estimatedSize := recordOverhead(int64(len(data)))
	if estimatedSize > wl.maxSegmentSize {
		return RecordPosition{}, ErrRecordTooLarge
	}

	// if current segment needs rotation rotate it.
	if wl.currentSegment.WillExceed(len(data)) {
		if err := wl.rotateSegment(); err != nil {
			return RecordPosition{}, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	pos, err := wl.currentSegment.Write(data)
	if err != nil {
		return RecordPosition{}, fmt.Errorf("write failed: %w", err)
	}

	wl.unSynced += estimatedSize

	if wl.bytesPerSync > 0 && wl.unSynced >= wl.bytesPerSync {
		if err := wl.currentSegment.MSync(); err != nil {
			return RecordPosition{}, err
		}
		wl.unSynced = 0
		wl.bytesPerSyncCalled.Add(1)
	}

	return *pos, nil
}

func recordOverhead(dataLen int64) int64 {
	return alignUp(dataLen) + recordHeaderSize + recordTrailerMarkerSize
}

// BytesPerSyncCallCount how many times this was called on current active segment.
func (wl *WALog) BytesPerSyncCallCount() int64 {
	return wl.bytesPerSyncCalled.Load()
}

func (wl *WALog) SegmentRotatedCount() int64 {
	return wl.segmentRotated.Load()
}

// Read returns the data from the provided record position if found.
// IMPORTANT: The returned `[]byte` is a slice of a memory-mapped file, so data must not be retained or modified.
// If the data needs to be used beyond the lifetime of the segment, the caller MUST copy it.
func (wl *WALog) Read(pos RecordPosition) ([]byte, error) {
	wl.mu.RLock()
	seg, ok := wl.segments[pos.SegmentID]
	wl.mu.RUnlock()

	if !ok {
		return nil, ErrSegmentNotFound
	}

	if pos.Offset < segmentHeaderSize {
		return nil, ErrOffsetBeforeHeader
	}

	if pos.Offset > seg.GetSegmentSize() {
		return nil, ErrOffsetOutOfBounds
	}

	data, _, err := seg.Read(pos.Offset)
	if err != nil {
		return nil, fmt.Errorf("read failed at segment %d offset %d: %w", pos.SegmentID, pos.Offset, err)
	}

	return data, nil
}

func (wl *WALog) Segments() map[SegmentID]*Segment {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	segmentsCopy := make(map[SegmentID]*Segment, len(wl.segments))
	for id, seg := range wl.segments {
		segmentsCopy[id] = seg
	}
	return segmentsCopy
}

func (wl *WALog) Current() *Segment {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	return wl.currentSegment
}

// RotateSegment rotates the current segment and create a new active segment.
func (wl *WALog) RotateSegment() error {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	return wl.rotateSegment()
}

func (wl *WALog) rotateSegment() error {
	if wl.currentSegment != nil && !IsSealed(wl.currentSegment.GetFlags()) {
		if err := wl.currentSegment.SealSegment(); err != nil {
			return fmt.Errorf("failed to seal current segment: %w", err)
		}
		err := wl.currentSegment.Sync()
		if err != nil {
			return err
		}
	}

	var newID SegmentID = 1
	if wl.currentSegment != nil {
		newID = wl.currentSegment.ID() + 1
	}

	newSegment, err := wl.openSegment(newID)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %w", err)
	}

	wl.segments[newID] = newSegment
	wl.currentSegment = newSegment
	wl.bytesPerSyncCalled.Store(0)
	wl.segmentRotated.Add(1)
	wl.rotationCallback()
	return nil
}

// StartPendingSegmentCleaner starts a background goroutine that periodically
// inspects segments marked for pending deletion and attempts to safely remove them.
// If there are any current reader it will mark it for deletion.
func (wl *WALog) StartPendingSegmentCleaner(ctx context.Context,
	interval time.Duration,
	canDeleteFn func(segID SegmentID) bool,
) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				wl.deletionMu.Lock()
				for id, seg := range wl.pendingDeletion {
					if canDeleteFn != nil && canDeleteFn(id) {
						seg.MarkForDeletion()
					}
				}
				wl.deletionMu.Unlock()
				wl.CleanupStalePendingSegments()
			}
		}
	}()
}

// MarkSegmentsForDeletion identifies and queues WAL segments for deletion based on
// their age and segment count retention constraints.
func (wl *WALog) MarkSegmentsForDeletion() {
	if !wl.enableAutoCleanup {
		return
	}

	wl.mu.RLock()
	currentSegments := len(wl.segments)
	clonedSegments := maps.Clone(wl.segments)
	wl.mu.RUnlock()

	if wl.minSegmentsToRetain > 0 && currentSegments <= wl.minSegmentsToRetain {
		return
	}

	var candidates []*Segment
	for _, seg := range clonedSegments {
		if IsSealed(seg.GetFlags()) {
			candidates = append(candidates, seg)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID() < candidates[j].ID()
	})

	segmentsToDelete := 0

	segmentsAboveMin := currentSegments - wl.minSegmentsToRetain
	if segmentsAboveMin <= 0 {
		return
	}

	if wl.maxSegmentsToRetain > 0 && currentSegments > wl.maxSegmentsToRetain {
		segmentsToDelete = currentSegments - wl.maxSegmentsToRetain
	} else if wl.segmentMaxAge > 0 {
		now := time.Now().UnixNano()
		for _, seg := range candidates {
			if now-seg.GetLastModifiedAt() >= wl.segmentMaxAge.Nanoseconds() {
				segmentsToDelete++
			}
		}
	}

	if segmentsToDelete == 0 {
		return
	}

	wl.deletionMu.Lock()
	defer wl.deletionMu.Unlock()

	for _, seg := range candidates {
		if segmentsToDelete <= 0 {
			break
		}
		if _, alreadyQueued := wl.pendingDeletion[seg.ID()]; !alreadyQueued {
			wl.pendingDeletion[seg.ID()] = seg
			segmentsToDelete--
		}
	}
}

func (wl *WALog) QueuedSegmentsForDeletion() map[SegmentID]*Segment {
	wl.deletionMu.Lock()
	defer wl.deletionMu.Unlock()

	segmentsCopy := make(map[SegmentID]*Segment, len(wl.segments))
	for id, seg := range wl.pendingDeletion {
		segmentsCopy[id] = seg
	}
	return segmentsCopy
}

// CleanupStalePendingSegments scans pendingDeletion and segments maps.
// If a segment's file no longer exists on disk, it removes those entries from both maps.
func (wl *WALog) CleanupStalePendingSegments() {
	wl.deletionMu.Lock()
	defer wl.deletionMu.Unlock()

	for id, seg := range wl.pendingDeletion {
		if _, err := os.Stat(seg.path); os.IsNotExist(err) {
			// deleted; remove from both maps
			delete(wl.pendingDeletion, id)
			wl.mu.Lock()
			delete(wl.segments, id)
			wl.mu.Unlock()
			slog.Debug("[unisondb.walfs]",
				slog.String("event_type", "old.segment.cleanup"),
				slog.String("path", seg.path),
			)
		}
	}
}

// Reader represents a high-level sequential reader over a WALog.
// It reads across all available WAL segments in order, automatically
// advancing from one segment to the next.
type Reader struct {
	segmentReaders []*SegmentReader
	currentReader  int
	lastPos        *RecordPosition
}

// NewReader returns a new Reader that sequentially reads all segments in the WALog,
// starting from the beginning (lowest SegmentID).
func (wl *WALog) NewReader() *Reader {
	wl.mu.RLock()
	segments := maps.Clone(wl.segments)
	wl.mu.RUnlock()

	var ids []SegmentID
	for id := range segments {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	var readers []*SegmentReader
	for _, id := range ids {
		if reader := segments[id].NewReader(); reader != nil {
			readers = append(readers, reader)
		}
	}

	return &Reader{
		segmentReaders: readers,
		currentReader:  0,
	}
}

// Close closes all segment readers to release their references.
// IMPORTANT: This method MUST be called after the Reader is no longer needed.
func (r *Reader) Close() {
	for _, sr := range r.segmentReaders {
		sr.Close()
	}
}

// Next returns the next available WAL record data and its current position.
// IMPORTANT: The returned `[]byte` is a slice of a memory-mapped file, so data must not be retained or modified.
// If the data needs to be used beyond the lifetime of the segment, the caller MUST copy it.
func (r *Reader) Next() ([]byte, *RecordPosition, error) {
	for r.currentReader < len(r.segmentReaders) {
		data, pos, err := r.segmentReaders[r.currentReader].Next()
		if err == nil {
			r.lastPos = r.segmentReaders[r.currentReader].LastRecordPosition()
			return data, pos, nil
		}
		if errors.Is(err, io.EOF) {
			r.segmentReaders[r.currentReader].Close()
			r.currentReader++
			continue
		}
		return nil, nil, err
	}
	return nil, nil, io.EOF
}

// SeekNext advances the reader by one record, discarding the data.
func (r *Reader) SeekNext() error {
	_, _, err := r.Next()
	return err
}

// LastRecordPosition returns the RecordPosition of the last successfully read entry.
func (r *Reader) LastRecordPosition() *RecordPosition {
	return r.lastPos
}

// NewReaderAfter returns a reader that starts after the given RecordPosition.
// It first creates a reader from that position, then skips one record.
func (wl *WALog) NewReaderAfter(pos RecordPosition) (*Reader, error) {
	reader, err := wl.NewReaderWithStart(pos)
	if err != nil {
		return nil, err
	}
	if err := reader.SeekNext(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return reader, nil
}

// NewReaderWithStart returns a new Reader that begins reading from the specified position.
// If SegmentID is 0, the reader will begin from the very start of the WAL.
func (wl *WALog) NewReaderWithStart(pos RecordPosition) (*Reader, error) {
	if pos.SegmentID == 0 {
		// interpreting SegmentID == 0 as: read from the beginning
		return wl.NewReader(), nil
	}

	wl.mu.RLock()
	segments := make([]*Segment, 0, len(wl.segments))
	for _, seg := range wl.segments {
		segments = append(segments, seg)
	}
	wl.mu.RUnlock()

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID() < segments[j].ID()
	})

	var (
		readers      []*SegmentReader
		segmentFound bool
	)

	for _, seg := range segments {
		if seg.ID() < pos.SegmentID {
			continue
		}
		reader := seg.NewReader()
		if reader == nil {
			continue
		}

		if seg.ID() == pos.SegmentID {
			segmentFound = true
			size := seg.GetSegmentSize()
			if pos.Offset > size {
				return nil, ErrOffsetOutOfBounds
			}

			reader.readOffset = pos.Offset
			if reader.readOffset <= segmentHeaderSize {
				reader.readOffset = segmentHeaderSize
			}
		}

		readers = append(readers, reader)
	}

	if !segmentFound {
		return nil, ErrSegmentNotFound
	}

	return &Reader{
		segmentReaders: readers,
		currentReader:  0,
	}, nil
}
