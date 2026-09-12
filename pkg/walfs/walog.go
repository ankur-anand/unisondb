package walfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	ErrRecoveryRequired   = errors.New("WAL truncation interrupted; close and reopen for recovery")
	ErrRecordTooLarge     = errors.New("record size exceeds maximum segment capacity")
)

// LSNNotYetAvailableError indicates the requested LSN is beyond the current max.
type LSNNotYetAvailableError struct {
	RequestedLSN  uint64
	CurrentMaxLSN uint64
}

func (e *LSNNotYetAvailableError) Error() string {
	return fmt.Sprintf("LSN %d not yet available (current max: %d)", e.RequestedLSN, e.CurrentMaxLSN)
}

// LSNTruncatedError indicates the requested LSN has been garbage collected.
type LSNTruncatedError struct {
	RequestedLSN      uint64
	FirstAvailableLSN uint64
}

func (e *LSNTruncatedError) Error() string {
	return fmt.Sprintf("LSN %d truncated (first available: %d)", e.RequestedLSN, e.FirstAvailableLSN)
}

// DeletionPredicate is a function that determines if a segment ID is safe to delete.
type DeletionPredicate func(segID SegmentID) bool

type WALogOptions func(*WALog)

// DirectorySyncer syncs a directory path to stable storage.
type DirectorySyncer interface {
	SyncDir(dir string) error
}

// DirectorySyncFunc adapts a function to act as a DirectorySyncer.
type DirectorySyncFunc func(dir string) error

// SyncDir implements DirectorySyncer.
func (f DirectorySyncFunc) SyncDir(dir string) error {
	return f(dir)
}

// WithMaxSegmentSize sets the capacity of newly created segment files.
// Existing segments retain their on-disk size when reopened.
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
// maxAge: Segments older than this duration are eligible for deletion.
// minSegments: Minimum number of WAL segments to always retain, regardless of age.
// maxSegments: If the total number of segments exceeds this limit, older segments will be deleted irrespective of its age.
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

// WithDirectorySyncer overrides directory syncing for segment, index and intent files.
func WithDirectorySyncer(syncer DirectorySyncer) WALogOptions {
	return func(sm *WALog) {
		if syncer != nil {
			sm.dirSyncer = syncer
		}
	}
}

// WithClearIndexOnFlush enables clearing segment's in-memory index after it's flushed to disk.
// This is useful when an external index is maintained.
func WithClearIndexOnFlush() WALogOptions {
	return func(sm *WALog) {
		sm.clearIndexOnFlush = true
	}
}

// WithReaderCommitCheck enables commit offset checking for readers.
// When enabled, readers will check against the committed position before advancing.
// If the reader's current position is at or beyond the committed position,
// Next() returns ErrNoNewData without advancing.
// Use Commit() to advance the committed position.
// Use this when writes must not be visible to readers until they are committed.
func WithReaderCommitCheck() WALogOptions {
	return func(sm *WALog) {
		sm.readerCommitCheck = true
	}
}

// WithCustomMarker sets the 4-byte marker written to new segments.
func WithCustomMarker(marker uint32) WALogOptions {
	return func(sm *WALog) {
		sm.customMarker = marker
	}
}

// WithCustomMarkerValidator sets a validator for stored markers on open.
func WithCustomMarkerValidator(validator MarkerValidator) WALogOptions {
	return func(sm *WALog) {
		sm.markerValidator = validator
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

	// this mutex is used in the write path.
	// it protects the writer path.
	writeMu        sync.RWMutex
	currentSegment *Segment
	segments       map[SegmentID]*Segment

	// in the read-path we will update the snapshot segment that reader can
	// use exclusively.
	// optimizes the read path and prevent Read-Stall Lock in hot path.
	segmentSnapshot atomic.Pointer[[]*Segment]

	// committedPos is the logical boundary for readers when readerCommitCheck is enabled.
	// Readers cannot advance beyond this position.
	// The writer advances it via Commit() once an entry is durable/committed.
	committedPos atomic.Pointer[RecordPosition]
	// readerCommitCheck when true, readers check committed position before advancing.
	// Set to true when writes must not be visible to readers until committed.
	readerCommitCheck bool

	segmentMaxAge       time.Duration
	minSegmentsToRetain int
	maxSegmentsToRetain int
	enableAutoCleanup   bool
	deletionMu          sync.Mutex
	pendingDeletion     map[SegmentID]*Segment
	dirSyncer           DirectorySyncer
	clearIndexOnFlush   bool
	logIndex            *ShardedIndex
	firstLSN            atomic.Uint64
	lastLSN             atomic.Uint64

	customMarker    uint32
	markerValidator MarkerValidator

	recoveryErr atomic.Pointer[error]
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
		dirSyncer:           DirectorySyncFunc(syncDir),
		logIndex:            NewShardedIndex(),
	}

	for _, opt := range opts {
		opt(manager)
	}

	// Finish any durable truncation before ordinary header/index recovery. The
	// target header may have been torn while the operation was in progress.
	if err := manager.recoverTruncation(); err != nil {
		return nil, fmt.Errorf("truncation recovery failed: %w", err)
	}
	// recover existing segments
	if err := manager.recoverSegments(); err != nil {
		_ = manager.Close()
		return nil, fmt.Errorf("segment recovery failed: %w", err)
	}

	return manager, nil
}

// openSegment opens segment with the provided ID.
func (wl *WALog) openSegment(id uint32) (*Segment, error) {
	segmentPath := SegmentFileName(wl.dir, wl.ext, id)

	isNew, err := isNewSegment(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("checking segment %d state: %w", id, err)
	}

	opts := []func(*Segment){
		WithSegmentSize(wl.maxSegmentSize),
		WithSyncOption(wl.forceSyncEveryWrite),
		WithSegmentDirectorySyncer(wl.dirSyncer),
	}
	if wl.clearIndexOnFlush {
		opts = append(opts, withClearIndexOnFlush())
	}

	if wl.logIndex != nil {
		opts = append(opts, withLogIndex(wl.logIndex))
	}

	if wl.customMarker != 0 {
		opts = append(opts, WithSegmentCustomMarker(wl.customMarker))
	}
	if wl.markerValidator != nil {
		opts = append(opts, WithSegmentCustomMarkerValidator(wl.markerValidator))
	}

	seg, err := OpenSegmentFile(wl.dir, wl.ext, id, opts...)
	if err != nil {
		return nil, err
	}

	if isNew {
		if err := wl.dirSyncer.SyncDir(wl.dir); err != nil {
			_ = seg.Close()
			return nil, fmt.Errorf("fsync wal directory: %w", err)
		}
	}

	return seg, nil
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
		wl.snapshotSegments()
		wl.recomputeBounds()
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

	wl.snapshotSegments()
	wl.recomputeBounds()
	return nil
}

func (wl *WALog) snapshotSegments() {
	segments := make([]*Segment, 0, len(wl.segments))
	for _, seg := range wl.segments {
		segments = append(segments, seg)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID() < segments[j].ID()
	})

	wl.segmentSnapshot.Store(&segments)
}

// Sync flushes the current active segment's data to disk.
func (wl *WALog) Sync() error {
	wl.writeMu.Lock()
	defer wl.writeMu.Unlock()
	if err := wl.RecoveryError(); err != nil {
		return err
	}
	activeSegment := wl.currentSegment
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

// Commit advances the committed position to the given position.
// When readerCommitCheck is enabled, readers cannot advance beyond this position.
// Call this once the log entry is confirmed committed.
// This method is safe for concurrent use.
func (wl *WALog) Commit(pos RecordPosition) {
	wl.committedPos.Store(&pos)
}

// CommittedPosition returns the current committed position.
// Returns NilRecordPosition if no position has been committed yet.
func (wl *WALog) CommittedPosition() RecordPosition {
	pos := wl.committedPos.Load()
	if pos == nil {
		return NilRecordPosition
	}
	return *pos
}

// Close gracefully shuts down all segments managed by the WALog.
func (wl *WALog) Close() error {
	wl.writeMu.Lock()
	defer wl.writeMu.Unlock()
	var cErr error
	for _, seg := range wl.segments {
		err := seg.Close()
		if err != nil {
			cErr = errors.Join(cErr, err)
		}
	}

	if wl.dirSyncer != nil {
		if err := wl.dirSyncer.SyncDir(wl.dir); err != nil {
			cErr = errors.Join(cErr, fmt.Errorf("fsync wal directory: %w", err))
		}
	}
	return cErr
}

// Write appends the given data as a new record to the active segment.
// It returns RecordPosition indicating where the data was written.
func (wl *WALog) Write(data []byte, logIndex uint64) (RecordPosition, error) {
	wl.writeMu.Lock()
	defer wl.writeMu.Unlock()
	if err := wl.RecoveryError(); err != nil {
		return RecordPosition{}, err
	}

	if wl.currentSegment == nil {
		return RecordPosition{}, errors.New("no active segment")
	}

	estimatedSize := recordOverhead(int64(len(data)))
	if wl.currentSegment.WillExceed(len(data)) && estimatedSize > (wl.maxSegmentSize-int64(segmentHeaderSize)) {
		return RecordPosition{}, ErrRecordTooLarge
	}

	// if current segment needs rotation rotate it.
	if wl.currentSegment.WillExceed(len(data)) {
		if err := wl.rotateSegment(); err != nil {
			return RecordPosition{}, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	pos, err := wl.currentSegment.Write(data, logIndex)
	if err != nil {
		return RecordPosition{}, fmt.Errorf("write failed: %w", err)
	}

	if logIndex > 0 {
		wl.logIndex.Set(logIndex, pos)
		wl.updateBoundsForIndex(logIndex)
	}

	wl.unSynced += estimatedSize

	if wl.bytesPerSync > 0 && wl.unSynced >= wl.bytesPerSync {
		if err := wl.currentSegment.MSync(); err != nil {
			// Bytes were already appended. Signal a durability failure so callers
			// cannot treat this as an unwritten record and reuse its LSN.
			return RecordPosition{}, fmt.Errorf("%w: %w", ErrFsync, err)
		}
		wl.unSynced = 0
		wl.bytesPerSyncCalled.Add(1)
	}

	return pos, nil
}

// WriteBatch appends multiple records to the active segment in a single batched operation.
// If the batch cannot fit entirely in the current segment, it handles automatic rotation:
// Returns a slice of RecordPositions for all successfully written records.
func (wl *WALog) WriteBatch(records [][]byte, logIndexes []uint64) ([]RecordPosition, error) {
	if len(records) == 0 {
		return nil, nil
	}

	wl.writeMu.Lock()
	defer wl.writeMu.Unlock()
	if err := wl.RecoveryError(); err != nil {
		return nil, err
	}

	if wl.currentSegment == nil {
		return nil, errors.New("no active segment")
	}

	// Pre-check each record to ensure it can fit in a segment
	remaining := wl.currentSegment.GetSegmentSize() - wl.currentSegment.WriteOffset()
	for _, data := range records {
		estimatedSize := recordOverhead(int64(len(data)))
		if estimatedSize > remaining {
			remaining = wl.maxSegmentSize - segmentHeaderSize
		}
		if estimatedSize > remaining {
			return nil, ErrRecordTooLarge
		}
		remaining -= estimatedSize
	}

	return wl.writeBatchLocked(records, logIndexes)
}

// nolint:gocognit
func (wl *WALog) writeBatchLocked(records [][]byte, logIndexes []uint64) ([]RecordPosition, error) {
	var allPositions []RecordPosition
	remaining := records
	remainingIndexes := logIndexes
	indexOffset := 0

	for len(remaining) > 0 {
		positions, written, batchErr := wl.currentSegment.WriteBatch(remaining, remainingIndexes)

		if batchErr != nil && !errors.Is(batchErr, ErrSegmentFull) {
			return allPositions, batchErr
		}

		// successfully written positions
		allPositions = append(allPositions, positions...)

		if logIndexes != nil && written > 0 {
			var minIdx uint64
			var maxIdx uint64
			for i := 0; i < written; i++ {
				idx := logIndexes[indexOffset+i]
				if idx > 0 {
					wl.logIndex.Set(idx, positions[i])
					if minIdx == 0 || idx < minIdx {
						minIdx = idx
					}
					if idx > maxIdx {
						maxIdx = idx
					}
				}
			}
			if minIdx > 0 {
				wl.updateBoundsRange(minIdx, maxIdx)
			}
		}

		// unsynced bytes counter
		for i := 0; i < written; i++ {
			wl.unSynced += recordOverhead(int64(len(remaining[i])))
		}

		if wl.bytesPerSync > 0 && wl.unSynced >= wl.bytesPerSync {
			if syncErr := wl.currentSegment.MSync(); syncErr != nil {
				return allPositions, fmt.Errorf("%w: %w", ErrFsync, syncErr)
			}
			wl.unSynced = 0
			wl.bytesPerSyncCalled.Add(1)
		}

		if written == len(remaining) {
			return allPositions, nil
		}

		// 1. Writes as many records as possible to the current segment
		// 2. Rotates to a new segment
		// 3. Writes remaining records to the new segment
		//
		// segment is full - rotate and continue with remaining records
		if errors.Is(batchErr, ErrSegmentFull) && written < len(remaining) {
			indexOffset += written
			remaining = remaining[written:]
			if remainingIndexes != nil {
				remainingIndexes = remainingIndexes[written:]
			}
			if rotateErr := wl.rotateSegment(); rotateErr != nil {
				return allPositions, fmt.Errorf("failed to rotate segment during batch write: %w", rotateErr)
			}
		} else {
			// don't know what to do, but handle it gracefully
			return allPositions, fmt.Errorf("unexpected state: written=%d, remaining=%d, err=%v", written, len(remaining), batchErr)
		}
	}

	return allPositions, nil
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
	wl.writeMu.RLock()
	defer wl.writeMu.RUnlock()
	if err := wl.RecoveryError(); err != nil {
		return nil, err
	}
	seg, ok := wl.segments[pos.SegmentID]

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

// Segments returns a snapshot (shallow) copy of all active segments managed by the WAL.
func (wl *WALog) Segments() map[SegmentID]*Segment {
	wl.writeMu.RLock()
	defer wl.writeMu.RUnlock()

	segmentsCopy := make(map[SegmentID]*Segment, len(wl.pendingDeletion))
	for id, seg := range wl.segments {
		segmentsCopy[id] = seg
	}
	return segmentsCopy
}

// Current returns a pointer to the currently active WAL segment.
func (wl *WALog) Current() *Segment {
	wl.writeMu.RLock()
	defer wl.writeMu.RUnlock()
	return wl.currentSegment
}

// LogIndex returns the shared sharded index mapping log index to record position.
func (wl *WALog) LogIndex() *ShardedIndex {
	return wl.logIndex
}

// GetBounds returns the first and last available log indices.
// Returns (0, 0) when no indexed entries exist.
func (wl *WALog) GetBounds() (first, last uint64) {
	return wl.firstLSN.Load(), wl.lastLSN.Load()
}

func (wl *WALog) setBounds(first, last uint64) {
	wl.firstLSN.Store(first)
	wl.lastLSN.Store(last)
}

func (wl *WALog) recomputeBounds() {
	if wl.logIndex == nil {
		wl.setBounds(0, 0)
		return
	}
	first, last, ok := wl.logIndex.GetFirstLast()
	if !ok {
		wl.setBounds(0, 0)
		return
	}
	wl.setBounds(first, last)
}

func (wl *WALog) deleteIndexForSegments(segIDs map[SegmentID]struct{}) {
	if wl.logIndex == nil || len(segIDs) == 0 {
		return
	}
	var indices []uint64
	wl.logIndex.Range(func(idx uint64, pos RecordPosition) bool {
		if _, ok := segIDs[pos.SegmentID]; ok {
			indices = append(indices, idx)
		}
		return true
	})
	for _, idx := range indices {
		wl.logIndex.Delete(idx)
	}
}

func (wl *WALog) updateBoundsForIndex(idx uint64) {
	if idx == 0 {
		return
	}
	first := wl.firstLSN.Load()
	if first == 0 || idx < first {
		wl.firstLSN.Store(idx)
	}
	last := wl.lastLSN.Load()
	if idx > last {
		wl.lastLSN.Store(idx)
	}
}

func (wl *WALog) updateBoundsRange(minIdx, maxIdx uint64) {
	if minIdx == 0 || maxIdx == 0 {
		return
	}
	first := wl.firstLSN.Load()
	if first == 0 || minIdx < first {
		wl.firstLSN.Store(minIdx)
	}
	last := wl.lastLSN.Load()
	if maxIdx > last {
		wl.lastLSN.Store(maxIdx)
	}
}

// PositionForIndex returns the RecordPosition for the given log index.
func (wl *WALog) PositionForIndex(idx uint64) (RecordPosition, error) {
	if err := wl.RecoveryError(); err != nil {
		return NilRecordPosition, err
	}
	if wl.logIndex == nil {
		return NilRecordPosition, errors.New("log index not initialized")
	}

	pos, ok := wl.logIndex.Get(idx)
	if !ok {
		return NilRecordPosition, fmt.Errorf("log index %d not found", idx)
	}
	return pos, nil
}

// RotateSegment rotates the current segment and create a new active segment.
func (wl *WALog) RotateSegment() error {
	wl.writeMu.Lock()
	defer wl.writeMu.Unlock()
	if err := wl.RecoveryError(); err != nil {
		return err
	}

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
		// Mark the sealed segment as in-memory sealed
		wl.currentSegment.MarkSealedInMemory()
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
	wl.snapshotSegments()
	wl.rotationCallback()
	return nil
}

// BackupLastRotatedSegment copies the most recently sealed WAL segment into backupDir and returns the backup path.
func (wl *WALog) BackupLastRotatedSegment(backupDir string) (string, error) {
	if backupDir == "" {
		return "", errors.New("backup directory cannot be empty")
	}
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}

	wl.writeMu.RLock()
	if err := wl.RecoveryError(); err != nil {
		wl.writeMu.RUnlock()
		return "", err
	}
	if wl.currentSegment == nil {
		wl.writeMu.RUnlock()
		return "", errors.New("no active segment")
	}
	if wl.currentSegment.ID() <= 1 {
		wl.writeMu.RUnlock()
		return "", fmt.Errorf("%w: no rotated segment available", ErrSegmentNotFound)
	}

	lastID := wl.currentSegment.ID() - 1
	segment, ok := wl.segments[lastID]
	if !ok {
		wl.writeMu.RUnlock()
		return "", fmt.Errorf("%w: segment %d", ErrSegmentNotFound, lastID)
	}

	segment.incrRef()
	wl.writeMu.RUnlock()
	defer segment.releaseRef()

	if !segment.IsInMemorySealed() && !IsSealed(segment.GetFlags()) {
		return "", fmt.Errorf("segment %d has not been sealed yet", segment.ID())
	}

	srcPath := segment.path
	dstPath := filepath.Join(backupDir, filepath.Base(srcPath))
	if filepath.Clean(srcPath) == filepath.Clean(dstPath) {
		return "", errors.New("backup destination must differ from WAL directory")
	}

	if err := copySegmentFile(srcPath, dstPath, wl.dirSyncer); err != nil {
		return "", fmt.Errorf("backup segment %d: %w", segment.ID(), err)
	}

	return dstPath, nil
}

// BackupSegmentsAfter copies every sealed segment with ID > afterID into backupDir.
// Returns a map of SegmentID to the backup file path.
func (wl *WALog) BackupSegmentsAfter(afterID SegmentID, backupDir string) (map[SegmentID]string, error) {
	if backupDir == "" {
		return nil, errors.New("backup directory cannot be empty")
	}
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	wl.writeMu.RLock()
	if err := wl.RecoveryError(); err != nil {
		wl.writeMu.RUnlock()
		return nil, err
	}
	var (
		targets   []*Segment
		currentID SegmentID
	)
	if wl.currentSegment != nil {
		currentID = wl.currentSegment.ID()
	}
	for id, seg := range wl.segments {
		if id <= afterID {
			continue
		}
		if id == currentID && !seg.IsInMemorySealed() && !IsSealed(seg.GetFlags()) {
			continue
		}
		seg.incrRef()
		targets = append(targets, seg)
	}
	wl.writeMu.RUnlock()

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: no segments after %d", ErrSegmentNotFound, afterID)
	}

	sort.Slice(targets, func(i, j int) bool {
		return targets[i].ID() < targets[j].ID()
	})

	defer func() {
		for _, seg := range targets {
			seg.releaseRef()
		}
	}()

	backups := make(map[SegmentID]string, len(targets))

	for _, seg := range targets {
		if !seg.IsInMemorySealed() && !IsSealed(seg.GetFlags()) {
			return nil, fmt.Errorf("segment %d has not been sealed yet", seg.ID())
		}

		srcPath := seg.path
		dstPath := filepath.Join(backupDir, filepath.Base(srcPath))
		if filepath.Clean(srcPath) == filepath.Clean(dstPath) {
			return nil, errors.New("backup destination must differ from WAL directory")
		}

		if err := copySegmentFile(srcPath, dstPath, wl.dirSyncer); err != nil {
			return nil, fmt.Errorf("backup segment %d: %w", seg.ID(), err)
		}

		backups[seg.ID()] = dstPath
	}

	return backups, nil
}

func copySegmentFile(srcPath, dstPath string, syncer DirectorySyncer) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source segment: %w", err)
	}
	defer src.Close()

	tmpPath := dstPath + ".tmp"
	dst, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fileModePerm)
	if err != nil {
		return fmt.Errorf("create temp backup: %w", err)
	}

	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("copy contents: %w", err)
	}

	if err := dst.Sync(); err != nil {
		dst.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync backup: %w", err)
	}

	if err := dst.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close backup: %w", err)
	}

	_ = os.Remove(dstPath)
	if err := os.Rename(tmpPath, dstPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("finalize backup: %w", err)
	}

	dir := filepath.Dir(dstPath)
	if syncer == nil {
		syncer = DirectorySyncFunc(syncDir)
	}
	if err := syncer.SyncDir(dir); err != nil {
		return fmt.Errorf("sync backup directory: %w", err)
	}

	return nil
}

// StartPendingSegmentCleaner starts a background goroutine that periodically
// inspects queued segments and removes them after the predicate allows deletion
// and all references have been released.
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
				wl.cleanPendingSegments(canDeleteFn)
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

	wl.deletionMu.Lock()
	defer wl.deletionMu.Unlock()
	wl.writeMu.RLock()
	defer wl.writeMu.RUnlock()
	if wl.RecoveryError() != nil {
		return
	}
	currentSegments := len(wl.segments)

	if wl.minSegmentsToRetain > 0 && currentSegments <= wl.minSegmentsToRetain {
		return
	}

	var candidates []*Segment
	for _, seg := range wl.segments {
		if seg.closed.Load() {
			// already closed
			continue
		}
		if seg.markedForDeletion.Load() {
			// already queued
			continue
		}
		if len(seg.mmapData) < segmentHeaderSize {
			continue
		}
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
	if segmentsToDelete > segmentsAboveMin {
		segmentsToDelete = segmentsAboveMin
	}

	for _, seg := range candidates {
		if segmentsToDelete <= 0 {
			break
		}
		if _, alreadyQueued := wl.pendingDeletion[seg.ID()]; !alreadyQueued {
			wl.pendingDeletion[seg.ID()] = seg
		}
		segmentsToDelete--
	}
}

func (wl *WALog) QueuedSegmentsForDeletion() map[SegmentID]*Segment {
	wl.deletionMu.Lock()
	defer wl.deletionMu.Unlock()

	segmentsCopy := make(map[SegmentID]*Segment, len(wl.pendingDeletion))
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
	wl.writeMu.Lock()
	defer wl.writeMu.Unlock()
	if wl.RecoveryError() != nil {
		return
	}
	removedIDs := make(map[SegmentID]struct{})
	for id, seg := range wl.pendingDeletion {
		if _, err := os.Stat(seg.path); os.IsNotExist(err) {
			removedIDs[id] = struct{}{}
		}
	}
	wl.deleteIndexForSegments(removedIDs)
	for id := range removedIDs {
		delete(wl.segments, id)
		delete(wl.pendingDeletion, id)
	}
	wl.snapshotSegments()
	wl.recomputeBounds()
}

// Selection only queues candidates. Perform deletion under the WAL lock, after
// checking the predicate and references, so no deferred deletion can later remove
// a segment that has since been unsealed and reused as a truncation target.
func (wl *WALog) cleanPendingSegments(canDeleteFn func(SegmentID) bool) {
	wl.deletionMu.Lock()
	defer wl.deletionMu.Unlock()
	for id, seg := range wl.pendingDeletion {
		if canDeleteFn == nil || !canDeleteFn(id) {
			continue
		}
		wl.writeMu.Lock()
		if wl.RecoveryError() == nil && wl.segments[id] == seg && seg != wl.currentSegment && seg.IsSealed() && seg.refCount.Load() == 0 {
			if err := wl.deleteSegments([]SegmentID{id}); err != nil {
				slog.Error("[walfs]", "message", "failed to remove pending segment", "segment_id", id, "error", err)
			} else {
				delete(wl.pendingDeletion, id)
				wl.snapshotSegments()
				wl.recomputeBounds()
			}
		}
		wl.writeMu.Unlock()
	}
}

// deleteSegments requires writeMu. Publish index/map removals only after the
// corresponding file removal succeeds.
func (wl *WALog) deleteSegments(ids []SegmentID) error {
	for _, id := range ids {
		seg := wl.segments[id]
		first, count := seg.FirstLogIndex(), seg.GetEntryCount()
		if err := seg.Remove(); err != nil {
			return fmt.Errorf("failed to remove segment %d: %w", id, err)
		}
		if first > 0 && count > 0 {
			wl.logIndex.DeleteRange(first, first+uint64(count)-1)
		}
		delete(wl.segments, id)
		if wl.currentSegment == seg {
			wl.currentSegment = nil
		}
	}
	return nil
}

// https://man7.org/linux/man-pages/man2/fsync.2.html
// Calling fsync() does not necessarily ensure that the entry in the
// directory containing the file has also reached disk.  For that an
// explicit fsync() on a file descriptor for the directory is also
// needed.
func syncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync()
}

// ReaderOption configures a Reader.
type ReaderOption func(*Reader)

// WithDecoder sets a custom decoder for the reader.
// When set, NextRecord() will use this decoder to convert raw bytes to decoded data.
func WithDecoder(decoder RecordDecoder) ReaderOption {
	return func(r *Reader) {
		r.decoder = decoder
	}
}

// Reader represents a high-level sequential reader over a WALog.
// It reads across all available WAL segments in order, automatically
// advancing from one segment to the next.
// Reader is not safe for concurrent use.
type Reader struct {
	wal      *WALog
	segments []*Segment
	// index in segments
	segmentIndex  int
	currentReader *SegmentReader
	startOffset   int64
	decoder       RecordDecoder
}

// NewReader returns a new Reader that sequentially reads all segments in the WALog,
// starting from the beginning (lowest SegmentID).
func (wl *WALog) NewReader(opts ...ReaderOption) *Reader {
	segments := *wl.segmentSnapshot.Load()

	r := &Reader{
		wal:           wl,
		segments:      segments,
		currentReader: nil,
		decoder:       NoopDecoder{},
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Close closes all segment readers to release their references.
// IMPORTANT: This method MUST be called after the Reader is no longer needed.
func (r *Reader) Close() {
	if r.currentReader != nil {
		r.currentReader.Close()
	}
}

// Next returns the next available WAL record data and its current position.
// IMPORTANT: The returned `[]byte` is a slice of a memory-mapped file, so data must not be retained or modified.
// If the data needs to be used beyond the lifetime of the segment, the caller MUST copy it.
// When readerCommitCheck is enabled on the WALog, returns ErrNoNewData if the reader
// has reached the committed boundary.
func (r *Reader) Next() ([]byte, RecordPosition, error) {
	if r.wal != nil {
		if err := r.wal.RecoveryError(); err != nil {
			return nil, NilRecordPosition, err
		}
	}
	for {
		// no current segment reader, it advances to the next segment in r.segments until all are exhausted.
		if r.currentReader == nil {
			if r.segmentIndex >= len(r.segments) {
				return nil, NilRecordPosition, io.EOF
			}
			seg := r.segments[r.segmentIndex]
			r.segmentIndex++

			reader := seg.NewReader()
			if reader == nil {
				continue
			}
			// make sure to advance to correct offset
			if r.segmentIndex == 1 && r.startOffset > segmentHeaderSize {
				reader.readOffset = r.startOffset
			}
			r.currentReader = reader
		}

		// Check committed boundary before reading.
		// If readerCommitCheck is enabled and we're at or beyond the committed position,
		// return ErrNoNewData without advancing.
		if r.wal != nil && r.wal.readerCommitCheck {
			if r.isAtOrBeyondCommitted() {
				return nil, NilRecordPosition, ErrNoNewData
			}
		}

		reader := r.currentReader
		data, pos, err := reader.Next()
		if err == nil {
			decoded, decErr := r.decoder.Decode(data)
			if decErr != nil {
				return nil, pos, decErr
			}
			return decoded, pos, nil
		}
		// the current segment is exhausted, moves to the next segment.
		if errors.Is(err, io.EOF) {
			reader.Close()
			r.currentReader = nil
			continue
		}
		return nil, NilRecordPosition, err
	}
}

// isAtOrBeyondCommitted checks if the reader's current position is at or beyond
// the committed position. Returns true if reader should not advance further.
func (r *Reader) isAtOrBeyondCommitted() bool {
	committed := r.wal.committedPos.Load()
	if committed == nil {
		// No committed position yet - nothing to read
		return true
	}

	// Get the current read position
	// current segment (segmentIndex was already incremented)
	seg := r.segments[r.segmentIndex-1]
	nextPos := RecordPosition{
		SegmentID: seg.ID(),
		Offset:    r.currentReader.readOffset,
	}

	// Reader is beyond committed if:
	// - segment ID > committed segment ID, OR
	// - same segment but offset > committed offset
	if nextPos.SegmentID > committed.SegmentID {
		return true
	}
	if nextPos.SegmentID == committed.SegmentID && nextPos.Offset > committed.Offset {
		return true
	}
	return false
}

// SeekNext advances the reader by one record, discarding the data.
func (r *Reader) SeekNext() error {
	_, _, err := r.Next()
	return err
}

// LastRecordPosition returns the RecordPosition of the last successfully read entry.
func (r *Reader) LastRecordPosition() RecordPosition {
	return r.currentReader.LastRecordPosition()
}

// NewReaderAfter returns a reader that starts after the given RecordPosition.
// It first creates a reader from that position, then skips one record.
func (wl *WALog) NewReaderAfter(pos RecordPosition, opts ...ReaderOption) (*Reader, error) {
	reader, err := wl.NewReaderWithStart(pos, opts...)
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
func (wl *WALog) NewReaderWithStart(pos RecordPosition, opts ...ReaderOption) (*Reader, error) {
	if pos.SegmentID == 0 {
		return wl.NewReader(opts...), nil
	}

	segments := *wl.segmentSnapshot.Load()

	var (
		filtered     []*Segment
		segmentFound bool
		startOffset  int64 = segmentHeaderSize
	)

	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]
		if seg.ID() < pos.SegmentID {
			// we've gone past the matching segment; stop
			break
		}
		if seg.ID() == pos.SegmentID {
			segmentFound = true
			if pos.Offset > seg.GetSegmentSize() {
				return nil, ErrOffsetOutOfBounds
			}
			if pos.Offset > segmentHeaderSize {
				startOffset = pos.Offset
			}
		}
		// prepend to preserve ascending order
		filtered = append([]*Segment{seg}, filtered...)
	}

	if !segmentFound {
		return nil, ErrSegmentNotFound
	}

	r := &Reader{
		wal:           wl,
		segments:      filtered,
		startOffset:   startOffset,
		currentReader: nil,
		decoder:       NoopDecoder{},
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// PositionForIndexWithBounds returns the RecordPosition for the given log index,
// returning typed errors for "not yet available" vs "truncated".
func (wl *WALog) PositionForIndexWithBounds(idx uint64) (RecordPosition, error) {
	if err := wl.RecoveryError(); err != nil {
		return NilRecordPosition, err
	}
	if wl.logIndex == nil {
		return NilRecordPosition, errors.New("log index not initialized")
	}

	first, last := wl.GetBounds()
	if first == 0 && last == 0 {
		return NilRecordPosition, &LSNNotYetAvailableError{
			RequestedLSN:  idx,
			CurrentMaxLSN: 0,
		}
	}
	if idx > last {
		return NilRecordPosition, &LSNNotYetAvailableError{
			RequestedLSN:  idx,
			CurrentMaxLSN: last,
		}
	}
	if idx < first {
		return NilRecordPosition, &LSNTruncatedError{
			RequestedLSN:      idx,
			FirstAvailableLSN: first,
		}
	}

	pos, ok := wl.logIndex.Get(idx)
	if !ok {
		return NilRecordPosition, fmt.Errorf(
			"log index %d not found (expected range [%d, %d])",
			idx, first, last,
		)
	}
	return pos, nil
}
