package walfs

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrSegmentNotFound    = errors.New("segment not found")
	ErrOffsetOutOfBounds  = errors.New("start offset is beyond segment size")
	ErrOffsetBeforeHeader = errors.New("start offset is within reserved segment header")
	ErrFsync              = errors.New("fsync error")
	ErrRecordTooLarge     = errors.New("record size exceeds maximum segment capacity")
)

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

	mu             sync.RWMutex
	currentSegment *Segment
	segments       map[SegmentID]*Segment
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
func (sm *WALog) openSegment(id uint32) (*Segment, error) {
	return OpenSegmentFile(sm.dir, sm.ext, id, WithSegmentSize(sm.maxSegmentSize), WithSyncOption(sm.forceSyncEveryWrite))
}

func (sm *WALog) recoverSegments() error {
	files, err := os.ReadDir(sm.dir)
	if err != nil {
		return fmt.Errorf("failed to read segment directory: %w", err)
	}

	var segmentIDs []SegmentID

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), sm.ext) {
			continue
		}
		// e.g. "000000001.wal" -> 1
		base := strings.TrimSuffix(file.Name(), sm.ext)
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
		seg, err := sm.openSegment(1)
		if err != nil {
			return fmt.Errorf("failed to create initial segment: %w", err)
		}

		sm.segments[1] = seg
		sm.currentSegment = seg
		return nil
	}

	for i, id := range segmentIDs {
		seg, err := sm.openSegment(id)
		if err != nil {
			return fmt.Errorf("failed to open segment %d: %w", id, err)
		}
		if i < len(segmentIDs)-1 && !IsSealed(seg.GetFlags()) {
			err := seg.SealSegment()
			if err != nil {
				return err
			}
		}
		sm.segments[id] = seg
		sm.currentSegment = seg
	}

	return nil
}

func (sm *WALog) Sync() error {
	sm.mu.Lock()
	activeSegment := sm.currentSegment
	sm.mu.Unlock()
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

func (sm *WALog) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var cErr error
	for _, seg := range sm.segments {
		err := seg.Close()
		if err != nil {
			cErr = errors.Join(cErr, err)
		}
	}
	return cErr
}

// Write appends the given data as a new record to the active segment.
func (sm *WALog) Write(data []byte) (RecordPosition, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.currentSegment == nil {
		return RecordPosition{}, errors.New("no active segment")
	}

	estimatedSize := recordOverhead(int64(len(data)))
	if estimatedSize > sm.maxSegmentSize {
		return RecordPosition{}, ErrRecordTooLarge
	}

	// if current segment needs rotation rotate it.
	if sm.currentSegment.WillExceed(len(data)) {
		if err := sm.rotateSegment(); err != nil {
			return RecordPosition{}, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	pos, err := sm.currentSegment.Write(data)
	if err != nil {
		return RecordPosition{}, fmt.Errorf("write failed: %w", err)
	}

	sm.unSynced += estimatedSize

	if sm.bytesPerSync > 0 && sm.unSynced >= sm.bytesPerSync {
		if err := sm.currentSegment.MSync(); err != nil {
			return RecordPosition{}, err
		}
		sm.unSynced = 0
		sm.bytesPerSyncCalled.Add(1)
	}

	return *pos, nil
}

func recordOverhead(dataLen int64) int64 {
	return alignUp(dataLen) + recordHeaderSize + recordTrailerMarkerSize
}

// BytesPerSyncCallCount how many times this was called on current active segment.
func (sm *WALog) BytesPerSyncCallCount() int64 {
	return sm.bytesPerSyncCalled.Load()
}

func (sm *WALog) Read(pos RecordPosition) ([]byte, error) {
	sm.mu.RLock()
	seg, ok := sm.segments[pos.SegmentID]
	sm.mu.RUnlock()

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

func (sm *WALog) Segments() map[SegmentID]*Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	segmentsCopy := make(map[SegmentID]*Segment, len(sm.segments))
	for id, seg := range sm.segments {
		segmentsCopy[id] = seg
	}
	return segmentsCopy
}

func (sm *WALog) Current() *Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentSegment
}

func (sm *WALog) RotateSegment() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.rotateSegment()
}

func (sm *WALog) rotateSegment() error {
	if sm.currentSegment != nil && !IsSealed(sm.currentSegment.GetFlags()) {
		if err := sm.currentSegment.SealSegment(); err != nil {
			return fmt.Errorf("failed to seal current segment: %w", err)
		}
		err := sm.currentSegment.Sync()
		if err != nil {
			return err
		}
	}

	var newID SegmentID = 1
	if sm.currentSegment != nil {
		newID = sm.currentSegment.ID() + 1
	}

	newSegment, err := sm.openSegment(newID)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %w", err)
	}

	sm.segments[newID] = newSegment
	sm.currentSegment = newSegment
	sm.bytesPerSyncCalled.Store(0)

	return nil
}

type Reader struct {
	segmentReaders []*SegmentReader
	currentReader  int
}

func (sm *WALog) NewReader() *Reader {
	sm.mu.RLock()
	segments := maps.Clone(sm.segments)
	sm.mu.RUnlock()

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

func (r *Reader) Next() ([]byte, *RecordPosition, error) {
	for r.currentReader < len(r.segmentReaders) {
		data, pos, err := r.segmentReaders[r.currentReader].Next()
		if err == nil {
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

func (sm *WALog) NewReaderWithStart(startOffset RecordPosition) (*Reader, error) {
	if startOffset.SegmentID == 0 {
		// interpreting SegmentID == 0 as: read from the beginning
		return sm.NewReader(), nil
	}

	sm.mu.RLock()
	segments := make([]*Segment, 0, len(sm.segments))
	for _, seg := range sm.segments {
		segments = append(segments, seg)
	}
	sm.mu.RUnlock()

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID() < segments[j].ID()
	})

	var (
		readers      []*SegmentReader
		segmentFound bool
	)

	for _, seg := range segments {
		if seg.ID() < startOffset.SegmentID {
			continue
		}
		reader := seg.NewReader()
		if reader == nil {
			continue
		}

		if seg.ID() == startOffset.SegmentID {
			segmentFound = true
			size := seg.GetSegmentSize()
			if startOffset.Offset > size {
				return nil, ErrOffsetOutOfBounds
			}

			reader.readOffset = startOffset.Offset
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
