package walfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
)

var (
	ErrClosed              = errors.New("the Segment file is closed")
	ErrInvalidCRC          = errors.New("invalid crc, the data may be corrupted")
	ErrCorruptHeader       = errors.New("corrupt record header, invalid length")
	ErrIncompleteChunk     = errors.New("incomplete or torn write detected at record trailer")
	ErrSegmentSealed       = errors.New("cannot write to sealed segment")
	ErrSegmentReaderClosed = errors.New("segment reader is closed")
	ErrNoNewData           = errors.New("no new data yet")
	ErrSegmentFull         = errors.New("segment is full, cannot write more records")
	// ErrSegmentCorrupt reports a sealed segment whose valid records do not
	// cover the count and end recorded in its header.
	ErrSegmentCorrupt = errors.New("sealed segment records do not match its header")
)

type MarkerValidator func(storedMarker uint32) error

var (
	// NilRecordPosition is a sentinel value representing an nil RecordPosition.
	NilRecordPosition = RecordPosition{}

	crcTable = crc32.MakeTable(crc32.Castagnoli)
	// marker written after every WAL record to detect torn/incomplete writes.
	trailerMarker = []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0xED, 0xFA, 0xCE}
)

const trailerWord uint64 = 0xCEFAEDFEEFBEADDE

const (
	StateOpen = iota
	StateClosing

	// 4 GiB.
	maxSegmentSize = 4 * 1024 * 1024 * 1024

	FlagActive uint32 = 1 << iota
	FlagSealed uint32 = 1 << 1

	segmentHeaderSize = 64
	// just a string of "UWAL"
	// 'U' = 0x55 and so on. Unison Write ahead log.
	segmentMagicNumber   = 0x5557414C
	segmentHeaderVersion = 2 // 2: sealed segments carry a footer; no .idx sidecar

	// layout: 4 (checksum) + 4 (length) = 8 bytes
	recordHeaderSize = 8
	// default Segment size of 16MB.
	segmentSize  = 16 * 1024 * 1024
	fileModePerm = 0644
	// each index entry stores offset + length (16 bytes).
	indexEntrySize = 16

	// size of the trailer used to detect torn writes.
	// We are writing this to detect torn or partial writes caused by unexpected shutdowns or disk failures.
	// This is inspired by a real-world issue observed in etcd v2.3:
	// SEE: https://github.com/etcd-io/etcd/issues/6191#issuecomment-240268979
	// By adding a known trailer marker (e.g., 0xDEADBEEF), we can explicitly validate that a record entry.
	// was fully persisted, and safely stop recovery at the first missing or corrupted trailer.
	recordTrailerMarkerSize = 8
	// alignSize defines the boundary (in bytes) to which all WAL entries (headers, payloads, trailers) are aligned.
	// helps us reduce the chance of partially written headers/trailers across page boundaries during crashes.
	// atomic sector writes are not used for the correctness but gives us better chance for recovery.
	// SEE: https://github.com/boltdb/bolt/issues/548
	alignSize int64 = 8
	alignMask int64 = alignSize - 1
)

type MsyncOption int

const (
	// MsyncNone skips msync after write.
	MsyncNone MsyncOption = iota

	// MsyncOnWrite calls msync (Flush) after every write.
	MsyncOnWrite
)

type SegmentID = uint32

// SegmentHeader encodes all the necessary information about the segment file at the top of the file.
// Its Size is 64 byte once encoded.
type SegmentHeader struct {
	// at 0
	Magic uint32
	// at 4
	Version uint32
	// at 8
	CreatedAt int64
	// at 16
	LastModifiedAt int64
	// at 24
	WriteOffset int64
	// at 32
	EntryCount int64
	// at 40
	Flags uint32

	// at 44 -51
	FirstLogIndex uint64
	// - Reserved for future use
	// 52-55
	_ [4]byte

	// at 56 byte: - CRC32 of first 56 bytes
	CRC uint32
	// at 60 - padding to align to 64B
	_ uint32
}

/* Record Layout:
┌──────────────────────────────────────────────────────────────┐
│ 0..3   CRC32C(header[4:8] || data)                           │
│ 4..7   u32 length                                            │
│ 8..(8+len-1)   data                                          │
│ (8+len)..(16+len-1)  trailer 0xDEADBEEFFEEDFACE              │
│ ... zero padding to next 8-byte boundary                     │
└──────────────────────────────────────────────────────────────┘
*/

func decodeSegmentHeader(buf []byte) (*SegmentHeader, error) {
	if len(buf) < 64 {
		return nil, io.ErrUnexpectedEOF
	}

	crc := binary.LittleEndian.Uint32(buf[56:60])
	computed := crc32.Checksum(buf[0:56], crcTable)
	if crc != computed {
		return nil, fmt.Errorf("segment metadata CRC mismatch: expected %08x, got %08x", crc, computed)
	}

	meta := &SegmentHeader{
		Magic:          binary.LittleEndian.Uint32(buf[0:4]),
		Version:        binary.LittleEndian.Uint32(buf[4:8]),
		CreatedAt:      int64(binary.LittleEndian.Uint64(buf[8:16])),
		LastModifiedAt: int64(binary.LittleEndian.Uint64(buf[16:24])),
		WriteOffset:    int64(binary.LittleEndian.Uint64(buf[24:32])),
		EntryCount:     int64(binary.LittleEndian.Uint64(buf[32:40])),
		Flags:          binary.LittleEndian.Uint32(buf[40:44]),
		FirstLogIndex:  binary.LittleEndian.Uint64(buf[44:52]),
	}
	return meta, nil
}

// RecordPosition is the logical location of a record entry within a WAL Segment.
type RecordPosition struct {
	SegmentID SegmentID
	Offset    int64
}

func (rp RecordPosition) String() string {
	return fmt.Sprintf("SegmentID=%d, Offset=%d", rp.SegmentID, rp.Offset)
}

// Encode serializes the RecordPosition into a fixed-length byte slice.
func (rp RecordPosition) Encode() []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], rp.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(rp.Offset))
	return buf
}

// EncodeRecordPositionTo serializes a RecordPosition into the provided buffer.
// The buffer must be at least 12 bytes long. If it's shorter, a new 12-byte slice is allocated.
func EncodeRecordPositionTo(pos RecordPosition, buf []byte) []byte {
	if len(buf) < 12 {
		buf = make([]byte, 12)
	} else {
		buf = buf[:12]
	}
	binary.LittleEndian.PutUint32(buf[0:4], pos.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(pos.Offset))
	return buf
}

// IsZero returns true if the RecordPosition is uninitialized,
// meaning both SegmentID and Offset are zero.
func (rp RecordPosition) IsZero() bool {
	return rp.SegmentID == 0 && rp.Offset == 0
}

// DecodeRecordPosition deserializes a byte slice into a RecordPosition.
func DecodeRecordPosition(data []byte) (RecordPosition, error) {
	if len(data) < 12 {
		return RecordPosition{}, io.ErrUnexpectedEOF
	}
	cp := RecordPosition{
		SegmentID: binary.LittleEndian.Uint32(data[0:4]),
		Offset:    int64(binary.LittleEndian.Uint64(data[4:12])),
	}
	return cp, nil
}

type segmentIndexEntry struct {
	Offset uint64
	Length uint32
}

// SegmentIndexEntry exposes a record's physical location within a WAL segment.
type SegmentIndexEntry struct {
	SegmentID SegmentID
	Offset    int64
	Length    uint32
}

// Segment represents a single WAL segment backed by a memory-mapped file.
type Segment struct {
	path        string
	id          SegmentID
	fd          *os.File
	mmapData    mmap.MMap
	mmapSize    int64
	writeOffset atomic.Int64
	closed      atomic.Bool
	header      []byte

	refCount          atomic.Int64
	state             atomic.Int64
	markedForDeletion atomic.Bool
	readerIDCounter   atomic.Uint64
	activeReaders     *readerTracker
	closeCond         *sync.Cond

	isSealed       atomic.Bool
	inMemorySealed atomic.Bool
	lifecycleMu    sync.Mutex // serializes sealing, truncation and close
	writeMu        sync.RWMutex
	syncOption     MsyncOption
	dirSyncer      DirectorySyncer

	dense         atomic.Pointer[denseIndex]  // unsealed segments: one offset per record
	sparse        atomic.Pointer[sparseIndex] // sealed segments: from the footer
	firstLogIndex uint64
	firstLSN      atomic.Uint64 // lock-free copy of firstLogIndex for lookups

	customMarker    uint32
	markerValidator MarkerValidator
}

// WithSyncOption sets the sync option for the Segment.
func WithSyncOption(opt MsyncOption) func(*Segment) {
	return func(s *Segment) {
		s.syncOption = opt
	}
}

// WithSegmentDirectorySyncer sets the directory syncer used after destructive operations.
func WithSegmentDirectorySyncer(syncer DirectorySyncer) func(*Segment) {
	return func(s *Segment) {
		if syncer != nil {
			s.dirSyncer = syncer
		}
	}
}

// WithSegmentSize sets the size for the Segment.
func WithSegmentSize(size int64) func(*Segment) {
	return func(s *Segment) {
		s.mmapSize = size
	}
}

// WithSegmentCustomMarker sets the 4-byte marker written for new segments.
// Use WithSegmentCustomMarkerValidator to validate stored markers on open.
func WithSegmentCustomMarker(marker uint32) func(*Segment) {
	return func(s *Segment) {
		s.customMarker = marker
	}
}

// WithSegmentCustomMarkerValidator sets a validator for stored markers when opening segments.
func WithSegmentCustomMarkerValidator(validator MarkerValidator) func(*Segment) {
	return func(s *Segment) {
		s.markerValidator = validator
	}
}

// OpenSegmentFile opens an existing segment file or create a new one if not present.
// If SegmentFile is sealed it doesn't scan its content while opening.
func OpenSegmentFile(dirPath, extName string, id uint32, opts ...func(*Segment)) (*Segment, error) {
	path := SegmentFileName(dirPath, extName, id)
	isNew, err := isNewSegment(path)
	if err != nil {
		return nil, err
	}

	s := &Segment{
		path:          path,
		id:            id,
		header:        make([]byte, recordHeaderSize),
		mmapSize:      segmentSize,
		syncOption:    MsyncNone,
		activeReaders: newReaderTracker(),
		dirSyncer:     DirectorySyncFunc(syncDir),
	}
	s.state.Store(StateOpen)
	s.closeCond = sync.NewCond(&sync.Mutex{})

	for _, opt := range opts {
		opt(s)
	}

	if err := validateSegmentSize(s.mmapSize); err != nil {
		return nil, err
	}

	fd, mmapData, err := s.prepareSegmentFile(path, isNew)
	if err != nil {
		return nil, err
	}
	s.fd = fd
	s.mmapData = mmapData
	opened := false
	defer func() {
		if !opened {
			_ = mmapData.Unmap()
			_ = fd.Close()
		}
	}()

	offset := int64(segmentHeaderSize)
	if isNew {
		// for a new Segment file we initialize it with default metadata.
		writeInitialMetadata(mmapData, s.customMarker)
	} else {
		// decode the Segment metadata header
		meta, err := decodeSegmentHeader(mmapData[:segmentHeaderSize])
		if err != nil {
			return nil, fmt.Errorf("failed to decode metadata: %w", err)
		}
		if meta.Magic != segmentMagicNumber || meta.Version != segmentHeaderVersion {
			return nil, fmt.Errorf("unsupported WAL segment format: magic %08x version %d, want %08x version %d",
				meta.Magic, meta.Version, segmentMagicNumber, segmentHeaderVersion)
		}

		if IsSealed(meta.Flags) && (meta.WriteOffset < segmentHeaderSize || meta.WriteOffset > s.mmapSize) {
			return nil, fmt.Errorf("segment write offset %d outside file size %d", meta.WriteOffset, s.mmapSize)
		}

		storedMarker := binary.LittleEndian.Uint32(mmapData[52:56])
		if s.markerValidator != nil {
			if err := s.markerValidator(storedMarker); err != nil {
				return nil, err
			}
		}

		if IsSealed(meta.Flags) {
			// for the sealed Segment our active offset is already saved
			offset = meta.WriteOffset
			s.isSealed.Store(true)
		} else {
			// while we trust the write offset we are scanning the Segment
			// to find the true end of valid data for safe appends,
			// while in most cases this would not happen but if crashed
			// we don't know if the written header metadata offset is valid enough.
			offset = s.scanForLastOffset()
		}
	}
	s.writeOffset.Store(offset)

	if !isNew {
		s.firstLogIndex = binary.LittleEndian.Uint64(mmapData[44:52])
		s.firstLSN.Store(s.firstLogIndex)
	}

	if err := s.setupIndex(isNew); err != nil {
		return nil, err
	}

	if !isNew && !s.isSealed.Load() {
		// Record the recovered end and count, and clear everything after the
		// valid prefix. A crash can leave a later record intact behind a torn
		// one; without clearing, a same-size append would reconnect it. Make
		// both durable before this writer accepts appends.
		binary.LittleEndian.PutUint64(mmapData[24:32], uint64(offset))
		binary.LittleEndian.PutUint64(mmapData[32:40], s.indexedCount())
		binary.LittleEndian.PutUint32(mmapData[56:60], crc32.Checksum(mmapData[0:56], crcTable))
		s.zeroDiscardedTail(offset, s.mmapSize)
		if err := s.Sync(); err != nil {
			return nil, fmt.Errorf("sync recovered segment %d: %w", id, err)
		}
	}

	opened = true
	return s, nil
}

// setupIndex installs the in-memory index when a segment is opened: a
// sealed segment's sparse index comes from its footer, an unsealed segment is
// scanned. A sealed segment whose footer is invalid is corrupt.
func (seg *Segment) setupIndex(isNew bool) error {
	if isNew {
		seg.dense.Store(newDenseIndex())
		return nil
	}
	if seg.isSealed.Load() {
		sparse, err := seg.readFooterLocked()
		if err != nil {
			return err
		}
		seg.sparse.Store(sparse)
		return nil
	}
	var entries []segmentIndexEntry
	seg.iterateValidEntries(func(offset int64, length uint32) bool {
		entries = append(entries, segmentIndexEntry{Offset: uint64(offset), Length: length})
		return true
	})
	seg.installDense(entries)
	return nil
}

// appendIndexEntry requires writeMu; the record bytes are already written.
func (seg *Segment) appendIndexEntry(offset int64) {
	d := seg.dense.Load()
	if d == nil {
		d = newDenseIndex()
		seg.dense.Store(d)
	}
	d.append(offset)
}

// IndexEntries returns the offset and length of every indexed record. Sealed
// segments hold only a sparse index, so their entries are read from the file.
func (seg *Segment) IndexEntries() []SegmentIndexEntry {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	if seg.closed.Load() {
		return nil
	}
	raw := seg.entriesLocked()
	entries := make([]SegmentIndexEntry, len(raw))
	for i, entry := range raw {
		entries[i] = SegmentIndexEntry{
			SegmentID: seg.id,
			Offset:    int64(entry.Offset),
			Length:    entry.Length,
		}
	}
	return entries
}

// ClearIndexFromMemory is retained for compatibility. Sealed segments always
// keep only a sparse index, so there is nothing further to release.
//
// Deprecated: the per-record index of a sealed segment is never retained.
func (seg *Segment) ClearIndexFromMemory() {}

// IsSealed returns if teh provided flag has sealed bit set.
func IsSealed(flags uint32) bool {
	return flags&FlagSealed != 0
}

func IsActive(flags uint32) bool {
	return flags&FlagActive != 0
}

// SealSegment seals the given segment.
func (seg *Segment) SealSegment() error {
	seg.lifecycleMu.Lock()
	defer seg.lifecycleMu.Unlock()
	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	if seg.closed.Load() {
		return ErrClosed
	}

	if seg.isSealed.Load() {
		return nil
	}

	// Footer first, made durable before the header marks the segment sealed:
	// write-back order is not guaranteed, so one sync for both could persist a
	// sealed header without its footer.
	end := seg.writeOffset.Load()
	count := seg.indexedCount()
	d := seg.dense.Load()
	sparse := buildSparseIndex(count, func(i uint64) int64 { off, _ := d.get(i); return off })
	if err := seg.writeFooterLocked(sparse, end); err != nil {
		return err
	}
	if err := seg.Sync(); err != nil {
		// Leave no footer bytes behind in a segment that stays unsealed.
		clear(seg.mmapData[end : end+footerSize(sparse)])
		return fmt.Errorf("%w: sync footer of segment %d: %w", ErrFsync, seg.id, err)
	}

	mmapData := seg.mmapData
	now := uint64(time.Now().UnixNano())
	binary.LittleEndian.PutUint64(mmapData[16:24], now)
	binary.LittleEndian.PutUint64(mmapData[24:32], uint64(end))
	binary.LittleEndian.PutUint64(mmapData[32:40], count)
	flags := binary.LittleEndian.Uint32(mmapData[40:44])
	// clear 'active' bit
	flags &^= FlagActive
	// set 'sealed' bit
	flags |= FlagSealed
	binary.LittleEndian.PutUint32(mmapData[40:44], flags)

	crc := crc32.Checksum(mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(mmapData[56:60], crc)
	if err := seg.Sync(); err != nil {
		// The header may or may not have reached disk; either state is valid
		// for recovery (sealed with a durable footer, or unsealed and scanned),
		// but this handle can no longer tell which. Report it as fatal.
		return fmt.Errorf("%w: sync sealed header of segment %d: %w", ErrFsync, seg.id, err)
	}
	seg.isSealed.Store(true)
	seg.sparse.Store(sparse)
	seg.dense.Store(nil)
	return nil
}

// MarkSealedInMemory marks the segment as sealed in memory.
func (seg *Segment) MarkSealedInMemory() {
	seg.inMemorySealed.Store(true)
}

// IsInMemorySealed returns true if the segment has been marked as sealed in memory.
func (seg *Segment) IsInMemorySealed() bool {
	return seg.inMemorySealed.Load()
}

// IsSealed returns true if the segment is sealed (on-disk flag).
func (seg *Segment) IsSealed() bool {
	return seg.isSealed.Load()
}

func isNewSegment(path string) (bool, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("stat error: %w", err)
	}
	return false, nil
}

func (seg *Segment) prepareSegmentFile(path string, isNew bool) (*os.File, mmap.MMap, error) {
	flags := os.O_RDWR
	if isNew {
		flags |= os.O_CREATE | os.O_EXCL
	}
	fd, err := os.OpenFile(path, flags, fileModePerm)
	if err != nil {
		return nil, nil, err
	}
	if isNew {
		if err := fd.Truncate(seg.mmapSize); err != nil {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("allocate segment: %w", err)
		}
	} else {
		info, err := fd.Stat()
		if err != nil {
			_ = fd.Close()
			return nil, nil, err
		}
		if err := validateSegmentSize(info.Size()); err != nil {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("invalid existing segment: %w", err)
		}
		// Configuration determines the capacity of NEW segments only.
		seg.mmapSize = info.Size()
	}
	mmapData, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		_ = fd.Close()
		return nil, nil, fmt.Errorf("mmap error: %w", err)
	}
	return fd, mmapData, nil
}

func writeInitialMetadata(mmapData mmap.MMap, marker uint32) {
	binary.LittleEndian.PutUint32(mmapData[0:4], segmentMagicNumber)
	binary.LittleEndian.PutUint32(mmapData[4:8], segmentHeaderVersion)
	now := uint64(time.Now().UnixNano())
	binary.LittleEndian.PutUint64(mmapData[8:16], now)
	binary.LittleEndian.PutUint64(mmapData[16:24], now)
	binary.LittleEndian.PutUint64(mmapData[24:32], segmentHeaderSize)
	binary.LittleEndian.PutUint64(mmapData[32:40], 0)
	binary.LittleEndian.PutUint32(mmapData[40:44], FlagActive)
	binary.LittleEndian.PutUint32(mmapData[52:56], marker)
	crc := crc32.Checksum(mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(mmapData[56:60], crc)
}

func (seg *Segment) scanForLastOffset() int64 {
	return seg.iterateValidEntries(nil)
}

func (seg *Segment) iterateValidEntries(visitor func(offset int64, length uint32) bool) int64 {
	var offset int64 = segmentHeaderSize

	for offset+recordHeaderSize <= seg.mmapSize {
		offset = alignUp(offset)
		if offset+recordHeaderSize > seg.mmapSize {
			break
		}

		header := seg.mmapData[offset : offset+recordHeaderSize]
		length := binary.LittleEndian.Uint32(header[4:8])
		entrySize := alignUp(int64(recordHeaderSize) + int64(length) + recordTrailerMarkerSize)

		if offset+entrySize > seg.mmapSize {
			break
		}

		data := seg.mmapData[offset+recordHeaderSize : offset+recordHeaderSize+int64(length)]
		trailer := seg.mmapData[offset+recordHeaderSize+int64(length) : offset+recordHeaderSize+int64(length)+recordTrailerMarkerSize]

		savedSum := binary.LittleEndian.Uint32(header[:4])
		computedSum := crc32Checksum(header[4:], data)

		if savedSum == 0 && length == 0 {
			break
		}
		if savedSum == 0 || savedSum != computedSum || !bytes.Equal(trailer, trailerMarker) {
			slog.Warn("[walfs]",
				slog.String("message", "Failed to recover segment: checksum mismatch"),
				slog.Int64("offset", offset),
				slog.Uint64("saved", uint64(savedSum)),
				slog.Uint64("computed", uint64(computedSum)),
				slog.String("Segment", seg.path),
				slog.Bool("trailer_corrupted", !bytes.Equal(trailer, trailerMarker)),
			)
			break
		}

		if visitor != nil {
			if !visitor(offset, length) {
				offset += entrySize
				break
			}
		}

		offset += entrySize
	}

	return offset
}

// alignUp returns the next multiple of alignSize greater than or equal to n.
//
//go:inline
func alignUp(n int64) int64 {
	return (n + alignMask) & ^alignMask
}

// Write writes the provided slice of bytes to the open mmap file.
// It appends data to the segment and returns the offset where
// the record was written in the given segment.
func (seg *Segment) Write(data []byte, logIndex uint64) (RecordPosition, error) {
	if seg.closed.Load() || seg.state.Load() != StateOpen {
		return NilRecordPosition, ErrClosed
	}

	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	flags := binary.LittleEndian.Uint32(seg.mmapData[40:44])
	if IsSealed(flags) {
		return NilRecordPosition, ErrSegmentSealed
	}

	offset := seg.writeOffset.Load()

	seg.writeFirstIndexEntry(logIndex)

	headerSize := int64(recordHeaderSize)
	dataSize := int64(len(data))
	trailerSize := int64(recordTrailerMarkerSize)
	rawSize := headerSize + dataSize + trailerSize
	entrySize := alignUp(rawSize)

	if offset+entrySize > seg.recordLimit() {
		return NilRecordPosition, errors.New("write exceeds Segment size")
	}

	binary.LittleEndian.PutUint32(seg.header[4:8], uint32(len(data)))
	sum := crc32Checksum(seg.header[4:], data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	copy(seg.mmapData[offset:], seg.header[:])
	copy(seg.mmapData[offset+recordHeaderSize:], data)

	canaryOffset := offset + headerSize + dataSize
	copy(seg.mmapData[canaryOffset:], trailerMarker)

	paddingStart := offset + rawSize
	paddingEnd := offset + entrySize
	// ensuring alignment to 8 bytes
	for i := paddingStart; i < paddingEnd; i++ {
		seg.mmapData[i] = 0
	}

	newOffset := offset + entrySize
	seg.writeOffset.Store(newOffset)

	binary.LittleEndian.PutUint32(seg.mmapData[24:32], uint32(newOffset))
	prevCount := binary.LittleEndian.Uint64(seg.mmapData[32:40])
	binary.LittleEndian.PutUint64(seg.mmapData[32:40], prevCount+1)
	binary.LittleEndian.PutUint64(seg.mmapData[16:24], uint64(time.Now().UnixNano()))

	crc := crc32.Checksum(seg.mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)

	seg.appendIndexEntry(offset)

	// MSync if option is set
	if seg.syncOption == MsyncOnWrite {
		if err := seg.mmapData.Flush(); err != nil {
			return NilRecordPosition, fmt.Errorf("%w: mmap flush error after write: %w", ErrFsync, err)
		}
	}

	return RecordPosition{
		SegmentID: seg.id,
		Offset:    offset,
	}, nil
}

func (seg *Segment) writeFirstIndexEntry(logIndex uint64) {
	if seg.firstLogIndex == 0 {
		seg.firstLogIndex = logIndex
		seg.firstLSN.Store(logIndex)
		binary.LittleEndian.PutUint64(seg.mmapData[44:52], logIndex)
		crc := crc32.Checksum(seg.mmapData[0:56], crcTable)
		binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)
	}
}

// WriteBatch writes multiple records to the segment in a single operation.
// Returns a slice of RecordPositions for successfully written records and the number written.
// If the segment fills up mid-batch, it returns positions for records that fit,
// the count of records written, and ErrSegmentFull.
// Callers should retry remaining records in a new segment.
// nolint: funlen
func (seg *Segment) WriteBatch(records [][]byte, logIndexes []uint64) ([]RecordPosition, int, error) {
	if len(records) == 0 {
		return nil, 0, nil
	}

	if seg.closed.Load() || seg.state.Load() != StateOpen {
		return nil, 0, ErrClosed
	}

	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	flags := binary.LittleEndian.Uint32(seg.mmapData[40:44])
	if IsSealed(flags) {
		return nil, 0, ErrSegmentSealed
	}

	// firstLogIndex if this is the first write to the segment
	if seg.firstLogIndex == 0 && logIndexes != nil && len(logIndexes) > 0 {
		seg.writeFirstIndexEntry(logIndexes[0])
	}

	startOffset := seg.writeOffset.Load()
	currentOffset := startOffset
	positions := make([]RecordPosition, 0, len(records))

	headerSize := int64(recordHeaderSize)
	trailerSize := int64(recordTrailerMarkerSize)

	// determine how many records we can fit
	var recordsToWrite int
	for i, data := range records {
		dataSize := int64(len(data))
		rawSize := headerSize + dataSize + trailerSize
		entrySize := alignUp(rawSize)

		if entrySize > seg.recordLimit()-segmentHeaderSize {
			return nil, 0, fmt.Errorf("record at index %d (size %d bytes) exceeds maximum segment capacity", i, len(data))
		}

		if currentOffset+entrySize > seg.recordLimit() {
			// can't fit from this record - stop here
			recordsToWrite = i
			break
		}

		positions = append(positions, RecordPosition{
			SegmentID: seg.id,
			Offset:    currentOffset,
		})
		currentOffset += entrySize
		recordsToWrite = i + 1
	}

	if recordsToWrite == 0 {
		return nil, 0, ErrSegmentFull
	}

	currentOffset = startOffset
	for i := 0; i < recordsToWrite; i++ {
		data := records[i]
		dataSize := int64(len(data))
		rawSize := headerSize + dataSize + trailerSize
		entrySize := alignUp(rawSize)

		// header
		binary.LittleEndian.PutUint32(seg.header[4:8], uint32(len(data)))
		sum := crc32Checksum(seg.header[4:], data)
		binary.LittleEndian.PutUint32(seg.header[:4], sum)
		copy(seg.mmapData[currentOffset:], seg.header[:])

		// data
		copy(seg.mmapData[currentOffset+recordHeaderSize:], data)

		// trailer
		canaryOffset := currentOffset + headerSize + dataSize
		copy(seg.mmapData[canaryOffset:], trailerMarker)

		// padding
		paddingStart := currentOffset + rawSize
		paddingEnd := currentOffset + entrySize
		for i := paddingStart; i < paddingEnd; i++ {
			seg.mmapData[i] = 0
		}

		currentOffset += entrySize
	}

	// metadata once at the end
	newOffset := currentOffset
	seg.writeOffset.Store(newOffset)

	binary.LittleEndian.PutUint32(seg.mmapData[24:32], uint32(newOffset))
	prevCount := binary.LittleEndian.Uint64(seg.mmapData[32:40])
	binary.LittleEndian.PutUint64(seg.mmapData[32:40], prevCount+uint64(recordsToWrite))
	binary.LittleEndian.PutUint64(seg.mmapData[16:24], uint64(time.Now().UnixNano()))

	crc := crc32.Checksum(seg.mmapData[0:56], crcTable)
	binary.LittleEndian.PutUint32(seg.mmapData[56:60], crc)

	for i := 0; i < recordsToWrite; i++ {
		seg.appendIndexEntry(positions[i].Offset)
	}

	// MSync if option is set
	if seg.syncOption == MsyncOnWrite {
		if err := seg.mmapData.Flush(); err != nil {
			return positions, recordsToWrite, fmt.Errorf("%w: mmap flush error after batch write: %w", ErrFsync, err)
		}
	}

	// Return partial success if we couldn't write all records
	var err error
	if recordsToWrite < len(records) {
		err = ErrSegmentFull
	}

	return positions, recordsToWrite, err
}

// Read reads the record data at the specified offset within the segment.
// IMP: Don't retain any data.
// This method returns a slice of the mmap'd file content corresponding to the record payload.
// so slice becomes invalid immediately after the segment is closed or unmapped.
func (seg *Segment) Read(offset int64) ([]byte, RecordPosition, error) {
	if seg.closed.Load() {
		return nil, NilRecordPosition, ErrClosed
	}
	if offset+recordHeaderSize > seg.mmapSize {
		return nil, NilRecordPosition, io.EOF
	}

	header := seg.mmapData[offset : offset+recordHeaderSize]
	length := binary.LittleEndian.Uint32(header[4:8])
	dataSize := int64(length)

	rawSize := int64(recordHeaderSize) + dataSize + recordTrailerMarkerSize
	entrySize := alignUp(rawSize)

	if length > uint32(seg.WriteOffset()-offset-recordHeaderSize) {
		return nil, NilRecordPosition, ErrCorruptHeader
	}

	if offset+entrySize > seg.WriteOffset() {
		return nil, NilRecordPosition, io.EOF
	}

	// validating  the trailer before reading data
	// we are ensuring no oob access even if length is corrupted.
	trailerOffset := offset + recordHeaderSize + dataSize
	end := trailerOffset + recordTrailerMarkerSize
	if end > seg.mmapSize {
		return nil, NilRecordPosition, ErrIncompleteChunk
	}

	// previously we were doing byte which did show in pprof as runtime.memequal
	// switching to uint64 comparison removed it altogether.
	word := binary.LittleEndian.Uint64(seg.mmapData[trailerOffset:end])
	// validating trailer marker to detect torn/incomplete writes.
	if word != trailerWord {
		return nil, NilRecordPosition, ErrIncompleteChunk
	}

	data := seg.mmapData[offset+recordHeaderSize : offset+recordHeaderSize+dataSize]

	// sealed segments are immutable and may have been recovered
	// from disk after a crash or shutdown. CRC validation ensures that data
	// persisted to disk is still intact and wasn't partially written or corrupted.
	// for active segment, we do one validation at start if not sealed, else it's in the
	// same process memory, so having corruption of the same byte is very unlikely, until
	// done from some external forces.
	// doing this in the hot-path is CPU intensive and most of the read are towards the tail.
	if seg.isSealed.Load() && !seg.inMemorySealed.Load() {
		savedSum := binary.LittleEndian.Uint32(header[:4])
		computedSum := crc32Checksum(header[4:], data)
		if savedSum != computedSum {
			return nil, NilRecordPosition, ErrInvalidCRC
		}
	}

	next := RecordPosition{
		SegmentID: seg.id,
		Offset:    offset + entrySize,
	}

	return data, next, nil
}

// Sync Msync the Memory mapped file and the FSync the underlying file.
func (seg *Segment) Sync() error {
	if seg.closed.Load() {
		return ErrClosed
	}

	if err := seg.mmapData.Flush(); err != nil {
		return fmt.Errorf("mmap flush error: %w", err)
	}

	if err := fsyncFile(seg.fd); err != nil {
		return fmt.Errorf("fsync error: %w", err)
	}

	return nil
}

// fsyncFile is replaced in tests to inject fsync failures.
var fsyncFile = func(f *os.File) error { return f.Sync() }

// newReaderPinnedHook is set in tests to run between NewReader's pin and its
// state check.
var newReaderPinnedHook func(*Segment)

func (seg *Segment) MSync() error {
	if seg.closed.Load() {
		return ErrClosed
	}

	if err := seg.mmapData.Flush(); err != nil {
		return fmt.Errorf("mmap flush error: %w", err)
	}
	return nil
}

// WillExceed returns true if a record of the given dataSize would not fit
// before the space reserved for the segment's footer.
func (seg *Segment) WillExceed(dataSize int) bool {
	rawSize := int64(recordHeaderSize + dataSize + recordTrailerMarkerSize)
	entrySize := alignUp(rawSize)
	offset := seg.writeOffset.Load()
	return offset+entrySize > seg.recordLimit()
}

// Close gracefully shuts down the segment by waiting for all active readers to complete.
// It unmap the segment file and closes file descriptor.
func (seg *Segment) Close() error {
	seg.lifecycleMu.Lock()
	defer seg.lifecycleMu.Unlock()
	if !seg.state.CompareAndSwap(StateOpen, StateClosing) {
		return nil
	}

	seg.closeCond.L.Lock()
	for seg.refCount.Load() > 0 {
		seg.closeCond.Wait()
	}
	seg.closeCond.L.Unlock()

	syncErr := seg.Sync()

	// Lookups and IndexEntries check closed and read the mapping under
	// writeMu's read lock and hold no reference; take the write lock so none
	// is mid-read when the mapping goes away, on every path.
	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()
	seg.closed.Store(true)
	unmapErr := seg.mmapData.Unmap()
	closeErr := seg.fd.Close()

	switch {
	case syncErr != nil:
		return fmt.Errorf("sync error during close: %w", syncErr)
	case unmapErr != nil:
		return fmt.Errorf("unmap error: %w", unmapErr)
	case closeErr != nil:
		return fmt.Errorf("file close error: %w", closeErr)
	}
	return nil
}

// HasActiveReaders returns true if there are any currently active readers on the segment.
func (seg *Segment) HasActiveReaders() bool {
	return seg.activeReaders.HasAny()
}

// WriteOffset returns the current write offset of the segment.
//
//go:inline
func (seg *Segment) WriteOffset() int64 {
	return seg.writeOffset.Load()
}

// GetLastModifiedAt returns the last modified time of the segment.
func (seg *Segment) GetLastModifiedAt() int64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	if err != nil {
		panic(err)
	}
	return meta.LastModifiedAt
}

// GetEntryCount returns the total entry count in segment.
func (seg *Segment) GetEntryCount() int64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	if err != nil {
		panic(err)
	}
	return meta.EntryCount
}

func (seg *Segment) FirstLogIndex() uint64 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	return seg.firstLogIndex
}

// GetFlags returns the flags stored in segment header.
func (seg *Segment) GetFlags() uint32 {
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	meta, err := decodeSegmentHeader(seg.mmapData[:segmentHeaderSize])
	if err != nil {
		panic(err)
	}
	return meta.Flags
}

func (seg *Segment) GetSegmentSize() int64 {
	return seg.mmapSize
}

func (seg *Segment) incrRef() {
	seg.refCount.Add(1)
}

func (seg *Segment) decrRef(id uint64) {
	if ok := seg.activeReaders.Remove(id); ok {
		seg.releaseRef()
	}
}

// releaseRef decrements the reference count and performs any deferred cleanup.
func (seg *Segment) releaseRef() {
	count := seg.refCount.Add(-1)
	if count == 0 {
		seg.closeCond.L.Lock()
		seg.closeCond.Broadcast()
		seg.closeCond.L.Unlock()
		if seg.markedForDeletion.Load() {
			seg.cleanup()
		}
	}
}

// MarkForDeletion marks the segment as candidate for deletion.
// If no active readers, it will immediately call cleanup.
// Otherwise, cleanup will be deferred until the last reference is released.
func (seg *Segment) MarkForDeletion() {
	if seg.markedForDeletion.CompareAndSwap(false, true) {
		if seg.refCount.Load() == 0 {
			seg.cleanup()
		}
	}
}

// cleanup closes and deletes the underlying segment file from disk.
func (seg *Segment) cleanup() {
	if err := seg.Close(); err != nil {
		slog.Error("[walfs]", slog.String("message", "Failed to close segment"), slog.String("path", seg.path), slog.Any("error", err))
	}
	deletedSegment := false
	if err := os.Remove(seg.path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Error("[walfs]", slog.String("message", "Failed to delete segment"), slog.String("path", seg.path), slog.Any("error", err))
		}
	} else {
		deletedSegment = true
	}

	if seg.dirSyncer != nil && deletedSegment {
		dir := filepath.Dir(seg.path)
		if err := seg.dirSyncer.SyncDir(dir); err != nil {
			slog.Error("[walfs]",
				slog.String("message", "Failed to sync WAL directory after deletion"),
				slog.String("path", dir),
				slog.Any("error", err),
			)
		}
	}
	slog.Debug("[walfs]", slog.String("message", "Removed segment"), slog.Int("segment_id", int(seg.id)))
}

// ID returns the unique number of the Segment.
func (seg *Segment) ID() SegmentID {
	return seg.id
}

// SegmentReader is an iterator over records in a WAL segment.
// It maintains its own read offset and provides safe iteration over a Segment.
type SegmentReader struct {
	id               uint64
	segment          *Segment
	readOffset       int64
	lastRecordOffset int64
	closed           atomic.Bool
}

// Close closes the SegmentReader and decrements the segment's reference count.
func (r *SegmentReader) Close() {
	if r.closed.CompareAndSwap(false, true) {
		r.segment.decrRef(r.id)
	}
}

// NewReader creates a new SegmentReader for reading from the segment.
func (seg *Segment) NewReader() *SegmentReader {
	// Pin first, then check. Close stores Closing and then loads refCount, so
	// either Close sees this pin and waits, or this check sees Closing and the
	// pin is withdrawn. Checking first would let both checks pass and the
	// reader use a mapping Close has already removed.
	seg.incrRef()
	if newReaderPinnedHook != nil {
		newReaderPinnedHook(seg)
	}
	// prevent new readers to segments marked for deletion or not opened
	if seg.markedForDeletion.Load() || seg.state.Load() != StateOpen {
		seg.releaseRef()
		return nil
	}

	id := seg.readerIDCounter.Add(1)
	seg.activeReaders.Add(id)

	reader := &SegmentReader{
		segment:    seg,
		readOffset: segmentHeaderSize,
		id:         id,
	}

	// safety net in case caller doesn't call Close()
	runtime.AddCleanup(reader, func(seg *Segment) {
		seg.decrRef(id)
	}, seg)

	return reader
}

// Next reads the next record from the segment and also advances the read position.
// It returns the data, the record's position, or an error.
// Returns io.EOF if the segment is sealed and all data has been read.
// Returns ErrNoNewData if unsealed and no new data is available yet.
func (r *SegmentReader) Next() ([]byte, RecordPosition, error) {
	if r.closed.Load() {
		return nil, NilRecordPosition, ErrSegmentReaderClosed
	}

	isSealed := r.segment.isSealed.Load()
	writeOffset := r.segment.WriteOffset()

	if r.readOffset >= writeOffset {
		if isSealed {
			return nil, NilRecordPosition, io.EOF
		}
		return nil, NilRecordPosition, ErrNoNewData
	}

	currentOffset := r.readOffset
	data, next, err := r.segment.Read(r.readOffset)
	if err != nil {
		// If the read fails due to being too close to write head, treat as "no new data" if unsealed
		if !isSealed && errors.Is(err, io.EOF) {
			return nil, NilRecordPosition, ErrNoNewData
		}

		return nil, NilRecordPosition, err
	}
	r.lastRecordOffset = currentOffset
	r.readOffset = next.Offset

	currentPos := RecordPosition{
		SegmentID: r.segment.ID(),
		Offset:    currentOffset,
	}

	return data, currentPos, nil
}

func (r *SegmentReader) LastRecordPosition() RecordPosition {
	return RecordPosition{
		SegmentID: r.segment.ID(),
		Offset:    r.lastRecordOffset,
	}
}

func crc32Checksum(header []byte, data []byte) uint32 {
	sum := crc32.Checksum(header, crcTable)
	return crc32.Update(sum, crcTable, data)
}

// SegmentFileName returns the file name of a Segment file.
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// TruncateTo truncates the segment to the specified log index.
// All entries after the given log index will be discarded.
// If the log index is not found in this segment, it returns an error.
func (seg *Segment) TruncateTo(logIndex uint64) error {
	seg.lifecycleMu.Lock()
	defer seg.lifecycleMu.Unlock()
	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	entries, err := seg.prepareTruncateLocked(logIndex)
	if err != nil {
		return err
	}
	return seg.applyTruncate(entries)
}

// prepareTruncateLocked validates the retained prefix without mutating storage or
// the shared LSN index. The segment bytes are authoritative; the index is a cache.
func (seg *Segment) prepareTruncateLocked(logIndex uint64) ([]segmentIndexEntry, error) {
	if seg.closed.Load() {
		return nil, ErrClosed
	}
	if seg.writeOffset.Load() <= segmentHeaderSize {
		return nil, fmt.Errorf("segment is empty, cannot truncate to %d", logIndex)
	}
	if logIndex < seg.firstLogIndex {
		return nil, fmt.Errorf("log index %d is before segment start %d", logIndex, seg.firstLogIndex)
	}
	count := logIndex - seg.firstLogIndex + 1
	if count == 0 || count > uint64((seg.writeOffset.Load()-segmentHeaderSize)/recordOverhead(0)) {
		return nil, fmt.Errorf("log index %d not found in segment index", logIndex)
	}
	// Truncation is a cold path. Scanning the retained prefix also detects stale
	// or corrupt sidecars, and works when the sealed segment's cache was cleared.
	var entries []segmentIndexEntry
	seg.iterateValidEntries(func(offset int64, length uint32) bool {
		if offset+recordOverhead(int64(length)) > seg.writeOffset.Load() {
			return false
		}
		entries = append(entries, segmentIndexEntry{Offset: uint64(offset), Length: length})
		return uint64(len(entries)) < count
	})
	if uint64(len(entries)) != count {
		return nil, fmt.Errorf("log index %d not found in segment index: invalid retained prefix", logIndex)
	}
	return entries, nil
}

func (seg *Segment) applyTruncate(entries []segmentIndexEntry) error {
	last := entries[len(entries)-1]
	end := int64(last.Offset) + recordOverhead(int64(last.Length))
	// Clear the entire unused tail, including bytes beyond a stale write offset
	// and a sealed segment's footer.
	seg.zeroDiscardedTail(end, seg.mmapSize)
	seg.applyTruncateHeader(end, int64(len(entries)))
	seg.writeOffset.Store(end)
	seg.installDense(entries)
	if err := seg.Sync(); err != nil {
		return fmt.Errorf("failed to sync truncated segment: %w", err)
	}
	return nil
}

func (seg *Segment) applyTruncateHeader(newWriteOffset int64, newEntryCount int64) {
	setTruncateHeader(seg.mmapData, newWriteOffset, newEntryCount)
	seg.isSealed.Store(false)
	seg.inMemorySealed.Store(false)
}

func setTruncateHeader(data []byte, end, count int64) {
	binary.LittleEndian.PutUint64(data[24:32], uint64(end))
	binary.LittleEndian.PutUint64(data[32:40], uint64(count))
	binary.LittleEndian.PutUint64(data[16:24], uint64(time.Now().UnixNano()))
	flags := binary.LittleEndian.Uint32(data[40:44])
	flags &^= FlagSealed
	flags |= FlagActive
	binary.LittleEndian.PutUint32(data[40:44], flags)
	binary.LittleEndian.PutUint32(data[56:60], crc32.Checksum(data[0:56], crcTable))
}

func (seg *Segment) zeroDiscardedTail(from, to int64) {
	if to > seg.mmapSize {
		to = seg.mmapSize
	}
	if from >= to {
		return
	}
	// Recovery scans unsealed segments from the beginning, so leaving valid
	// records anywhere in the discarded range can reconnect them to later
	// rewrites and resurrect truncated entries. Only dirty chunks that hold
	// data, so untouched sparse regions are not allocated.
	const chunk = 4096
	for start := from; start < to; {
		stop := min(to, (start/chunk+1)*chunk)
		for _, b := range seg.mmapData[start:stop] {
			if b != 0 {
				clear(seg.mmapData[start:stop])
				break
			}
		}
		start = stop
	}
}

// Remove closes the segment and removes its file.
func (seg *Segment) Remove() error {
	if err := seg.Close(); err != nil {
		return fmt.Errorf("failed to close segment %d: %w", seg.id, err)
	}

	dir := filepath.Dir(seg.path)

	if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove segment file %s: %w", seg.path, err)
	}

	if seg.dirSyncer != nil {
		if err := seg.dirSyncer.SyncDir(dir); err != nil {
			return fmt.Errorf("failed to sync directory after removal: %w", err)
		}
	}

	return nil
}
