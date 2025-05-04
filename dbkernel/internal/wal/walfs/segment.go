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

	"github.com/edsrzf/mmap-go"
)

// For now each value is not chunked into smaller blocks, but we will benchmark and see later
// if this improves things we can decide.
// For Now every entry will be of type full.
const (
	ChunkTypeFull   = 1
	ChunkTypeFirst  = 2
	ChunkTypeMiddle = 3
	ChunkTypeLast   = 4
)

type MsyncOption int

const (
	// MsyncNone skips msync after write.
	MsyncNone MsyncOption = iota

	// MsyncOnWrite calls msync (Flush) after every write.
	MsyncOnWrite
)

type SegmentID = uint32

const (
	// layout: 4 (checksum) + 4 (length) + 1 (type) (if we decide to divide into block) = 9 bytes
	chunkHeaderSize = 9
	// fixed segment size of 16MB to avoid accidental different segment in different nodes.
	segmentSize  = 16 * 1024 * 1024
	fileModePerm = 0644
)

var (
	ErrClosed          = errors.New("the segment file is closed")
	ErrInvalidCRC      = errors.New("invalid crc, the data may be corrupted")
	ErrCorruptHeader   = errors.New("corrupt chunk header, invalid length")
	ErrIncompleteChunk = errors.New("incomplete or torn write detected at chunk trailer")
)

const (
	// size of the trailer used to detect torn writes.
	// We are writing this to detect torn or partial writes caused by unexpected shutdowns or disk failures.
	// This is inspired by a real-world issue observed in etcd v2.3:
	// SEE: https://github.com/etcd-io/etcd/issues/6191#issuecomment-240268979
	// By adding a known trailer marker (e.g., 0xDEADBEEF), we can explicitly validate that a chunk
	// was fully persisted, and safely stop recovery at the first missing or corrupted trailer.
	chunkTrailerSize = 4
)

var (
	// marker written after every WAL chunk to detect torn/incomplete writes.
	trailerCanary = []byte{0xDE, 0xAD, 0xBE, 0xEF}
)

type segmentReader struct {
	segment    *segment
	readOffset int64
}

// ChunkPosition is the logical location of a chunk within a WAL segment.
type ChunkPosition struct {
	SegmentID SegmentID
	Offset    int64
}

func (cp ChunkPosition) String() string {
	return fmt.Sprintf("SegmentID=%d, Offset=%d", cp.SegmentID, cp.Offset)
}

// Encode serializes the ChunkPosition into a fixed-length byte slice.
func (cp ChunkPosition) Encode() []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], cp.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(cp.Offset))
	return buf
}

// DecodeChunkPosition deserializes a byte slice into a ChunkPosition.
func DecodeChunkPosition(data []byte) (ChunkPosition, error) {
	if len(data) < 12 {
		return ChunkPosition{}, io.ErrUnexpectedEOF
	}
	cp := ChunkPosition{
		SegmentID: binary.LittleEndian.Uint32(data[0:4]),
		Offset:    int64(binary.LittleEndian.Uint64(data[4:12])),
	}
	return cp, nil
}

type segment struct {
	id           SegmentID
	fd           *os.File
	mmapData     mmap.MMap
	mmapSize     int64
	writeOffset  atomic.Int64
	activeReader atomic.Int32
	closed       atomic.Bool
	header       []byte
	writeMu      sync.Mutex
	syncOption   MsyncOption
}

// WithSyncOption sets the sync option for the segment.
func WithSyncOption(opt MsyncOption) func(*segment) {
	return func(s *segment) {
		s.syncOption = opt
	}
}

func openSegmentFile(dirPath, extName string, id uint32, opts ...func(*segment)) (*segment, error) {
	path := SegmentFileName(dirPath, extName, id)
	var isNew bool
	if _, err := os.Stat(path); os.IsNotExist(err) {
		isNew = true
	}

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileModePerm)
	if err != nil {
		return nil, err
	}
	if err := fd.Truncate(segmentSize); err != nil {
		fd.Close()
		return nil, fmt.Errorf("truncate error: %w", err)
	}

	// Use mmap-go to create memory mapping
	mmapData, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("mmap error: %w", err)
	}

	s := &segment{
		id:         id,
		fd:         fd,
		header:     make([]byte, chunkHeaderSize),
		mmapData:   mmapData,
		mmapSize:   segmentSize,
		syncOption: MsyncNone,
	}

	for _, opt := range opts {
		opt(s)
	}

	var offset int64 = 0
	if !isNew {
		// recovering scan to find the correct write offset
		for offset+chunkHeaderSize <= segmentSize {
			header := mmapData[offset : offset+chunkHeaderSize]
			length := binary.LittleEndian.Uint32(header[4:8])
			entrySize := int64(chunkHeaderSize + length + chunkTrailerSize)

			if offset+entrySize > segmentSize {
				break
			}

			data := mmapData[offset+chunkHeaderSize : offset+chunkHeaderSize+int64(length)]
			trailer := mmapData[offset+chunkHeaderSize+int64(length) : offset+entrySize]
			savedSum := binary.LittleEndian.Uint32(header[:4])
			computedSum := crc32Checksum(header[4:], data)

			// clean unused region no more entries ahead
			if savedSum == 0 && length == 0 {
				break
			}

			if savedSum == 0 || savedSum != computedSum || !bytes.Equal(trailer, trailerCanary) {
				slog.Warn("[unisondb.fswal]",
					slog.String("event_type", "segment.recovery.stopped.checksum.mismatch"),
					slog.Int64("offset", offset),
					slog.Uint64("saved", uint64(savedSum)),
					slog.Uint64("computed", uint64(computedSum)),
					slog.String("segment", path),
				)
				break
			}

			offset += entrySize
		}
	}
	s.writeOffset.Store(offset)

	return s, nil
}

func (seg *segment) Write(data []byte) (*ChunkPosition, error) {
	if seg.closed.Load() {
		return nil, ErrClosed
	}

	seg.writeMu.Lock()
	defer seg.writeMu.Unlock()

	offset := seg.writeOffset.Load()
	entrySize := int64(chunkHeaderSize + len(data) + chunkTrailerSize)
	if offset+entrySize > seg.mmapSize {
		return nil, errors.New("write exceeds segment size")
	}

	binary.LittleEndian.PutUint32(seg.header[4:8], uint32(len(data)))
	seg.header[8] = ChunkTypeFull
	sum := crc32Checksum(seg.header[4:], data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	if offset+entrySize > seg.mmapSize {
		return nil, errors.New("write exceeds segment size")
	}

	copy(seg.mmapData[offset:], seg.header[:])
	copy(seg.mmapData[offset+chunkHeaderSize:], data)

	canaryOffset := offset + chunkHeaderSize + int64(len(data))
	copy(seg.mmapData[canaryOffset:], trailerCanary)

	seg.writeOffset.Add(entrySize)
	// MSync if option is set
	if seg.syncOption == MsyncOnWrite {
		if err := seg.mmapData.Flush(); err != nil {
			return nil, fmt.Errorf("mmap flush error after write: %w", err)
		}
	}

	return &ChunkPosition{
		SegmentID: seg.id,
		Offset:    offset,
	}, nil
}

func (seg *segment) Read(offset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed.Load() {
		return nil, nil, ErrClosed
	}
	if offset+chunkHeaderSize > seg.mmapSize {
		return nil, nil, io.EOF
	}

	header := seg.mmapData[offset : offset+chunkHeaderSize]
	length := binary.LittleEndian.Uint32(header[4:8])
	if length > uint32(seg.Size()-offset-chunkHeaderSize) {
		return nil, nil, ErrCorruptHeader
	}
	entrySize := chunkHeaderSize + int64(length) + chunkTrailerSize
	if offset+entrySize > seg.Size() {
		return nil, nil, io.EOF
	}

	data := seg.mmapData[offset+chunkHeaderSize : offset+chunkHeaderSize+int64(length)]
	trailer := seg.mmapData[offset+chunkHeaderSize+int64(length) : offset+entrySize]

	savedSum := binary.LittleEndian.Uint32(header[:4])
	computedSum := crc32Checksum(header[4:], data)
	if savedSum != computedSum {
		return nil, nil, ErrInvalidCRC
	}

	if !bytes.Equal(trailer, trailerCanary) {
		return nil, nil, ErrIncompleteChunk
	}

	next := &ChunkPosition{
		SegmentID: seg.id,
		Offset:    offset + entrySize,
	}
	return data, next, nil
}

func (seg *segment) Sync() error {
	if seg.closed.Load() {
		return ErrClosed
	}

	if err := seg.mmapData.Flush(); err != nil {
		return fmt.Errorf("mmap flush error: %w", err)
	}

	if err := seg.fd.Sync(); err != nil {
		return fmt.Errorf("fsync error: %w", err)
	}

	return nil
}

func (seg *segment) MSync() error {
	if seg.closed.Load() {
		return ErrClosed
	}

	if err := seg.mmapData.Flush(); err != nil {
		return fmt.Errorf("mmap flush error: %w", err)
	}
	return nil
}

func (seg *segment) WillExceed(dataSize int) bool {
	entrySize := int64(chunkHeaderSize + dataSize)
	offset := seg.writeOffset.Load()
	return offset+entrySize > seg.mmapSize
}

func (seg *segment) Close() error {
	if seg.closed.Load() {
		return nil
	}

	for seg.activeReader.Load() > 0 {
		runtime.Gosched()
	}

	if err := seg.Sync(); err != nil {
		defer func() {
			_ = seg.mmapData.Unmap()
			_ = seg.fd.Close()
		}()
		return fmt.Errorf("sync error during close: %w", err)
	}
	seg.closed.Store(true)

	if err := seg.mmapData.Unmap(); err != nil {
		_ = seg.fd.Close()
		return fmt.Errorf("unmap error: %w", err)
	}

	if err := seg.fd.Close(); err != nil {
		return fmt.Errorf("file close error: %w", err)
	}

	return nil
}

func (seg *segment) Size() int64 {
	return seg.writeOffset.Load()
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:    seg,
		readOffset: 0,
	}
}

func (r *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if r.readOffset >= r.segment.Size() {
		return nil, nil, io.EOF
	}
	data, next, err := r.segment.Read(r.readOffset)
	if err != nil {
		return nil, nil, err
	}
	r.readOffset = next.Offset
	return data, next, nil
}

func crc32Checksum(header []byte, data []byte) uint32 {
	sum := crc32.ChecksumIEEE(header)
	return crc32.Update(sum, crc32.IEEETable, data)
}

// SegmentFileName returns the file name of a segment file.
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}
