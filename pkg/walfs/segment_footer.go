package walfs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// A sealed segment carries its sparse index in a footer written right after
// its last record, at the header's write offset:
//
//	[header 64][records ... end][footer][zeros to file size]
//
// Footer head (32 bytes):
//
//	0..3   magic
//	4..7   0xFFFFFFFF   read as a record length it exceeds any segment, so a
//	                    scan of an unsealed segment always stops here
//	8..15  record count
//	16..19 entry count
//	20..23 CRC of the entries
//	24..25 footer version
//	26..27 reserved
//	28..31 CRC of bytes 0..27
//
// followed by entry count pairs {ordinal u32, offset u32}: one per
// sparseInterval bytes of records, the first always {0, segmentHeaderSize}.
//
// Records may only end at or before recordLimit(size) = size - maxFooterSize,
// which leaves room for the largest footer any record sequence can produce:
// samples are at least sparseInterval apart and start at the header, so a
// segment has at most (size-header)/sparseInterval + 1 of them.
// pkg/walfs/tla/WalFooterSpace.tla checks this bound exhaustively.
//
// Sealing writes the footer, syncs, then marks the header sealed and syncs
// again, so a durable sealed header always has a durable footer
// (pkg/walfs/tla/WalFooterCrash.tla).

const (
	footerHeadSize    = 32
	footerEntrySize   = 8
	footerMagic       = 0x464C4157 // "WALF"
	footerLenSentinel = 0xFFFFFFFF
	footerVersion     = 1
)

func maxFooterSize(segSize int64) int64 {
	return footerHeadSize + footerEntrySize*((segSize-segmentHeaderSize)/sparseInterval+1)
}

// recordLimitFor is the offset every record must end at or before.
func recordLimitFor(segSize int64) int64 {
	return segSize - maxFooterSize(segSize)
}

func (seg *Segment) recordLimit() int64 {
	return recordLimitFor(seg.mmapSize)
}

// validateSegmentSize rejects sizes that cannot hold the header, one empty
// record and the footer reserve.
func validateSegmentSize(size int64) error {
	if size > maxSegmentSize {
		return fmt.Errorf("segment size exceeds 4 GiB limit: %d bytes", size)
	}
	if size < segmentHeaderSize || recordLimitFor(size) < segmentHeaderSize+recordOverhead(0) {
		return fmt.Errorf("segment size %d is too small: minimum is %d bytes", size, minSegmentSize())
	}
	return nil
}

func minSegmentSize() int64 {
	size := int64(segmentHeaderSize)
	for recordLimitFor(size) < segmentHeaderSize+recordOverhead(0) {
		size++
	}
	return size
}

func footerSize(s *sparseIndex) int64 {
	return footerHeadSize + footerEntrySize*int64(len(s.ordinals))
}

// writeFooterLocked encodes s at end. Requires writeMu. It fails without
// writing if the footer would not fit; the limit makes that unreachable.
func (seg *Segment) writeFooterLocked(s *sparseIndex, end int64) error {
	size := footerSize(s)
	if end+size > seg.mmapSize {
		return fmt.Errorf("segment %d: footer of %d bytes at %d exceeds segment size %d", seg.id, size, end, seg.mmapSize)
	}
	entries := seg.mmapData[end+footerHeadSize : end+size]
	for i := range s.ordinals {
		binary.LittleEndian.PutUint32(entries[i*footerEntrySize:], s.ordinals[i])
		binary.LittleEndian.PutUint32(entries[i*footerEntrySize+4:], s.offsets[i])
	}
	head := seg.mmapData[end : end+footerHeadSize]
	binary.LittleEndian.PutUint32(head[0:4], footerMagic)
	binary.LittleEndian.PutUint32(head[4:8], footerLenSentinel)
	binary.LittleEndian.PutUint64(head[8:16], s.count)
	binary.LittleEndian.PutUint32(head[16:20], uint32(len(s.ordinals)))
	binary.LittleEndian.PutUint32(head[20:24], crc32.Checksum(entries, crcTable))
	binary.LittleEndian.PutUint16(head[24:26], footerVersion)
	binary.LittleEndian.PutUint16(head[26:28], 0)
	binary.LittleEndian.PutUint32(head[28:32], crc32.Checksum(head[0:28], crcTable))
	return nil
}

// readFooterLocked validates the footer of a sealed segment against its
// header and returns its sparse index.
func (seg *Segment) readFooterLocked() (*sparseIndex, error) {
	end := seg.writeOffset.Load()
	count := binary.LittleEndian.Uint64(seg.mmapData[32:40])
	corrupt := func(reason string) error {
		return fmt.Errorf("%w: segment %d footer at %d: %s", ErrSegmentCorrupt, seg.id, end, reason)
	}
	if end < segmentHeaderSize || end+footerHeadSize > seg.mmapSize {
		return nil, corrupt("does not fit in the segment")
	}
	head := seg.mmapData[end : end+footerHeadSize]
	switch {
	case binary.LittleEndian.Uint32(head[28:32]) != crc32.Checksum(head[0:28], crcTable):
		return nil, corrupt("head checksum mismatch")
	case binary.LittleEndian.Uint32(head[0:4]) != footerMagic,
		binary.LittleEndian.Uint32(head[4:8]) != footerLenSentinel,
		binary.LittleEndian.Uint16(head[24:26]) != footerVersion:
		return nil, corrupt("unsupported footer")
	case binary.LittleEndian.Uint64(head[8:16]) != count:
		return nil, corrupt(fmt.Sprintf("record count %d, header says %d", binary.LittleEndian.Uint64(head[8:16]), count))
	}
	n := int64(binary.LittleEndian.Uint32(head[16:20]))
	if end+footerHeadSize+n*footerEntrySize > seg.mmapSize || (count == 0) != (n == 0) {
		return nil, corrupt(fmt.Sprintf("invalid entry count %d", n))
	}
	entries := seg.mmapData[end+footerHeadSize : end+footerHeadSize+n*footerEntrySize]
	if crc32.Checksum(entries, crcTable) != binary.LittleEndian.Uint32(head[20:24]) {
		return nil, corrupt("entries checksum mismatch")
	}
	s := &sparseIndex{count: count, ordinals: make([]uint32, n), offsets: make([]uint32, n)}
	for i := int64(0); i < n; i++ {
		ord := binary.LittleEndian.Uint32(entries[i*footerEntrySize:])
		off := binary.LittleEndian.Uint32(entries[i*footerEntrySize+4:])
		first := i == 0 && (ord != 0 || off != segmentHeaderSize)
		unordered := i > 0 && (ord <= s.ordinals[i-1] || off <= s.offsets[i-1])
		if first || unordered || uint64(ord) >= count || int64(off)+recordHeaderSize > end {
			return nil, corrupt(fmt.Sprintf("invalid entry %d {%d, %d}", i, ord, off))
		}
		s.ordinals[i], s.offsets[i] = ord, off
	}
	return s, nil
}
