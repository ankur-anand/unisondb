package walfs

import (
	"encoding/binary"
	"sort"
	"sync/atomic"
)

// Record lookup is positional: within a segment, the record with ordinal i
// (0-based) has log index firstLogIndex+i. Recovery and truncation already
// rely on this, so only offsets are stored, never keys.
//
// An unsealed segment keeps one uint32 offset per record (denseIndex). Sealing
// replaces it with a sparseIndex holding one offset per sparseInterval bytes of
// records; a lookup walks record headers from the nearest sampled offset.

const (
	denseBlockShift = 12
	denseBlockSize  = 1 << denseBlockShift
	sparseInterval  = 4 << 10
)

// denseIndex is appended by the writer under the segment write lock and read
// without locks: blocks never move once published, and n is stored after the
// entry it covers.
type denseIndex struct {
	blocks atomic.Pointer[[]*[denseBlockSize]uint32]
	n      atomic.Uint64
}

func newDenseIndex() *denseIndex {
	d := &denseIndex{}
	empty := []*[denseBlockSize]uint32{}
	d.blocks.Store(&empty)
	return d
}

func (d *denseIndex) append(offset int64) {
	i := d.n.Load()
	blocks := *d.blocks.Load()
	b := int(i >> denseBlockShift)
	if b == len(blocks) {
		grown := make([]*[denseBlockSize]uint32, b+1)
		copy(grown, blocks)
		grown[b] = new([denseBlockSize]uint32)
		d.blocks.Store(&grown)
		blocks = grown
	}
	blocks[b][i&(denseBlockSize-1)] = uint32(offset)
	d.n.Store(i + 1)
}

func (d *denseIndex) get(ordinal uint64) (int64, bool) {
	if ordinal >= d.n.Load() {
		return 0, false
	}
	blocks := *d.blocks.Load()
	return int64(blocks[ordinal>>denseBlockShift][ordinal&(denseBlockSize-1)]), true
}

// sparseIndex is immutable. ordinals[0] is always 0.
type sparseIndex struct {
	count    uint64
	ordinals []uint32
	offsets  []uint32
}

func buildSparseIndex(count uint64, offsetAt func(uint64) int64) *sparseIndex {
	s := &sparseIndex{count: count}
	last := int64(-sparseInterval)
	for i := uint64(0); i < count; i++ {
		off := offsetAt(i)
		if off-last >= sparseInterval {
			s.ordinals = append(s.ordinals, uint32(i))
			s.offsets = append(s.offsets, uint32(off))
			last = off
		}
	}
	return s
}

// nearest returns the last sampled record at or before ordinal.
func (s *sparseIndex) nearest(ordinal uint64) (uint64, int64) {
	i := sort.Search(len(s.ordinals), func(i int) bool { return uint64(s.ordinals[i]) > ordinal }) - 1
	return uint64(s.ordinals[i]), int64(s.offsets[i])
}

// offsetForOrdinal returns the offset of the ordinal-th record in the segment.
func (seg *Segment) offsetForOrdinal(ordinal uint64) (int64, bool) {
	if d := seg.dense.Load(); d != nil {
		return d.get(ordinal)
	}
	s := seg.sparse.Load()
	if s == nil || ordinal >= s.count {
		return 0, false
	}
	start, offset := s.nearest(ordinal)
	seg.writeMu.RLock()
	defer seg.writeMu.RUnlock()
	if seg.closed.Load() {
		return 0, false
	}
	end := seg.writeOffset.Load()
	for k := start; k < ordinal; k++ {
		if offset+recordHeaderSize > end {
			return 0, false
		}
		offset += recordOverhead(int64(binary.LittleEndian.Uint32(seg.mmapData[offset+4 : offset+8])))
	}
	return offset, offset+recordHeaderSize <= end
}

// positionForIndex maps a log index within this segment to its position.
func (seg *Segment) positionForIndex(idx uint64) (RecordPosition, bool) {
	first := seg.firstLSN.Load()
	if first == 0 || idx < first {
		return NilRecordPosition, false
	}
	off, ok := seg.offsetForOrdinal(idx - first)
	if !ok {
		return NilRecordPosition, false
	}
	return RecordPosition{SegmentID: seg.id, Offset: off}, true
}

// indexedCount is the number of records the in-memory index covers.
func (seg *Segment) indexedCount() uint64 {
	if d := seg.dense.Load(); d != nil {
		return d.n.Load()
	}
	if s := seg.sparse.Load(); s != nil {
		return s.count
	}
	return 0
}

// entriesLocked materializes offsets and lengths for every indexed record.
// It is used for sidecar writes, truncation and diagnostics. Requires writeMu.
func (seg *Segment) entriesLocked() []segmentIndexEntry {
	count := seg.indexedCount()
	entries := make([]segmentIndexEntry, 0, count)
	if d := seg.dense.Load(); d != nil {
		for i := uint64(0); i < count; i++ {
			off, _ := d.get(i)
			entries = append(entries, segmentIndexEntry{Offset: uint64(off), Length: seg.recordLength(off)})
		}
		return entries
	}
	offset := int64(segmentHeaderSize)
	end := seg.writeOffset.Load()
	for i := uint64(0); i < count && offset+recordHeaderSize <= end; i++ {
		length := seg.recordLength(offset)
		entries = append(entries, segmentIndexEntry{Offset: uint64(offset), Length: length})
		offset += recordOverhead(int64(length))
	}
	return entries
}

func (seg *Segment) recordLength(offset int64) uint32 {
	return binary.LittleEndian.Uint32(seg.mmapData[offset+4 : offset+8])
}

// installDense replaces the index with a dense index covering entries.
func (seg *Segment) installDense(entries []segmentIndexEntry) {
	d := newDenseIndex()
	for _, e := range entries {
		d.append(int64(e.Offset))
	}
	seg.dense.Store(d)
	seg.sparse.Store(nil)
}
