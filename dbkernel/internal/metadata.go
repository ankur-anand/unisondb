package internal

import (
	"encoding/binary"

	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
)

var (
	SysKeyWalCheckPoint = []byte("sys.kv.unisondb.key.wal.checkpoint")
	SysKeyBloomFilter   = []byte("sys.kv.unisondb.key.bloom-filter")
)

// Metadata represents a checkpoint in the Write-Ahead Log (WAL).
// It encodes the last known chunk position (`Pos`) within the segment file. This is primarily used for
// recovery and replication tracking.
type Metadata struct {
	// (monotonic)
	RecordProcessed uint64
	// Position of the last written chunk in WAL
	Pos *wal.Offset
	// Raft mode fields (only used when engine is in raft mode)
	// Last raft log index flushed to B-tree
	RaftIndex uint64
	// Last raft log term flushed to B-tree
	RaftTerm uint64
}

// SaveMetadata saves the WAL checkpoint to BTreeStore (standalone mode).
func SaveMetadata(db BTreeStore, pos *wal.Offset, index uint64) error {
	metaData := Metadata{
		RecordProcessed: index,
		Pos:             pos,
	}
	value := metaData.MarshalBinary()

	return db.StoreMetadata(SysKeyWalCheckPoint, value)
}

// SaveMetadataWithRaft saves the WAL checkpoint to BTreeStore with raft position (raft mode).
func SaveMetadataWithRaft(db BTreeStore, pos *wal.Offset, index uint64, raftIndex, raftTerm uint64) error {
	metaData := Metadata{
		RecordProcessed: index,
		Pos:             pos,
		RaftIndex:       raftIndex,
		RaftTerm:        raftTerm,
	}
	value := metaData.MarshalBinary()

	return db.StoreMetadata(SysKeyWalCheckPoint, value)
}

const posEncodedSize = 12

// MarshalBinary encodes a Metadata struct to a byte slice.
// Format: [RecordProcessed:8][Pos:12][RaftIndex:8][RaftTerm:8] = 36 bytes
func (m *Metadata) MarshalBinary() []byte {
	encodedPos := make([]byte, posEncodedSize)
	if m.Pos != nil {
		copy(encodedPos, m.Pos.Encode())
	}

	result := make([]byte, 8+posEncodedSize+16)
	binary.LittleEndian.PutUint64(result[0:8], m.RecordProcessed)
	copy(result[8:20], encodedPos)
	binary.LittleEndian.PutUint64(result[20:28], m.RaftIndex)
	binary.LittleEndian.PutUint64(result[28:36], m.RaftTerm)

	return result
}

// UnmarshalMetadata decodes a Metadata struct from a byte slice.
// Format: [RecordProcessed:8][Pos:12][RaftIndex:8][RaftTerm:8] = 36 bytes
func UnmarshalMetadata(data []byte) Metadata {
	index := binary.LittleEndian.Uint64(data[:8])
	pos := wal.DecodeOffset(data[8:20])

	var raftIndex, raftTerm uint64
	// Check for raft fields (36 bytes total: 8 + 12 + 8 + 8)
	if len(data) >= 36 {
		raftIndex = binary.LittleEndian.Uint64(data[20:28])
		raftTerm = binary.LittleEndian.Uint64(data[28:36])
	}

	return Metadata{
		RecordProcessed: index,
		Pos:             pos,
		RaftIndex:       raftIndex,
		RaftTerm:        raftTerm,
	}
}
