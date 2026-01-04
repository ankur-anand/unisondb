package output

import (
	"io"
	"time"
)

// Format represents the output format type.
type Format string

const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
)

// SegmentInfo contains metadata about a WAL segment.
type SegmentInfo struct {
	ID            uint32    `json:"id"`
	Status        string    `json:"status"`
	Size          int64     `json:"size"`
	SizeHuman     string    `json:"size_human"`
	EntryCount    int64     `json:"entry_count"`
	FirstLogIndex uint64    `json:"first_log_index"`
	LastModified  time.Time `json:"last_modified"`
}

// IndexEntryInfo contains offset and length for a WAL record.
type IndexEntryInfo struct {
	Index  int   `json:"index"`
	Offset int64 `json:"offset"`
	Length int64 `json:"length"`
}

// SegmentDetail contains detailed info for a single segment.
type SegmentDetail struct {
	ID            uint32           `json:"id"`
	Status        string           `json:"status"`
	Size          int64            `json:"size"`
	SizeHuman     string           `json:"size_human"`
	WriteOffset   int64            `json:"write_offset"`
	EntryCount    int64            `json:"entry_count"`
	FirstLogIndex uint64           `json:"first_log_index"`
	Flags         uint32           `json:"flags"`
	LastModified  time.Time        `json:"last_modified"`
	IndexEntries  []IndexEntryInfo `json:"index_entries,omitempty"`
}

// WalStats contains aggregate WAL statistics.
type WalStats struct {
	TotalSegments  int    `json:"total_segments"`
	SealedCount    int    `json:"sealed_count"`
	ActiveCount    int    `json:"active_count"`
	TotalEntries   int64  `json:"total_entries"`
	TotalSize      int64  `json:"total_size"`
	TotalSizeHuman string `json:"total_size_human"`
	FirstLogIndex  uint64 `json:"first_log_index"`
	LastLogIndex   uint64 `json:"last_log_index"`
}

// RestoreResult contains the result of a restore operation.
type RestoreResult struct {
	DryRun            bool   `json:"dry_run"`
	BTreeRestored     bool   `json:"btree_restored"`
	BTreePath         string `json:"btree_path,omitempty"`
	BTreeBytesWritten int64  `json:"btree_bytes_written,omitempty"`
	WALRestored       bool   `json:"wal_restored"`
	WALPath           string `json:"wal_path,omitempty"`
	SegmentsRestored  int    `json:"segments_restored,omitempty"`
}

// Formatter is the interface for output formatting.
type Formatter interface {
	WriteSegmentList(w io.Writer, segments []SegmentInfo) error
	WriteSegmentDetail(w io.Writer, detail SegmentDetail) error
	WriteWalStats(w io.Writer, stats WalStats) error
	WriteRestoreResult(w io.Writer, result RestoreResult) error
}

// NewFormatter creates a new formatter for the given format.
func NewFormatter(format Format) Formatter {
	switch format {
	case FormatJSON:
		return &JSONFormatter{}
	default:
		return &TableFormatter{}
	}
}
