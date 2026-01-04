package wal

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ankur-anand/unisondb/internal/udbctl/output"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/dustin/go-humanize"
)

// maxUnixSec is the Unix timestamp in seconds for year 9999 upper bound (RFC 3339).
var maxUnixSec = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).Unix()

func ListSegments(walDir string) ([]output.SegmentInfo, error) {
	wal, err := walfs.NewWALog(walDir, ".seg")
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	defer wal.Close()

	segments := wal.Segments()
	if len(segments) == 0 {
		return []output.SegmentInfo{}, nil
	}

	ids := make([]walfs.SegmentID, 0, len(segments))
	for id := range segments {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	result := make([]output.SegmentInfo, 0, len(segments))
	for _, id := range ids {
		seg := segments[id]
		info := output.SegmentInfo{
			ID:            uint32(seg.ID()),
			Status:        statusString(seg.IsSealed()),
			Size:          seg.GetSegmentSize(),
			SizeHuman:     humanize.Bytes(uint64(seg.GetSegmentSize())),
			EntryCount:    seg.GetEntryCount(),
			FirstLogIndex: seg.FirstLogIndex(),
			LastModified:  safeTime(seg.GetLastModifiedAt()),
		}
		result = append(result, info)
	}

	return result, nil
}

func InspectSegment(walDir string, segmentID uint32, showIndex bool) (*output.SegmentDetail, error) {
	wal, err := walfs.NewWALog(walDir, ".seg")
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	defer wal.Close()

	segments := wal.Segments()
	seg, ok := segments[walfs.SegmentID(segmentID)]
	if !ok {
		return nil, fmt.Errorf("segment %d not found", segmentID)
	}

	detail := &output.SegmentDetail{
		ID:            uint32(seg.ID()),
		Status:        statusString(seg.IsSealed()),
		Size:          seg.GetSegmentSize(),
		SizeHuman:     humanize.Bytes(uint64(seg.GetSegmentSize())),
		WriteOffset:   seg.WriteOffset(),
		EntryCount:    seg.GetEntryCount(),
		FirstLogIndex: seg.FirstLogIndex(),
		Flags:         seg.GetFlags(),
		LastModified:  safeTime(seg.GetLastModifiedAt()),
	}

	if showIndex {
		entries := seg.IndexEntries()
		detail.IndexEntries = make([]output.IndexEntryInfo, len(entries))
		for i, e := range entries {
			detail.IndexEntries[i] = output.IndexEntryInfo{
				Index:  i + 1,
				Offset: e.Offset,
				Length: int64(e.Length),
			}
		}
	}

	return detail, nil
}

func GetStats(walDir string) (*output.WalStats, error) {
	wal, err := walfs.NewWALog(walDir, ".seg")
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	defer wal.Close()

	segments := wal.Segments()

	stats := &output.WalStats{
		TotalSegments: len(segments),
	}

	if len(segments) == 0 {
		return stats, nil
	}

	var firstIdx uint64 = math.MaxUint64
	var lastIdx uint64 = 0

	for _, seg := range segments {
		if seg.IsSealed() {
			stats.SealedCount++
		} else {
			stats.ActiveCount++
		}
		stats.TotalEntries += seg.GetEntryCount()
		stats.TotalSize += seg.GetSegmentSize()

		first := seg.FirstLogIndex()
		if first > 0 && first < firstIdx {
			firstIdx = first
		}

		entryCount := seg.GetEntryCount()
		if entryCount > 0 {
			last := first + uint64(entryCount) - 1
			if last > lastIdx {
				lastIdx = last
			}
		}
	}

	stats.TotalSizeHuman = humanize.Bytes(uint64(stats.TotalSize))
	if firstIdx != math.MaxUint64 {
		stats.FirstLogIndex = firstIdx
		stats.LastLogIndex = lastIdx
	}

	return stats, nil
}

func statusString(isSealed bool) string {
	if isSealed {
		return "Sealed"
	}
	return "Active"
}

// safeTime converts a Unix timestamp in nanoseconds to time.Time,
// returning zero time for invalid values.
func safeTime(unixNano int64) time.Time {
	if unixNano <= 0 {
		return time.Time{}
	}
	// Convert to seconds for validation against RFC 3339 year 9999 bound
	unixSec := unixNano / 1e9
	if unixSec > maxUnixSec {
		return time.Time{}
	}
	return time.Unix(0, unixNano)
}
