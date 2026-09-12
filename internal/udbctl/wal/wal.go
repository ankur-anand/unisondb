package wal

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ankur-anand/unisondb/internal/udbctl/output"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/dustin/go-humanize"
)

// maxUnixSec is the Unix timestamp in seconds for year 9999 upper bound (RFC 3339).
var maxUnixSec = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).Unix()

func ListSegments(walDir string) ([]output.SegmentInfo, error) {
	segments, err := readSegments(walDir)
	if err != nil {
		return nil, err
	}

	result := make([]output.SegmentInfo, 0, len(segments))
	for _, seg := range segments {
		info := output.SegmentInfo{
			ID:            seg.id,
			Status:        statusString(walfs.IsSealed(seg.Header.Flags)),
			Size:          seg.Size,
			SizeHuman:     humanize.Bytes(uint64(seg.Size)),
			EntryCount:    seg.Header.EntryCount,
			FirstLogIndex: seg.Header.FirstLogIndex,
			LastModified:  safeTime(seg.Header.LastModifiedAt),
		}
		result = append(result, info)
	}

	return result, nil
}

func InspectSegment(walDir string, segmentID uint32, showIndex bool) (*output.SegmentDetail, error) {
	files, err := segmentFiles(walDir)
	if err != nil {
		return nil, err
	}
	path, ok := files[segmentID]
	if !ok {
		return nil, fmt.Errorf("segment %d not found", segmentID)
	}
	seg, err := walfs.InspectSegmentFile(path, showIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect segment %d: %w", segmentID, err)
	}

	detail := &output.SegmentDetail{
		ID:            segmentID,
		Status:        statusString(walfs.IsSealed(seg.Header.Flags)),
		Size:          seg.Size,
		SizeHuman:     humanize.Bytes(uint64(seg.Size)),
		WriteOffset:   seg.Header.WriteOffset,
		EntryCount:    seg.Header.EntryCount,
		FirstLogIndex: seg.Header.FirstLogIndex,
		Flags:         seg.Header.Flags,
		LastModified:  safeTime(seg.Header.LastModifiedAt),
	}

	if showIndex {
		entries := seg.IndexEntries
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
	segments, err := readSegments(walDir)
	if err != nil {
		return nil, err
	}

	stats := &output.WalStats{
		TotalSegments: len(segments),
	}

	if len(segments) == 0 {
		return stats, nil
	}

	var firstIdx uint64 = math.MaxUint64
	var lastIdx uint64 = 0

	for _, seg := range segments {
		if walfs.IsSealed(seg.Header.Flags) {
			stats.SealedCount++
		} else {
			stats.ActiveCount++
		}
		stats.TotalEntries += seg.Header.EntryCount
		stats.TotalSize += seg.Size

		first := seg.Header.FirstLogIndex
		if first > 0 && first < firstIdx {
			firstIdx = first
		}

		entryCount := seg.Header.EntryCount
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

type inspectedSegment struct {
	id uint32
	*walfs.SegmentInspection
}

func segmentFiles(walDir string) (map[uint32]string, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}
	files := make(map[uint32]string)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".seg"), 10, 32)
		if err != nil {
			continue
		}
		if _, exists := files[uint32(id)]; exists {
			return nil, fmt.Errorf("duplicate segment ID %d", id)
		}
		files[uint32(id)] = filepath.Join(walDir, entry.Name())
	}
	return files, nil
}

func readSegments(walDir string) ([]inspectedSegment, error) {
	files, err := segmentFiles(walDir)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, 0, len(files))
	for id := range files {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	segments := make([]inspectedSegment, 0, len(ids))
	for _, id := range ids {
		info, err := walfs.InspectSegmentFile(files[id], false)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect segment %d: %w", id, err)
		}
		segments = append(segments, inspectedSegment{id: id, SegmentInspection: info})
	}
	return segments, nil
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
