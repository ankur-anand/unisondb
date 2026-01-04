package output

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
)

// TableFormatter outputs data in human-readable table format.
type TableFormatter struct{}

// WriteSegmentList writes segment list as a table.
func (f *TableFormatter) WriteSegmentList(w io.Writer, segments []SegmentInfo) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATUS\tSIZE\tENTRIES\tFIRST_INDEX\tLAST_MODIFIED")

	for _, seg := range segments {
		fmt.Fprintf(tw, "%08d\t%s\t%s\t%s\t%s\t%s\n",
			seg.ID,
			seg.Status,
			seg.SizeHuman,
			humanize.Comma(seg.EntryCount),
			humanize.Comma(int64(seg.FirstLogIndex)),
			formatTime(seg.LastModified),
		)
	}

	return tw.Flush()
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

// WriteSegmentDetail writes detailed segment info.
func (f *TableFormatter) WriteSegmentDetail(w io.Writer, detail SegmentDetail) error {
	fmt.Fprintln(w, "Segment Details")
	fmt.Fprintln(w, "===============")
	fmt.Fprintf(w, "ID:             %d\n", detail.ID)
	fmt.Fprintf(w, "Status:         %s\n", detail.Status)
	fmt.Fprintf(w, "Size:           %s (%s bytes)\n", detail.SizeHuman, humanize.Comma(detail.Size))
	fmt.Fprintf(w, "Write Offset:   %s\n", humanize.Comma(detail.WriteOffset))
	fmt.Fprintf(w, "Entry Count:    %s\n", humanize.Comma(detail.EntryCount))
	fmt.Fprintf(w, "First LogIndex: %s\n", humanize.Comma(int64(detail.FirstLogIndex)))
	fmt.Fprintf(w, "Flags:          0x%02x\n", detail.Flags)
	fmt.Fprintf(w, "Last Modified:  %s\n", formatTime(detail.LastModified))

	if len(detail.IndexEntries) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Index Entries (%s total):\n", humanize.Comma(int64(len(detail.IndexEntries))))

		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  #\tOFFSET\tLENGTH")

		// first 10 entries only
		limit := min(len(detail.IndexEntries), 10)
		for i := range limit {
			e := detail.IndexEntries[i]
			fmt.Fprintf(tw, "  %s\t%s\t%s\n",
				humanize.Comma(int64(e.Index)),
				humanize.Comma(e.Offset),
				humanize.Comma(e.Length),
			)
		}
		if len(detail.IndexEntries) > 10 {
			fmt.Fprintf(tw, "  ...\t\t\n")
		}
		return tw.Flush()
	}

	return nil
}

// WriteWalStats writes aggregate WAL statistics.
func (f *TableFormatter) WriteWalStats(w io.Writer, stats WalStats) error {
	fmt.Fprintln(w, "WAL Statistics")
	fmt.Fprintln(w, "==============")
	fmt.Fprintf(w, "Total Segments:    %d\n", stats.TotalSegments)
	fmt.Fprintf(w, "  Sealed:          %d\n", stats.SealedCount)
	fmt.Fprintf(w, "  Active:          %d\n", stats.ActiveCount)
	fmt.Fprintf(w, "Total Entries:     %s\n", humanize.Comma(stats.TotalEntries))
	fmt.Fprintf(w, "Total Size:        %s\n", stats.TotalSizeHuman)
	if stats.TotalEntries > 0 {
		fmt.Fprintf(w, "Log Index Range:   %s - %s\n",
			humanize.Comma(int64(stats.FirstLogIndex)),
			humanize.Comma(int64(stats.LastLogIndex)),
		)
	}
	return nil
}

// WriteRestoreResult writes restore operation result.
func (f *TableFormatter) WriteRestoreResult(w io.Writer, result RestoreResult) error {
	if result.DryRun {
		fmt.Fprintln(w, "[DRY RUN] The following would be restored:")
		fmt.Fprintln(w)
	}

	if result.BTreeRestored {
		action := "Restored"
		if result.DryRun {
			action = "Would restore"
		}
		fmt.Fprintf(w, "%s B-Tree: %s (%s)\n",
			action,
			result.BTreePath,
			humanize.Bytes(uint64(result.BTreeBytesWritten)),
		)
	}
	if result.WALRestored {
		action := "Restored"
		if result.DryRun {
			action = "Would restore"
		}
		fmt.Fprintf(w, "%s %d WAL segments to %s\n",
			action,
			result.SegmentsRestored,
			result.WALPath,
		)
	}
	fmt.Fprintln(w)

	if result.DryRun {
		fmt.Fprintln(w, "Run without --dry-run to perform the restore.")
	} else {
		fmt.Fprintln(w, "Restore completed successfully!")
	}
	return nil
}
