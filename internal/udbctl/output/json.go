package output

import (
	"encoding/json"
	"io"
)

// JSONFormatter outputs data in JSON format.
type JSONFormatter struct{}

// WriteSegmentList writes segment list as JSON.
func (f *JSONFormatter) WriteSegmentList(w io.Writer, segments []SegmentInfo) error {
	return writeJSON(w, segments)
}

// WriteSegmentDetail writes detailed segment info as JSON.
func (f *JSONFormatter) WriteSegmentDetail(w io.Writer, detail SegmentDetail) error {
	return writeJSON(w, detail)
}

// WriteWalStats writes aggregate WAL statistics as JSON.
func (f *JSONFormatter) WriteWalStats(w io.Writer, stats WalStats) error {
	return writeJSON(w, stats)
}

// WriteRestoreResult writes restore operation result as JSON.
func (f *JSONFormatter) WriteRestoreResult(w io.Writer, result RestoreResult) error {
	return writeJSON(w, result)
}

func writeJSON(w io.Writer, v any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(v)
}
