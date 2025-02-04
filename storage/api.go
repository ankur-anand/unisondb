package storage

import "github.com/rosedblabs/wal"

// Default permission values.
const (
	defaultBytesPerSync = 1 * wal.MB  // 1MB
	defaultSegmentSize  = 16 * wal.MB // 16MB
)

func newWALOptions(dirPath string) wal.Options {
	return wal.Options{
		DirPath:        dirPath,
		SegmentSize:    defaultSegmentSize,
		SegmentFileExt: ".seg.wal",
		// writes are buffered to OS.
		// Logs will be only lost, if the machine itself crashes.
		// Not the process, data will be still safe as os buffer persists.
		Sync:         false,
		BytesPerSync: defaultBytesPerSync,
	}
}

// LogOperation represents the type of WAL operation.
type LogOperation = byte

const (
	OpNoop        LogOperation = iota // Noop
	OpInsert                          // Insert operation
	OpDelete                          // Delete operation
	OpBatchStart                      // OnBatchStart
	OpBatchCommit                     // Marks batch commit
)
