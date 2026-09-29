package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ankur-anand/unisondb/internal/udbctl/output"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/gofrs/flock"
)

// Same names dbkernel uses under <data-dir>/<namespace>.
const (
	walDirName  = "wal"
	pidLockName = "pid.lock"
	segmentExt  = ".seg"
)

// TruncateOptions configures an offline WAL truncation.
type TruncateOptions struct {
	DataDir   string
	Namespace string
	// KeepThrough is the last LSN to keep; every later record is removed.
	KeepThrough uint64
	DryRun      bool
}

// Truncate removes every WAL record after opts.KeepThrough from a namespace
// whose server is stopped. It holds the namespace's pid.lock, the lock the
// server takes at startup, for the whole operation: it refuses to run while
// the server holds it, and the server cannot start until it is done.
//
// Opening the WAL runs the same crash recovery the server runs at startup,
// also for a dry run. If the B-tree checkpoint lies after KeepThrough, the
// server refuses to start afterwards; restore an older B-tree first.
func Truncate(opts TruncateOptions) (*output.TruncateResult, error) {
	if opts.KeepThrough == 0 {
		return nil, errors.New("keep-through must be at least 1; to discard the whole WAL, restore or remove it")
	}
	nsDir := filepath.Join(opts.DataDir, opts.Namespace)
	walDir := filepath.Join(nsDir, walDirName)
	info, err := os.Stat(walDir)
	if err != nil {
		return nil, fmt.Errorf("WAL directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("WAL path %s is not a directory", walDir)
	}

	lock := flock.New(filepath.Join(nsDir, pidLockName))
	locked, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !locked {
		return nil, errors.New("database is locked (pid.lock held) - stop the server before truncating its WAL")
	}
	defer func() { _ = lock.Unlock() }()

	wl, err := walfs.NewWALog(walDir, segmentExt)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	result, err := truncateOpen(wl, walDir, opts)
	if closeErr := wl.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("close WAL: %w", closeErr)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func truncateOpen(wl *walfs.WALog, walDir string, opts TruncateOptions) (*output.TruncateResult, error) {
	first, last := wl.GetBounds()
	switch {
	case last == 0:
		return nil, errors.New("WAL has no records")
	case opts.KeepThrough < first:
		return nil, fmt.Errorf("LSN %d is older than the oldest record in the WAL (%d)", opts.KeepThrough, first)
	case opts.KeepThrough >= last:
		return nil, fmt.Errorf("nothing to truncate: the last LSN is %d", last)
	}

	removed := 0
	for _, seg := range wl.Segments() {
		if seg.FirstLogIndex() > opts.KeepThrough {
			removed++
		}
	}
	result := &output.TruncateResult{
		DryRun:          opts.DryRun,
		WALPath:         walDir,
		FirstLSN:        first,
		LastLSNBefore:   last,
		KeepThrough:     opts.KeepThrough,
		RecordsRemoved:  last - opts.KeepThrough,
		SegmentsRemoved: removed,
	}
	if opts.DryRun {
		return result, nil
	}
	if err := wl.Truncate(opts.KeepThrough); err != nil {
		return nil, fmt.Errorf("truncate WAL: %w", err)
	}
	if _, newLast := wl.GetBounds(); newLast != opts.KeepThrough {
		return nil, fmt.Errorf("truncate WAL: last LSN is %d after truncation, want %d", newLast, opts.KeepThrough)
	}
	return result, nil
}
