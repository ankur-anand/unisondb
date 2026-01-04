package restore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ankur-anand/unisondb/internal/udbctl/output"
	"github.com/gofrs/flock"
)

// Options configures a restore operation.
type Options struct {
	DataDir   string
	Namespace string
	BTreePath string
	WALPath   string
	DryRun    bool
}

// acquireLock attempts to acquire an exclusive lock on the namespace.
// this is in same path as our dbkernel pid.lock to prevent server start.
// if dbkernel is running, this will fail.
func acquireLock(dataDir, namespace string) (*flock.Flock, error) {
	nsDir := filepath.Join(dataDir, namespace)
	if err := os.MkdirAll(nsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create namespace directory: %w", err)
	}

	pidLockFile := filepath.Join(nsDir, "pid.lock")
	fileLock := flock.New(pidLockFile)

	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !locked {
		return nil, errors.New("database is locked (pid.lock held) - server or another restore is running")
	}

	return fileLock, nil
}

func RestoreBTree(backupPath, dataDir, namespace string) (*output.RestoreResult, error) {
	info, err := os.Stat(backupPath)
	if err != nil {
		return nil, fmt.Errorf("backup file not found: %w", err)
	}
	if info.IsDir() {
		return nil, errors.New("backup path is a directory, expected file")
	}

	targetDir := filepath.Join(dataDir, namespace)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target directory: %w", err)
	}

	targetFile := filepath.Join(targetDir, "data.mdb")
	if err := copyFileAtomic(backupPath, targetFile); err != nil {
		return nil, fmt.Errorf("failed to copy backup: %w", err)
	}

	return &output.RestoreResult{
		BTreeRestored:     true,
		BTreePath:         targetFile,
		BTreeBytesWritten: info.Size(),
	}, nil
}

func RestoreWAL(backupDir, dataDir, namespace string) (*output.RestoreResult, error) {
	info, err := os.Stat(backupDir)
	if err != nil {
		return nil, fmt.Errorf("backup directory not found: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("backup path is not a directory")
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup directory: %w", err)
	}

	var segmentFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".seg") {
			segmentFiles = append(segmentFiles, entry.Name())
		}
	}

	if len(segmentFiles) == 0 {
		return nil, errors.New("no segment files found in backup directory")
	}

	walDir := filepath.Join(dataDir, namespace, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	var copiedFiles []string
	for _, name := range segmentFiles {
		srcPath := filepath.Join(backupDir, name)
		dstPath := filepath.Join(walDir, name)

		if err := copyFileAtomic(srcPath, dstPath); err != nil {
			for _, copied := range copiedFiles {
				os.Remove(copied)
			}
			return nil, fmt.Errorf("failed to copy %s: %w", name, err)
		}
		copiedFiles = append(copiedFiles, dstPath)
	}

	return &output.RestoreResult{
		WALRestored:      true,
		WALPath:          walDir,
		SegmentsRestored: len(segmentFiles),
	}, nil
}

func validateBTree(backupPath, dataDir, namespace string) (*output.RestoreResult, error) {
	info, err := os.Stat(backupPath)
	if err != nil {
		return nil, fmt.Errorf("backup file not found: %w", err)
	}
	if info.IsDir() {
		return nil, errors.New("backup path is a directory, expected file")
	}

	targetFile := filepath.Join(dataDir, namespace, "data.mdb")
	return &output.RestoreResult{
		BTreeRestored:     true,
		BTreePath:         targetFile,
		BTreeBytesWritten: info.Size(),
	}, nil
}

func validateWAL(backupDir, dataDir, namespace string) (*output.RestoreResult, error) {
	info, err := os.Stat(backupDir)
	if err != nil {
		return nil, fmt.Errorf("backup directory not found: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("backup path is not a directory")
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup directory: %w", err)
	}

	var segmentCount int
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".seg") {
			segmentCount++
		}
	}

	if segmentCount == 0 {
		return nil, errors.New("no segment files found in backup directory")
	}

	walDir := filepath.Join(dataDir, namespace, "wal")
	return &output.RestoreResult{
		WALRestored:      true,
		WALPath:          walDir,
		SegmentsRestored: segmentCount,
	}, nil
}

// Restore performs a full restore operation based on the provided options.
// It holds an exclusive lock during the entire operation to prevent the server
// from starting and other restore processes from running concurrently.
// If DryRun is true, it validates the backup files but doesn't copy them.
func Restore(opts Options) (*output.RestoreResult, error) {
	if opts.BTreePath == "" && opts.WALPath == "" {
		return nil, errors.New("must specify btree and/or wal backup path")
	}

	// exclusive lock for the entire restore operation
	lock, err := acquireLock(opts.DataDir, opts.Namespace)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Unlock() }()

	result := &output.RestoreResult{
		DryRun: opts.DryRun,
	}

	// Restore/validate B-Tree
	if opts.BTreePath != "" {
		var btreeResult *output.RestoreResult
		var err error

		if opts.DryRun {
			btreeResult, err = validateBTree(opts.BTreePath, opts.DataDir, opts.Namespace)
		} else {
			btreeResult, err = RestoreBTree(opts.BTreePath, opts.DataDir, opts.Namespace)
		}
		if err != nil {
			return nil, fmt.Errorf("B-Tree restore failed: %w", err)
		}
		result.BTreeRestored = btreeResult.BTreeRestored
		result.BTreePath = btreeResult.BTreePath
		result.BTreeBytesWritten = btreeResult.BTreeBytesWritten
	}

	// Restore/validate WAL
	if opts.WALPath != "" {
		var walResult *output.RestoreResult
		var err error

		if opts.DryRun {
			walResult, err = validateWAL(opts.WALPath, opts.DataDir, opts.Namespace)
		} else {
			walResult, err = RestoreWAL(opts.WALPath, opts.DataDir, opts.Namespace)
		}
		if err != nil {
			return nil, fmt.Errorf("WAL restore failed: %w", err)
		}
		result.WALRestored = walResult.WALRestored
		result.WALPath = walResult.WALPath
		result.SegmentsRestored = walResult.SegmentsRestored
	}

	return result, nil
}

func copyFileAtomic(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	tmpDst := dst + ".tmp"
	dstFile, err := os.Create(tmpDst)
	if err != nil {
		return err
	}

	_, copyErr := io.Copy(dstFile, srcFile)
	syncErr := dstFile.Sync()
	closeErr := dstFile.Close()

	if copyErr != nil || syncErr != nil || closeErr != nil {
		os.Remove(tmpDst)
		if copyErr != nil {
			return copyErr
		}
		if syncErr != nil {
			return syncErr
		}
		return closeErr
	}

	return os.Rename(tmpDst, dst)
}
