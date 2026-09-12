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
	return Restore(Options{DataDir: dataDir, Namespace: namespace, BTreePath: backupPath})
}

func RestoreWAL(backupDir, dataDir, namespace string) (*output.RestoreResult, error) {
	return Restore(Options{DataDir: dataDir, Namespace: namespace, WALPath: backupDir})
}

func validateBTree(backupPath, dataDir, namespace string) (*output.RestoreResult, error) {
	info, err := os.Stat(backupPath)
	if err != nil {
		return nil, fmt.Errorf("backup file not found: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("backup path is not a regular file (possibly a directory)")
	}

	targetFile := filepath.Join(dataDir, namespace, "data.mdb")
	return &output.RestoreResult{
		BTreeRestored:     true,
		BTreePath:         targetFile,
		BTreeBytesWritten: info.Size(),
	}, nil
}

func validateWAL(backupDir, dataDir, namespace string) (*output.RestoreResult, []restoreFile, error) {
	info, err := os.Stat(backupDir)
	if err != nil {
		return nil, nil, fmt.Errorf("backup directory not found: %w", err)
	}
	if !info.IsDir() {
		return nil, nil, errors.New("backup path is not a directory")
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read backup directory: %w", err)
	}

	walDir := filepath.Join(dataDir, namespace, "wal")
	var files []restoreFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}
		source := filepath.Join(backupDir, entry.Name())
		info, err := os.Stat(source)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid segment %s: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			return nil, nil, fmt.Errorf("segment %s is not a regular file", entry.Name())
		}
		files = append(files, restoreFile{source: source, target: filepath.Join(walDir, entry.Name())})
	}

	if len(files) == 0 {
		return nil, nil, errors.New("no segment files found in backup directory")
	}

	return &output.RestoreResult{
		WALRestored:      true,
		WALPath:          walDir,
		SegmentsRestored: len(files),
	}, files, nil
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

	// Validate every input before staging or replacing any destination files.
	var files []restoreFile
	if opts.BTreePath != "" {
		btreeResult, err := validateBTree(opts.BTreePath, opts.DataDir, opts.Namespace)
		if err != nil {
			return nil, fmt.Errorf("B-Tree restore failed: %w", err)
		}
		result.BTreeRestored = btreeResult.BTreeRestored
		result.BTreePath = btreeResult.BTreePath
		result.BTreeBytesWritten = btreeResult.BTreeBytesWritten
		files = append(files, restoreFile{source: opts.BTreePath, target: result.BTreePath})
	}

	if opts.WALPath != "" {
		walResult, walFiles, err := validateWAL(opts.WALPath, opts.DataDir, opts.Namespace)
		if err != nil {
			return nil, fmt.Errorf("WAL restore failed: %w", err)
		}
		result.WALRestored = walResult.WALRestored
		result.WALPath = walResult.WALPath
		result.SegmentsRestored = walResult.SegmentsRestored
		files = append(files, walFiles...)
	}

	if !opts.DryRun {
		if err := installFiles(files, os.Rename); err != nil {
			return nil, fmt.Errorf("restore failed: %w", err)
		}
	}
	return result, nil
}

type restoreFile struct {
	source, target   string
	staged, original string
	installed        bool
}

// installFiles stages all copies before publishing them and retains overwritten
// files until the whole restore succeeds. Staging beside each destination keeps
// renames on the same filesystem, including separately mounted WAL directories.
// The rename argument allows tests to exercise failures during publication.
func installFiles(files []restoreFile, rename func(string, string) error) (err error) {
	stagingDirs := make(map[string]string)
	defer func() {
		if err != nil {
			if rollbackErr := rollbackFiles(files, rename); rollbackErr != nil {
				// Never delete the only remaining copies if rollback fails.
				err = errors.Join(err, rollbackErr)
				return
			}
		}
		for _, dir := range stagingDirs {
			_ = os.RemoveAll(dir)
		}
	}()

	for i := range files {
		file := &files[i]
		targetDir := filepath.Dir(file.target)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return fmt.Errorf("create target directory: %w", err)
		}
		if info, err := os.Lstat(file.target); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("destination %s is not a regular file", file.target)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat destination %s: %w", file.target, err)
		}
		stageDir, ok := stagingDirs[targetDir]
		if !ok {
			stageDir, err = os.MkdirTemp(targetDir, ".restore-*")
			if err != nil {
				return fmt.Errorf("create staging directory: %w", err)
			}
			stagingDirs[targetDir] = stageDir
		}
		file.staged = filepath.Join(stageDir, fmt.Sprintf("%d.new", i))
		if err := copyFileAtomic(file.source, file.staged); err != nil {
			return fmt.Errorf("stage %s: %w", file.source, err)
		}
	}

	for i := range files {
		file := &files[i]
		original := file.staged + ".original"
		if err := rename(file.target, original); err == nil {
			file.original = original
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("preserve destination %s: %w", file.target, err)
		}
		if err := rename(file.staged, file.target); err != nil {
			return fmt.Errorf("install %s: %w", file.target, err)
		}
		file.installed = true
	}
	return nil
}

func rollbackFiles(files []restoreFile, rename func(string, string) error) error {
	var rollbackErr error
	for i := len(files) - 1; i >= 0; i-- {
		file := &files[i]
		if file.original != "" {
			if err := rename(file.original, file.target); err != nil {
				rollbackErr = errors.Join(rollbackErr, fmt.Errorf("restore original %s (retained at %s): %w", file.target, file.original, err))
			}
		} else if file.installed {
			if err := os.Remove(file.target); err != nil && !errors.Is(err, os.ErrNotExist) {
				rollbackErr = errors.Join(rollbackErr, fmt.Errorf("remove restored file %s: %w", file.target, err))
			}
		}
	}
	return rollbackErr
}

func copyFileAtomic(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	info, err := srcFile.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("source %s is not a regular file", src)
	}

	dstFile, err := os.CreateTemp(filepath.Dir(dst), ".restore-copy-*")
	if err != nil {
		return err
	}
	tmpDst := dstFile.Name()
	defer os.Remove(tmpDst)
	if err := dstFile.Chmod(info.Mode().Perm()); err != nil {
		_ = dstFile.Close()
		return err
	}

	_, copyErr := io.Copy(dstFile, srcFile)
	syncErr := dstFile.Sync()
	closeErr := dstFile.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		return err
	}
	return os.Rename(tmpDst, dst)
}
