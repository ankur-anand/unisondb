package restore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofrs/flock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireLock(t *testing.T) {
	t.Run("acquires lock when no lock file exists", func(t *testing.T) {
		dataDir := t.TempDir()
		lock, err := acquireLock(dataDir, "test")
		require.NoError(t, err)
		require.NotNil(t, lock)
		defer lock.Unlock()

		pidLockFile := filepath.Join(dataDir, "test", "pid.lock")
		_, err = os.Stat(pidLockFile)
		require.NoError(t, err)
	})

	t.Run("acquires lock with stale pid.lock file", func(t *testing.T) {
		dataDir := t.TempDir()
		nsDir := filepath.Join(dataDir, "test")
		require.NoError(t, os.MkdirAll(nsDir, 0755))

		pidLockFile := filepath.Join(nsDir, "pid.lock")
		f, err := os.Create(pidLockFile)
		require.NoError(t, err)
		f.Close()

		lock, err := acquireLock(dataDir, "test")
		require.NoError(t, err)
		require.NotNil(t, lock)
		defer lock.Unlock()
	})

	t.Run("fails when pid.lock is held", func(t *testing.T) {
		dataDir := t.TempDir()
		nsDir := filepath.Join(dataDir, "test")
		require.NoError(t, os.MkdirAll(nsDir, 0755))

		pidLockFile := filepath.Join(nsDir, "pid.lock")
		fileLock := flock.New(pidLockFile)
		locked, err := fileLock.TryLock()
		require.NoError(t, err)
		require.True(t, locked)
		defer fileLock.Unlock()

		lock, err := acquireLock(dataDir, "test")
		require.Error(t, err)
		assert.Nil(t, lock)
		assert.Contains(t, err.Error(), "pid.lock held")
	})

	t.Run("prevents concurrent restores", func(t *testing.T) {
		dataDir := t.TempDir()

		lock1, err := acquireLock(dataDir, "test")
		require.NoError(t, err)
		require.NotNil(t, lock1)
		defer lock1.Unlock()

		lock2, err := acquireLock(dataDir, "test")
		require.Error(t, err)
		assert.Nil(t, lock2)
		assert.Contains(t, err.Error(), "pid.lock held")
	})
}

func TestRestoreBTree(t *testing.T) {
	t.Run("restores backup file successfully", func(t *testing.T) {
		backupDir := t.TempDir()
		backupFile := filepath.Join(backupDir, "backup.mdb")
		testData := []byte("test btree data content")
		require.NoError(t, os.WriteFile(backupFile, testData, 0644))

		dataDir := t.TempDir()
		result, err := RestoreBTree(backupFile, dataDir, "testns")
		require.NoError(t, err)

		assert.True(t, result.BTreeRestored)
		assert.Equal(t, int64(len(testData)), result.BTreeBytesWritten)
		assert.Contains(t, result.BTreePath, "data.mdb")

		restored, err := os.ReadFile(result.BTreePath)
		require.NoError(t, err)
		assert.Equal(t, testData, restored)
	})

	t.Run("creates target directory if not exists", func(t *testing.T) {
		backupDir := t.TempDir()
		backupFile := filepath.Join(backupDir, "backup.mdb")
		require.NoError(t, os.WriteFile(backupFile, []byte("data"), 0644))

		dataDir := t.TempDir()
		result, err := RestoreBTree(backupFile, dataDir, "newnamespace")
		require.NoError(t, err)

		assert.True(t, result.BTreeRestored)
		_, err = os.Stat(filepath.Join(dataDir, "newnamespace"))
		assert.NoError(t, err)
	})

	t.Run("returns error for nonexistent backup", func(t *testing.T) {
		dataDir := t.TempDir()
		_, err := RestoreBTree("/nonexistent/backup.mdb", dataDir, "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("returns error if backup is directory", func(t *testing.T) {
		backupDir := t.TempDir()
		dataDir := t.TempDir()

		_, err := RestoreBTree(backupDir, dataDir, "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "directory")
	})
}

func TestRestoreWAL(t *testing.T) {
	t.Run("restores segment files successfully", func(t *testing.T) {
		backupDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "000000001.seg"), []byte("seg1"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "000000002.seg"), []byte("seg2"), 0644))

		dataDir := t.TempDir()
		result, err := RestoreWAL(backupDir, dataDir, "testns")
		require.NoError(t, err)

		assert.True(t, result.WALRestored)
		assert.Equal(t, 2, result.SegmentsRestored)
		assert.Contains(t, result.WALPath, "wal")

		walDir := filepath.Join(dataDir, "testns", "wal")
		entries, err := os.ReadDir(walDir)
		require.NoError(t, err)
		assert.Len(t, entries, 2)
	})

	t.Run("ignores non-segment files", func(t *testing.T) {
		backupDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "000000001.seg"), []byte("seg1"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "readme.txt"), []byte("ignore"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "backup.log"), []byte("ignore"), 0644))

		dataDir := t.TempDir()
		result, err := RestoreWAL(backupDir, dataDir, "testns")
		require.NoError(t, err)

		assert.Equal(t, 1, result.SegmentsRestored)
	})

	t.Run("ignores subdirectories", func(t *testing.T) {
		backupDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "000000001.seg"), []byte("seg1"), 0644))
		require.NoError(t, os.MkdirAll(filepath.Join(backupDir, "subdir"), 0755))

		dataDir := t.TempDir()
		result, err := RestoreWAL(backupDir, dataDir, "testns")
		require.NoError(t, err)

		assert.Equal(t, 1, result.SegmentsRestored)
	})

	t.Run("returns error for nonexistent backup directory", func(t *testing.T) {
		dataDir := t.TempDir()
		_, err := RestoreWAL("/nonexistent/wal", dataDir, "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("returns error if backup is file not directory", func(t *testing.T) {
		backupFile := filepath.Join(t.TempDir(), "backup.seg")
		require.NoError(t, os.WriteFile(backupFile, []byte("data"), 0644))

		dataDir := t.TempDir()
		_, err := RestoreWAL(backupFile, dataDir, "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a directory")
	})

	t.Run("returns error if no segment files found", func(t *testing.T) {
		backupDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "readme.txt"), []byte("text"), 0644))

		dataDir := t.TempDir()
		_, err := RestoreWAL(backupDir, dataDir, "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no segment files found")
	})

	t.Run("creates WAL directory if not exists", func(t *testing.T) {
		backupDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, "000000001.seg"), []byte("seg1"), 0644))

		dataDir := t.TempDir()
		result, err := RestoreWAL(backupDir, dataDir, "newns")
		require.NoError(t, err)

		assert.True(t, result.WALRestored)
		walDir := filepath.Join(dataDir, "newns", "wal")
		info, err := os.Stat(walDir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})
}

func TestRestore(t *testing.T) {
	t.Run("restores both btree and wal", func(t *testing.T) {
		backupDir := t.TempDir()
		btreeFile := filepath.Join(backupDir, "backup.mdb")
		require.NoError(t, os.WriteFile(btreeFile, []byte("btree data"), 0644))

		walBackupDir := filepath.Join(backupDir, "wal")
		require.NoError(t, os.MkdirAll(walBackupDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(walBackupDir, "000000001.seg"), []byte("seg1"), 0644))

		dataDir := t.TempDir()
		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			BTreePath: btreeFile,
			WALPath:   walBackupDir,
		}

		result, err := Restore(opts)
		require.NoError(t, err)

		assert.True(t, result.BTreeRestored)
		assert.True(t, result.WALRestored)
		assert.Equal(t, 1, result.SegmentsRestored)
	})

	t.Run("restores only btree when wal path empty", func(t *testing.T) {
		backupDir := t.TempDir()
		btreeFile := filepath.Join(backupDir, "backup.mdb")
		require.NoError(t, os.WriteFile(btreeFile, []byte("btree data"), 0644))

		dataDir := t.TempDir()
		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			BTreePath: btreeFile,
		}

		result, err := Restore(opts)
		require.NoError(t, err)

		assert.True(t, result.BTreeRestored)
		assert.False(t, result.WALRestored)
	})

	t.Run("restores only wal when btree path empty", func(t *testing.T) {
		backupDir := t.TempDir()
		walBackupDir := filepath.Join(backupDir, "wal")
		require.NoError(t, os.MkdirAll(walBackupDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(walBackupDir, "000000001.seg"), []byte("seg1"), 0644))

		dataDir := t.TempDir()
		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			WALPath:   walBackupDir,
		}

		result, err := Restore(opts)
		require.NoError(t, err)

		assert.False(t, result.BTreeRestored)
		assert.True(t, result.WALRestored)
	})

	t.Run("returns error when neither btree nor wal specified", func(t *testing.T) {
		opts := Options{
			DataDir:   t.TempDir(),
			Namespace: "testns",
		}

		_, err := Restore(opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must specify")
	})

	t.Run("returns error when pid.lock is held", func(t *testing.T) {
		dataDir := t.TempDir()
		nsDir := filepath.Join(dataDir, "testns")
		require.NoError(t, os.MkdirAll(nsDir, 0755))

		// Hold the pid.lock (simulating running server)
		pidLockFile := filepath.Join(nsDir, "pid.lock")
		fileLock := flock.New(pidLockFile)
		locked, err := fileLock.TryLock()
		require.NoError(t, err)
		require.True(t, locked)
		defer fileLock.Unlock()

		backupDir := t.TempDir()
		btreeFile := filepath.Join(backupDir, "backup.mdb")
		require.NoError(t, os.WriteFile(btreeFile, []byte("data"), 0644))

		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			BTreePath: btreeFile,
		}

		_, err = Restore(opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pid.lock held")
	})

	t.Run("dry-run validates without copying btree", func(t *testing.T) {
		backupDir := t.TempDir()
		btreeFile := filepath.Join(backupDir, "backup.mdb")
		btreeData := []byte("btree data for dry run test")
		require.NoError(t, os.WriteFile(btreeFile, btreeData, 0644))

		dataDir := t.TempDir()
		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			BTreePath: btreeFile,
			DryRun:    true,
		}

		result, err := Restore(opts)
		require.NoError(t, err)
		assert.True(t, result.DryRun)
		assert.True(t, result.BTreeRestored)
		assert.Equal(t, int64(len(btreeData)), result.BTreeBytesWritten)

		targetFile := filepath.Join(dataDir, "testns", "data.mdb")
		_, err = os.Stat(targetFile)
		assert.True(t, os.IsNotExist(err), "data.mdb should not exist in dry-run mode")
	})

	t.Run("dry-run validates without copying wal", func(t *testing.T) {
		backupDir := t.TempDir()
		walBackupDir := filepath.Join(backupDir, "wal")
		require.NoError(t, os.MkdirAll(walBackupDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(walBackupDir, "000000001.seg"), []byte("seg1"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(walBackupDir, "000000002.seg"), []byte("seg2"), 0644))

		dataDir := t.TempDir()
		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			WALPath:   walBackupDir,
			DryRun:    true,
		}

		result, err := Restore(opts)
		require.NoError(t, err)

		assert.True(t, result.DryRun)
		assert.True(t, result.WALRestored)
		assert.Equal(t, 2, result.SegmentsRestored)

		walDir := filepath.Join(dataDir, "testns", "wal")
		_, err = os.Stat(walDir)
		assert.True(t, os.IsNotExist(err), "wal directory should not exist in dry-run mode")
	})

	t.Run("dry-run validates both btree and wal without copying", func(t *testing.T) {
		backupDir := t.TempDir()
		btreeFile := filepath.Join(backupDir, "backup.mdb")
		require.NoError(t, os.WriteFile(btreeFile, []byte("btree data"), 0644))

		walBackupDir := filepath.Join(backupDir, "wal")
		require.NoError(t, os.MkdirAll(walBackupDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(walBackupDir, "000000001.seg"), []byte("seg1"), 0644))

		dataDir := t.TempDir()
		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			BTreePath: btreeFile,
			WALPath:   walBackupDir,
			DryRun:    true,
		}

		result, err := Restore(opts)
		require.NoError(t, err)

		assert.True(t, result.DryRun)
		assert.True(t, result.BTreeRestored)
		assert.True(t, result.WALRestored)

		targetFile := filepath.Join(dataDir, "testns", "data.mdb")
		_, err = os.Stat(targetFile)
		assert.True(t, os.IsNotExist(err))

		walDir := filepath.Join(dataDir, "testns", "wal")
		_, err = os.Stat(walDir)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("dry-run fails for invalid backup", func(t *testing.T) {
		dataDir := t.TempDir()
		opts := Options{
			DataDir:   dataDir,
			Namespace: "testns",
			BTreePath: "/nonexistent/backup.mdb",
			DryRun:    true,
		}

		_, err := Restore(opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestRestorePreservesExistingDataOnInvalidWAL(t *testing.T) {
	for _, full := range []bool{false, true} {
		for _, invalid := range []string{"missing", "empty", "unreadable segment"} {
			t.Run(fmt.Sprintf("full=%t/%s", full, invalid), func(t *testing.T) {
				dataDir := t.TempDir()
				nsDir := filepath.Join(dataDir, "testns")
				walDir := filepath.Join(nsDir, "wal")
				require.NoError(t, os.MkdirAll(walDir, 0755))
				btree := filepath.Join(nsDir, "data.mdb")
				segment := filepath.Join(walDir, "000000001.seg")
				require.NoError(t, os.WriteFile(btree, []byte("original btree"), 0644))
				require.NoError(t, os.WriteFile(segment, []byte("original segment"), 0644))

				backupDir := t.TempDir()
				btreeBackup := filepath.Join(backupDir, "backup.mdb")
				require.NoError(t, os.WriteFile(btreeBackup, []byte("replacement btree"), 0644))
				walBackup := filepath.Join(backupDir, "wal")
				if invalid != "missing" {
					require.NoError(t, os.Mkdir(walBackup, 0755))
				}
				if invalid == "unreadable segment" {
					require.NoError(t, os.WriteFile(filepath.Join(walBackup, "000000001.seg"), []byte("replacement segment"), 0644))
					require.NoError(t, os.Symlink(filepath.Join(backupDir, "missing.seg"), filepath.Join(walBackup, "000000002.seg")))
				}

				var err error
				if full {
					_, err = Restore(Options{DataDir: dataDir, Namespace: "testns", BTreePath: btreeBackup, WALPath: walBackup})
				} else {
					_, err = RestoreWAL(walBackup, dataDir, "testns")
				}
				require.Error(t, err)
				contents, err := os.ReadFile(btree)
				require.NoError(t, err)
				assert.Equal(t, "original btree", string(contents))
				contents, err = os.ReadFile(segment)
				require.NoError(t, err)
				assert.Equal(t, "original segment", string(contents))
				entries, err := os.ReadDir(walDir)
				require.NoError(t, err)
				assert.Len(t, entries, 1)
			})
		}
	}
}

func TestInstallFilesRollsBackPublicationFailures(t *testing.T) {
	// Each file has a preserve and an install rename. Fail once at each step,
	// including after publishing both the B-tree and a newly added WAL segment.
	for failAt := 1; failAt <= 6; failAt++ {
		t.Run(fmt.Sprintf("rename %d", failAt), func(t *testing.T) {
			dir, backup := t.TempDir(), t.TempDir()
			files := []restoreFile{
				{source: filepath.Join(backup, "data.mdb"), target: filepath.Join(dir, "data.mdb")},
				{source: filepath.Join(backup, "000000001.seg"), target: filepath.Join(dir, "wal", "000000001.seg")},
				{source: filepath.Join(backup, "000000002.seg"), target: filepath.Join(dir, "wal", "000000002.seg")},
			}
			require.NoError(t, os.Mkdir(filepath.Join(dir, "wal"), 0755))
			for _, file := range files {
				require.NoError(t, os.WriteFile(file.source, []byte("replacement"), 0644))
			}
			require.NoError(t, os.WriteFile(files[0].target, []byte("original btree"), 0644))
			require.NoError(t, os.WriteFile(files[2].target, []byte("original segment"), 0644))
			failure := errors.New("injected rename failure")
			calls := 0
			err := installFiles(files, func(src, dst string) error {
				calls++
				if calls == failAt {
					return failure
				}
				return os.Rename(src, dst)
			})
			require.ErrorIs(t, err, failure)
			btree, err := os.ReadFile(files[0].target)
			require.NoError(t, err)
			assert.Equal(t, "original btree", string(btree))
			segment, err := os.ReadFile(files[2].target)
			require.NoError(t, err)
			assert.Equal(t, "original segment", string(segment))
			_, err = os.Stat(files[1].target)
			assert.ErrorIs(t, err, os.ErrNotExist)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			assert.Len(t, entries, 2)
			entries, err = os.ReadDir(filepath.Join(dir, "wal"))
			require.NoError(t, err)
			assert.Len(t, entries, 1)
		})
	}
}

func TestInstallFilesRetainsOriginalIfRollbackFails(t *testing.T) {
	dir := t.TempDir()
	source, target := filepath.Join(t.TempDir(), "backup"), filepath.Join(dir, "data.mdb")
	require.NoError(t, os.WriteFile(source, []byte("replacement"), 0644))
	require.NoError(t, os.WriteFile(target, []byte("original"), 0644))
	files := []restoreFile{{source: source, target: target}}
	calls := 0
	failure := errors.New("injected rename failure")
	err := installFiles(files, func(src, dst string) error {
		calls++
		if calls > 1 {
			return failure
		}
		return os.Rename(src, dst)
	})
	require.ErrorIs(t, err, failure)
	require.NotEmpty(t, files[0].original)
	assert.Contains(t, err.Error(), files[0].original)
	contents, err := os.ReadFile(files[0].original)
	require.NoError(t, err)
	assert.Equal(t, "original", string(contents))
}

func TestInstallFilesStagesAllSourcesBeforePublishing(t *testing.T) {
	dir, backup := t.TempDir(), t.TempDir()
	source, target := filepath.Join(backup, "data.mdb"), filepath.Join(dir, "data.mdb")
	require.NoError(t, os.WriteFile(source, []byte("replacement"), 0644))
	require.NoError(t, os.WriteFile(target, []byte("original"), 0644))
	err := installFiles([]restoreFile{
		{source: source, target: target},
		{source: filepath.Join(backup, "missing.seg"), target: filepath.Join(dir, "000000001.seg")},
	}, func(src, dst string) error {
		t.Fatal("publication started before all sources were copied")
		return nil
	})
	require.ErrorIs(t, err, os.ErrNotExist)
	contents, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, "original", string(contents))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestRestoreReplacesExistingFiles(t *testing.T) {
	dir, backup := t.TempDir(), t.TempDir()
	walDir := filepath.Join(dir, "testns", "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	for _, name := range []string{"000000001.seg", "000000002.seg"} {
		require.NoError(t, os.WriteFile(filepath.Join(backup, name), []byte("replacement "+name), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(walDir, name), []byte("original "+name), 0644))
	}
	btree := filepath.Join(dir, "testns", "data.mdb")
	btreeBackup := filepath.Join(backup, "backup.mdb")
	require.NoError(t, os.WriteFile(btree, []byte("original btree"), 0644))
	require.NoError(t, os.WriteFile(btreeBackup, []byte("replacement btree"), 0644))
	result, err := Restore(Options{DataDir: dir, Namespace: "testns", BTreePath: btreeBackup, WALPath: backup})
	require.NoError(t, err)
	assert.Equal(t, 2, result.SegmentsRestored)
	contents, err := os.ReadFile(btree)
	require.NoError(t, err)
	assert.Equal(t, "replacement btree", string(contents))
	entries, err := os.ReadDir(walDir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	for _, entry := range entries {
		contents, err := os.ReadFile(filepath.Join(walDir, entry.Name()))
		require.NoError(t, err)
		assert.Equal(t, "replacement "+entry.Name(), string(contents))
	}
}
