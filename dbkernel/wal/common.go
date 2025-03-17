package wal

import (
	"time"

	"github.com/ankur-anand/wal"
)

var (
	packageKey = []string{"kvalchemy"}
)

// Default permission values.
const (
	defaultBytesPerSync = 1 * wal.MB  // 1MB
	defaultSegmentSize  = 16 * wal.MB // 16MB
	defaultSyncInterval = 1 * time.Second
)

func newWALOptions(dirPath string, c *Config) wal.Options {
	return wal.Options{
		DirPath:        dirPath,
		SegmentSize:    c.SegmentSize,
		SegmentFileExt: ".seg.wal",
		Sync:           c.FSync,
		SyncInterval:   c.SyncInterval,
		BytesPerSync:   c.BytesPerSync,
	}
}

// Config stores all tunable parameters for WAL.
type Config struct {
	BytesPerSync uint32 `toml:"bytes_per_sync"`
	SegmentSize  int64  `toml:"segment_size"`
	// should fsync be done after every writes.
	// FSync are costly
	// writes are buffered to OS.
	// Logs will be only lost, if the machine itself crashes.
	// Not the process, data will be still safe as os buffer persists.
	FSync bool `toml:"fsync"`
	// call FSync with at this interval.
	SyncInterval time.Duration `toml:"sync_interval"`
}

func NewDefaultConfig() *Config {
	return &Config{
		BytesPerSync: defaultBytesPerSync,
		SegmentSize:  defaultSegmentSize,
		SyncInterval: defaultSyncInterval,
		FSync:        false,
	}
}

// applyDefaults applies default value on config if not set.
func (c *Config) applyDefaults() {
	if c.BytesPerSync == 0 {
		c.BytesPerSync = defaultBytesPerSync
	}
	if c.SegmentSize == 0 {
		c.SegmentSize = defaultSegmentSize
	}
	if c.FSync {
		c.FSync = true
	}
	if c.SyncInterval == 0 {
		c.SyncInterval = defaultSyncInterval
	}
}
