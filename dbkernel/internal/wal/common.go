package wal

import (
	"errors"
	"time"

	"github.com/ankur-anand/unisondb/pkg/walfs"
)

var (
	packageKey = []string{"unisondb"}
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// Default permission values.
const (
	defaultBytesPerSync = 1 * MB  // 1MB
	defaultSegmentSize  = 16 * MB // 16MB
	defaultSyncInterval = 1 * time.Second
)

var (
	ErrWalNextOffset = errors.New("wal next offset out of range")
	ErrWalFSync      = walfs.ErrFsync
)

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
