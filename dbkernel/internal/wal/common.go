package wal

import (
	"errors"
	"time"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/uber-go/tally/v4"
)

var (
	fsyncLatencyBuckets = tally.DurationBuckets{
		1 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		20 * time.Second,
		30 * time.Second,
		60 * time.Second,
		120 * time.Second,
	}

	readLatencyBuckets = tally.DurationBuckets{
		100 * time.Microsecond,
		300 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		25 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		250 * time.Millisecond,
		1 * time.Second,
	}

	writeLatencyBuckets = tally.DurationBuckets{
		1 * time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		25 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
	}
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
	ErrNoNewData     = walfs.ErrNoNewData
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
	SyncInterval    time.Duration `toml:"sync_interval"`
	AutoCleanup     bool          `toml:"auto_cleanup"`
	MaxAge          time.Duration `toml:"max_age"`
	MinSegment      int           `toml:"min_segment"`
	MaxSegment      int           `toml:"max_segment"`
	CleanupInterval time.Duration `toml:"cleanup_interval"`
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
	if c.CleanupInterval == 0 {
		c.CleanupInterval = 5 * time.Minute
	}
}
