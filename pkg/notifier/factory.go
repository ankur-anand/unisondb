package notifier

import (
	"context"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

// Config holds ZeroMQ notifier configuration.
type Config struct {
	BindAddress string `toml:"bind_address"`
	Namespace   string `toml:"namespace"`

	HighWaterMark int `toml:"high_water_mark"`
	LingerTime    int `toml:"linger_time"`
}

// Notifier defines the interface for change notifications.
type Notifier interface {
	Notify(ctx context.Context, key []byte, op logrecord.LogOperationType, entryType logrecord.LogEntryType)
	Close() error
	Stats() (sent uint64, errors uint64)
}
