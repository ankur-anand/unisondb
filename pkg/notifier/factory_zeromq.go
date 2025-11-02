//go:build zeromq

package notifier

import (
	"github.com/ankur-anand/unisondb/plugin/notifier/zeromq"
)

// NewZeroMQNotifier creates a new ZeroMQ notifier.
// This implementation is only available when building with -tags zeromq.
// nolint: ireturn
func NewZeroMQNotifier(config Config) (Notifier, error) {
	return zeromq.NewZeroMQNotifier(zeromq.Config{
		BindAddress:   config.BindAddress,
		Namespace:     config.Namespace,
		HighWaterMark: config.HighWaterMark,
		LingerTime:    config.LingerTime,
	})
}
