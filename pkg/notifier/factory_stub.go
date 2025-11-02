//go:build !zeromq

package notifier

import (
	"errors"
)

// NewZeroMQNotifier returns an error when ZeroMQ support is not compiled in.
// Build with -tags zeromq to enable ZeroMQ support.
func NewZeroMQNotifier(config Config) (Notifier, error) {
	return nil, errors.New("ZeroMQ support not compiled in. Build with -tags zeromq to enable")
}
