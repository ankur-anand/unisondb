package raftmux

import (
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

var (
	ErrStreamLayerClosed = errors.New("raftmux: stream layer closed")
)

// Dialer is the interface used by NamespaceStreamLayer to dial peers.
type Dialer interface {
	DialNamespace(addr string, namespace string, timeout time.Duration) (net.Conn, error)
}

// NamespaceStreamLayer implements raft.StreamLayer for a specific namespace.
type NamespaceStreamLayer struct {
	namespace string
	dialer    Dialer
	addr      net.Addr
	acceptCh  chan net.Conn
	closed    atomic.Bool
	closeCh   chan struct{}
}

// NewNamespaceStreamLayer creates a new NamespaceStreamLayer.
func NewNamespaceStreamLayer(namespace string, dialer Dialer, addr net.Addr, acceptBacklog int) *NamespaceStreamLayer {
	if acceptBacklog <= 0 {
		acceptBacklog = 256
	}
	return &NamespaceStreamLayer{
		namespace: namespace,
		dialer:    dialer,
		addr:      addr,
		acceptCh:  make(chan net.Conn, acceptBacklog),
		closeCh:   make(chan struct{}),
	}
}

// Accept waits for and returns the next inbound connection.
func (l *NamespaceStreamLayer) Accept() (net.Conn, error) {
	select {
	case conn := <-l.acceptCh:
		return conn, nil
	case <-l.closeCh:
		return nil, ErrStreamLayerClosed
	}
}

// Close closes the stream layer.
func (l *NamespaceStreamLayer) Close() error {
	if l.closed.CompareAndSwap(false, true) {
		close(l.closeCh)
	}
	return nil
}

func (l *NamespaceStreamLayer) Addr() net.Addr {
	return l.addr
}

// Dial creates a new outbound connection to the given address.
func (l *NamespaceStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if l.closed.Load() {
		return nil, ErrStreamLayerClosed
	}
	return l.dialer.DialNamespace(string(address), l.namespace, timeout)
}

// Enqueue adds an inbound connection to the accept queue.
// Returns false if the stream layer is closed or the queue is full.
func (l *NamespaceStreamLayer) Enqueue(conn net.Conn) bool {
	if l.closed.Load() {
		return false
	}
	select {
	case l.acceptCh <- conn:
		return true
	default:
		return false
	}
}

func (l *NamespaceStreamLayer) Namespace() string {
	return l.namespace
}

func (l *NamespaceStreamLayer) IsClosed() bool {
	return l.closed.Load()
}

var _ raft.StreamLayer = (*NamespaceStreamLayer)(nil)
