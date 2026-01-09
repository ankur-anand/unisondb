package raftmux

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// OutboundConn wraps a net.Conn and writes the namespace header on first Write().
type OutboundConn struct {
	conn       net.Conn
	namespace  string
	headerSent atomic.Bool
	writeMu    sync.Mutex
}

// NewOutboundConn creates a new OutboundConn that will write the namespace
// header on the first Write() call.
func NewOutboundConn(conn net.Conn, namespace string) *OutboundConn {
	return &OutboundConn{
		conn:      conn,
		namespace: namespace,
	}
}

// Read reads data from the connection.
func (c *OutboundConn) Read(b []byte) (int, error) {
	return c.conn.Read(b)
}

// Write writes data to the connection. On the first call, it writes the
// namespace header before the data.
func (c *OutboundConn) Write(b []byte) (int, error) {
	if !c.headerSent.Load() {
		c.writeMu.Lock()
		if !c.headerSent.Load() {
			if err := WriteHeader(c.conn, c.namespace); err != nil {
				c.writeMu.Unlock()
				return 0, err
			}
			c.headerSent.Store(true)
		}
		c.writeMu.Unlock()
	}
	return c.conn.Write(b)
}

func (c *OutboundConn) Close() error {
	return c.conn.Close()
}

func (c *OutboundConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *OutboundConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (c *OutboundConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (c *OutboundConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *OutboundConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// Namespace returns the namespace for this connection.
func (c *OutboundConn) Namespace() string {
	return c.namespace
}

var _ net.Conn = (*OutboundConn)(nil)

// InboundConn wraps a net.Conn for inbound connections where the header
// has already been read by the transport manager.
type InboundConn struct {
	net.Conn
	namespace string
}

// NewInboundConn creates a new InboundConn.
func NewInboundConn(conn net.Conn, namespace string) *InboundConn {
	return &InboundConn{
		Conn:      conn,
		namespace: namespace,
	}
}

// Namespace returns the namespace for this connection.
func (c *InboundConn) Namespace() string {
	return c.namespace
}
