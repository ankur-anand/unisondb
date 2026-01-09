package raftmux

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	closed   bool
	mu       sync.Mutex
}

func newMockConn() *mockConn {
	return &mockConn{
		readBuf:  new(bytes.Buffer),
		writeBuf: new(bytes.Buffer),
	}
}

func (m *mockConn) Read(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readBuf.Read(b)
}

func (m *mockConn) Write(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeBuf.Write(b)
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234} }
func (m *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5678} }
func (m *mockConn) SetDeadline(_ time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(_ time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(_ time.Time) error { return nil }

func (m *mockConn) Written() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeBuf.Bytes()
}

func (m *mockConn) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func TestOutboundConn_WriteHeaderOnFirstWrite(t *testing.T) {
	mock := newMockConn()
	conn := NewOutboundConn(mock, "orders")

	n, err := conn.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	written := mock.Written()
	assert.Equal(t, []byte{0x01, 0x06, 'o', 'r', 'd', 'e', 'r', 's', 'h', 'e', 'l', 'l', 'o'}, written)
}

func TestOutboundConn_HeaderOnlyOnce(t *testing.T) {
	mock := newMockConn()
	conn := NewOutboundConn(mock, "ns")

	_, err := conn.Write([]byte("first"))
	require.NoError(t, err)

	_, err = conn.Write([]byte("second"))
	require.NoError(t, err)

	_, err = conn.Write([]byte("third"))
	require.NoError(t, err)

	written := mock.Written()
	expected := []byte{0x01, 0x02, 'n', 's', 'f', 'i', 'r', 's', 't', 's', 'e', 'c', 'o', 'n', 'd', 't', 'h', 'i', 'r', 'd'}
	assert.Equal(t, expected, written)
}

func TestOutboundConn_ReadPassthrough(t *testing.T) {
	mock := newMockConn()
	mock.readBuf.WriteString("response data")
	conn := NewOutboundConn(mock, "ns")

	buf := make([]byte, 13)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 13, n)
	assert.Equal(t, "response data", string(buf))
}

func TestOutboundConn_Close(t *testing.T) {
	mock := newMockConn()
	conn := NewOutboundConn(mock, "ns")

	require.False(t, mock.IsClosed())
	require.NoError(t, conn.Close())
	require.True(t, mock.IsClosed())
}

func TestOutboundConn_Namespace(t *testing.T) {
	mock := newMockConn()
	conn := NewOutboundConn(mock, "my-namespace")
	assert.Equal(t, "my-namespace", conn.Namespace())
}

func TestOutboundConn_ConcurrentWrites(t *testing.T) {
	mock := newMockConn()
	conn := NewOutboundConn(mock, "ns")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = conn.Write([]byte("x"))
		}()
	}
	wg.Wait()

	written := mock.Written()
	headerCount := 0
	for i := 0; i < len(written)-1; i++ {
		if written[i] == 0x01 && written[i+1] == 0x02 {
			headerCount++
		}
	}
	assert.Equal(t, 1, headerCount)
}

func TestOutboundConn_NetConnInterface(t *testing.T) {
	mock := newMockConn()
	conn := NewOutboundConn(mock, "ns")

	var _ net.Conn = conn

	assert.Equal(t, mock.LocalAddr(), conn.LocalAddr())
	assert.Equal(t, mock.RemoteAddr(), conn.RemoteAddr())
	assert.NoError(t, conn.SetDeadline(time.Now()))
	assert.NoError(t, conn.SetReadDeadline(time.Now()))
	assert.NoError(t, conn.SetWriteDeadline(time.Now()))
}

func TestInboundConn_Passthrough(t *testing.T) {
	mock := newMockConn()
	mock.readBuf.WriteString("data from peer")
	conn := NewInboundConn(mock, "orders")

	buf := make([]byte, 14)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 14, n)
	assert.Equal(t, "data from peer", string(buf))

	n, err = conn.Write([]byte("response"))
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Equal(t, []byte("response"), mock.Written())
}

func TestInboundConn_Namespace(t *testing.T) {
	mock := newMockConn()
	conn := NewInboundConn(mock, "events")
	assert.Equal(t, "events", conn.Namespace())
}

func TestOutboundConn_EmptyWrite(t *testing.T) {
	mock := newMockConn()
	conn := NewOutboundConn(mock, "ns")

	n, err := conn.Write([]byte{})
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	written := mock.Written()
	assert.Equal(t, []byte{0x01, 0x02, 'n', 's'}, written)
}

func TestOutboundConn_RealPipe(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	conn := NewOutboundConn(client, "orders")

	done := make(chan struct{})
	go func() {
		defer close(done)

		buf := make([]byte, 100)
		n, err := io.ReadAtLeast(server, buf, 13)
		require.NoError(t, err)

		r := bytes.NewReader(buf[:n])
		ns, err := ReadHeader(r)
		require.NoError(t, err)
		assert.Equal(t, "orders", ns)

		remaining := make([]byte, 5)
		_, err = io.ReadFull(r, remaining)
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), remaining)
	}()

	_, err := conn.Write([]byte("hello"))
	require.NoError(t, err)

	<-done
}
