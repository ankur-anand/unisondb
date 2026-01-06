package raftmux

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDialer struct {
	dialFunc func(addr, namespace string, timeout time.Duration) (net.Conn, error)
}

func (m *mockDialer) DialNamespace(addr, namespace string, timeout time.Duration) (net.Conn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(addr, namespace, timeout)
	}
	server, client := net.Pipe()
	go func() { _ = server.Close() }()
	return client, nil
}

func TestNamespaceStreamLayer_Accept(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)
	defer layer.Close()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ok := layer.Enqueue(client)
	require.True(t, ok)

	conn, err := layer.Accept()
	require.NoError(t, err)
	assert.Equal(t, client, conn)
}

func TestNamespaceStreamLayer_AcceptBlocks(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)
	defer layer.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := layer.Accept()
		require.NoError(t, err)
		assert.NotNil(t, conn)
		conn.Close()
	}()

	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Accept should block until connection available")
	default:
	}

	server, client := net.Pipe()
	defer server.Close()
	layer.Enqueue(client)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Accept should return after connection enqueued")
	}
}

func TestNamespaceStreamLayer_AcceptAfterClose(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)

	layer.Close()

	_, err := layer.Accept()
	require.ErrorIs(t, err, ErrStreamLayerClosed)
}

func TestNamespaceStreamLayer_CloseUnblocksAccept(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)

	done := make(chan error)
	go func() {
		_, err := layer.Accept()
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	layer.Close()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrStreamLayerClosed)
	case <-time.After(time.Second):
		t.Fatal("Close should unblock Accept")
	}
}

func TestNamespaceStreamLayer_Dial(t *testing.T) {
	dialCalled := false
	dialer := &mockDialer{
		dialFunc: func(addr, namespace string, timeout time.Duration) (net.Conn, error) {
			dialCalled = true
			assert.Equal(t, "192.168.1.1:7000", addr)
			assert.Equal(t, "orders", namespace)
			assert.Equal(t, 5*time.Second, timeout)
			server, client := net.Pipe()
			go func() { _ = server.Close() }()
			return client, nil
		},
	}

	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)
	defer layer.Close()

	conn, err := layer.Dial(raft.ServerAddress("192.168.1.1:7000"), 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()

	assert.True(t, dialCalled)
}

func TestNamespaceStreamLayer_DialAfterClose(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)

	layer.Close()

	_, err := layer.Dial(raft.ServerAddress("192.168.1.1:7000"), 5*time.Second)
	require.ErrorIs(t, err, ErrStreamLayerClosed)
}

func TestNamespaceStreamLayer_Addr(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(192, 168, 1, 100), Port: 8000}
	layer := NewNamespaceStreamLayer("events", dialer, addr, 10)
	defer layer.Close()

	assert.Equal(t, addr, layer.Addr())
}

func TestNamespaceStreamLayer_Namespace(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("my-namespace", dialer, addr, 10)
	defer layer.Close()

	assert.Equal(t, "my-namespace", layer.Namespace())
}

func TestNamespaceStreamLayer_EnqueueAfterClose(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)

	layer.Close()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ok := layer.Enqueue(client)
	assert.False(t, ok)
}

func TestNamespaceStreamLayer_EnqueueQueueFull(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 2)
	defer layer.Close()

	for i := 0; i < 2; i++ {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		ok := layer.Enqueue(client)
		require.True(t, ok)
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	ok := layer.Enqueue(client)
	assert.False(t, ok)
}

func TestNamespaceStreamLayer_MultipleAccepts(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)
	defer layer.Close()

	conns := make([]net.Conn, 5)
	for i := 0; i < 5; i++ {
		server, client := net.Pipe()
		defer server.Close()
		conns[i] = client
		ok := layer.Enqueue(client)
		require.True(t, ok)
	}

	for i := 0; i < 5; i++ {
		conn, err := layer.Accept()
		require.NoError(t, err)
		assert.Equal(t, conns[i], conn)
	}
}

func TestNamespaceStreamLayer_ConcurrentAcceptEnqueue(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 100)
	defer layer.Close()

	var wg sync.WaitGroup
	count := 50

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server, client := net.Pipe()
			defer server.Close()
			layer.Enqueue(client)
		}()
	}

	var received atomic.Int32
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case conn := <-layer.acceptCh:
				if conn != nil {
					received.Add(1)
					conn.Close()
				}
			case <-time.After(100 * time.Millisecond):
			}
		}()
	}

	wg.Wait()
}

func TestNamespaceStreamLayer_IsClosed(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)

	assert.False(t, layer.IsClosed())
	layer.Close()
	assert.True(t, layer.IsClosed())
}

func TestNamespaceStreamLayer_DoubleClose(t *testing.T) {
	dialer := &mockDialer{}
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7000}
	layer := NewNamespaceStreamLayer("orders", dialer, addr, 10)

	require.NoError(t, layer.Close())
	require.NoError(t, layer.Close())
}

func TestNamespaceStreamLayer_ImplementsRaftStreamLayer(t *testing.T) {
	var _ raft.StreamLayer = (*NamespaceStreamLayer)(nil)
}
