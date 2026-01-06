package raftmux

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaftTransportManager_StartAndClose(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())

	addr := m.Addr()
	require.NotNil(t, addr)

	require.NoError(t, m.Close())
}

func TestRaftTransportManager_RegisterNamespace(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())
	defer m.Close()

	layer, err := m.RegisterNamespace("orders")
	require.NoError(t, err)
	require.NotNil(t, layer)
	assert.Equal(t, "orders", layer.Namespace())
	assert.Equal(t, 1, m.NamespaceCount())
}

func TestRaftTransportManager_RegisterDuplicateNamespace(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())
	defer m.Close()

	_, err := m.RegisterNamespace("orders")
	require.NoError(t, err)

	_, err = m.RegisterNamespace("orders")
	require.ErrorIs(t, err, ErrNamespaceExists)
}

func TestRaftTransportManager_UnregisterNamespace(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())
	defer m.Close()

	_, err := m.RegisterNamespace("orders")
	require.NoError(t, err)
	assert.Equal(t, 1, m.NamespaceCount())

	err = m.UnregisterNamespace("orders")
	require.NoError(t, err)
	assert.Equal(t, 0, m.NamespaceCount())
}

func TestRaftTransportManager_UnregisterNonexistentNamespace(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())
	defer m.Close()

	err := m.UnregisterNamespace("nonexistent")
	require.ErrorIs(t, err, ErrNamespaceNotFound)
}

func TestRaftTransportManager_MultipleNamespaces(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())
	defer m.Close()

	namespaces := []string{"orders", "users", "events", "logs"}
	for _, ns := range namespaces {
		layer, err := m.RegisterNamespace(ns)
		require.NoError(t, err)
		assert.Equal(t, ns, layer.Namespace())
	}

	assert.Equal(t, 4, m.NamespaceCount())
}

func TestRaftTransportManager_DialCreatesSession(t *testing.T) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	_, err := m1.RegisterNamespace("orders")
	require.NoError(t, err)

	_, err = m2.RegisterNamespace("orders")
	require.NoError(t, err)

	conn, err := m1.DialNamespace(m2.Addr().String(), "orders", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	assert.Equal(t, 1, m1.SessionCount())
}

func TestRaftTransportManager_SessionReuse(t *testing.T) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	_, err := m1.RegisterNamespace("orders")
	require.NoError(t, err)
	_, err = m1.RegisterNamespace("users")
	require.NoError(t, err)

	_, err = m2.RegisterNamespace("orders")
	require.NoError(t, err)
	_, err = m2.RegisterNamespace("users")
	require.NoError(t, err)

	addr := m2.Addr().String()

	conn1, err := m1.DialNamespace(addr, "orders", 5*time.Second)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := m1.DialNamespace(addr, "users", 5*time.Second)
	require.NoError(t, err)
	defer conn2.Close()

	assert.Equal(t, 1, m1.SessionCount())
}

func TestRaftTransportManager_DialAndAccept(t *testing.T) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	layer1, err := m1.RegisterNamespace("orders")
	require.NoError(t, err)

	layer2, err := m2.RegisterNamespace("orders")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := layer2.Accept()
		require.NoError(t, err)
		defer conn.Close()

		buf := make([]byte, 5)
		n, err := conn.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, "hello", string(buf[:n]))
	}()

	conn, err := layer1.Dial(raft.ServerAddress("127.0.0.1:"+portFromAddr(m2.Addr())), 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for accept")
	}
}

func TestRaftTransportManager_BidirectionalCommunication(t *testing.T) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	layer1, err := m1.RegisterNamespace("orders")
	require.NoError(t, err)

	layer2, err := m2.RegisterNamespace("orders")
	require.NoError(t, err)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := layer2.Accept()
		require.NoError(t, err)
		defer conn.Close()

		buf := make([]byte, 7)
		n, err := conn.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, "request", string(buf[:n]))

		_, err = conn.Write([]byte("response"))
		require.NoError(t, err)
	}()

	conn, err := layer1.Dial(raft.ServerAddress("127.0.0.1:"+portFromAddr(m2.Addr())), 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("request"))
	require.NoError(t, err)

	buf := make([]byte, 8)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "response", string(buf[:n]))

	wg.Wait()
}

func TestRaftTransportManager_MultipleNamespacesRouting(t *testing.T) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	layer1Orders, _ := m1.RegisterNamespace("orders")
	layer1Users, _ := m1.RegisterNamespace("users")
	layer2Orders, _ := m2.RegisterNamespace("orders")
	layer2Users, _ := m2.RegisterNamespace("users")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := layer2Orders.Accept()
		require.NoError(t, err)
		defer conn.Close()

		buf := make([]byte, 6)
		n, _ := conn.Read(buf)
		assert.Equal(t, "orders", string(buf[:n]))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := layer2Users.Accept()
		require.NoError(t, err)
		defer conn.Close()

		buf := make([]byte, 5)
		n, _ := conn.Read(buf)
		assert.Equal(t, "users", string(buf[:n]))
	}()

	addr := raft.ServerAddress("127.0.0.1:" + portFromAddr(m2.Addr()))

	connOrders, err := layer1Orders.Dial(addr, 5*time.Second)
	require.NoError(t, err)
	defer connOrders.Close()

	connUsers, err := layer1Users.Dial(addr, 5*time.Second)
	require.NoError(t, err)
	defer connUsers.Close()

	_, _ = connOrders.Write([]byte("orders"))
	_, _ = connUsers.Write([]byte("users"))

	wg.Wait()
}

func TestRaftTransportManager_CloseWhileDialing(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())

	m.Close()

	_, err := m.DialNamespace("127.0.0.1:9999", "orders", time.Second)
	require.ErrorIs(t, err, ErrManagerClosed)
}

func TestRaftTransportManager_RegisterAfterClose(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m.Start())
	m.Close()

	_, err := m.RegisterNamespace("orders")
	require.ErrorIs(t, err, ErrManagerClosed)
}

func TestRaftTransportManager_Options(t *testing.T) {
	m := NewRaftTransportManager("127.0.0.1:0",
		WithHeaderTimeout(10*time.Second),
		WithAcceptBacklog(512),
	)
	require.NoError(t, m.Start())
	defer m.Close()

	assert.Equal(t, 10*time.Second, m.headerTimeout)
	assert.Equal(t, 512, m.acceptBacklog)
}

func portFromAddr(addr net.Addr) string {
	_, port, _ := net.SplitHostPort(addr.String())
	return port
}
