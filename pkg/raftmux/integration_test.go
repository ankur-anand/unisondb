package raftmux

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiNodeMultiNamespace tests a 3-node cluster with 2 namespaces.
// Each node runs a transport manager with both namespaces registered.
// This simulates the real-world scenario where unisondb runs multiple
// Raft clusters (one per namespace) over a single TCP connection per peer.
func TestMultiNodeMultiNamespace(t *testing.T) {
	const nodeCount = 3

	namespaces := []string{"orders", "users"}
	// manager each node
	managers := make([]*RaftTransportManager, nodeCount)
	for i := 0; i < nodeCount; i++ {
		m := NewRaftTransportManager("127.0.0.1:0")
		require.NoError(t, m.Start())
		managers[i] = m
	}
	defer func() {
		for _, m := range managers {
			m.Close()
		}
	}()

	nodeLayers := make([]map[string]*NamespaceStreamLayer, nodeCount)

	for i := 0; i < nodeCount; i++ {
		nodeLayers[i] = make(map[string]*NamespaceStreamLayer)
		for _, ns := range namespaces {
			layer, err := managers[i].RegisterNamespace(ns)
			require.NoError(t, err)
			nodeLayers[i][ns] = layer
		}
	}

	for _, ns := range namespaces {
		ns := ns
		t.Run(fmt.Sprintf("namespace_%s", ns), func(t *testing.T) {
			// node 0 -> node 1 communication
			t.Run("node0_to_node1", func(t *testing.T) {
				testPeerCommunication(t, nodeLayers[0][ns], nodeLayers[1][ns], managers[1].Addr().String())
			})

			// node 1 -> node 2 communication
			t.Run("node1_to_node2", func(t *testing.T) {
				testPeerCommunication(t, nodeLayers[1][ns], nodeLayers[2][ns], managers[2].Addr().String())
			})

			// node 2 -> node 0 communication
			t.Run("node2_to_node0", func(t *testing.T) {
				testPeerCommunication(t, nodeLayers[2][ns], nodeLayers[0][ns], managers[0].Addr().String())
			})
		})
	}
}

func testPeerCommunication(t *testing.T, dialer, acceptor *NamespaceStreamLayer, targetAddr string) {
	message := []byte("raft-rpc-payload")

	acceptErr := make(chan error, 1)
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		conn, err := acceptor.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		defer conn.Close()

		buf := make([]byte, len(message))
		n, err := conn.Read(buf)
		if err != nil {
			acceptErr <- fmt.Errorf("read: %w", err)
			return
		}
		if string(buf[:n]) != string(message) {
			acceptErr <- fmt.Errorf("message mismatch: got %q, want %q", string(buf[:n]), string(message))
			return
		}

		if _, err = conn.Write([]byte("ack")); err != nil {
			acceptErr <- fmt.Errorf("write ack: %w", err)
			return
		}
	}()

	time.Sleep(10 * time.Millisecond)
	conn, err := dialer.Dial(raft.ServerAddress(targetAddr), 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write(message)
	require.NoError(t, err)
	buf := make([]byte, 3)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "ack", string(buf[:n]))

	select {
	case <-acceptDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for accept goroutine")
	}

	select {
	case err := <-acceptErr:
		t.Fatalf("acceptor error: %v", err)
	default:
	}
}

func TestConcurrentNamespaceCommunication(t *testing.T) {
	namespaces := []string{"ns1", "ns2", "ns3", "ns4"}
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	layers1 := make(map[string]*NamespaceStreamLayer)
	layers2 := make(map[string]*NamespaceStreamLayer)

	for _, ns := range namespaces {
		l1, err := m1.RegisterNamespace(ns)
		require.NoError(t, err)
		layers1[ns] = l1

		l2, err := m2.RegisterNamespace(ns)
		require.NoError(t, err)
		layers2[ns] = l2
	}

	var wg sync.WaitGroup
	errors := make(chan error, len(namespaces)*2)

	for _, ns := range namespaces {
		ns := ns
		wg.Add(2)

		go func() {
			defer wg.Done()
			conn, err := layers2[ns].Accept()
			if err != nil {
				errors <- fmt.Errorf("accept %s: %w", ns, err)
				return
			}
			defer conn.Close()

			buf := make([]byte, 100)
			n, err := conn.Read(buf)
			if err != nil {
				errors <- fmt.Errorf("read %s: %w", ns, err)
				return
			}

			expected := fmt.Sprintf("message-for-%s", ns)
			if string(buf[:n]) != expected {
				errors <- fmt.Errorf("unexpected message for %s: got %s, want %s", ns, string(buf[:n]), expected)
				return
			}

			_, err = conn.Write([]byte(fmt.Sprintf("ack-%s", ns)))
			if err != nil {
				errors <- fmt.Errorf("write %s: %w", ns, err)
			}
		}()

		go func() {
			defer wg.Done()
			conn, err := layers1[ns].Dial(raft.ServerAddress(m2.Addr().String()), 5*time.Second)
			if err != nil {
				errors <- fmt.Errorf("dial %s: %w", ns, err)
				return
			}
			defer conn.Close()

			_, err = conn.Write([]byte(fmt.Sprintf("message-for-%s", ns)))
			if err != nil {
				errors <- fmt.Errorf("write %s: %w", ns, err)
				return
			}

			buf := make([]byte, 100)
			n, err := conn.Read(buf)
			if err != nil {
				errors <- fmt.Errorf("read ack %s: %w", ns, err)
				return
			}

			expected := fmt.Sprintf("ack-%s", ns)
			if string(buf[:n]) != expected {
				errors <- fmt.Errorf("unexpected ack for %s: got %s, want %s", ns, string(buf[:n]), expected)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for concurrent communication")
	}

	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestSessionPersistenceAcrossNamespaces(t *testing.T) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	namespaces := []string{"ns1", "ns2", "ns3"}

	for _, ns := range namespaces {
		_, err := m1.RegisterNamespace(ns)
		require.NoError(t, err)
		_, err = m2.RegisterNamespace(ns)
		require.NoError(t, err)
	}

	addr := m2.Addr().String()

	conns := make([]interface{ Close() error }, 0, len(namespaces))
	for _, ns := range namespaces {
		conn, err := m1.DialNamespace(addr, ns, 5*time.Second)
		require.NoError(t, err)
		conns = append(conns, conn)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	assert.Equal(t, 1, m1.SessionCount(), "expected single session for multiple namespaces")
}

func TestGracefulShutdown(t *testing.T) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	layer1, err := m1.RegisterNamespace("test")
	require.NoError(t, err)
	_, err = m2.RegisterNamespace("test")
	require.NoError(t, err)

	conn, err := layer1.Dial(raft.ServerAddress(m2.Addr().String()), 5*time.Second)
	require.NoError(t, err)
	conn.Close()

	assert.Equal(t, 1, m1.SessionCount())
	require.NoError(t, m2.Close())
	require.NoError(t, m1.Close())
	assert.Equal(t, 0, m1.SessionCount())
	assert.Equal(t, 0, m1.NamespaceCount())
}

func TestHighConcurrencyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(t, m2.Start())
	defer m2.Close()

	layer1, err := m1.RegisterNamespace("stress")
	require.NoError(t, err)
	layer2, err := m2.RegisterNamespace("stress")
	require.NoError(t, err)

	const (
		numConnections  = 50
		messagesPerConn = 10
		messageSize     = 1024
	)

	var wg sync.WaitGroup
	errors := make(chan error, numConnections*2)

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := layer2.Accept()
			if err != nil {
				errors <- fmt.Errorf("accept %d: %w", id, err)
				return
			}
			defer conn.Close()
			for j := 0; j < messagesPerConn; j++ {
				buf := make([]byte, messageSize)
				n, err := io.ReadFull(conn, buf)
				if err != nil {
					errors <- fmt.Errorf("read %d-%d: %w", id, j, err)
					return
				}
				_, err = conn.Write(buf[:n])
				if err != nil {
					errors <- fmt.Errorf("write %d-%d: %w", id, j, err)
					return
				}
			}
		}(i)
	}

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := layer1.Dial(raft.ServerAddress(m2.Addr().String()), 10*time.Second)
			if err != nil {
				errors <- fmt.Errorf("dial %d: %w", id, err)
				return
			}
			defer conn.Close()

			message := make([]byte, messageSize)
			for j := 0; j < len(message); j++ {
				message[j] = byte((id + j) % 256)
			}

			for j := 0; j < messagesPerConn; j++ {
				_, err = conn.Write(message)
				if err != nil {
					errors <- fmt.Errorf("write %d-%d: %w", id, j, err)
					return
				}

				buf := make([]byte, messageSize)
				_, err = io.ReadFull(conn, buf)
				if err != nil {
					errors <- fmt.Errorf("read %d-%d: %w", id, j, err)
					return
				}

				for k := 0; k < len(buf); k++ {
					if buf[k] != message[k] {
						errors <- fmt.Errorf("message mismatch at %d-%d byte %d", id, j, k)
						return
					}
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("stress test timeout")
	}

	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
