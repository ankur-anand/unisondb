package raftmux

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type noopConn struct{}

func (c *noopConn) Read(b []byte) (int, error)       { return 0, io.EOF }
func (c *noopConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *noopConn) Close() error                     { return nil }
func (c *noopConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *noopConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *noopConn) SetDeadline(time.Time) error      { return nil }
func (c *noopConn) SetReadDeadline(time.Time) error  { return nil }
func (c *noopConn) SetWriteDeadline(time.Time) error { return nil }

func BenchmarkDialAccept(b *testing.B) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(b, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(b, m2.Start())
	defer m2.Close()

	layer1, err := m1.RegisterNamespace("orders")
	require.NoError(b, err)

	layer2, err := m2.RegisterNamespace("orders")
	require.NoError(b, err)

	addr := raft.ServerAddress("127.0.0.1:" + portFromAddr(m2.Addr()))

	iters := b.N
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 1)
		for i := 0; i < iters; i++ {
			conn, err := layer2.Accept()
			if err != nil {
				errCh <- err
				return
			}
			if _, err := io.ReadFull(conn, buf); err != nil {
				conn.Close()
				errCh <- err
				return
			}
			if err := conn.Close(); err != nil {
				errCh <- err
				return
			}
		}
	}()

	payload := []byte{0x01}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < iters; i++ {
		conn, err := layer1.Dial(addr, 5*time.Second)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		if _, err := conn.Write(payload); err != nil {
			conn.Close()
			b.Fatalf("write: %v", err)
		}
		if err := conn.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	b.StopTimer()

	<-done
	select {
	case err := <-errCh:
		b.Fatalf("accept: %v", err)
	default:
	}
}

func BenchmarkParallelDialSessionReuse(b *testing.B) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(b, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(b, m2.Start())
	defer m2.Close()

	layer1, err := m1.RegisterNamespace("orders")
	require.NoError(b, err)

	layer2, err := m2.RegisterNamespace("orders")
	require.NoError(b, err)

	addr := raft.ServerAddress("127.0.0.1:" + portFromAddr(m2.Addr()))
	payload := []byte{0x01}

	warmupErr := make(chan error, 1)
	go func() {
		conn, err := layer2.Accept()
		if err != nil {
			warmupErr <- err
			return
		}
		buf := make([]byte, 1)
		if _, err := io.ReadFull(conn, buf); err != nil {
			conn.Close()
			warmupErr <- err
			return
		}
		warmupErr <- conn.Close()
	}()

	conn, err := layer1.Dial(addr, 5*time.Second)
	require.NoError(b, err)
	_, err = conn.Write(payload)
	require.NoError(b, err)
	require.NoError(b, conn.Close())
	require.NoError(b, <-warmupErr)

	iters := b.N
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 1)
		for i := 0; i < iters; i++ {
			conn, err := layer2.Accept()
			if err != nil {
				errCh <- err
				return
			}
			if _, err := io.ReadFull(conn, buf); err != nil {
				conn.Close()
				errCh <- err
				return
			}
			if err := conn.Close(); err != nil {
				errCh <- err
				return
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := layer1.Dial(addr, 5*time.Second)
			if err != nil {
				b.Fatalf("dial: %v", err)
			}
			if _, err := conn.Write(payload); err != nil {
				conn.Close()
				b.Fatalf("write: %v", err)
			}
			if err := conn.Close(); err != nil {
				b.Fatalf("close: %v", err)
			}
		}
	})
	b.StopTimer()

	select {
	case <-done:
	case err := <-errCh:
		b.Fatalf("accept: %v", err)
	case <-time.After(10 * time.Second):
		b.Fatal("timeout waiting for accept")
	}
}

func BenchmarkSteadyStateStream(b *testing.B) {
	m1 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(b, m1.Start())
	defer m1.Close()

	m2 := NewRaftTransportManager("127.0.0.1:0")
	require.NoError(b, m2.Start())
	defer m2.Close()

	layer1, err := m1.RegisterNamespace("orders")
	require.NoError(b, err)

	layer2, err := m2.RegisterNamespace("orders")
	require.NoError(b, err)

	addr := raft.ServerAddress("127.0.0.1:" + portFromAddr(m2.Addr()))

	payload := []byte("raftmux-ping")
	ready := make(chan struct{})
	start := make(chan struct{})
	acceptErr := make(chan error, 1)
	iters := b.N
	go func() {
		conn, err := layer2.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		defer conn.Close()

		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, buf); err != nil {
			acceptErr <- err
			return
		}
		if _, err := conn.Write(buf); err != nil {
			acceptErr <- err
			return
		}

		close(ready)
		<-start

		for i := 0; i < iters; i++ {
			if _, err := io.ReadFull(conn, buf); err != nil {
				acceptErr <- err
				return
			}
			if _, err := conn.Write(buf); err != nil {
				acceptErr <- err
				return
			}
		}
		acceptErr <- nil
	}()

	clientConn, err := layer1.Dial(addr, 5*time.Second)
	require.NoError(b, err)
	defer clientConn.Close()

	if _, err := clientConn.Write(payload); err != nil {
		b.Fatalf("warmup write: %v", err)
	}
	resp := make([]byte, len(payload))
	if _, err := io.ReadFull(clientConn, resp); err != nil {
		b.Fatalf("warmup read: %v", err)
	}

	select {
	case err := <-acceptErr:
		if err != nil {
			b.Fatalf("accept: %v", err)
		}
	case <-ready:
	case <-time.After(5 * time.Second):
		b.Fatal("timeout waiting for accept")
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	for i := 0; i < iters; i++ {
		if _, err := clientConn.Write(payload); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := io.ReadFull(clientConn, resp); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
	b.StopTimer()

	if err := <-acceptErr; err != nil {
		b.Fatalf("server: %v", err)
	}
}
