package raftmux

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

var (
	ErrManagerClosed     = errors.New("raftmux: manager closed")
	ErrManagerNotStarted = errors.New("raftmux: manager not started")
	ErrNamespaceExists   = errors.New("raftmux: namespace already registered")
	ErrNamespaceNotFound = errors.New("raftmux: namespace not found")
	ErrAcceptBacklogFull = errors.New("raftmux: accept backlog full")
	ErrHeaderReadTimeout = errors.New("raftmux: header read timeout")
)

const (
	defaultAcceptBacklog   = 256
	defaultHeaderTimeout   = 5 * time.Second
	defaultYamuxAcceptBack = 256
)

// RaftTransportManager manages yamux-multiplexed connections for Raft clusters.
type RaftTransportManager struct {
	bindAddr string
	listener net.Listener

	sessionsMu sync.RWMutex
	sessions   map[string]*yamux.Session

	layersMu sync.RWMutex
	layers   map[string]*NamespaceStreamLayer

	yamuxConfig   *yamux.Config
	headerTimeout time.Duration
	acceptBacklog int

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type ManagerOption func(*RaftTransportManager)

// WithHeaderTimeout sets the timeout for reading namespace headers.
func WithHeaderTimeout(d time.Duration) ManagerOption {
	return func(m *RaftTransportManager) {
		m.headerTimeout = d
	}
}

// WithAcceptBacklog sets the accept backlog size for namespace stream layers.
func WithAcceptBacklog(size int) ManagerOption {
	return func(m *RaftTransportManager) {
		m.acceptBacklog = size
	}
}

// WithYamuxConfig sets a custom yamux configuration.
func WithYamuxConfig(cfg *yamux.Config) ManagerOption {
	return func(m *RaftTransportManager) {
		m.yamuxConfig = cfg
	}
}

// NewRaftTransportManager creates a new RaftTransportManager.
func NewRaftTransportManager(bindAddr string, opts ...ManagerOption) *RaftTransportManager {
	m := &RaftTransportManager{
		bindAddr:      bindAddr,
		sessions:      make(map[string]*yamux.Session),
		layers:        make(map[string]*NamespaceStreamLayer),
		headerTimeout: defaultHeaderTimeout,
		acceptBacklog: defaultAcceptBacklog,
		closeCh:       make(chan struct{}),
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.yamuxConfig == nil {
		m.yamuxConfig = yamux.DefaultConfig()
		m.yamuxConfig.AcceptBacklog = defaultYamuxAcceptBack
		m.yamuxConfig.LogOutput = io.Discard
	}

	return m
}

func (m *RaftTransportManager) Start() error {
	listener, err := net.Listen("tcp", m.bindAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", m.bindAddr, err)
	}
	m.listener = listener

	m.wg.Add(1)
	go m.acceptLoop()

	slog.Info("[raftmux] transport manager started",
		slog.String("bind_addr", m.listener.Addr().String()))

	return nil
}

func (m *RaftTransportManager) Addr() net.Addr {
	if m.listener == nil {
		return nil
	}
	return m.listener.Addr()
}

func (m *RaftTransportManager) Close() error {
	m.closeOnce.Do(func() {
		close(m.closeCh)

		if m.listener != nil {
			m.listener.Close()
		}

		m.sessionsMu.Lock()
		for addr, session := range m.sessions {
			session.Close()
			delete(m.sessions, addr)
			metricsActiveSessionsTotal.Dec()
		}
		m.sessionsMu.Unlock()

		m.layersMu.Lock()
		for ns, layer := range m.layers {
			layer.Close()
			delete(m.layers, ns)
			metricsActiveNamespacesTotal.Dec()
		}
		m.layersMu.Unlock()

		m.wg.Wait()
	})
	return nil
}

func (m *RaftTransportManager) RegisterNamespace(namespace string) (*NamespaceStreamLayer, error) {
	m.layersMu.Lock()
	defer m.layersMu.Unlock()

	if _, exists := m.layers[namespace]; exists {
		return nil, ErrNamespaceExists
	}

	select {
	case <-m.closeCh:
		return nil, ErrManagerClosed
	default:
	}

	if m.listener == nil {
		return nil, ErrManagerNotStarted
	}

	addr := m.listener.Addr()
	layer := NewNamespaceStreamLayer(namespace, m, addr, m.acceptBacklog)
	m.layers[namespace] = layer

	metricsActiveNamespacesTotal.Inc()

	slog.Info("[raftmux] namespace registered",
		slog.String("namespace", namespace))

	return layer, nil
}

func (m *RaftTransportManager) UnregisterNamespace(namespace string) error {
	m.layersMu.Lock()
	defer m.layersMu.Unlock()

	layer, exists := m.layers[namespace]
	if !exists {
		return ErrNamespaceNotFound
	}

	layer.Close()
	delete(m.layers, namespace)

	metricsActiveNamespacesTotal.Dec()

	slog.Info("[raftmux] namespace unregistered",
		slog.String("namespace", namespace))

	return nil
}

func (m *RaftTransportManager) DialNamespace(addr, namespace string, timeout time.Duration) (net.Conn, error) {
	select {
	case <-m.closeCh:
		metricsDialTotal.WithLabelValues(namespace, "closed").Inc()
		return nil, ErrManagerClosed
	default:
	}

	session, err := m.getOrCreateSession(addr, timeout)
	if err != nil {
		metricsDialTotal.WithLabelValues(namespace, "error").Inc()
		return nil, err
	}

	stream, err := session.OpenStream()
	if err != nil {
		m.removeSession(addr)
		metricsDialTotal.WithLabelValues(namespace, "error").Inc()
		return nil, fmt.Errorf("open stream: %w", err)
	}

	metricsDialTotal.WithLabelValues(namespace, "success").Inc()
	return NewOutboundConn(stream, namespace), nil
}

func (m *RaftTransportManager) getOrCreateSession(addr string, timeout time.Duration) (*yamux.Session, error) {
	m.sessionsMu.RLock()
	session, exists := m.sessions[addr]
	m.sessionsMu.RUnlock()

	if exists && !session.IsClosed() {
		return session, nil
	}

	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	session, exists = m.sessions[addr]
	if exists && !session.IsClosed() {
		return session, nil
	}

	if exists {
		delete(m.sessions, addr)
		metricsActiveSessionsTotal.Dec()
	}

	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	session, err = yamux.Client(conn, m.yamuxConfig)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("yamux client: %w", err)
	}

	m.sessions[addr] = session

	metricsActiveSessionsTotal.Inc()

	m.wg.Add(1)
	go m.handleSession(session, addr, false)

	return session, nil
}

func (m *RaftTransportManager) removeSession(addr string) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	if session, exists := m.sessions[addr]; exists {
		session.Close()
		delete(m.sessions, addr)
		metricsActiveSessionsTotal.Dec()
	}
}

func (m *RaftTransportManager) acceptLoop() {
	defer m.wg.Done()

	for {
		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.closeCh:
				return
			default:
				slog.Warn("[raftmux] accept error",
					slog.Any("error", err))
				continue
			}
		}

		m.wg.Add(1)
		go m.handleIncomingConn(conn)
	}
}

func (m *RaftTransportManager) handleIncomingConn(conn net.Conn) {
	defer m.wg.Done()

	session, err := yamux.Server(conn, m.yamuxConfig)
	if err != nil {
		slog.Warn("[raftmux] yamux server error",
			slog.Any("error", err),
			slog.String("remote", conn.RemoteAddr().String()))
		conn.Close()
		return
	}

	addr := conn.RemoteAddr().String()

	m.sessionsMu.Lock()
	m.sessions[addr] = session
	m.sessionsMu.Unlock()

	metricsActiveSessionsTotal.Inc()

	m.handleSession(session, addr, true)
}

func (m *RaftTransportManager) handleSession(session *yamux.Session, addr string, isInbound bool) {
	if !isInbound {
		defer m.wg.Done()
	}

	defer func() {
		m.sessionsMu.Lock()
		if s, exists := m.sessions[addr]; exists && s == session {
			delete(m.sessions, addr)
			metricsActiveSessionsTotal.Dec()
		}
		m.sessionsMu.Unlock()
	}()

	for {
		stream, err := session.AcceptStream()
		if err != nil {
			select {
			case <-m.closeCh:
				return
			default:
				if !session.IsClosed() {
					slog.Debug("[raftmux] accept stream error",
						slog.Any("error", err),
						slog.String("remote", addr))
				}
				return
			}
		}

		m.wg.Add(1)
		go m.routeStream(stream)
	}
}

func (m *RaftTransportManager) routeStream(stream net.Conn) {
	defer m.wg.Done()

	startTime := time.Now()

	if err := stream.SetReadDeadline(time.Now().Add(m.headerTimeout)); err != nil {
		stream.Close()
		return
	}

	namespace, err := ReadHeader(stream)
	if err != nil {
		metricsHeaderReadErrors.Inc()
		slog.Debug("[raftmux] read header error",
			slog.Any("error", err),
			slog.String("remote", stream.RemoteAddr().String()))
		stream.Close()
		return
	}

	if err := stream.SetReadDeadline(time.Time{}); err != nil {
		stream.Close()
		return
	}

	m.layersMu.RLock()
	layer, exists := m.layers[namespace]
	m.layersMu.RUnlock()

	if !exists {
		metricsUnknownNamespaceTotal.Inc()
		slog.Warn("[raftmux] unknown namespace",
			slog.String("namespace", namespace),
			slog.String("remote", stream.RemoteAddr().String()))
		stream.Close()
		return
	}

	inboundConn := NewInboundConn(stream, namespace)
	if !layer.Enqueue(inboundConn) {
		metricsAcceptBacklogFull.WithLabelValues(namespace).Inc()
		metricsAcceptTotal.WithLabelValues(namespace, "backlog_full").Inc()
		slog.Warn("[raftmux] accept backlog full",
			slog.String("namespace", namespace))
		stream.Close()
		return
	}

	metricsStreamRoutingLatency.Observe(time.Since(startTime).Seconds())
	metricsAcceptTotal.WithLabelValues(namespace, "success").Inc()
}

func (m *RaftTransportManager) GetSession(addr string) *yamux.Session {
	m.sessionsMu.RLock()
	defer m.sessionsMu.RUnlock()
	return m.sessions[addr]
}

func (m *RaftTransportManager) SessionCount() int {
	m.sessionsMu.RLock()
	defer m.sessionsMu.RUnlock()
	return len(m.sessions)
}

func (m *RaftTransportManager) NamespaceCount() int {
	m.layersMu.RLock()
	defer m.layersMu.RUnlock()
	return len(m.layers)
}

var _ Dialer = (*RaftTransportManager)(nil)
