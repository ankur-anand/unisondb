package raftcluster

import (
	"bytes"
	"encoding/base64"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureSerfEncryption_NoKeys(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	cfg := MembershipConfiguration{}
	beforeIncoming := conf.GossipVerifyIncoming
	beforeOutgoing := conf.GossipVerifyOutgoing

	err := configureSerfEncryption(conf, cfg)
	require.NoError(t, err)
	assert.Nil(t, conf.SecretKey)
	assert.Nil(t, conf.Keyring)
	assert.Equal(t, beforeIncoming, conf.GossipVerifyIncoming)
	assert.Equal(t, beforeOutgoing, conf.GossipVerifyOutgoing)
}

func TestConfigureSerfEncryption_KeyringRequiresSecret(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	cfg := MembershipConfiguration{
		KeyringBase64: []string{base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))},
	}

	err := configureSerfEncryption(conf, cfg)
	assert.Error(t, err)
}

func TestConfigureSerfEncryption_InvalidSecretKey(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	cfg := MembershipConfiguration{
		SecretKeyBase64: "not-base64",
	}

	err := configureSerfEncryption(conf, cfg)
	assert.Error(t, err)
}

func TestConfigureSerfEncryption_InvalidKeyLength(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	cfg := MembershipConfiguration{
		SecretKeyBase64: base64.StdEncoding.EncodeToString([]byte("short-key")),
	}

	err := configureSerfEncryption(conf, cfg)
	assert.Error(t, err)
}

func TestConfigureSerfEncryption_SetsKeyringAndVerify(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	primary := bytes.Repeat([]byte{0x01}, 16)
	secondary := bytes.Repeat([]byte{0x02}, 16)
	cfg := MembershipConfiguration{
		SecretKeyBase64: base64.StdEncoding.EncodeToString(primary),
		KeyringBase64:   []string{base64.StdEncoding.EncodeToString(secondary)},
		VerifyIncoming:  true,
		VerifyOutgoing:  true,
	}

	err := configureSerfEncryption(conf, cfg)
	require.NoError(t, err)
	require.NotNil(t, conf.SecretKey)
	assert.True(t, bytes.Equal(primary, conf.SecretKey))
	assert.NotNil(t, conf.Keyring)
	assert.True(t, conf.GossipVerifyIncoming)
	assert.True(t, conf.GossipVerifyOutgoing)

	keys := conf.Keyring.GetKeys()
	require.Len(t, keys, 2)
	assert.True(t, bytes.Equal(primary, keys[0]))
	assert.True(t, bytes.Equal(secondary, keys[1]))
}

func TestMemberEventString(t *testing.T) {
	tests := []struct {
		event MemberEvent
		want  string
	}{
		{MemberEventJoin, "member-join"},
		{MemberEventLeave, "member-leave"},
		{MemberEventFailed, "member-failed"},
		{MemberEventReap, "member-reap"},
		{MemberEventLeaderChange, "member-leader-change"},
		{MemberEventUpdate, "member-update"},
	}

	for _, tt := range tests {
		if got := tt.event.String(); got != tt.want {
			t.Fatalf("event %d: want %q got %q", tt.event, tt.want, got)
		}
	}
}

type testClusterListener struct {
	mu     sync.Mutex
	events []MemberEvent
	infos  []MemberInformation
}

func (t *testClusterListener) OnChangeEvent(event MemberEvent, info MemberInformation) {
	t.mu.Lock()
	t.events = append(t.events, event)
	t.infos = append(t.infos, info)
	t.mu.Unlock()
}

func (t *testClusterListener) ReconcileMembers([]MemberInformation) {}

func (t *testClusterListener) hasEvent(event MemberEvent, node string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, ev := range t.events {
		if ev == event && t.infos[i].NodeName == node {
			return true
		}
	}
	return false
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func findMember(members []serf.Member, name string) (serf.Member, bool) {
	for _, member := range members {
		if member.Name == name {
			return member, true
		}
	}
	return serf.Member{}, false
}

func newSerfTestConfig(t *testing.T, nodeName string, port int, events chan serf.Event, reconnectTimeout time.Duration, reapInterval time.Duration, tombstoneTimeout time.Duration) *serf.Config {
	t.Helper()
	conf := defaultConfig()
	conf.Init()
	conf.NodeName = nodeName
	conf.EventCh = events
	conf.ReconnectTimeout = reconnectTimeout
	conf.ReapInterval = reapInterval
	conf.TombstoneTimeout = tombstoneTimeout
	conf.MemberlistConfig.BindAddr = "127.0.0.1"
	conf.MemberlistConfig.BindPort = port
	conf.MemberlistConfig.AdvertiseAddr = "127.0.0.1"
	conf.MemberlistConfig.AdvertisePort = port
	conf.MemberlistConfig.GossipInterval = 10 * time.Millisecond
	conf.MemberlistConfig.ProbeInterval = 50 * time.Millisecond
	conf.MemberlistConfig.ProbeTimeout = 25 * time.Millisecond
	conf.MemberlistConfig.SuspicionMult = 1
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conf.Logger = slog.NewLogLogger(logger.Handler(), slog.LevelInfo)
	conf.MemberlistConfig.Logger = slog.NewLogLogger(logger.Handler(), slog.LevelInfo)
	return conf
}

func TestMembershipLiveClusterJoinLeave(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	listener := &testClusterListener{}

	m1, err := NewMembership(MembershipConfiguration{
		NodeName:      "node1",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port1,
	}, slog.Default())
	require.NoError(t, err)
	defer m1.Serf.Shutdown()
	m1.RegisterCluster(listener)

	m2, err := NewMembership(MembershipConfiguration{
		NodeName:      "node2",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port2,
	}, slog.Default())
	require.NoError(t, err)
	defer m2.Serf.Shutdown()

	stop1 := make(chan struct{})
	stop2 := make(chan struct{})
	go func() { _ = m1.EventHandler(stop1) }()
	go func() { _ = m2.EventHandler(stop2) }()
	defer close(stop1)
	defer close(stop2)

	_, err = m2.Join([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port1))})
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if listener.hasEvent(MemberEventJoin, "node2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected join event for node2")
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, m2.Close())

	deadline = time.Now().Add(5 * time.Second)
	for {
		if listener.hasEvent(MemberEventLeave, "node2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected leave event for node2")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMembershipLiveClusterUpdateTags(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	listener := &testClusterListener{}

	m1, err := NewMembership(MembershipConfiguration{
		NodeName:      "node1",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port1,
	}, logger)
	require.NoError(t, err)
	defer m1.Serf.Shutdown()
	m1.RegisterCluster(listener)

	m2, err := NewMembership(MembershipConfiguration{
		NodeName:      "node2",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port2,
	}, logger)
	require.NoError(t, err)
	defer m2.Serf.Shutdown()

	stop1 := make(chan struct{})
	stop2 := make(chan struct{})
	go func() { _ = m1.EventHandler(stop1) }()
	go func() { _ = m2.EventHandler(stop2) }()
	defer close(stop1)
	defer close(stop2)

	_, err = m2.Join([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port1))})
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if listener.hasEvent(MemberEventJoin, "node2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected join event for node2")
		}
		time.Sleep(50 * time.Millisecond)
	}

	err = m2.SetTags(map[string]string{
		"raft-addr":  "127.0.0.1:29001",
		"namespaces": "test",
	})
	require.NoError(t, err)

	deadline = time.Now().Add(5 * time.Second)
	for {
		if listener.hasEvent(MemberEventUpdate, "node2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected update event for node2")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMembershipClusterFilterBlocksClusterNotifications(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	listener := &testClusterListener{}

	blockAll := func(_ MemberEvent, _ MemberInformation) bool {
		return false
	}

	m1, err := NewMembership(MembershipConfiguration{
		NodeName:      "node1",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port1,
		ClusterFilter: blockAll,
	}, logger)
	require.NoError(t, err)
	defer m1.Serf.Shutdown()

	m1.RegisterCluster(listener)

	m2, err := NewMembership(MembershipConfiguration{
		NodeName:      "node2",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port2,
	}, logger)
	require.NoError(t, err)
	defer m2.Serf.Shutdown()

	stop1 := make(chan struct{})
	stop2 := make(chan struct{})
	go func() { _ = m1.EventHandler(stop1) }()
	go func() { _ = m2.EventHandler(stop2) }()
	defer close(stop1)
	defer close(stop2)

	_, err = m2.Join([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port1))})
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for {
		found := false
		for _, member := range m1.Serf.Members() {
			if member.Name == "node2" {
				found = true
				break
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected node2 to join membership")
		}
		time.Sleep(50 * time.Millisecond)
	}

	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if listener.hasEvent(MemberEventJoin, "node2") {
			t.Fatal("unexpected join event for node2 on cluster listener")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMembershipMemberStatesMarksMissingAsReaped(t *testing.T) {
	port := freePort(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	membership, err := NewMembership(MembershipConfiguration{
		NodeName:      "node1",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port,
	}, logger)
	require.NoError(t, err)
	defer membership.Serf.Shutdown()

	ghost := serf.Member{
		Name: "ghost",
		Tags: map[string]string{"raft-addr": "127.0.0.1:29000", "namespaces": "test"},
	}
	before := time.Now().Add(-1 * time.Minute)

	membership.statusMu.Lock()
	membership.memberStates["ghost"] = MemberInformation{
		NodeName:   ghost.Name,
		Tags:       ghost.Tags,
		Status:     MemberStatusFailed,
		UpdatedAt:  before,
		LastSeenAt: before,
	}
	membership.statusMu.Unlock()

	states := membership.MemberStates()

	var found MemberInformation
	var ok bool
	for _, state := range states {
		if state.NodeName == "ghost" {
			found = state
			ok = true
			break
		}
	}
	require.True(t, ok)
	require.Equal(t, MemberStatusReaped, found.Status)
	require.True(t, found.UpdatedAt.After(before))
}
func TestMembershipLiveClusterJoinLeaveWithEncryption(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	listener := &testClusterListener{}

	secret := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x0f}, 16))

	m1, err := NewMembership(MembershipConfiguration{
		NodeName:        "node1",
		AdvertiseAddr:   "127.0.0.1",
		AdvertisePort:   port1,
		SecretKeyBase64: secret,
		VerifyIncoming:  true,
		VerifyOutgoing:  true,
	}, logger)
	require.NoError(t, err)
	defer m1.Serf.Shutdown()
	m1.RegisterCluster(listener)

	m2, err := NewMembership(MembershipConfiguration{
		NodeName:        "node2",
		AdvertiseAddr:   "127.0.0.1",
		AdvertisePort:   port2,
		SecretKeyBase64: secret,
		VerifyIncoming:  true,
		VerifyOutgoing:  true,
	}, logger)
	require.NoError(t, err)
	defer m2.Serf.Shutdown()

	stop1 := make(chan struct{})
	stop2 := make(chan struct{})
	go func() { _ = m1.EventHandler(stop1) }()
	go func() { _ = m2.EventHandler(stop2) }()
	defer close(stop1)
	defer close(stop2)

	_, err = m2.Join([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port1))})
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if listener.hasEvent(MemberEventJoin, "node2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected join event for node2")
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, m2.Close())

	deadline = time.Now().Add(5 * time.Second)
	for {
		if listener.hasEvent(MemberEventLeave, "node2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected leave event for node2")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMembershipLiveClusterJoinWithEncryptionMismatch(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	listener := &testClusterListener{}

	secret1 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x0a}, 16))
	secret2 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x0b}, 16))

	m1, err := NewMembership(MembershipConfiguration{
		NodeName:        "node1",
		AdvertiseAddr:   "127.0.0.1",
		AdvertisePort:   port1,
		SecretKeyBase64: secret1,
		VerifyIncoming:  true,
		VerifyOutgoing:  true,
	}, logger)
	require.NoError(t, err)
	defer m1.Serf.Shutdown()
	m1.RegisterCluster(listener)

	m2, err := NewMembership(MembershipConfiguration{
		NodeName:        "node2",
		AdvertiseAddr:   "127.0.0.1",
		AdvertisePort:   port2,
		SecretKeyBase64: secret2,
		VerifyIncoming:  true,
		VerifyOutgoing:  true,
	}, logger)
	require.NoError(t, err)
	defer m2.Serf.Shutdown()

	stop1 := make(chan struct{})
	stop2 := make(chan struct{})
	go func() { _ = m1.EventHandler(stop1) }()
	go func() { _ = m2.EventHandler(stop2) }()
	defer close(stop1)
	defer close(stop2)

	_, err = m2.Join([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port1))})
	require.Error(t, err)

	time.Sleep(500 * time.Millisecond)
	if listener.hasEvent(MemberEventJoin, "node2") {
		t.Fatal("unexpected join event for node2")
	}
}

func TestSerfReconnectTimeoutReapsFailedMember(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	events1 := make(chan serf.Event, 256)
	events2 := make(chan serf.Event, 256)

	reconnectTimeout := 1 * time.Second
	reapInterval := 50 * time.Millisecond
	tombstoneTimeout := 1 * time.Second

	conf1 := newSerfTestConfig(t, "node1", port1, events1, reconnectTimeout, reapInterval, tombstoneTimeout)
	conf2 := newSerfTestConfig(t, "node2", port2, events2, reconnectTimeout, reapInterval, tombstoneTimeout)

	s1, err := serf.Create(conf1)
	require.NoError(t, err)
	defer s1.Shutdown()

	s2, err := serf.Create(conf2)
	require.NoError(t, err)

	_, err = s2.Join([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port1))}, true)
	require.NoError(t, err)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := findMember(s1.Members(), "node2"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected node2 to join")
		}
		time.Sleep(20 * time.Millisecond)
	}

	require.NoError(t, s2.Shutdown())

	deadline = time.Now().Add(reconnectTimeout / 2)
	for {
		member, ok := findMember(s1.Members(), "node2")
		if ok && member.Status == serf.StatusFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected node2 to be failed before reconnect timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}

	deadline = time.Now().Add(3 * time.Second)
	for {
		if _, ok := findMember(s1.Members(), "node2"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected node2 to be reaped")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
