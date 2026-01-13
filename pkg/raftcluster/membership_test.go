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
		{MemberEventLeaderChange, "member-leader-change"},
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

func TestMembershipLiveClusterJoinLeave(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	listener := &testClusterListener{}

	m1, err := NewMembership(MembershipConfiguration{
		NodeName:      "node1",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port1,
		MinLogLevel:   LoglevelInfo,
	}, logger)
	require.NoError(t, err)
	defer m1.Serf.Shutdown()
	m1.RegisterCluster(listener)

	m2, err := NewMembership(MembershipConfiguration{
		NodeName:      "node2",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port2,
		MinLogLevel:   LoglevelInfo,
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
		MinLogLevel:   LoglevelInfo,
		ClusterFilter: blockAll,
	}, logger)
	require.NoError(t, err)
	defer m1.Serf.Shutdown()

	m1.RegisterCluster(listener)

	m2, err := NewMembership(MembershipConfiguration{
		NodeName:      "node2",
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port2,
		MinLogLevel:   LoglevelInfo,
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
		MinLogLevel:     LoglevelInfo,
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
		MinLogLevel:     LoglevelInfo,
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
		MinLogLevel:     LoglevelInfo,
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
		MinLogLevel:     LoglevelInfo,
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
