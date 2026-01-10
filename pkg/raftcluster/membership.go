package raftcluster

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/hashicorp/logutils"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

// MemberEvent denotes the kind of event that has happened inside the cluster.
type MemberEvent uint

const (
	// MemberEventJoin denotes the member(node) have Joined the Membership cluster.
	MemberEventJoin MemberEvent = 1
	// MemberEventLeave denotes the member(node) have Left the Membership cluster.
	MemberEventLeave MemberEvent = 2
	// MemberEventLeaderChange denotes there is change in the Leadership inside the cluster.
	MemberEventLeaderChange MemberEvent = 3
)

func (m MemberEvent) String() string {
	switch m {
	case MemberEventJoin:
		return "member-join"
	case MemberEventLeave:
		return "member-leave"
	case MemberEventLeaderChange:
		return "member-leader-change"
	default:
		panic(fmt.Sprintf("unknown event type: %d", m))
	}
}

// LogLevel defines log level for clustering.
type LogLevel int

const (
	// logLevelUnset prevents the default value for go type system becoming the log level.
	logLevelUnset LogLevel = iota
	LogLevelDebug
	LoglevelInfo
	LogLevelWarn
	LogLevelError
)

func (l LogLevel) ToString(level LogLevel) string {
	switch level {
	case LogLevelDebug:
		return "DEBUG"
	case LoglevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "INFO"
	}
}

func defaultConfig() *serf.Config {
	base := serf.DefaultConfig()

	base.QueueDepthWarning = 1000000
	base.MinQueueDepth = 4096
	base.LeavePropagateDelay = 3 * time.Second
	base.CoalescePeriod = 3 * time.Second
	base.QuiescentPeriod = 1 * time.Second

	return base
}

// Membership provides the gossip Membership inside the cluster.
// It help's discover the nodes and it's metadata dynamically and quickly inside the cluster.
type Membership struct {
	*serf.Serf
	eventsCh chan serf.Event
	en       EventNotifier
}

// SendEvent generates the provided named event inside the cluster with the provided payload.
// IMP: Payload size is limited: inMemoryStore gossips via UDP, so the payload must fit within a single UDP packet.
func (m *Membership) sendEvent(name string, payload []byte) error {
	return m.Serf.UserEvent(name, payload, false)
}

// Close first leave the Membership gracefully and then call shutdown.
func (m *Membership) Close() error {
	slog.Info("[raftcluster]", "message", "leaving Membership")
	if err := m.Leave(); err != nil {
		slog.Error("[raftcluster]", "message", "error leaving Membership", "err", err)
	}
	return m.Shutdown()
}

// EventHandler handles events operation for Membership cluster.
func (m *Membership) EventHandler(shutdownCh <-chan struct{}) error {
	for {
		select {
		case <-shutdownCh:
			return nil
		case event := <-m.eventsCh:
			switch event.EventType() {
			case serf.EventMemberJoin:
				for _, member := range event.(serf.MemberEvent).Members {
					if m.isLocal(member) {
						continue
					}
					mi := MemberInformation{
						NodeName: member.Name,
						Tags:     member.Tags,
					}
					m.en.OnChangeEvent(MemberEventJoin, mi)
				}
			case serf.EventMemberLeave, serf.EventMemberFailed, serf.EventMemberReap:
				for _, member := range event.(serf.MemberEvent).Members {
					if m.isLocal(member) {
						continue
					}
					mi := MemberInformation{
						NodeName: member.Name,
						Tags:     member.Tags,
					}
					m.en.OnChangeEvent(MemberEventLeave, mi)
				}
			case serf.EventUser:
				ue := event.(serf.UserEvent)
				m.en.OnEvent(ue.Name, ue.Payload)
			default:
				panic("unhandled default case")

			}
		}
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.Serf.LocalMember().Name == member.Name
}

// MembershipConfiguration groups all the configuration object for current member(node) to initialize the Gossip protocol
// communication.
type MembershipConfiguration struct {
	NodeName        string
	Tags            map[string]string
	AdvertiseAddr   string
	AdvertisePort   int
	MinLogLevel     LogLevel
	SecretKeyBase64 string
	KeyringBase64   []string
	VerifyIncoming  bool
	VerifyOutgoing  bool
}

// NewMembership is used to setup and initialize a gossip Membership
func NewMembership(c MembershipConfiguration, logger *slog.Logger, en EventNotifier) (Membership, error) {
	// serfEventChSize is the size of the buffered channel to get Serf
	// events. If this is exhausted we will block Serf and Memberlist.
	serfEventChSize := 2048
	serfEventsCh := make(chan serf.Event, serfEventChSize)
	conf := defaultConfig()
	conf.Init()
	conf.NodeName = c.NodeName

	conf.Tags = c.Tags
	conf.EventCh = serfEventsCh

	if err := configureSerfEncryption(conf.MemberlistConfig, c); err != nil {
		return Membership{}, err
	}

	// This is the best effort to convert the standard log
	// from the serf and gossip library into slog output.
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel(c.MinLogLevel.ToString(c.MinLogLevel)),
		Writer:   &lwr{logger: logger},
	}

	stdLog := log.New(filter, "", 0)

	conf.MemberlistConfig.Logger = stdLog
	conf.MemberlistConfig.AdvertiseAddr = c.AdvertiseAddr
	conf.MemberlistConfig.AdvertisePort = c.AdvertisePort
	conf.MemberlistConfig.BindPort = c.AdvertisePort
	conf.Logger = stdLog
	s, err := serf.Create(conf)
	return Membership{Serf: s, eventsCh: serfEventsCh, en: en}, err
}

func configureSerfEncryption(conf *memberlist.Config, cfg MembershipConfiguration) error {
	if cfg.SecretKeyBase64 == "" {
		if len(cfg.KeyringBase64) > 0 {
			return errors.New("keyring requires secret key")
		}
		return nil
	}

	primaryKey, err := decodeMemberlistKey(cfg.SecretKeyBase64)
	if err != nil {
		return fmt.Errorf("decode secret key: %w", err)
	}
	conf.SecretKey = primaryKey

	if len(cfg.KeyringBase64) > 0 {
		keyring, err := memberlist.NewKeyring(nil, primaryKey)
		if err != nil {
			return fmt.Errorf("create keyring: %w", err)
		}
		for _, enc := range cfg.KeyringBase64 {
			key, err := decodeMemberlistKey(enc)
			if err != nil {
				return fmt.Errorf("decode keyring key: %w", err)
			}
			if err := keyring.AddKey(key); err != nil {
				return fmt.Errorf("add keyring key: %w", err)
			}
		}
		conf.Keyring = keyring
	}

	conf.GossipVerifyIncoming = cfg.VerifyIncoming
	conf.GossipVerifyOutgoing = cfg.VerifyOutgoing
	return nil
}

func decodeMemberlistKey(enc string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, err
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("invalid key length %d", len(key))
	}
}
