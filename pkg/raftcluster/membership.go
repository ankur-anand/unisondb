package raftcluster

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

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
	// MemberEventFailed denotes the member(node) have Failed in the Membership cluster.
	MemberEventFailed MemberEvent = 3
	// MemberEventReap denotes the member(node) have been Reaped in the Membership cluster.
	MemberEventReap MemberEvent = 4
	// MemberEventLeaderChange denotes there is change in the Leadership inside the cluster.
	MemberEventLeaderChange MemberEvent = 5
	// MemberEventUpdate denotes the member(node) metadata was updated in the Membership cluster.
	MemberEventUpdate MemberEvent = 6
)

func (m MemberEvent) String() string {
	switch m {
	case MemberEventJoin:
		return "member-join"
	case MemberEventLeave:
		return "member-leave"
	case MemberEventFailed:
		return "member-failed"
	case MemberEventReap:
		return "member-reap"
	case MemberEventLeaderChange:
		return "member-leader-change"
	case MemberEventUpdate:
		return "member-update"
	default:
		return "unknown"
	}
}

// ClusterEventFilter decides whether to notify cluster listeners about a membership event.
// Returning false skips ClusterDiscover notifications for the event.
type ClusterEventFilter func(event MemberEvent, info MemberInformation) bool

const (
	reconcileInterval     = 10 * time.Second
	reconcileJitter       = 2 * time.Second
	reconcileMemberChSize = 256
)

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
type Membership struct {
	*serf.Serf
	eventsCh chan serf.Event

	clustersMu sync.RWMutex
	clusters   []ClusterDiscover

	clusterFilter ClusterEventFilter

	statusMu     sync.RWMutex
	memberStates map[string]MemberInformation

	reconcileOnce  sync.Once
	memberUpdateCh chan MemberInformation
}

// Join attempts to join an existing membership cluster using the provided addresses.
func (m *Membership) Join(addrs []string) (int, error) {
	if len(addrs) == 0 {
		return 0, nil
	}

	slog.Info("[raftcluster]",
		slog.String("message", "joining membership"), slog.Any("addresses", addrs))
	n, err := m.Serf.Join(addrs, true)
	if err != nil {
		slog.Error("[raftcluster]",
			slog.String("message", "error joining membership"), slog.Any("err", err))
	}
	return n, err
}

// Close first leave the Membership gracefully and then call shutdown.
func (m *Membership) Close() error {
	slog.Info("[raftcluster]", "message", "leaving Membership")
	if err := m.Leave(); err != nil {
		slog.Error("[raftcluster]", "message", "error leaving Membership", "err", err)
	}
	return m.Shutdown()
}

// RegisterCluster adds a ClusterDiscover listener that will receive membership events.
// This allows multiple Raft clusters (one per namespace) to react to Serf membership changes.
func (m *Membership) RegisterCluster(c ClusterDiscover) {
	m.clustersMu.Lock()
	defer m.clustersMu.Unlock()
	m.clusters = append(m.clusters, c)
}

func (m *Membership) MemberStates() []MemberInformation {
	members := m.Members()
	now := time.Now()
	present := make(map[string]struct{}, len(members))

	m.statusMu.Lock()
	if m.memberStates == nil {
		m.memberStates = make(map[string]MemberInformation)
	}
	for _, member := range members {
		present[member.Name] = struct{}{}
		m.recordMemberLocked(member, now)
	}
	for name, state := range m.memberStates {
		if _, ok := present[name]; ok {
			continue
		}
		if state.Status == MemberStatusLeft || state.Status == MemberStatusReaped {
			continue
		}
		state.Status = MemberStatusReaped
		state.UpdatedAt = now
		m.memberStates[name] = state
	}
	out := make([]MemberInformation, 0, len(m.memberStates))
	for _, state := range m.memberStates {
		out = append(out, state.Clone())
	}
	m.statusMu.Unlock()
	return out
}

// notifyClusters sends membership events to all registered cluster listeners.
func (m *Membership) notifyClusters(event MemberEvent, info MemberInformation) {
	m.clustersMu.RLock()
	defer m.clustersMu.RUnlock()
	for _, c := range m.clusters {
		c.OnChangeEvent(event, info)
	}
}

// EventHandler handles events operation for Membership cluster.
func (m *Membership) EventHandler(shutdownCh <-chan struct{}) error {
	m.startReconcileLoop(shutdownCh)
	for {
		select {
		case <-shutdownCh:
			return nil
		case event, ok := <-m.eventsCh:
			if !ok {
				// Channel closed during shutdown
				return nil
			}
			m.handleEvent(event)
		}
	}
}

func (m *Membership) handleEvent(event serf.Event) {
	switch event.EventType() {
	case serf.EventMemberJoin:
		m.handleMemberEvent(event.(serf.MemberEvent), MemberEventJoin, nil)
	case serf.EventMemberLeave:
		m.handleMemberEvent(event.(serf.MemberEvent), MemberEventLeave, nil)
	case serf.EventMemberFailed:
		m.handleMemberEvent(event.(serf.MemberEvent), MemberEventFailed, nil)
	case serf.EventMemberReap:
		override := MemberStatusReaped
		m.handleMemberEvent(event.(serf.MemberEvent), MemberEventReap, &override)
	case serf.EventMemberUpdate:
		m.handleMemberEvent(event.(serf.MemberEvent), MemberEventUpdate, nil)
	case serf.EventUser:
		ue := event.(serf.UserEvent)
		slog.Debug("[raftcluster]",
			slog.String("message", "ignored serf user event"),
			slog.String("name", ue.Name),
		)
	default:
		slog.Warn("[raftcluster]", slog.String("message", "unhandled serf event type"),
			slog.String("type", event.EventType().String()))
	}
}

func (m *Membership) handleMemberEvent(me serf.MemberEvent, eventType MemberEvent, override *MemberStatus) {
	for _, member := range me.Members {
		if m.isLocal(member) {
			continue
		}
		var mi MemberInformation
		if override != nil {
			mi = m.recordMemberWithStatus(member, *override)
		} else {
			mi = m.recordMember(member)
		}
		if m.clusterFilter == nil || m.clusterFilter(eventType, mi) {
			m.notifyClusters(eventType, mi)
		}
		m.enqueueMemberUpdate(mi)
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
	SecretKeyBase64 string
	KeyringBase64   []string
	VerifyIncoming  bool
	VerifyOutgoing  bool
	ClusterFilter   ClusterEventFilter
}

// NewMembership is used to setup and initialize a gossip Membership.
func NewMembership(c MembershipConfiguration, logger *slog.Logger) (Membership, error) {
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

	clusterFilter := c.ClusterFilter
	if clusterFilter == nil {
		clusterFilter = func(_ MemberEvent, _ MemberInformation) bool { return true }
	}

	conf.MemberlistConfig.LogOutput = NewHashicorpLogWriter(logger, "memberlist")
	conf.MemberlistConfig.AdvertiseAddr = c.AdvertiseAddr
	conf.MemberlistConfig.AdvertisePort = c.AdvertisePort
	conf.MemberlistConfig.BindPort = c.AdvertisePort
	conf.LogOutput = NewHashicorpLogWriter(logger, "serf")
	s, err := serf.Create(conf)

	return Membership{
		Serf:           s,
		eventsCh:       serfEventsCh,
		clusterFilter:  clusterFilter,
		memberStates:   make(map[string]MemberInformation),
		memberUpdateCh: make(chan MemberInformation, reconcileMemberChSize),
	}, err
}

func (m *Membership) recordMember(member serf.Member) MemberInformation {
	now := time.Now()
	m.statusMu.Lock()
	if m.memberStates == nil {
		m.memberStates = make(map[string]MemberInformation)
	}
	state := m.recordMemberLocked(member, now)
	m.statusMu.Unlock()
	return state
}

func (m *Membership) recordMemberLocked(member serf.Member, now time.Time) MemberInformation {
	status := serfStatusToMemberStatus(member.Status)
	return m.recordMemberWithStatusLocked(member, status, now)
}

func (m *Membership) recordMemberWithStatus(member serf.Member, status MemberStatus) MemberInformation {
	now := time.Now()
	m.statusMu.Lock()
	if m.memberStates == nil {
		m.memberStates = make(map[string]MemberInformation)
	}
	state := m.recordMemberWithStatusLocked(member, status, now)
	m.statusMu.Unlock()
	return state
}

func (m *Membership) recordMemberWithStatusLocked(member serf.Member, status MemberStatus, now time.Time) MemberInformation {
	state, ok := m.memberStates[member.Name]
	if !ok || state.Status != status {
		state.UpdatedAt = now
	}
	state.NodeName = member.Name
	state.Tags = cloneTags(member.Tags)
	state.Status = status
	if status == MemberStatusAlive {
		state.LastSeenAt = now
	}
	m.memberStates[member.Name] = state
	return state
}

func (m *Membership) startReconcileLoop(shutdownCh <-chan struct{}) {
	m.reconcileOnce.Do(func() {
		if m.memberUpdateCh == nil {
			m.memberUpdateCh = make(chan MemberInformation, reconcileMemberChSize)
		}
		go func() {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			timer := time.NewTimer(nextReconcileInterval(rng))
			defer timer.Stop()
			for {
				select {
				case <-shutdownCh:
					return
				case <-m.memberUpdateCh:
					m.drainMemberUpdates()
					m.reconcileClusters()
					resetTimer(timer, nextReconcileInterval(rng))
				case <-timer.C:
					m.reconcileClusters()
					resetTimer(timer, nextReconcileInterval(rng))
				}
			}
		}()
	})
}

func (m *Membership) enqueueMemberUpdate(mi MemberInformation) {
	if m.memberUpdateCh == nil {
		return
	}
	select {
	case m.memberUpdateCh <- mi:
	default:
	}
}

func (m *Membership) drainMemberUpdates() {
	if m.memberUpdateCh == nil {
		return
	}
	for {
		select {
		case <-m.memberUpdateCh:
		default:
			return
		}
	}
}

func (m *Membership) reconcileClusters() {
	members := m.MemberStates()
	m.clustersMu.RLock()
	clusters := make([]ClusterDiscover, len(m.clusters))
	copy(clusters, m.clusters)
	m.clustersMu.RUnlock()

	for _, c := range clusters {
		c.ReconcileMembers(members)
	}
}

func nextReconcileInterval(rng *rand.Rand) time.Duration {
	return reconcileInterval + time.Duration(rng.Int63n(int64(reconcileJitter)))
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func serfStatusToMemberStatus(status serf.MemberStatus) MemberStatus {
	switch status {
	case serf.StatusAlive:
		return MemberStatusAlive
	case serf.StatusFailed:
		return MemberStatusFailed
	case serf.StatusLeft:
		return MemberStatusLeft
	default:
		return MemberStatusUnknown
	}
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
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
