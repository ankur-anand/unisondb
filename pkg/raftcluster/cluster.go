package raftcluster

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

var (
	ErrClusterClosed = errors.New("cluster is closed")
)

type NotLeaderError struct {
	LeaderAddr raft.ServerAddress
	LeaderID   raft.ServerID
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr == "" {
		return "not leader: leader unknown"
	}
	return fmt.Sprintf("not leader: leader is %s (id: %s)", e.LeaderAddr, e.LeaderID)
}

// LeaderAddress returns the current leader address, or empty if unknown.
func (e *NotLeaderError) LeaderAddress() raft.ServerAddress {
	return e.LeaderAddr
}

const (
	// raftRemoveGracePeriod is how long we wait for ourselves to be removed
	// from the Raft cluster before giving up and shutting down anyway.
	raftRemoveGracePeriod = 5 * time.Second
	leaderBarrierTimeout  = 5 * time.Second
)

// Cluster wraps a single Raft instance.
type Cluster struct {
	raft      *raft.Raft
	localID   raft.ServerID
	namespace string // namespace this cluster belongs to

	// configMu serializes Raft configuration changes (AddServer/RemoveServer).
	// This prevents races between event handling and reconciliation since
	// Raft rejects concurrent config changes with ErrConfigurationBusy.
	configMu        sync.Mutex
	leaderBarrierMu sync.Mutex
	leaderReady     atomic.Bool
	leaderStopCh    chan struct{}
	leaderStopOnce  sync.Once

	closed atomic.Bool
}

// NewCluster creates a cluster with the given Raft instance, local server ID, and namespace.
func NewCluster(r *raft.Raft, localID raft.ServerID, namespace string) *Cluster {
	c := &Cluster{
		raft:         r,
		localID:      localID,
		namespace:    namespace,
		leaderStopCh: make(chan struct{}),
	}
	go c.monitorLeadership()
	return c
}

// Namespace returns the namespace this cluster belongs to.
func (c *Cluster) Namespace() string {
	return c.namespace
}

func (c *Cluster) Raft() *raft.Raft {
	return c.raft
}

func (c *Cluster) Apply(data []byte, timeout time.Duration) raft.ApplyFuture {
	return c.raft.Apply(data, timeout)
}

func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// LeaderWithID returns the address and ID of the current leader.
func (c *Cluster) LeaderWithID() (raft.ServerAddress, raft.ServerID) {
	return c.raft.LeaderWithID()
}

func (c *Cluster) State() raft.RaftState {
	return c.raft.State()
}

// LastIndex returns the last log index.
func (c *Cluster) LastIndex() uint64 {
	return c.raft.LastIndex()
}

func (c *Cluster) ApplyBatch(payloads [][]byte, timeout time.Duration) []error {
	if c.closed.Load() {
		errs := make([]error, len(payloads))
		for i := range errs {
			errs[i] = ErrClusterClosed
		}
		return errs
	}

	if !c.IsLeader() {
		leaderAddr, leaderID := c.raft.LeaderWithID()
		notLeaderErr := &NotLeaderError{LeaderAddr: leaderAddr, LeaderID: leaderID}
		errs := make([]error, len(payloads))
		for i := range errs {
			errs[i] = notLeaderErr
		}
		return errs
	}

	// fire all Apply() without waiting
	futures := make([]raft.ApplyFuture, len(payloads))
	for i, payload := range payloads {
		futures[i] = c.raft.Apply(payload, timeout)
	}

	errs := make([]error, len(payloads))
	for i, f := range futures {
		errs[i] = f.Error()
	}
	return errs
}

// Close shuts down the Raft instance gracefully.
func (c *Cluster) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	wait := c.leave()
	return c.Shutdown(wait)
}

// Leave prepares the cluster for shutdown and returns true if the caller should
// wait for removal before shutting down the Raft instance.
func (c *Cluster) Leave() bool {
	if c.closed.Swap(true) {
		return false
	}
	return c.leave()
}

// Shutdown finalizes shutdown, optionally waiting for removal from the Raft config.
func (c *Cluster) Shutdown(waitForRemoval bool) error {
	if waitForRemoval {
		c.waitForRemoval(c.localID)
	}
	c.leaderStopOnce.Do(func() {
		if c.leaderStopCh != nil {
			close(c.leaderStopCh)
		}
	})
	return c.raft.Shutdown().Error()
}

func (c *Cluster) leave() bool {
	localID := c.localID
	isLeader := c.IsLeader()

	slog.Info("[raftcluster]",
		slog.String("message", "leaving cluster"),
		slog.String("local_id", string(localID)),
		slog.Bool("is_leader", isLeader))

	if !isLeader {
		return true
	}

	return c.tryLeaderLeave(localID)
}

func (c *Cluster) tryLeaderLeave(localID raft.ServerID) bool {
	numVoters, err := c.NumVoters()
	if err != nil {
		slog.Warn("[raftcluster]",
			slog.String("message", "failed to get number of voters"),
			slog.Any("error", err))
		return false
	}
	if numVoters <= 1 {
		return false
	}

	if err := c.raft.LeadershipTransfer().Error(); err == nil {
		return true
	}

	slog.Info("[raftcluster]",
		slog.String("message", "leader removing self from cluster"),
		slog.Int("num_voters", numVoters))
	if err := c.RemoveServer(localID); err != nil {
		slog.Warn("[raftcluster]",
			slog.String("message", "failed to remove self from cluster"),
			slog.Any("error", err))
	}
	return false
}

func (c *Cluster) LocalID() raft.ServerID {
	return c.localID
}

// waitForRemoval waits until this server is removed from the Raft configuration
// or until the grace period expires.
func (c *Cluster) waitForRemoval(localID raft.ServerID) {
	if localID == "" {
		return
	}

	left := false
	limit := time.Now().Add(raftRemoveGracePeriod)

	for !left && time.Now().Before(limit) {
		time.Sleep(50 * time.Millisecond)

		future := c.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			slog.Error("[raftcluster]",
				slog.String("message", "failed to get raft configuration while waiting for removal"),
				slog.Any("error", err))
			break
		}

		// Check if we are no longer in the configuration
		left = true
		for _, server := range future.Configuration().Servers {
			if server.ID == localID {
				left = false
				break
			}
		}
	}

	if left {
		slog.Info("[raftcluster]",
			slog.String("message", "successfully removed from raft configuration"))
	} else {
		slog.Warn("[raftcluster]",
			slog.String("message", "failed to leave raft configuration gracefully, timeout"))
	}
}

// NumVoters helper functions returns the number of voting peers in the current raft configurations.
func (c *Cluster) NumVoters() (int, error) {
	config := c.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		return 0, err
	}
	cfg := config.Configuration()
	var numVoters int
	for _, server := range cfg.Servers {
		if server.Suffrage == raft.Voter {
			numVoters++
		}
	}
	return numVoters, nil
}

func (c *Cluster) RaftConfiguration() (*raft.Configuration, error) {
	config := c.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		return nil, err
	}
	cfg := config.Configuration()
	return &cfg, nil
}

// RemoveServer removes a server from Raft if it exists in the latest configuration.
// This must be run on the leader or it will fail.
func (c *Cluster) RemoveServer(id raft.ServerID) error {
	c.configMu.Lock()
	defer c.configMu.Unlock()

	cfg, err := c.RaftConfiguration()
	if err != nil {
		return err
	}
	for _, server := range cfg.Servers {
		if server.ID != id {
			continue
		}
		slog.Info("[raftcluster]",
			slog.String("message", "removing raft server"),
			slog.String("server_id", string(id)),
			slog.String("address", string(server.Address)),
			slog.String("state", c.raft.State().String()),
		)
		future := c.raft.RemoveServer(server.ID, 0, 0)
		if err := future.Error(); err != nil {
			slog.Error("[raftcluster]",
				slog.String("message", "failed to remove raft server"),
				slog.String("server_id", string(id)),
				slog.String("address", string(server.Address)),
				slog.String("state", c.raft.State().String()),
				slog.Any("error", err))
			return err
		}
		slog.Info("[raftcluster]",
			slog.String("message", "removed raft server"),
			slog.String("server_id", string(id)),
			slog.String("address", string(server.Address)),
			slog.String("state", c.raft.State().String()),
		)
		return nil
	}
	return nil
}

// AddServer will add the given server to the cluster as a staging server.
func (c *Cluster) AddServer(id raft.ServerID, addr raft.ServerAddress) error {
	c.configMu.Lock()
	defer c.configMu.Unlock()

	cfg, err := c.RaftConfiguration()
	if err != nil {
		return err
	}

	for _, server := range cfg.Servers {
		if server.ID == id {
			// if address is the same - no update needed
			if server.Address == addr {
				slog.Debug("[raftcluster]",
					slog.String("message", "server already in cluster with same address"),
					slog.String("server_id", string(id)),
					slog.String("address", string(addr)))
				return nil
			}
			// Address changed: need to update it
			// AddVoter with existing ID updates the address in raft
			slog.Info("[raftcluster]",
				slog.String("message", "updating server address"),
				slog.String("server_id", string(id)),
				slog.String("old_address", string(server.Address)),
				slog.String("new_address", string(addr)))
			addFut := c.raft.AddVoter(id, addr, 0, 0)
			if err := addFut.Error(); err != nil {
				slog.Error("[raftcluster]",
					slog.String("message", "failed to update server address"),
					slog.String("server_id", string(id)),
					slog.String("address", string(addr)),
					slog.String("state", c.raft.State().String()),
					slog.Any("error", err))
				return err
			}
			slog.Info("[raftcluster]",
				slog.String("message", "updated raft server address"),
				slog.String("server_id", string(id)),
				slog.String("address", string(addr)))
			return nil
		}
	}

	slog.Info("[raftcluster]",
		slog.String("message", "adding raft server"),
		slog.String("server_id", string(id)),
		slog.String("address", string(addr)))
	addFut := c.raft.AddVoter(id, addr, 0, 0)
	if err := addFut.Error(); err != nil {
		slog.Error("[raftcluster]",
			slog.String("message", "failed to add raft server"),
			slog.String("server_id", string(id)),
			slog.String("address", string(addr)),
			slog.String("state", c.raft.State().String()),
			slog.Any("error", err))
		return err
	}
	slog.Info("[raftcluster]",
		slog.String("message", "added raft server"),
		slog.String("server_id", string(id)),
		slog.String("address", string(addr)))
	return nil
}

// Stats holds cluster statistics.
type Stats struct {
	IsLeader   bool
	State      raft.RaftState
	LastIdx    uint64
	LeaderAddr raft.ServerAddress
	LeaderID   raft.ServerID
}

// Stats returns cluster statistics.
func (c *Cluster) Stats() Stats {
	state := c.raft.State()
	leaderAddr, leaderID := c.raft.LeaderWithID()
	return Stats{
		IsLeader:   state == raft.Leader,
		State:      state,
		LastIdx:    c.raft.LastIndex(),
		LeaderAddr: leaderAddr,
		LeaderID:   leaderID,
	}
}

func (c *Cluster) ensureLeaderReady() bool {
	if c.leaderReady.Load() {
		return true
	}

	c.leaderBarrierMu.Lock()
	defer c.leaderBarrierMu.Unlock()

	if c.leaderReady.Load() {
		return true
	}

	future := c.raft.Barrier(leaderBarrierTimeout)
	if err := future.Error(); err != nil {
		slog.Error("[raftcluster]",
			slog.String("message", "reconcile: raft barrier failed"),
			slog.String("namespace", c.namespace),
			slog.Any("error", err))
		return false
	}

	c.leaderReady.Store(true)
	return true
}

func (c *Cluster) monitorLeadership() {
	if c.raft == nil {
		return
	}

	leaderCh := c.raft.LeaderCh()
	for {
		select {
		case <-leaderCh:
			c.leaderReady.Store(false)
		case <-c.leaderStopCh:
			return
		}
	}
}

// OnChangeEvent handles membership changes from Serf and updates Raft configuration.
// IMPORTANT: Serf NodeName must equal Raft ServerID for consistency.
// The "raft-addr" tag must contain the Raft transport address.
// The "namespaces" tag must contain a comma-separated list of namespaces the node hosts.
// This function exit here to satisfy the interface, and in test cases this is actively used.
func (c *Cluster) OnChangeEvent(event MemberEvent, info MemberInformation) {
}

// ReconcileMembers compares Serf membership with Raft config and reconciles servers.
// This handles cases where join events were missed during leadership changes.
// Only the leader should call this.
func (c *Cluster) ReconcileMembers(members []MemberInformation) {
	if c.closed.Load() {
		return
	}
	if !c.IsLeader() {
		c.leaderReady.Store(false)
		return
	}
	if !c.ensureLeaderReady() {
		return
	}

	cfg, err := c.RaftConfiguration()
	if err != nil {
		slog.Error("[raftcluster]",
			slog.String("message", "reconcile: failed to get config"),
			slog.String("namespace", c.namespace),
			slog.Any("error", err))
		return
	}

	currentServers := make(map[raft.ServerID]raft.ServerAddress)
	for _, s := range cfg.Servers {
		currentServers[s.ID] = s.Address
	}

	memberByID := c.collectMembers(members)
	c.reconcileAddsAndUpdates(currentServers, memberByID)
	c.reconcileRemovals(currentServers, memberByID)
}

func (c *Cluster) collectMembers(members []MemberInformation) map[raft.ServerID]MemberInformation {
	memberByID := make(map[raft.ServerID]MemberInformation)
	for _, m := range members {
		if _, ok := m.Tags["raft-addr"]; !ok {
			continue
		}

		namespacesTag, ok := m.Tags["namespaces"]
		if !ok {
			continue
		}
		namespaces := SplitAndTrim(namespacesTag)
		if !slices.Contains(namespaces, c.namespace) {
			continue
		}

		nodeID := raft.ServerID(m.NodeName)
		memberByID[nodeID] = m
	}
	return memberByID
}

func (c *Cluster) reconcileAddsAndUpdates(currentServers map[raft.ServerID]raft.ServerAddress, memberByID map[raft.ServerID]MemberInformation) {
	for nodeID, m := range memberByID {
		if m.Status != MemberStatusAlive {
			continue
		}
		raftAddr := m.Tags["raft-addr"]
		expectedAddr := raft.ServerAddress(raftAddr)

		currentAddr, exists := currentServers[nodeID]
		if !exists {
			slog.Info("[raftcluster]",
				slog.String("message", "reconcile: adding missing server"),
				slog.String("server_id", string(nodeID)),
				slog.String("address", raftAddr),
				slog.String("namespace", c.namespace))
			if err := c.AddServer(nodeID, expectedAddr); err != nil {
				slog.Error("[raftcluster]",
					slog.String("message", "reconcile: failed to add server"),
					slog.String("server_id", string(nodeID)),
					slog.String("namespace", c.namespace),
					slog.Any("error", err))
			}
			continue
		}

		if currentAddr != expectedAddr {
			slog.Info("[raftcluster]",
				slog.String("message", "reconcile: updating stale address"),
				slog.String("server_id", string(nodeID)),
				slog.String("old_address", string(currentAddr)),
				slog.String("new_address", raftAddr),
				slog.String("namespace", c.namespace))
			if err := c.AddServer(nodeID, expectedAddr); err != nil {
				slog.Error("[raftcluster]",
					slog.String("message", "reconcile: failed to update server address"),
					slog.String("server_id", string(nodeID)),
					slog.String("namespace", c.namespace),
					slog.Any("error", err))
			}
		}
	}
}

func (c *Cluster) reconcileRemovals(currentServers map[raft.ServerID]raft.ServerAddress, memberByID map[raft.ServerID]MemberInformation) {
	for id := range currentServers {
		if id == c.localID {
			continue
		}
		m, ok := memberByID[id]
		if !ok {
			continue
		}
		switch m.Status {
		case MemberStatusLeft:
			if err := c.RemoveServer(id); err != nil {
				slog.Error("[raftcluster]",
					slog.String("message", "reconcile: failed to remove server"),
					slog.String("server_id", string(id)),
					slog.String("namespace", c.namespace),
					slog.Any("error", err))
			}
		case MemberStatusReaped:
			if err := c.RemoveServer(id); err != nil {
				slog.Error("[raftcluster]",
					slog.String("message", "reconcile: failed to remove reaped server"),
					slog.String("server_id", string(id)),
					slog.String("namespace", c.namespace),
					slog.Any("error", err))
			}
		case MemberStatusAlive, MemberStatusFailed, MemberStatusUnknown:
		default:
		}
	}
}

// SplitAndTrim splits a comma-separated string, trims whitespace, and filters empty strings.
func SplitAndTrim(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

var _ ClusterDiscover = (*Cluster)(nil)
