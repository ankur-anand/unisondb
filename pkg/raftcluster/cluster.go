package raftcluster

import (
	"errors"
	"log/slog"
	"time"

	"github.com/hashicorp/raft"
)

var (
	ErrNoLeader      = errors.New("no leader available")
	ErrClusterClosed = errors.New("cluster is closed")
)

// Cluster wraps a single Raft instance.
type Cluster struct {
	raft   *raft.Raft
	closed bool
}

// NewCluster creates a cluster with the given Raft instance.
func NewCluster(r *raft.Raft) *Cluster {
	return &Cluster{raft: r}
}

// Raft returns the underlying Raft instance.
func (c *Cluster) Raft() *raft.Raft {
	return c.raft
}

// Apply submits a command to the Raft group.
func (c *Cluster) Apply(data []byte, timeout time.Duration) raft.ApplyFuture {
	return c.raft.Apply(data, timeout)
}

// IsLeader returns true if this node is the leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// Leader returns the address of the current leader.
func (c *Cluster) Leader() raft.ServerAddress {
	return c.raft.Leader()
}

// State returns the current Raft state.
func (c *Cluster) State() raft.RaftState {
	return c.raft.State()
}

// LastIndex returns the last log index.
func (c *Cluster) LastIndex() uint64 {
	return c.raft.LastIndex()
}

// ApplyBatch applies multiple operations with pipelining for efficiency.
func (c *Cluster) ApplyBatch(payloads [][]byte, timeout time.Duration) []error {
	if c.closed {
		errs := make([]error, len(payloads))
		for i := range errs {
			errs[i] = ErrClusterClosed
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

// Close shuts down the Raft instance.
func (c *Cluster) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true

	slog.Info("raftcluster: leaving cluster")
	// number of known peers
	if c.IsLeader() {

	}

	return c.raft.Shutdown().Error()
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
func (c *Cluster) RemoveServer(id raft.ServerID) error {
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

// Stats holds cluster statistics.
type Stats struct {
	IsLeader bool
	State    raft.RaftState
	LastIdx  uint64
	Leader   raft.ServerAddress
}

// Stats returns cluster statistics.
func (c *Cluster) Stats() Stats {
	return Stats{
		IsLeader: c.raft.State() == raft.Leader,
		State:    c.raft.State(),
		LastIdx:  c.raft.LastIndex(),
		Leader:   c.raft.Leader(),
	}
}
