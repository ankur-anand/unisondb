package cliapp

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaftService_Name(t *testing.T) {
	svc := &RaftService{}
	assert.Equal(t, "raft", svc.Name())
}

func TestRaftService_SetupDisabled(t *testing.T) {
	svc := &RaftService{}
	deps := &Dependencies{
		Config: config.Config{
			RaftConfig: config.RaftConfig{
				Enabled: false,
			},
		},
	}

	err := svc.Setup(context.Background(), deps)
	require.NoError(t, err)
	assert.False(t, svc.enabled)
}

func TestRaftService_SetupMissingNodeID(t *testing.T) {
	svc := &RaftService{}
	deps := &Dependencies{
		Config: config.Config{
			RaftConfig: config.RaftConfig{
				Enabled:  true,
				NodeID:   "",
				BindAddr: "127.0.0.1:5000",
			},
		},
	}

	err := svc.Setup(context.Background(), deps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node_id is required")
}

func TestRaftService_SetupMissingBindAddr(t *testing.T) {
	svc := &RaftService{}
	deps := &Dependencies{
		Config: config.Config{
			RaftConfig: config.RaftConfig{
				Enabled:  true,
				NodeID:   "node1",
				BindAddr: "",
			},
		},
	}

	err := svc.Setup(context.Background(), deps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bind_addr is required")
}

func TestRaftService_GetClusterUnknown(t *testing.T) {
	svc := &RaftService{
		clusters: make(map[string]*namespaceRaft),
	}

	cluster := svc.GetCluster("unknown")
	assert.Nil(t, cluster)
}

func TestRaftService_IsLeaderUnknown(t *testing.T) {
	svc := &RaftService{
		clusters: make(map[string]*namespaceRaft),
	}

	isLeader := svc.IsLeader("unknown")
	assert.False(t, isLeader)
}

func TestRaftService_EnabledWhenNotSetup(t *testing.T) {
	svc := &RaftService{}
	assert.False(t, svc.enabled)
}

func TestRaftService_RunDisabled(t *testing.T) {
	svc := &RaftService{enabled: false}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := svc.Run(ctx)
	assert.NoError(t, err)
}

func TestRaftService_CloseDisabled(t *testing.T) {
	svc := &RaftService{enabled: false}

	err := svc.Close(context.Background())
	assert.NoError(t, err)
}

func TestRaftService_BuildRaftConfigDefaults(t *testing.T) {
	svc := &RaftService{}
	cfg := config.RaftConfig{
		NodeID: "test-node",
	}

	rc := svc.buildRaftConfig(cfg)

	assert.Equal(t, raft.ServerID("test-node"), rc.LocalID)
	assert.True(t, rc.BatchApplyCh)
	assert.Equal(t, 256, rc.MaxAppendEntries)
	assert.Equal(t, uint64(20480), rc.TrailingLogs)
}

func TestRaftService_BuildRaftConfigWithTimeouts(t *testing.T) {
	svc := &RaftService{}
	cfg := config.RaftConfig{
		NodeID:           "test-node",
		HeartbeatTimeout: "500ms",
		ElectionTimeout:  "2s",
		CommitTimeout:    "100ms",
	}

	rc := svc.buildRaftConfig(cfg)

	assert.Equal(t, 500*time.Millisecond, rc.HeartbeatTimeout)
	assert.Equal(t, 2*time.Second, rc.ElectionTimeout)
	assert.Equal(t, 100*time.Millisecond, rc.CommitTimeout)
}

func TestRaftService_BuildRaftConfigWithSnapshot(t *testing.T) {
	svc := &RaftService{}
	cfg := config.RaftConfig{
		NodeID:            "test-node",
		SnapshotThreshold: 5000,
		SnapshotInterval:  "1m",
	}

	rc := svc.buildRaftConfig(cfg)

	assert.Equal(t, uint64(5000), rc.SnapshotThreshold)
	assert.Equal(t, 1*time.Minute, rc.SnapshotInterval)
}

func TestRaftService_BuildRaftConfigInvalidTimeout(t *testing.T) {
	svc := &RaftService{}
	cfg := config.RaftConfig{
		NodeID:           "test-node",
		HeartbeatTimeout: "invalid",
	}

	rc := svc.buildRaftConfig(cfg)
	assert.Equal(t, raft.DefaultConfig().HeartbeatTimeout, rc.HeartbeatTimeout)
}

func TestRaftService_GetApplyTimeoutDefault(t *testing.T) {
	svc := &RaftService{}
	cfg := config.RaftConfig{}

	timeout := svc.getApplyTimeout(cfg)
	assert.Equal(t, 10*time.Second, timeout)
}

func TestRaftService_GetApplyTimeoutCustom(t *testing.T) {
	svc := &RaftService{}
	cfg := config.RaftConfig{
		ApplyTimeout: "5s",
	}

	timeout := svc.getApplyTimeout(cfg)
	assert.Equal(t, 5*time.Second, timeout)
}

func TestRaftService_GetApplyTimeoutInvalid(t *testing.T) {
	svc := &RaftService{}
	cfg := config.RaftConfig{
		ApplyTimeout: "not-a-duration",
	}

	timeout := svc.getApplyTimeout(cfg)
	assert.Equal(t, 10*time.Second, timeout)
}

func TestRaftService_CloseAllEmpty(t *testing.T) {
	svc := &RaftService{
		enabled:  true,
		clusters: make(map[string]*namespaceRaft),
	}

	svc.closeAll()
}
