package cliapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/pkg/raftcluster"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPService_Name(t *testing.T) {
	svc := &HTTPService{}
	assert.Equal(t, "http", svc.Name())
}

func TestHTTPService_Setup(t *testing.T) {
	deps := &Dependencies{
		Config: config.Config{
			ListenIP: "127.0.0.1",
			HTTPPort: 0,
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &HTTPService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	require.NotNil(t, svc.server)
	require.NotNil(t, svc.httpAPISvc)
	assert.Equal(t, "127.0.0.1:0", svc.Addr())
}

func TestHTTPService_Setup_DefaultIP(t *testing.T) {
	deps := &Dependencies{
		Config: config.Config{
			ListenIP: "",
			HTTPPort: 8080,
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &HTTPService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.Equal(t, "0.0.0.0:8080", svc.Addr())
}

func TestHTTPService_RunAndClose(t *testing.T) {
	deps := &Dependencies{
		Config: config.Config{
			ListenIP: "127.0.0.1",
			HTTPPort: 0,
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &HTTPService{}
	err := svc.Setup(context.Background(), deps)
	require.NoError(t, err)

	svc.addr = "127.0.0.1:0"

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)

	go func() {
		runDone <- svc.Run(ctx)
	}()

	select {
	case <-svc.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not become ready in time")
	}

	cancel()

	closeErr := svc.Close(context.Background())
	require.NoError(t, closeErr)

	select {
	case err := <-runDone:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestHTTPService_MetricsEndpoint(t *testing.T) {
	deps := &Dependencies{
		Config: config.Config{
			ListenIP: "127.0.0.1",
			HTTPPort: 0,
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &HTTPService{}
	err := svc.Setup(context.Background(), deps)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- svc.Run(ctx)
	}()

	select {
	case <-svc.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not become ready in time")
	}

	actualAddr := svc.BoundAddr()
	require.NotEmpty(t, actualAddr)

	resp, err := http.Get("http://" + actualAddr + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get("http://" + actualAddr + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	require.NoError(t, svc.Close(context.Background()))
}

func TestHTTPService_Close_NilServer(t *testing.T) {
	svc := &HTTPService{}
	err := svc.Close(context.Background())
	assert.NoError(t, err)
}

func TestHTTPService_Readyz_RaftDisabled(t *testing.T) {
	svc := &HTTPService{}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	svc.handleReadyz(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp raftReadyStatus
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "ready", resp.Status)
}

func TestHTTPService_Readyz_RequiresNamespace(t *testing.T) {
	svc := &HTTPService{
		raftService: &RaftService{
			enabled:  true,
			clusters: map[string]*namespaceRaft{"ns1": {}, "ns2": {}},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	svc.handleReadyz(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	var resp map[string]string
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Contains(t, resp["error"], "namespace")
}

func TestHTTPService_Readyz_NotLeader(t *testing.T) {
	cluster, cleanup := newTestCluster(t, "test", false)
	defer cleanup()

	svc := &HTTPService{
		raftService: &RaftService{
			enabled:  true,
			clusters: map[string]*namespaceRaft{"test": {cluster: cluster}},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz?namespace=test", nil)
	rr := httptest.NewRecorder()

	svc.handleReadyz(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	var resp raftReadyStatus
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "not_leader", resp.Status)
	assert.Equal(t, "test", resp.Namespace)
}

func TestHTTPService_Readyz_Leader(t *testing.T) {
	cluster, cleanup := newTestCluster(t, "test", true)
	defer cleanup()

	svc := &HTTPService{
		raftService: &RaftService{
			enabled:  true,
			clusters: map[string]*namespaceRaft{"test": {cluster: cluster}},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz?namespace=test", nil)
	rr := httptest.NewRecorder()

	svc.handleReadyz(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp raftReadyStatus
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "ready", resp.Status)
	assert.Equal(t, "test", resp.Namespace)
	assert.NotEmpty(t, resp.LeaderID)
}

func TestHTTPService_StatusLeader(t *testing.T) {
	cluster, cleanup := newTestCluster(t, "test", true)
	defer cleanup()

	svc := &HTTPService{
		raftService: &RaftService{
			enabled:  true,
			clusters: map[string]*namespaceRaft{"test": {cluster: cluster}},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/status/leader?namespace=test", nil)
	rr := httptest.NewRecorder()

	svc.handleRaftLeader(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp raftLeaderStatus
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "test", resp.Namespace)
	assert.NotEmpty(t, resp.LeaderID)
}

func TestHTTPService_StatusPeers(t *testing.T) {
	cluster, cleanup := newTestCluster(t, "test", true)
	defer cleanup()

	leaderAddr, _ := cluster.LeaderWithID()
	svc := &HTTPService{
		raftService: &RaftService{
			enabled:  true,
			clusters: map[string]*namespaceRaft{"test": {cluster: cluster}},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/status/peers?namespace=test", nil)
	rr := httptest.NewRecorder()

	svc.handleRaftPeers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp raftPeersStatus
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "test", resp.Namespace)
	assert.Contains(t, resp.Peers, string(leaderAddr))
}

func newTestCluster(t *testing.T, namespace string, bootstrap bool) (*raftcluster.Cluster, func()) {
	t.Helper()

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID("node-" + namespace)
	cfg.Logger = hclog.NewNullLogger()

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewInmemSnapshotStore()
	addr, transport := raft.NewInmemTransport(raft.ServerAddress(cfg.LocalID))

	r, err := raft.NewRaft(cfg, &raft.MockFSM{}, logStore, stableStore, snapStore, transport)
	require.NoError(t, err)

	if bootstrap {
		future := r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{ID: cfg.LocalID, Address: addr},
			},
		})
		require.NoError(t, future.Error())

		deadline := time.Now().Add(2 * time.Second)
		for r.State() != raft.Leader {
			if time.Now().After(deadline) {
				t.Fatal("raft did not become leader in time")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	cluster := raftcluster.NewCluster(r, cfg.LocalID, namespace)
	cleanup := func() {
		_ = cluster.Shutdown(false)
		_ = transport.Close()
	}
	return cluster, cleanup
}
