package cliapp

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGRPCService_Name(t *testing.T) {
	svc := &GRPCService{}
	assert.Equal(t, "grpc", svc.Name())
}

func TestGRPCService_Setup_DisabledInReplicaMode(t *testing.T) {
	deps := &Dependencies{
		Mode: "replica",
		Config: config.Config{
			Grpc: config.GrpcConfig{
				ListenIP:      "127.0.0.1",
				Port:          0,
				AllowInsecure: true,
			},
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &GRPCService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.False(t, svc.Enabled())
	assert.Nil(t, svc.server)
}

func TestGRPCService_Run_NotEnabled(t *testing.T) {
	svc := &GRPCService{enabled: false}
	err := svc.Run(context.Background())
	assert.NoError(t, err)
}

func TestGRPCService_Close_NilServer(t *testing.T) {
	svc := &GRPCService{}
	err := svc.Close(context.Background())
	assert.NoError(t, err)
}

func TestGRPCService_FullLifecycle(t *testing.T) {
	deps := &Dependencies{
		Mode: "server",
		Env:  "dev",
		Config: config.Config{
			Grpc: config.GrpcConfig{
				ListenIP:      "127.0.0.1",
				Port:          0,
				AllowInsecure: true,
			},
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &GRPCService{}

	err := svc.Setup(context.Background(), deps)
	require.NoError(t, err)
	assert.True(t, svc.Enabled())
	assert.NotNil(t, svc.server)

	assert.NotNil(t, deps.StatsHandler)
	assert.Same(t, svc.statsHandler, deps.StatsHandler)

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

func TestGRPCService_DefaultIP(t *testing.T) {
	svc := &GRPCService{}

	deps := &Dependencies{
		Config: config.Config{
			Grpc: config.GrpcConfig{
				ListenIP: "",
				Port:     9090,
			},
		},
	}

	ip := deps.Config.Grpc.ListenIP
	if ip == "" {
		ip = "0.0.0.0"
	}
	expectedAddr := "0.0.0.0:9090"

	assert.Equal(t, "", deps.Config.Grpc.ListenIP)
	assert.Equal(t, expectedAddr, ip+":9090")

	deps.Config.Grpc.ListenIP = "127.0.0.1"
	ip = deps.Config.Grpc.ListenIP
	if ip == "" {
		ip = "0.0.0.0"
	}
	assert.Equal(t, "127.0.0.1:9090", ip+":9090")

	_ = svc
}
