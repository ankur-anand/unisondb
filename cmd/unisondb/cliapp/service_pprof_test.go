package cliapp

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPProfService_Name(t *testing.T) {
	svc := &PProfService{}
	assert.Equal(t, "pprof", svc.Name())
}

func TestPProfService_Setup_Disabled(t *testing.T) {
	deps := &Dependencies{
		Config: config.Config{
			PProfConfig: config.PProfConfig{
				Enabled: false,
			},
		},
	}

	svc := &PProfService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.False(t, svc.Enabled())
	assert.Nil(t, svc.server)
}

func TestPProfService_Setup_Enabled(t *testing.T) {
	deps := &Dependencies{
		Config: config.Config{
			PProfConfig: config.PProfConfig{
				Enabled: true,
				Port:    6060,
			},
		},
	}

	svc := &PProfService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.True(t, svc.Enabled())
	assert.NotNil(t, svc.server)
	assert.Equal(t, "localhost:6060", svc.server.Addr)
}

func TestPProfService_Run_NotEnabled(t *testing.T) {
	svc := &PProfService{enabled: false}
	err := svc.Run(context.Background())
	assert.NoError(t, err)
}

func TestPProfService_RunAndClose(t *testing.T) {
	deps := &Dependencies{
		Config: config.Config{
			PProfConfig: config.PProfConfig{
				Enabled: true,
				Port:    0,
			},
		},
	}

	svc := &PProfService{}
	err := svc.Setup(context.Background(), deps)
	require.NoError(t, err)

	svc.server.Addr = "127.0.0.1:0"

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

	actualAddr := svc.BoundAddr()
	require.NotEmpty(t, actualAddr)

	resp, err := http.Get("http://" + actualAddr + "/debug/pprof/")
	if err == nil {
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
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

func TestPProfService_Close_NilServer(t *testing.T) {
	svc := &PProfService{}
	err := svc.Close(context.Background())
	assert.NoError(t, err)
}
