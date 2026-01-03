package cliapp

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
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

	resp, err = http.Get("http://" + actualAddr + "/health")
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
