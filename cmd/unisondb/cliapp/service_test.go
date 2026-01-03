package cliapp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockService struct {
	name        string
	setupCalled atomic.Bool
	runCalled   atomic.Bool
	closeCalled atomic.Bool
	setupErr    error
	runErr      error
	closeErr    error
	runBlocking bool
}

func (m *mockService) Name() string {
	return m.name
}

func (m *mockService) Setup(ctx context.Context, deps *Dependencies) error {
	m.setupCalled.Store(true)
	return m.setupErr
}

func (m *mockService) Run(ctx context.Context) error {
	m.runCalled.Store(true)
	if m.runBlocking {
		<-ctx.Done()
	}
	return m.runErr
}

func (m *mockService) Close(ctx context.Context) error {
	m.closeCalled.Store(true)
	return m.closeErr
}

func TestServer_Register(t *testing.T) {
	srv := &Server{}

	svc1 := &mockService{name: "service1"}
	svc2 := &mockService{name: "service2"}

	srv.Register(svc1)
	srv.Register(svc2)

	require.Len(t, srv.services, 2)
	assert.Equal(t, "service1", srv.services[0].Name())
	assert.Equal(t, "service2", srv.services[1].Name())
}

func TestServer_SetupServices(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		srv := &Server{deps: &Dependencies{}}

		svc1 := &mockService{name: "service1"}
		svc2 := &mockService{name: "service2"}

		srv.Register(svc1)
		srv.Register(svc2)

		err := srv.SetupServices(context.Background())
		require.NoError(t, err)

		assert.True(t, svc1.setupCalled.Load())
		assert.True(t, svc2.setupCalled.Load())
	})

	t.Run("error stops setup", func(t *testing.T) {
		srv := &Server{deps: &Dependencies{}}

		svc1 := &mockService{name: "service1", setupErr: errors.New("setup failed")}
		svc2 := &mockService{name: "service2"}

		srv.Register(svc1)
		srv.Register(svc2)

		err := srv.SetupServices(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service1 setup failed")

		assert.True(t, svc1.setupCalled.Load())
		assert.False(t, svc2.setupCalled.Load())
	})
}

func TestServer_RunServices(t *testing.T) {
	t.Run("runs all services concurrently", func(t *testing.T) {
		srv := &Server{deps: &Dependencies{}}

		svc1 := &mockService{name: "service1", runBlocking: true}
		svc2 := &mockService{name: "service2", runBlocking: true}

		srv.Register(svc1)
		srv.Register(svc2)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := srv.RunServices(ctx)
		require.NoError(t, err)

		assert.True(t, svc1.runCalled.Load())
		assert.True(t, svc2.runCalled.Load())
	})

	t.Run("error from one service", func(t *testing.T) {
		srv := &Server{deps: &Dependencies{}}

		svc1 := &mockService{name: "service1", runErr: errors.New("run failed")}
		svc2 := &mockService{name: "service2", runBlocking: true}

		srv.Register(svc1)
		srv.Register(svc2)

		err := srv.RunServices(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "run failed")
	})
}

func TestServer_BuildDeps(t *testing.T) {
	srv := &Server{
		mode:               "replicator",
		env:                "test",
		relayerGRPCEnabled: true,
	}

	deps := srv.BuildDeps()

	require.NotNil(t, deps)
	assert.Equal(t, "replicator", deps.Mode)
	assert.Equal(t, "test", deps.Env)
	assert.True(t, deps.RelayerGRPCEnabled)
	assert.Same(t, deps, srv.deps)
}
